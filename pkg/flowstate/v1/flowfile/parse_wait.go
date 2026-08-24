package flowfile

import (
	"strings"
	"time"

	"github.com/goccy/go-yaml/ast"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Waiting, as an author writes it.
//
// Three spellings rather than one `wait:` block with a nested choice, because each
// of them is a different thing to say and the shortest way to say it should be the
// way it reads:
//
//	- id: settle
//	  sleep: 30s
//
//	- id: window
//	  wait_until: ${maintenance.opens_at}
//
//	- id: approval
//	  wait_for_signal: deploy-approved
//
//	- id: approval
//	  wait_for_signal:
//	    name: deploy-approved
//	    timeout: 24h
//
// A signal takes either form: the scalar for the common case, and the mapping when
// there is more to say. That is worth the extra branch in the parser, because
// `wait_for_signal: deploy-approved` is what someone writes when they are learning
// the DSL and having to write a two-line mapping to say one thing is the kind of
// friction that makes a feature feel heavier than it is.

// signalKeys are the keys of the mapping form of wait_for_signal.
var signalKeys = []string{"name", "timeout", "prompt", "outputs"}

// parseDuration reads a duration the way the DSL writes one, which is
// [v1.ParseDuration] — Go's syntax, plus days.
//
// It moved to `v1` when `sleep:` learned to take an expression, because a string
// an expression produces has to mean exactly what the same characters mean written
// literally, and two parsers would eventually disagree about `7d`. Kept as a name
// here so the diagnostics in this file read unchanged.
func parseDuration(s string) (time.Duration, error) { return v1.ParseDuration(s) }

// StepKinds returns the keys that spell the kinds of work a step can be, in the
// order a diagnostic lists them.
//
// Exported because this package is not the only surface that has to describe the
// DSL. The language server carries its own table of the document shape — the shape
// lives in unexported structs here, so it has no choice — and that table told
// authors a step was "one of task, for_each, and parallel" for as long as waiting
// had existed. Three shipped kinds, reachable from a Flowfile and exercised by
// examples in CI, that the editor said were not there.
//
// A copy is returned because the caller must not be able to edit the DSL by
// editing a slice header.
func StepKinds() []string {
	return stepKindKeys()
}

// StepKindList renders the kinds a step may be as prose: "a, b, or c".
//
// Built from [stepKindKeys] rather than written out, so that adding a kind cannot
// leave a diagnostic — or an editor — describing the DSL as it used to be.
func StepKindList() string {
	kinds := stepKindKeys()
	if len(kinds) < 2 {
		return strings.Join(kinds, "")
	}
	return strings.Join(kinds[:len(kinds)-1], ", ") + ", or " + kinds[len(kinds)-1]
}

// stepKindList is the internal spelling, kept so the diagnostics that call it read
// unchanged.
func stepKindList() string { return StepKindList() }

// sleep compiles `sleep: 30s` — or `sleep: ${...}` — into a durable timer.
func (c *compiler) sleep(n ast.Node, path string, r ref) *v1.Wait {
	sleepRef := ref{step: r.step, path: path, label: "sleep"}

	if computed, isExpr := c.computedDuration(n, path, sleepRef); isExpr {
		if computed == nil {
			return nil
		}
		return &v1.Wait{Kind: &v1.Wait_DurationExpr{DurationExpr: computed}}
	}

	duration, ok := c.duration(n, path, sleepRef)
	if !ok {
		return nil
	}

	return &v1.Wait{Kind: &v1.Wait_Duration{Duration: duration}}
}

// computedDuration compiles a duration position written as an expression, and
// reports whether it was one.
//
// The fence decides, and it has to, because this is a field that can hold either.
// `sleep: 30s` is a duration and `sleep: ${inputs.grace}` is an expression, and
// nothing between them is ambiguous — which is constitution rule 3 (`docs/DSL.md`)
// applied to a position that grew a second reading. An unfenced string stays a
// literal duration, so every file written before this existed compiles to exactly
// the bytes it did.
//
// This is deliberately *not* [compiler.exprValue]. That one treats a bare string as
// expression source, which is right for `wait_until:` — a field the schema has
// always typed as an expression — and would be catastrophic here: `sleep: 30s`
// would become the CEL expression `30s`, which does not parse, and `sleep: 5m`
// would become a reference to a name nobody bound. The two positions differ in what
// their unfenced form means, so they differ in which helper reads them.
//
// isExpr false means "not written as one"; the caller falls through to the literal
// reading. isExpr true with a nil value means it was written as one and a
// diagnostic has already been reported.
func (c *compiler) computedDuration(n ast.Node, path string, r ref) (value *v1.Value, isExpr bool) {
	resolved := c.resolve(n, path, r)
	if resolved == nil {
		return nil, false
	}

	var text string
	switch node := resolved.(type) {
	case *ast.StringNode:
		text = node.Value
	case *ast.LiteralNode:
		text = blockText(node)
	default:
		// A number, a mapping, a list. None of them is an expression, and the
		// literal reading has the better diagnostic for each — it names what was
		// written and what a duration looks like.
		return nil, false
	}

	if _, fenced := SplitFence(text); !fenced {
		return nil, false
	}

	// secretNotEvaluable, matching every other field the workflow evaluates
	// itself: a `${secret(...)}` here would have to be resolved in workflow code
	// to know how long to wait, and a secret reaching workflow code is the
	// invariant this engine will not trade. The refusal comes from
	// [compiler.expression] with the message that placement carries.
	return c.expression(resolved, mustFence(text), path, r, secretNotEvaluable), true
}

// mustFence returns the source inside a fence that [computedDuration] has already
// confirmed is one.
func mustFence(text string) string {
	inner, _ := SplitFence(text)
	return inner
}

// waitUntil compiles `wait_until: <expression>` into a timer to a moment.
func (c *compiler) waitUntil(n ast.Node, path string, r ref) *v1.Wait {
	until := c.exprValue(n, path, ref{step: r.step, path: path, label: "wait_until"})
	if until == nil {
		return nil
	}

	return &v1.Wait{Kind: &v1.Wait_Until{Until: until}}
}

// waitForSignal compiles `wait_for_signal`, in either the scalar or the mapping
// form.
func (c *compiler) waitForSignal(n ast.Node, path string, r ref) *v1.Wait {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil
	}
	c.pos.record(path, spanOfNode(n))

	// The scalar form: the signal's name and nothing else.
	if _, isMapping := n.(*ast.MappingNode); !isMapping {
		if _, isValue := n.(*ast.MappingValueNode); !isValue {
			name, ok := c.text(n, path, ref{step: r.step, path: path, label: "wait_for_signal"})
			if !ok {
				return nil
			}
			return c.signalWait(n, name, r, path)
		}
	}

	fields, ok := c.fields(n, path, r, signalKeys)
	if !ok {
		return nil
	}

	var name string
	if f, found := fields.get("name"); found {
		name, _ = c.text(f.value, fieldPath(path, "name"),
			ref{step: r.step, path: fieldPath(path, "name"), label: "signal name"})
	} else {
		c.report(spanOfNode(n), ref{step: r.step, path: path, label: "wait_for_signal"},
			"needs a name, which is what a sender addresses; write `wait_for_signal: deploy-approved`, or give the mapping a `name:`")
		return nil
	}

	wait := c.signalWait(n, name, r, path)
	if wait == nil {
		return nil
	}

	// `outputs:` shapes what the waiting step produces, the way the http task's
	// own `outputs:` shapes what a request produces — and it is read here, on the
	// signal, because only a signal has a result worth deriving. See
	// [v1.ShapeSignalOutputs]; `sleep:` and `wait_until:` are refused the key by
	// the grammar rather than by a check, since neither takes a mapping at all.
	if f, found := fields.get("outputs"); found {
		outputsPath := fieldPath(path, "outputs")
		shaped := c.waitOutputs(f.value, outputsPath, r)
		if shaped == nil {
			return nil
		}
		wait.GetSignal().Outputs = shaped
	}

	// `prompt:` is what the gate is asking for, in the author's own words -
	// read here for `outputs:`'s reason, because only a signal wait asks anybody
	// anything. `sleep:` and `wait_until:` are refused the key by the grammar
	// rather than by a check, since neither takes a mapping at all.
	//
	// [compiler.inputValue] is the reader, which is the same helper a task input
	// and an `outputs:` entry go through: a fence means here what it means
	// everywhere else, and an unfenced string stays the plain sentence it looks
	// like. What is different is only what a prompt may *reach*, which is
	// [checkSensitivePrompt]'s business and not this function's.
	if f, found := fields.get("prompt"); found {
		promptPath := fieldPath(path, "prompt")

		// Recorded for `timeout:`'s reason: [compiler.expression] records only the
		// expression's own span, so without a value span at this path a diagnostic
		// about the prompt would land on the whole step instead of on the line.
		c.pos.record(promptPath, spanOfNode(c.resolveQuiet(f.value)))

		prompt := c.inputValue(f.value, promptPath,
			ref{step: r.step, path: promptPath, label: "wait_for_signal prompt"})
		if prompt == nil {
			return nil
		}
		wait.GetSignal().Prompt = prompt
	}

	if f, found := fields.get("timeout"); found {
		timeoutPath := fieldPath(path, "timeout")
		timeoutRef := ref{step: r.step, path: timeoutPath, label: "wait_for_signal timeout"}

		// Recorded here because neither reading below does: a literal duration
		// compiles through [compiler.duration] and an expression through
		// [compiler.expression], which records only the expression's own span.
		// Without a value span at this path, a validator diagnostic about the
		// timeout — an unknown name in `timeout: ${...}` — had nowhere to land
		// but the whole step (#318).
		c.pos.record(timeoutPath, spanOfNode(c.resolveQuiet(f.value)))

		if computed, isExpr := c.computedDuration(f.value, timeoutPath, timeoutRef); isExpr {
			if computed == nil {
				return nil
			}
			wait.TimeoutExpr = computed

			return wait
		}

		timeout, ok := c.duration(f.value, timeoutPath, timeoutRef)
		if !ok {
			return nil
		}
		wait.Timeout = timeout
	}

	return wait
}

// waitOutputs compiles a `wait_for_signal:`'s `outputs:` mapping.
//
// Each value is an ordinary expression position, compiled through
// [compiler.waitOutputValue] rather than [compiler.inputValue] directly — a fence
// means what it means everywhere else, but a bare `${secret(...)}` is refused
// here, unlike a task input, because a shaped output is evaluated by the workflow
// and its value is recorded on the run rather than reaching an activity that could
// resolve it. See [notInWaitOutputsHelp]. What is otherwise ordinary about this
// position is only the scope these expressions resolve in, and scope is not this
// function's business: it is [validateWait]'s, which adds the wait's own bound
// names, and [v1.ShapeSignalOutputs]'s, which binds them.
//
// An empty mapping is a diagnostic rather than a no-op, because `outputs:`
// *replaces* the step's outputs — an empty one is a step that deliberately
// produces nothing, which nobody writes on purpose and which would silently
// break every later reference.
func (c *compiler) waitOutputs(n ast.Node, path string, r ref) map[string]*v1.Value {
	c.pos.record(path, spanOfNode(c.resolveQuiet(n)))

	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil
	}

	if len(entries) == 0 {
		c.report(spanOfNode(n), ref{step: r.step, path: path, label: "outputs"},
			"is empty, and `outputs:` replaces what the wait produces, so this step would have no outputs at all; write the names this wait should produce, or remove the key")
		return nil
	}

	compiled := make(map[string]*v1.Value, len(entries))
	for _, e := range entries {
		valuePath := fieldPath(path, e.name)
		value := c.waitOutputValue(e.value, valuePath,
			ref{step: r.step, path: valuePath, label: "outputs." + e.name})
		if value != nil {
			compiled[e.name] = value
		}
	}

	if len(compiled) == 0 {
		return nil
	}

	return compiled
}

// signalWait builds the wait, reporting a name the schema will not accept.
//
// Checked here rather than left to schema validation, because a diagnostic that
// names the line is worth more than one that names a field path — and because the
// name is part of the workload's contract with whoever approves it, so a typo in it
// is a workload that waits for something nobody will ever send.
func (c *compiler) signalWait(n ast.Node, name string, r ref, path string) *v1.Wait {
	signalRef := ref{step: r.step, path: path, label: "wait_for_signal"}

	switch {
	case name == "":
		c.report(spanOfNode(n), signalRef, "needs a signal name; it is what a sender addresses")
		return nil
	case len(name) > 128:
		c.report(spanOfNode(n), signalRef, "signal name is longer than 128 characters")
		return nil
	case !validSignalName(name):
		c.report(spanOfNode(n), signalRef,
			"signal name %q may only contain letters, digits, dashes, and underscores, and must start with a letter or digit", name)
		return nil
	}

	return &v1.Wait{Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: name}}}
}

// validSignalName reports whether a name matches what the schema permits.
func validSignalName(name string) bool {
	for i := range len(name) {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case (c == '-' || c == '_') && i > 0:
		default:
			return false
		}
	}
	return true
}

// checkPolicyPlacement reports a step-level `timeout:` or `retry:` that binds
// nothing, on any kind of step that schedules no single activity for either key
// to bound or re-run.
//
// `StepPolicy` compiles onto every step kind, but only a task's arm ever reads it
// — see `activityOptionsFor` in the engine and its counterpart in `eval.go`, the
// one place per driver that consumes `timeout`/`retry`. Everywhere else the two
// keys parse, validate, and vanish, which is worse than a parse error: an author
// who writes `retry:` on a step believes they bounded something, and nothing
// downstream — not the parser, not either driver, not a lint — ever tells them
// otherwise. This is the fix: refuse both keys, with a position and a remedy,
// everywhere they cannot act.
//
// Two kinds schedule none, in a sense settled and tested from the start. A wait
// is a timer, so a `timeout:` beside a `sleep:` is not a shorter sleep, and
// `retry:` cannot re-run one. A `value:` is an expression evaluated in workflow
// code, so it has nothing to bound either, and nothing a second attempt could
// change, because it is deterministic and an expression that failed will fail
// identically however many times it is asked.
//
// Five more schedule zero or more *of something else* rather than one activity of
// their own: `for_each:`, `parallel:`, `call:`, `loop:`, and `switch:` are
// composites over steps that do the scheduling. Retrying or bounding the
// composite itself is a live feature request — see flowstate#286 — but it is not
// implemented on either driver today, and R6 of the style charter (#543) admits
// no interim in which a key silently binds nothing: refuse now, on both drivers
// identically because this refusal runs at compile time before either driver sees
// the file, and lift it if and when #286 lands real semantics. Each of the five
// has a place these keys already work: the steps inside its own body.
//
// Silently ignoring either would leave an author believing they had bounded or
// re-attempted something. The diagnostics say where the key does work instead.
func (c *compiler) checkPolicyPlacement(step *v1.Node, fields *fieldSet, path string, r ref) {
	var subject string
	advice := map[string]string{}

	switch step.GetKind().(type) {
	case *v1.Node_Wait:
		subject = "a waiting step"
		advice["timeout"] = "a wait is bounded by `wait_for_signal:`'s own `timeout:`, or by the duration of a `sleep:`"
		advice["retry"] = "there is no activity to attempt again; a wait either happens or times out"

	case *v1.Node_Value:
		subject = "a `value:` step"
		advice["timeout"] = "a value is an expression evaluated in the workflow rather than work scheduled somewhere, " +
			"and it is already bounded by the evaluation cost limit every expression in the file shares"
		advice["retry"] = "a value is deterministic, so a second attempt computes exactly what the first one did; " +
			"if the expression is wrong, it is wrong every time"

	case *v1.Node_ForEach:
		subject = "a `for_each:` step"
		advice["timeout"] = "a for_each step fans out over its items rather than scheduling one activity itself; " +
			"put `timeout:` on the steps under `for_each.steps:`, which run once per item"
		advice["retry"] = "a for_each step fans out over its items rather than scheduling one activity itself; " +
			"put `retry:` on the steps under `for_each.steps:`, which run once per item"

	case *v1.Node_Parallel:
		subject = "a `parallel:` step"
		advice["timeout"] = "a parallel step is branches, each with its own steps, rather than one activity itself; " +
			"put `timeout:` on the steps inside the branch that needs it"
		advice["retry"] = "a parallel step is branches, each with its own steps, rather than one activity itself; " +
			"put `retry:` on the steps inside the branch that needs it"

	case *v1.Node_Call:
		subject = "a `call:` step"
		advice["timeout"] = "a call step runs another workflow's steps rather than scheduling one activity itself; " +
			"put `timeout:` on the steps inside the called workflow that need it"
		advice["retry"] = "a call step runs another workflow's steps rather than scheduling one activity itself; " +
			"put `retry:` on the steps inside the called workflow that need it"

	case *v1.Node_Loop:
		subject = "a `loop:` step"
		advice["timeout"] = "a loop step repeats its body rather than scheduling one activity itself; " +
			"put `timeout:` on the steps under `loop.steps:`, which run once per iteration — " +
			"`max_iterations:` is the loop's own bound, and it limits the count, not the time"
		advice["retry"] = "a loop step repeats its body rather than scheduling one activity itself; " +
			"put `retry:` on the steps under `loop.steps:`, which run once per iteration"

	case *v1.Node_Switch:
		subject = "a `switch:` step"
		advice["timeout"] = "a switch step dispatches to exactly one case and schedules no activity itself; " +
			"put `timeout:` on the steps inside the `cases:` or `default:` branch that needs it"
		advice["retry"] = "a switch step dispatches to exactly one case and schedules no activity itself; " +
			"put `retry:` on the steps inside the `cases:` or `default:` branch that needs it"

	default:
		return
	}

	for _, name := range []string{"timeout", "retry"} {
		f, found := fields.get(name)
		if !found {
			continue
		}

		c.report(spanOfNode(f.key), ref{step: r.step, path: fieldPath(path, name), label: name},
			"does nothing on %s: %s", subject, advice[name])
	}
}
