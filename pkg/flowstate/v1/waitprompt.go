package flowstatev1

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// What an approval gate is asking for, and why the question is a field rather
// than a convention.
//
// A gate names the signal that releases it, and a signal name is a routing key:
// `deploy-approved` tells an operator what to send and nothing about what
// agreeing means. Everything that made the decision a decision - which build,
// which environment, how much money - lived in the file, and whoever was being
// asked had the run id instead of the file. So the question was asked out of
// band, in a chat message somebody wrote by hand, and the run and the question
// could drift apart with nothing to notice.
//
// [Signal.Prompt] is that sentence, computed by the workflow from the same
// inputs the gate is about, and carried on [PendingWait] so that every surface
// already reporting the gate reports it.
//
// # This file is the containment argument
//
// A prompt is the only member of [PendingWait] that is not simply a name written
// in the specification: it is a value the specification *computed*, and it is
// rendered into other people's clients. That combination is what everything
// below is for.
//
// The rule is wider than the one `flowfile`'s `log:` lint draws, and the
// difference is the sink. A log message is written where the run's own operator
// reads it, so that lint refuses only *direct* surfacing and leaves a derived
// value alone - `${hash(inputs.token)}` is a digest the author chose precisely
// so the value does not appear. A prompt goes to an approver who was handed a
// run id, so a prompt may not *reach* a `sensitive:` input at all, by any
// spelling, and may not hold a `${secret(...)}` reference. Refused at compile
// where a file exists to be told about it, refused again at submit by
// [CheckWaitPromptsAreAskable], and refused a third time - as a marker, in place
// of the value - at evaluation, which is the only layer a specification built in
// process and never parsed can still reach.

// MaxWaitPromptBytes bounds how long the evaluated text of one gate's prompt may
// be.
//
// Read by both drivers from the package both import, for [MaxPendingWaits]'s
// reason: a bound that disagreed with itself would make [PendingWait.PromptTruncated]
// mean one thing in a rehearsal and another in production, which is precisely
// the class of disagreement a local run exists to rule out.
//
// The resource is bytes in a query answer that an operator did not ask the size
// of. It is the author who chooses the expression, but not the author who
// chooses the values it interpolates: `${"approve " + inputs.reason}` is as long
// as whatever a caller passed, and a run parked on [MaxPendingWaits] gates could
// otherwise multiply that by 64. Set to 2 KiB, which is longer than any question
// a person reads off a terminal and short enough that the whole reporting bound
// stays a well-understood size.
const MaxWaitPromptBytes = 2048

// PromptWithheldSecret is what a prompt renders as when it still holds a secret
// reference at the moment it would have been evaluated.
//
// A marker rather than an error, and rather than the value. An error would fail
// a run at the gate for something no approver can fix, and the value is the one
// thing that must never be produced; what is left is telling the reader plainly
// that there was a question and that this system refused to ask it. Spelled the
// way `flow`'s own redaction marker is - unmistakably this system's annotation
// and not text a workload could have produced - so a reader is never left
// wondering whether the author wrote it.
const PromptWithheldSecret = "[prompt withheld: it names a secret]"

// EvalSignalPrompt resolves what a gate is asking for, at the moment the gate
// begins parking.
//
// Called by both drivers at their own announcement point, which is after both
// ways a wait can resolve without ever parking - a signal that arrived early, a
// bound that had already lapsed. A gate a run walks straight through asks
// nobody anything, so it evaluates nothing; that keeps the cost of the feature
// off the path of a run that never blocks, and keeps the two drivers agreeing
// about which gates ever had a question at all.
//
// Evaluated unconditionally rather than only when somebody is watching, even
// though the answer is only ever *reported* to a watcher. A prompt that fails to
// evaluate fails the step, and a run that failed only when observed would be the
// worst kind of disagreement: an author would find the bug in production and not
// in the rehearsal that exists to show it to them.
//
// What it sees is the enclosing scope and [NowIdentifier], the same names
// `timeout:` sees, through the same [evalWaitExpr] every other wait expression
// goes through. The wait's own result is deliberately absent: `payload`,
// `sender` and `timed_out` do not exist yet when the question is asked.
func EvalSignalPrompt(ctx context.Context, signal *Signal, scope *Scope, now time.Time) (prompt string, truncated bool, err error) {
	return evalWaitPrompt(ctx, signal.GetPrompt(), scope, now, "wait_for_signal")
}

// EvalSignalBatchPrompt resolves a `wait_for_signals:`'s `prompt:`.
//
// The same function as [EvalSignalPrompt] under a different name, and that is
// the whole intent: a prompt is a prompt whichever spelling carries it, so the
// secret backstop, the type refusal, the bound and the evaluation position are
// one implementation rather than two that must be kept in step. Only the label
// in a diagnostic differs, because an author reading it needs to be told which
// key they wrote.
func EvalSignalBatchPrompt(ctx context.Context, batch *SignalBatch, scope *Scope, now time.Time) (prompt string, truncated bool, err error) {
	return evalWaitPrompt(ctx, batch.GetPrompt(), scope, now, "wait_for_signals")
}

// evalWaitPrompt is the one evaluator behind both spellings' `prompt:`.
func evalWaitPrompt(ctx context.Context, value *Value, scope *Scope, now time.Time, key string) (prompt string, truncated bool, err error) {
	if value == nil {
		return "", false, nil
	}

	// The fail-closed layer a specification built in process can still reach.
	// [CheckWaitPromptsAreAskable] refuses this at submit and the compiler
	// refuses it against a line and a column, so arriving here means neither ran
	// - a `*Workflow` assembled in Go and executed directly. Answered with the
	// marker rather than the secret, and rather than an error, per this file's
	// doc.
	if holdsSecretRef(value, 0) {
		return PromptWithheldSecret, false, nil
	}

	evaluated, err := evalWaitExpr(ctx, value, scope, now, nil)
	if err != nil {
		return "", false, fmt.Errorf("evaluating %s prompt: %w", key, err)
	}

	text, ok := evaluated.Value().(string)
	if !ok {
		// Named rather than coerced. A prompt is a sentence somebody reads, and
		// rendering a map or a number through whatever Go's default formatting
		// happens to produce would put an author's mistake in front of an
		// approver instead of in front of the author.
		return "", false, fmt.Errorf(
			"%s prompt produced %s, and a prompt is the sentence an approver reads, so it has to be a string",
			key, evaluated.Type())
	}

	bounded, cut := boundPrompt(text)

	return bounded, cut, nil
}

// boundPrompt cuts text to [MaxWaitPromptBytes] and reports whether it cut.
//
// On a rune boundary, so a prompt that is cut is still text: half a rune renders
// as a replacement character in whatever is showing the question, which reads as
// corruption rather than as a bound being reached. The flag is what says it was
// cut - nothing is appended, because an ellipsis is a convention a reader has to
// already know, and [PendingWait.PromptTruncated] is a fact they cannot miss.
func boundPrompt(text string) (string, bool) {
	if len(text) <= MaxWaitPromptBytes {
		return text, false
	}

	cut := text[:MaxWaitPromptBytes]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}

	return cut, true
}

// CheckWaitPromptsAreAskable refuses a workflow whose gate prompts could put
// something private in front of whoever is being asked.
//
// The fail-closed half of the positioned diagnostics `flowfile` reports, called
// from [BindRunInputs] beside [CheckVarsHoldNoSecretRef] - the one function every
// submit path already calls, which is the same boundary and the same reasoning
// that file's own doc gives. A specification can be built by something that never
// was a Flowfile, and this rule is not a property of the parser.
//
// Exported so a caller assembling a specification by hand can ask the question
// the submit boundary asks rather than discovering the refusal at submit.
func CheckWaitPromptsAreAskable(wf *Workflow) error {
	problems := WaitPromptProblems(wf, DescendCalls)
	if len(problems) == 0 {
		return nil
	}

	// The first, so the submit boundary refuses with one sentence about one step.
	// Every problem is reported to a caller that positions them; see
	// [WaitPromptProblems].
	return problems[0].Err
}

// WaitPromptProblem is one gate prompt this rule refuses, and the step it was
// written in.
type WaitPromptProblem struct {
	// StepID is the id of the step whose `prompt:` is refused, which is what a
	// surface holding positions resolves a line and a column from.
	//
	// Empty only where the refusal is not about a particular prompt at all - the
	// depth bound, which is refused before the walk can say what is down there.
	StepID string

	// Err is the refusal, worded once so the sentence an author reads in their
	// editor and the sentence a submitted specification is refused with are the
	// same sentence.
	Err error
}

// CallDescent says whether a walk follows an inlined `call:`.
//
// Named rather than a bare bool because the two callers want opposite answers
// for reasons neither would recover from reading `false` at a call site. The
// submit boundary descends: by then the callee is inlined into the specification
// being run and there is no separate file left to have been validated. `flow
// validate` does not: a callee is a different workflow with its own declared
// inputs, validated in its own right, and `inputs.` inside it names the callee's
// arguments rather than the caller's.
type CallDescent bool

const (
	// DescendCalls follows an inlined callee, checking its prompts against its
	// own declared inputs.
	DescendCalls CallDescent = true

	// SkipCalls leaves a callee to its own validation.
	SkipCalls CallDescent = false
)

// WaitPromptProblems reports every gate prompt this rule refuses, with the names
// the *grammar* bound around each one resolved.
//
// A list rather than the first refusal, because `flowfile` positions every
// diagnostic a file earns. It used to get that by asking
// [CheckWaitPromptsAreAskable] about one rebuilt single-step workflow at a time,
// and a step in isolation is exactly what cannot see the bindings written around
// it - which is the hole this walk closes (#976).
func WaitPromptProblems(wf *Workflow, calls CallDescent) []WaitPromptProblem {
	walk := &promptWalk{calls: calls}
	walk.nodes(wf.GetSteps(), sensitiveInputNames(wf), nil, 0, "")

	return walk.problems
}

// SensitiveInputNames is the set of a workflow's inputs declared `sensitive:`.
//
// Exported because two packages ask the same question about the same
// declaration and had no reason to answer it twice: `flowfile`'s `log:` lint
// reads it to decide what a message may surface, and this file reads it to
// decide what a prompt may reach.
func SensitiveInputNames(wf *Workflow) map[string]bool {
	return sensitiveInputNames(wf)
}

// sensitiveInputNames is the unexported spelling the walk below uses.
func sensitiveInputNames(wf *Workflow) map[string]bool {
	var names map[string]bool

	for _, declared := range wf.GetDeclaredInputs() {
		if !declared.GetSensitive() {
			continue
		}
		if names == nil {
			names = make(map[string]bool)
		}
		names[declared.GetName()] = true
	}

	return names
}

// promptReach is what one value reaches: which of the workflow's inputs it names,
// and whether it reaches the `inputs` root in some way this walk cannot name.
//
// The unit the bindings map carries, which is what makes a use site one lookup
// rather than a traversal: a binding is resolved where the *engine* evaluates it,
// so by the time a prompt reads the name, the answer for that name is already the
// whole answer. That is #953's one-level completeness argument, restated for a
// walk that now crosses several scopes rather than one - see [promptWalk.nodes].
type promptReach struct {
	// named are the input keys the value reaches by a name this walk could read.
	named map[string]bool

	// opaque is set where the value reaches `inputs` in a way that names no key -
	// `inputs[whicheverKey]`, or `inputs` passed whole to a function - which
	// [promptWalk.check] answers as a refusal rather than as silence.
	opaque bool
}

// merge folds another reach into this one, which is what a name bound from two
// places (a loop's `initial:` and its `update:`) needs.
func (r promptReach) merge(other promptReach) promptReach {
	if len(other.named) > 0 && r.named == nil {
		r.named = make(map[string]bool, len(other.named))
	}
	maps.Copy(r.named, other.named)
	r.opaque = r.opaque || other.opaque

	return r
}

// resolveReach reports what a value reaches, following every bare name it reads
// into whatever the grammar bound that name to.
//
// Free, in the CEL sense: [collectFreeIdentifiers] binds a comprehension's own
// iteration and accumulator variables, so `list.map(x, x)` does not send this
// walk looking up `x` among the bindings and finding an unrelated one of the same
// name. That is the same rule `flow fix` had to learn twice about the names the
// grammar binds.
//
// A name that is bound to nothing here is left alone, which is what keeps a step
// id, `now`, and a wait's own shaping names from being looked up as though they
// were bindings: this walk knows what it bound and claims nothing about the rest.
func resolveReach(value *Value, bindings map[string]promptReach) promptReach {
	named, opaque := promptInputReach(value)
	reach := promptReach{named: named, opaque: opaque}

	if len(bindings) == 0 {
		return reach
	}

	for name := range promptFreeIdentifiers(value) {
		if bound, ok := bindings[name]; ok {
			reach = reach.merge(bound)
		}
	}

	return reach
}

// bind returns bindings with one more name in them, leaving the caller's map
// alone - an inner scope must not leak a binding back out to its siblings.
func bind(bindings map[string]promptReach, name string, reach promptReach) map[string]promptReach {
	if name == "" {
		return bindings
	}

	inner := make(map[string]promptReach, len(bindings)+1)
	maps.Copy(inner, bindings)
	// Assigned rather than merged, so an inner binding shadows an outer one of the
	// same name the way the engine's scope does. `flow validate` refuses that
	// collision outright, but a specification built in Go never met the validator,
	// and answering it by unioning the two would report a reach the run cannot have.
	inner[name] = reach

	return inner
}

// unbind returns bindings without one name in them, leaving the caller's map
// alone for [bind]'s reason.
func unbind(bindings map[string]promptReach, name string) map[string]promptReach {
	if _, bound := bindings[name]; !bound {
		return bindings
	}

	inner := make(map[string]promptReach, len(bindings)-1)
	maps.Copy(inner, bindings)
	delete(inner, name)

	return inner
}

// promptWalk walks every node that can hold a wait, and every node that can hold
// one inside it, carrying what the grammar bound on the way down.
type promptWalk struct {
	calls    CallDescent
	problems []WaitPromptProblem
}

// nodes walks a body of steps in the scope bindings describes.
//
// # What the grammar binds, and where
//
// The bindings are the four bare names the language introduces around a step,
// taken from where the *engine* evaluates each one rather than from where it is
// written - CLAUDE.md's rule, and the one `flow fix` had to learn twice:
//
//   - A step's own `vars:` are bound for that step's inputs and its body, by
//     [EvalStepVars] on the local driver and the scope swap in engine/execute.go
//     on the durable one. They are resolved against the scope *without* their
//     siblings, which both drivers do and which is why one lookup is the whole
//     answer for one of them.
//   - A `for_each` binds [IteratorName] - the `as:`, or `item` when none is
//     written - for the body only, to an element of `items:`. The reach of an
//     element is the reach of the list, so the binding carries the reach of the
//     `items:` expression, resolved in the scope the loop node sits in.
//   - A `loop:` binds its `state:` bare for the body, `until:` and `update:`, to
//     `initial:` on the first iteration and to whatever `update:` computed on
//     every one after.
//   - `now` is bound inside a wait, to a clock reading, which reaches no input -
//     so a wait's prompt is checked with that name *removed* from the scope
//     rather than merely not added to it. [evalWaitExpr] overlays it onto the
//     activation last, so it wins over anything an enclosing loop or `vars:`
//     block bound under that spelling, and following the enclosing binding would
//     report a reach the run cannot have. `flow validate` refuses the collision
//     outright, but a specification built in Go never met the validator, which is
//     the whole reason this check exists separately from it.
//
// A step's `if:` is deliberately not among them, and the omission is the reason
// to say so: both drivers evaluate a condition *before* installing the step's own
// `vars:`, so the vars are not in scope there. Nothing in this walk reads an
// `if:`, so the distinction costs nothing today and is what anyone extending it
// has to know first.
//
// A callee is descended into with *its own* declared inputs and with no bindings
// at all, not the caller's. A call is inlined in the caller's specification, so a
// submission carries the callee's prompts too - but `inputs.` inside a callee
// names the callee's arguments, and no bare name the caller bound is in scope
// across that boundary.
func (w *promptWalk) nodes(nodes []*Node, sensitive map[string]bool, bindings map[string]promptReach, depth int, enclosing string) {
	if depth > maxVarScanDepth {
		// Refused rather than returned clean, per fail closed and for
		// [checkNodeVars]'s reason: past this the walk cannot say there is no
		// prompt down there reaching something private, and a check that cannot
		// decide must not allow.
		w.problems = append(w.problems, WaitPromptProblem{
			StepID: enclosing,
			Err: fmt.Errorf("steps nest more than %d deep, past what a specification is checked to; "+
				"nothing this deep can be confirmed to keep private values out of a gate's `prompt:`", maxVarScanDepth),
		})

		return
	}

	// A workflow that declared nothing private is one no prompt can reach anything
	// in, so the bindings are never consulted and are not built: the walk still
	// descends, because a `call:` brings its own declarations with it and a prompt
	// holding a secret reference is refused whatever a file declared. That keeps
	// the common submit - which is every workflow with no `sensitive:` input at
	// all - reading nothing but the prompts, as it did before this walk carried a
	// scope.
	binding := len(sensitive) > 0

	for _, node := range nodes {
		// The step's own `vars:`, in scope for everything below except the `if:`
		// this walk never reads.
		inner := bindings
		if binding {
			inner = w.bindVars(node, bindings)
		}

		if signal := node.GetWait().GetSignal(); signal != nil {
			// Without whatever the enclosing scope bound as `now`: a wait binds that
			// name over anything above it. See [promptWalk.check].
			w.check(signal.GetPrompt(), unbind(inner, NowIdentifier), node.GetId(), sensitive)
		}
		if batch := node.GetWait().GetSignalBatch(); batch != nil {
			// The other spelling that carries a `prompt:`, checked here rather
			// than left to its own pass. A prompt is rendered into an
			// approver's client whichever key wrote it, so a rule that covered
			// one arm would be a containment argument with a hole in it — and
			// the hole would be in the newer arm, which is the one nobody
			// remembers to re-read.
			w.check(batch.GetPrompt(), unbind(inner, NowIdentifier), node.GetId(), sensitive)
		}

		if loop := node.GetForEach(); loop != nil {
			body := inner
			if binding {
				body = bind(inner, IteratorName(loop), resolveReach(loop.GetItems(), inner))
			}
			w.nodes(loop.GetBody(), sensitive, body, depth+1, node.GetId())
		}
		if loop := node.GetLoop(); loop != nil {
			body := inner
			if binding {
				body = w.bindLoopState(loop, inner)
			}
			w.nodes(loop.GetBody(), sensitive, body, depth+1, node.GetId())
		}
		if callee := node.GetCall().GetWorkflow(); callee != nil && w.calls == DescendCalls {
			w.nodes(callee.GetSteps(), sensitiveInputNames(callee), nil, depth+1, node.GetId())
		}
		if sw := node.GetSwitch(); sw != nil {
			for _, body := range SwitchBodies(sw) {
				w.nodes(body, sensitive, inner, depth+1, node.GetId())
			}
		}
		if parallel := node.GetParallel(); parallel != nil {
			for _, branch := range parallel.GetBranches() {
				w.nodes(branch.GetSteps(), sensitive, inner, depth+1, node.GetId())
			}
		}
	}
}

// bindVars returns the scope inside a step: the enclosing bindings plus the
// step's own `vars:`.
//
// Each var is resolved against the enclosing bindings rather than against the
// block being built, because that is what both drivers do - [EvalStepVars]
// evaluates a block against the scope without its siblings, so
// `vars: {a: ${inputs.token}, b: ${a}}` is not expressible and there is no
// sibling chain left to follow.
func (w *promptWalk) bindVars(node *Node, bindings map[string]promptReach) map[string]promptReach {
	declared := node.GetVars()
	if len(declared) == 0 {
		return bindings
	}

	inner := make(map[string]promptReach, len(bindings)+len(declared))
	maps.Copy(inner, bindings)
	for name, value := range declared {
		inner[name] = resolveReach(value, bindings)
	}

	return inner
}

// bindLoopState returns the scope inside a `loop:` body: the enclosing bindings
// plus the carried state, under its bare name.
//
// The state holds `initial:` on the first iteration and whatever `update:`
// computed on each one after, so it reaches whatever either of them reaches.
// `initial:` is evaluated in the scope the loop node sits in ([LoopInitialState]);
// `update:` is evaluated in the scope the body finished in, where the state is
// already bound - so it is resolved with the state bound to what `initial:`
// reached, and the union is the answer.
//
// Two passes are not needed and one is not a first cut. Substituting again could
// only add the state's own reach to itself, which the union already holds:
// `reach(update)` is some fixed set plus, where `update:` reads the state, the
// state's reach, and that has been folded in by the time this returns.
//
// What is deliberately absent is the body's step outputs, which `update:` may
// also read. This rule follows `inputs.` and the bare names the grammar binds; a
// value that has been through a step's outputs is the whole-program taint
// question sensitive_log.go's doc declines, and it is out of scope here for the
// same reason (#976 asks about the grammar's names).
func (w *promptWalk) bindLoopState(loop *Loop, bindings map[string]promptReach) map[string]promptReach {
	if !LoopCarriesState(loop) {
		return bindings
	}

	name := loop.GetState()
	reach := resolveReach(loop.GetInitial(), bindings)
	reach = reach.merge(resolveReach(loop.GetUpdate(), bind(bindings, name, reach)))

	return bind(bindings, name, reach)
}

// check records what is wrong with one gate's prompt, by step id.
func (w *promptWalk) check(value *Value, bindings map[string]promptReach, stepID string, sensitive map[string]bool) {
	if value == nil {
		return
	}

	if holdsSecretRef(value, 0) {
		w.problems = append(w.problems, WaitPromptProblem{StepID: stepID,
			Err: fmt.Errorf("step %q asks for approval with a `prompt:` that is a secret reference, "+
				"which a prompt may not hold: a prompt is rendered to whoever is being asked to approve, "+
				"so write the question without the secret in it", stepID)})

		return
	}

	if len(sensitive) == 0 {
		// Nothing this workflow declared is private, so there is nothing for a
		// prompt to reach. The common case, and it costs one length check.
		return
	}

	// Follow the prompt into whatever the grammar bound around it, because that is
	// where the engine evaluates it from: a bare name would otherwise be a way to
	// launder a sensitive input past a check that only read the prompt expression.
	// An opaque reach found through a binding is carried out the same way one found
	// in the prompt is - the refusal is what "could not tell" means here, not
	// silence.
	reach := resolveReach(value, bindings)

	for _, name := range slices.Sorted(maps.Keys(reach.named)) {
		if !sensitive[name] {
			continue
		}

		w.problems = append(w.problems, WaitPromptProblem{StepID: stepID,
			Err: fmt.Errorf("step %q asks for approval with a `prompt:` that reads input %q, "+
				"which is declared `sensitive:`: a prompt is rendered to whoever is being asked, "+
				"who is not this run's author and was given a run id rather than this file, "+
				"so a prompt may not reach it even to derive from it. "+
				"Ask the question without it, or drop the `sensitive:` declaration if the value was never private",
				stepID, name)})

		return
	}

	if reach.opaque {
		w.problems = append(w.problems, WaitPromptProblem{StepID: stepID,
			Err: fmt.Errorf("step %q asks for approval with a `prompt:` that reads `%s` by a name this check "+
				"cannot resolve, and this workflow declares an input `sensitive:`, so whether the prompt reaches it "+
				"cannot be decided here. A prompt is rendered to whoever is being asked, so an undecidable reach is "+
				"refused rather than allowed: index `%s` with a literal name",
				stepID, InputsRoot, InputsRoot)})
	}
}

// promptFreeIdentifiers returns the bare names a prompt reads. See
// [resolveReach], which is the only caller and which says what they are followed
// into.
func promptFreeIdentifiers(value *Value) map[string]struct{} {
	free := make(map[string]struct{})
	for _, e := range promptExpressions(value, 0) {
		collectFreeIdentifiers(e, map[string]struct{}{}, free)
	}

	return free
}

// promptInputReach reports which inputs a prompt names, and whether it reaches
// the `inputs` root in some way this walk cannot name.
//
// The distinction is the whole point. `inputs.salary` and `inputs["salary"]`
// name a key, and a check can compare that key against what the file declared
// `sensitive:`. `inputs[key]`, or `inputs` passed whole to a function, name
// nothing statically - so the honest answer is "could not tell", which
// [checkPrompt] turns into a refusal rather than into silence.
func promptInputReach(value *Value) (named map[string]bool, opaque bool) {
	named = make(map[string]bool)

	for _, e := range promptExpressions(value, 0) {
		walkInputReach(e, named, &opaque)
	}

	return named, opaque
}

// promptExpressions collects every parsed expression a prompt value holds,
// descending structures for [holdsSecretRef]'s reason: a structure's entries are
// values in their own right, so an expression can sit arbitrarily deep in one.
func promptExpressions(value *Value, depth int) []*expr.Expr {
	if depth > maxVarScanDepth {
		return nil
	}

	switch kind := value.GetKind().(type) {
	case *Value_Expr:
		return []*expr.Expr{kind.Expr.GetExpr()}
	case *Value_Structure_:
		var out []*expr.Expr
		switch structure := kind.Structure.GetKind().(type) {
		case *Value_Structure_List_:
			for _, element := range structure.List.GetValues() {
				out = append(out, promptExpressions(element, depth+1)...)
			}
		case *Value_Structure_Map_:
			for _, entry := range structure.Map.GetEntries() {
				out = append(out, promptExpressions(entry, depth+1)...)
			}
		}

		return out
	}

	return nil
}

// walkInputReach records every named reach into `inputs` and sets opaque for
// every reach it cannot name.
//
// Written as its own walk rather than through [collectFreeIdentifiers], because
// that one answers "which roots does this mention" and the question here is
// "which *keys* of one root", which needs the parent of each identifier.
//
// A comprehension's own iteration variables can shadow `inputs`, so they are
// tracked: `${[1].map(inputs, "x")}` binds the name and reaches nothing, and
// treating that as an opaque reach would refuse a file that is fine. Nothing
// else binds a name inside an expression.
func walkInputReach(e *expr.Expr, named map[string]bool, opaque *bool) {
	walkInputReachBound(e, nil, named, opaque)
}

func walkInputReachBound(e *expr.Expr, bound map[string]struct{}, named map[string]bool, opaque *bool) {
	if e == nil {
		return
	}

	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		if kind.IdentExpr.GetName() != InputsRoot {
			return
		}
		if _, shadowed := bound[InputsRoot]; shadowed {
			return
		}
		// A bare `inputs` that nothing above claimed as a named select or a
		// constant index: the whole map is in play, so every declared input is
		// reachable and none of them is nameable.
		*opaque = true

	case *expr.Expr_SelectExpr:
		sel := kind.SelectExpr
		if isInputsRoot(sel.GetOperand(), bound) {
			// `has(inputs.salary)` - a test-only select - is a presence check
			// rather than a read, and is recorded all the same: a prompt saying
			// "a salary was supplied" is still a prompt whose text varies with a
			// value the author declared private, and this rule is about reaching
			// at all rather than about surfacing.
			named[sel.GetField()] = true

			return
		}
		walkInputReachBound(sel.GetOperand(), bound, named, opaque)

	case *expr.Expr_CallExpr:
		call := kind.CallExpr
		if call.GetFunction() == "_[_]" && len(call.GetArgs()) == 2 && isInputsRoot(call.GetArgs()[0], bound) {
			if key, ok := stringConstantExpr(call.GetArgs()[1]); ok {
				named[key] = true

				return
			}
			// `inputs[somethingComputed]` - a real reach, whose target cannot be
			// named here.
			*opaque = true
			walkInputReachBound(call.GetArgs()[1], bound, named, opaque)

			return
		}

		walkInputReachBound(call.GetTarget(), bound, named, opaque)
		for _, arg := range call.GetArgs() {
			walkInputReachBound(arg, bound, named, opaque)
		}

	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.GetElements() {
			walkInputReachBound(element, bound, named, opaque)
		}

	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			walkInputReachBound(entry.GetMapKey(), bound, named, opaque)
			walkInputReachBound(entry.GetValue(), bound, named, opaque)
		}

	case *expr.Expr_ComprehensionExpr:
		c := kind.ComprehensionExpr
		walkInputReachBound(c.GetIterRange(), bound, named, opaque)
		walkInputReachBound(c.GetAccuInit(), bound, named, opaque)

		inner := make(map[string]struct{}, len(bound)+3)
		maps.Copy(inner, bound)
		for _, name := range []string{c.GetIterVar(), c.GetIterVar2(), c.GetAccuVar()} {
			if name != "" {
				inner[name] = struct{}{}
			}
		}

		walkInputReachBound(c.GetLoopCondition(), inner, named, opaque)
		walkInputReachBound(c.GetLoopStep(), inner, named, opaque)
		walkInputReachBound(c.GetResult(), inner, named, opaque)
	}
}

// isInputsRoot reports whether e is the bare `inputs` identifier, unshadowed.
func isInputsRoot(e *expr.Expr, bound map[string]struct{}) bool {
	id, ok := e.GetExprKind().(*expr.Expr_IdentExpr)
	if !ok || id.IdentExpr.GetName() != InputsRoot {
		return false
	}
	_, shadowed := bound[InputsRoot]

	return !shadowed
}

// stringConstantExpr returns the value of e when it is a string literal.
func stringConstantExpr(e *expr.Expr) (string, bool) {
	c, ok := e.GetExprKind().(*expr.Expr_ConstExpr)
	if !ok {
		return "", false
	}
	s, ok := c.ConstExpr.GetConstantKind().(*expr.Constant_StringValue)
	if !ok {
		return "", false
	}

	return s.StringValue, true
}

// WaitPromptDescription renders a parked gate's prompt for a log line or a
// summary, in one place so every renderer says the same thing about a prompt
// that was cut.
//
// Empty for a gate with no prompt, so a caller can print what comes back and
// nothing when there is nothing.
func WaitPromptDescription(wait *PendingWait) string {
	prompt := wait.GetPrompt()
	if prompt == "" {
		return ""
	}
	if !wait.GetPromptTruncated() {
		return prompt
	}

	return strings.TrimRight(prompt, " ") +
		fmt.Sprintf(" (cut at %d bytes; this is part of the question)", MaxWaitPromptBytes)
}
