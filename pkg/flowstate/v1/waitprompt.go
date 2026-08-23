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
	value := signal.GetPrompt()
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
		return "", false, fmt.Errorf("evaluating wait_for_signal prompt: %w", err)
	}

	text, ok := evaluated.Value().(string)
	if !ok {
		// Named rather than coerced. A prompt is a sentence somebody reads, and
		// rendering a map or a number through whatever Go's default formatting
		// happens to produce would put an author's mistake in front of an
		// approver instead of in front of the author.
		return "", false, fmt.Errorf(
			"wait_for_signal prompt produced %s, and a prompt is the sentence an approver reads, so it has to be a string",
			evaluated.Type())
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
	return checkNodePrompts(wf.GetSteps(), sensitiveInputNames(wf), 0)
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

// checkNodePrompts walks every node that can hold a wait, and every node that
// can hold one inside it.
//
// A callee is descended into with *its own* declared inputs, not the caller's.
// A call is inlined in the caller's specification, so a submission carries the
// callee's prompts too - but `inputs.` inside a callee names the callee's
// arguments, so checking those references against the caller's declarations
// would ask the wrong file's question and get the wrong answer in both
// directions.
func checkNodePrompts(nodes []*Node, sensitive map[string]bool, depth int) error {
	if depth > maxVarScanDepth {
		// Refused rather than returned clean, per fail closed and for
		// [checkNodeVars]'s reason: past this the walk cannot say there is no
		// prompt down there reaching something private, and a check that cannot
		// decide must not allow.
		return fmt.Errorf("steps nest more than %d deep, past what a specification is checked to; "+
			"nothing this deep can be confirmed to keep private values out of a gate's `prompt:`", maxVarScanDepth)
	}

	for _, node := range nodes {
		if signal := node.GetWait().GetSignal(); signal != nil {
			if err := checkPrompt(signal.GetPrompt(), node.GetVars(), node.GetId(), sensitive); err != nil {
				return err
			}
		}

		if loop := node.GetForEach(); loop != nil {
			if err := checkNodePrompts(loop.GetBody(), sensitive, depth+1); err != nil {
				return err
			}
		}
		if loop := node.GetLoop(); loop != nil {
			if err := checkNodePrompts(loop.GetBody(), sensitive, depth+1); err != nil {
				return err
			}
		}
		if callee := node.GetCall().GetWorkflow(); callee != nil {
			if err := checkNodePrompts(callee.GetSteps(), sensitiveInputNames(callee), depth+1); err != nil {
				return err
			}
		}
		if sw := node.GetSwitch(); sw != nil {
			for _, body := range SwitchBodies(sw) {
				if err := checkNodePrompts(body, sensitive, depth+1); err != nil {
					return err
				}
			}
		}
		if parallel := node.GetParallel(); parallel != nil {
			for _, branch := range parallel.GetBranches() {
				if err := checkNodePrompts(branch.GetSteps(), sensitive, depth+1); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// checkPrompt reports what is wrong with one gate's prompt, by step id.
func checkPrompt(value *Value, vars map[string]*Value, stepID string, sensitive map[string]bool) error {
	if value == nil {
		return nil
	}

	if holdsSecretRef(value, 0) {
		return fmt.Errorf("step %q asks for approval with a `prompt:` that is a secret reference, "+
			"which a prompt may not hold: a prompt is rendered to whoever is being asked to approve, "+
			"so write the question without the secret in it", stepID)
	}

	if len(sensitive) == 0 {
		// Nothing this workflow declared is private, so there is nothing for a
		// prompt to reach. The common case, and it costs one length check.
		return nil
	}

	reached, opaque := promptInputReach(value)
	for name := range promptFreeIdentifiers(value) {
		declared, ok := vars[name]
		if !ok {
			continue
		}

		varReached, varOpaque := promptInputReach(declared)
		maps.Copy(reached, varReached)
		opaque = opaque || varOpaque
	}

	if named := slices.Sorted(maps.Keys(reached)); len(named) > 0 {
		for _, name := range named {
			if !sensitive[name] {
				continue
			}

			return fmt.Errorf("step %q asks for approval with a `prompt:` that reads input %q, "+
				"which is declared `sensitive:`: a prompt is rendered to whoever is being asked, "+
				"who is not this run's author and was given a run id rather than this file, "+
				"so a prompt may not reach it even to derive from it. "+
				"Ask the question without it, or drop the `sensitive:` declaration if the value was never private",
				stepID, name)
		}
	}

	if opaque {
		return fmt.Errorf("step %q asks for approval with a `prompt:` that reads `%s` by a name this check "+
			"cannot resolve, and this workflow declares an input `sensitive:`, so whether the prompt reaches it "+
			"cannot be decided here. A prompt is rendered to whoever is being asked, so an undecidable reach is "+
			"refused rather than allowed: index `%s` with a literal name",
			stepID, InputsRoot, InputsRoot)
	}

	return nil
}

// promptFreeIdentifiers returns the bare names a prompt reads. A step's own
// `vars:` are installed under bare names before its prompt is evaluated, so the
// sensitivity walk must follow any such name into its declaration rather than
// treating the prompt expression in isolation.
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
