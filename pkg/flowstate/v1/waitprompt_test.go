package flowstatev1_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// What a gate's `prompt:` may reach, and what it renders as when it reaches
// something it may not.
//
// The shared driver table (tests/pendingwaits.go) pins that both drivers report
// the same question for a prompt that is fine. These pin the other direction:
// the three layers that refuse one that is not, and the shapes a refused prompt
// is formatted in.

// promptGate builds a one-step workflow whose gate asks the given question.
func promptGate(prompt *v1.Value, declared ...*v1.InputDeclaration) *v1.Workflow {
	return &v1.Workflow{
		Name:           "asking",
		DeclaredInputs: declared,
		Steps: []*v1.Node{{
			Id: "gate",
			Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "approve", Prompt: prompt}},
			}},
		}},
	}
}

// secretPrompt is a prompt written as a `${secret(...)}` reference, which is the
// one shape [v1.EvalSignalPrompt] still has to answer for: the compiler refuses
// it against a line, the submit boundary refuses it again, and a specification
// assembled in Go and executed directly reaches neither.
func secretPrompt() *v1.Value {
	return &v1.Value{Kind: &v1.Value_SecretRef{
		SecretRef: &v1.SecretRef{Scheme: "env", Name: "APPROVAL_QUESTION"},
	}}
}

// TestASecretPromptRendersAsARefusalRatherThanTheSecret is the evaluation-time
// layer: nothing is resolved, and what comes back is this system's own marker.
func TestASecretPromptRendersAsARefusalRatherThanTheSecret(t *testing.T) {
	t.Parallel()

	signal := &v1.Signal{Name: "approve", Prompt: secretPrompt()}

	prompt, truncated, err := v1.EvalSignalPrompt(t.Context(), signal, &v1.Scope{}, time.Now())
	require.NoError(t, err,
		"a prompt naming a secret failed the gate rather than refusing to ask it; an approver "+
			"cannot fix a specification, and a run that dies at the gate is worse than one that "+
			"says the question was withheld")
	assert.Equal(t, v1.PromptWithheldSecret, prompt)
	assert.False(t, truncated)
	assert.NotContains(t, prompt, "APPROVAL_QUESTION",
		"the refusal named the secret it refused to resolve")
}

// TestARefusedPromptStaysContainedInEveryFormattingShape is CLAUDE.md's
// containment-shape rule applied to the value that carries a gate's question.
//
// `fmt` cannot call a method on a value it reaches through an unexported field,
// so a redacting String method protects a value printed directly and does
// nothing for the same value inside a struct or a slice. The answer here is not
// a String method at all (the secret is never resolved, so there is nothing to
// redact), and that is exactly the claim worth pinning: a [v1.PendingWait]
// carrying a refused prompt holds the marker and not the material, whichever way
// it is printed, and whatever it is printed inside.
func TestARefusedPromptStaysContainedInEveryFormattingShape(t *testing.T) {
	t.Parallel()

	signal := &v1.Signal{Name: "approve", Prompt: secretPrompt()}

	prompt, _, err := v1.EvalSignalPrompt(t.Context(), signal, &v1.Scope{}, time.Now())
	require.NoError(t, err)

	wait := &v1.PendingWait{StepId: "gate", SignalName: "approve", Prompt: prompt}

	// A struct holding it, which is the shape that defeats a String method, and
	// a slice of those, which is the shape a query answer actually travels in.
	type holder struct {
		Wait  *v1.PendingWait
		Waits []*v1.PendingWait
	}

	held := holder{Wait: wait, Waits: []*v1.PendingWait{wait}}

	shapes := []string{
		fmt.Sprintf("%v", wait),
		fmt.Sprintf("%+v", wait),
		fmt.Sprintf("%#v", wait),
		// Spelling `%s` rather than calling String(): the verb a careless log
		// line reaches for is the one under test, which is exactly the case
		// CLAUDE.md says never to quiet by changing what it asserts.
		//lint:ignore S1025 the wrong verb is the point, since that is what a careless log line writes
		fmt.Sprintf("%s", wait),
		fmt.Sprintf("%v", held),
		fmt.Sprintf("%+v", held),
		fmt.Sprintf("%#v", held),
		fmt.Sprintf("%v", []*v1.PendingWait{wait}),
		fmt.Sprintf("%+v", []*v1.PendingWait{wait}),
		fmt.Sprintf("%#v", []*v1.PendingWait{wait}),
	}

	for i, rendered := range shapes {
		assert.NotContains(t, rendered, "APPROVAL_QUESTION",
			"formatting shape %d put the name of the secret a prompt refused to resolve into text", i)
	}

	// And the refusal is *present* wherever the text is rendered at all, which is
	// the half a containment test on its own cannot claim. Only the shapes that
	// reach the message's own formatting are asserted on: a struct or a slice
	// printed with %v renders a pointer address and no content, which is a
	// property of Go rather than of this field, and demanding the marker there
	// would be asserting that Go prints something it does not.
	for i, rendered := range []string{
		fmt.Sprintf("%v", wait),
		fmt.Sprintf("%+v", wait),
		//lint:ignore S1025 as above: the verb an operator's log line uses is what is under test
		fmt.Sprintf("%s", wait),
	} {
		assert.Contains(t, rendered, "withheld",
			"rendered shape %d dropped the refusal, so a reader is shown a gate with no question "+
				"rather than a gate whose question was refused", i)
	}
}

// TestSubmitRefusesAPromptHoldingASecretReference is the fail-closed layer, at
// the one boundary every submit path already crosses.
func TestSubmitRefusesAPromptHoldingASecretReference(t *testing.T) {
	t.Parallel()

	err := v1.CheckWaitPromptsAreAskable(promptGate(secretPrompt()))
	require.Error(t, err, "a specification asking for approval with a secret in the question was accepted")
	assert.Contains(t, err.Error(), "gate")

	_, bindErr := v1.BindRunInputs(promptGate(secretPrompt()), nil)
	require.Error(t, bindErr,
		"the submit boundary accepted a prompt the shared rule refuses, so a specification that "+
			"never was a Flowfile reaches a running gate with a secret in its question")
}

// TestSubmitRefusesAPromptThatReachesASensitiveInput is the wider half of the
// rule, and the direction that separates it from the `log:` lint it is a sibling
// of: the value is *derived* rather than surfaced, and it is still refused.
//
// A prompt is rendered to somebody who was handed a run id rather than the file,
// so "approve a large raise" discloses the bracket to a reader the author never
// decided to disclose it to, without the number appearing anywhere.
func TestSubmitRefusesAPromptThatReachesASensitiveInput(t *testing.T) {
	t.Parallel()

	salary := &v1.InputDeclaration{Name: "salary", Sensitive: true}

	derived := promptGate(
		v1.NewExpr(`inputs.salary > 100000 ? "a large raise" : "a small raise"`), salary)

	err := v1.CheckWaitPromptsAreAskable(derived)
	require.Error(t, err, "a prompt deriving its text from a sensitive input was accepted")
	assert.Contains(t, err.Error(), "salary")
}

// TestSubmitRefusesAPromptReachingASensitiveInputThroughAStepVar pins the
// evaluation order at the privacy boundary: step vars are installed before the
// prompt runs, so checking only the prompt's own expression would let a bare
// name conceal its reach into a sensitive input.
func TestSubmitRefusesAPromptReachingASensitiveInputThroughAStepVar(t *testing.T) {
	t.Parallel()

	wf := promptGate(v1.NewExpr(`question`),
		&v1.InputDeclaration{Name: "token", Sensitive: true})
	wf.Steps[0].Vars = map[string]*v1.Value{
		"question": v1.NewExpr(`"approve " + inputs.token`),
	}

	err := v1.CheckWaitPromptsAreAskable(wf)
	require.Error(t, err, "a step var hid a prompt's reach into a sensitive input")
	assert.Contains(t, err.Error(), "token")
}

// TestSubmitRefusesAPromptWhoseReachCannotBeDecided is fail-closed for a lint:
// `inputs[someComputedKey]` names no key statically, so whether it reaches the
// sensitive one cannot be answered, and a check that cannot decide must not
// allow.
func TestSubmitRefusesAPromptWhoseReachCannotBeDecided(t *testing.T) {
	t.Parallel()

	salary := &v1.InputDeclaration{Name: "salary", Sensitive: true}
	which := &v1.InputDeclaration{Name: "which", Type: v1.InputDeclaration_TYPE_STRING}

	err := v1.CheckWaitPromptsAreAskable(
		promptGate(v1.NewExpr(`"approve " + string(inputs[inputs.which])`), salary, which))
	require.Error(t, err,
		"a prompt indexing inputs by a computed key was accepted while the workflow declares a "+
			"sensitive input, so a rule that cannot decide allowed")
}

// TestAPromptIsLeftAloneWhenNothingIsDeclaredSensitive is the control, and the
// reason the rule above can afford to be wide.
//
// False diagnostics are worse than missing ones, so the only authors who can
// ever meet this refusal are the ones who already told the system some input of
// theirs is private. A workflow that declares nothing sensitive is never
// examined at all.
func TestAPromptIsLeftAloneWhenNothingIsDeclaredSensitive(t *testing.T) {
	t.Parallel()

	require.NoError(t, v1.CheckWaitPromptsAreAskable(
		promptGate(v1.NewExpr(`"approve " + string(inputs[inputs.which])`),
			&v1.InputDeclaration{Name: "which", Type: v1.InputDeclaration_TYPE_STRING})),
		"a workflow declaring nothing sensitive was told its prompt reaches something private")
}

// TestAPromptReadingAnOrdinaryInputIsAccepted is the other control: the rule is
// about `sensitive:` inputs specifically, not about `inputs.` at all. Without
// this, a refusal of every input reference would pass every test above.
func TestAPromptReadingAnOrdinaryInputIsAccepted(t *testing.T) {
	t.Parallel()

	require.NoError(t, v1.CheckWaitPromptsAreAskable(
		promptGate(v1.NewExpr(`"deploy " + inputs.version + "?"`),
			&v1.InputDeclaration{Name: "version", Type: v1.InputDeclaration_TYPE_STRING},
			&v1.InputDeclaration{Name: "token", Sensitive: true})),
		"a prompt naming an ordinary input was refused because some *other* input is sensitive")
}

// TestALongPromptIsCutAtTheBoundAndSaysSo pins the bound and the honesty of the
// truncation. Both drivers read [v1.MaxWaitPromptBytes] from here, which is what
// the shared table then checks they agree about.
func TestALongPromptIsCutAtTheBoundAndSaysSo(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("q", v1.MaxWaitPromptBytes+64)

	prompt, truncated, err := v1.EvalSignalPrompt(
		t.Context(), &v1.Signal{Name: "approve", Prompt: v1.NewValue(long)}, &v1.Scope{}, time.Now())
	require.NoError(t, err)

	assert.Len(t, prompt, v1.MaxWaitPromptBytes)
	assert.True(t, truncated,
		"a question that was cut short reported itself whole, so somebody may answer having read "+
			"half of it")
}

// TestAPromptIsCutOnARuneBoundary is the half a byte-count bound gets wrong.
//
// A question cut mid-rune renders as a replacement character in whatever is
// showing it, which reads as corruption rather than as a bound being reached;
// and the reader cannot tell which.
func TestAPromptIsCutOnARuneBoundary(t *testing.T) {
	t.Parallel()

	// A multi-byte rune straddling the bound: enough single-byte characters that
	// the cut lands inside the next one.
	long := strings.Repeat("q", v1.MaxWaitPromptBytes-1) + strings.Repeat("é", 32)

	prompt, truncated, err := v1.EvalSignalPrompt(
		t.Context(), &v1.Signal{Name: "approve", Prompt: v1.NewValue(long)}, &v1.Scope{}, time.Now())
	require.NoError(t, err)
	require.True(t, truncated)

	assert.True(t, utf8ValidString(prompt),
		"a cut question ended in half a rune, which renders as corruption rather than as a bound")
	assert.LessOrEqual(t, len(prompt), v1.MaxWaitPromptBytes)
}

// TestAPromptThatIsNotAStringFailsTheStep pins the one evaluation outcome that
// is an error rather than a marker: an author's mistake, reported to the author,
// rather than rendered to an approver through Go's default formatting.
func TestAPromptThatIsNotAStringFailsTheStep(t *testing.T) {
	t.Parallel()

	_, _, err := v1.EvalSignalPrompt(
		t.Context(), &v1.Signal{Name: "approve", Prompt: v1.NewExpr("1 + 2")}, &v1.Scope{}, time.Now())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "string")
}

// TestAGateWithNoPromptAsksNothing is the zero case: the overwhelmingly common
// shape, and one that must stay distinguishable from an empty question.
func TestAGateWithNoPromptAsksNothing(t *testing.T) {
	t.Parallel()

	prompt, truncated, err := v1.EvalSignalPrompt(
		t.Context(), &v1.Signal{Name: "approve"}, &v1.Scope{}, time.Now())
	require.NoError(t, err)
	assert.Empty(t, prompt)
	assert.False(t, truncated)
}

// utf8ValidString is spelled here rather than imported so the assertion above
// reads as the claim it makes.
func utf8ValidString(s string) bool {
	for _, r := range s {
		if r == '�' {
			return false
		}
	}

	return true
}

// The names the *grammar* binds around a gate (#976).
//
// The rule above follows a prompt into the wait step's own `vars:`, because that
// is where the engine evaluates it from. The language binds three more bare names
// around a step — a `for_each`'s `as:` (or the `item` it binds when none is
// written), a `loop:`'s carried state, and an enclosing step's `vars:` — and a
// prompt reading one of them reaches whatever that name was bound to. These are
// the same four names `flow fix` had to learn about twice, for the same reason:
// each is legal alongside a step of the same id, so a walk that does not know
// what the grammar binds reads the wrong thing.
//
// Each scope is taken from where the engine evaluates the thing: a loop's binding
// is in scope for the body only, and a container step's `vars:` are in scope for
// what is inside it.

// gateAsking is a one-step gate node asking the given question, for nesting
// inside the containers below.
func gateAsking(id string, prompt *v1.Value) *v1.Node {
	return &v1.Node{
		Id: id,
		Kind: &v1.Node_Wait{Wait: &v1.Wait{
			Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "approve", Prompt: prompt}},
		}},
	}
}

// forEachAsking builds the issue's own shape: a `for_each` over items, binding
// `as` (empty for the default), whose body holds one gate.
func forEachAsking(items *v1.Value, as string, prompt *v1.Value, declared ...*v1.InputDeclaration) *v1.Workflow {
	return &v1.Workflow{
		Name:           "asking",
		DeclaredInputs: declared,
		Steps: []*v1.Node{{
			Id: "review",
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:    items,
				Iterator: as,
				Body:     []*v1.Node{gateAsking("approve", prompt)},
			}},
		}},
	}
}

// TestSubmitRefusesAPromptReachingASensitiveInputThroughALoopBinding is #976's
// example, exactly as the issue writes it.
func TestSubmitRefusesAPromptReachingASensitiveInputThroughALoopBinding(t *testing.T) {
	t.Parallel()

	err := v1.CheckWaitPromptsAreAskable(forEachAsking(
		v1.NewExpr(`inputs.customers`), "cust", v1.NewExpr(`cust`),
		&v1.InputDeclaration{Name: "customers", Sensitive: true}))

	require.Error(t, err,
		"a `for_each`'s `as:` laundered a sensitive input into the question an approver is shown")
	assert.Contains(t, err.Error(), "customers")
	assert.Contains(t, err.Error(), "approve", "the refusal named the loop rather than the gate inside it")
}

// TestSubmitRefusesAPromptReachingASensitiveInputThroughTheImplicitItem is the
// same reach through the name a loop binds when it writes no `as:`, which is the
// spelling that has no name in the file at all to notice.
func TestSubmitRefusesAPromptReachingASensitiveInputThroughTheImplicitItem(t *testing.T) {
	t.Parallel()

	err := v1.CheckWaitPromptsAreAskable(forEachAsking(
		v1.NewExpr(`inputs.customers`), "", v1.NewExpr(v1.DefaultIterator),
		&v1.InputDeclaration{Name: "customers", Sensitive: true}))

	require.Error(t, err,
		"a `for_each` with no `as:` still binds "+v1.DefaultIterator+", and the prompt reading it was accepted")
	assert.Contains(t, err.Error(), "customers")
}

// TestSubmitRefusesAPromptReachingASensitiveInputThroughLoopState covers the
// third bare name: a `loop:`'s carried state, read under its own name inside the
// body. Reached through `update:` rather than `initial:` on purpose — the state
// holds whatever `update:` computed on every iteration after the first, so a walk
// that only read the initial value would miss every one of them.
func TestSubmitRefusesAPromptReachingASensitiveInputThroughLoopState(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Name:           "asking",
		DeclaredInputs: []*v1.InputDeclaration{{Name: "token", Sensitive: true}},
		Steps: []*v1.Node{{
			Id: "poll",
			Kind: &v1.Node_Loop{Loop: &v1.Loop{
				State:         "cursor",
				Initial:       v1.NewLiteral(""),
				Update:        v1.NewExpr(`"page " + inputs.token`),
				Until:         v1.NewExpr("true"),
				MaxIterations: 2,
				Body:          []*v1.Node{gateAsking("approve", v1.NewExpr(`cursor`))},
			}},
		}},
	}

	err := v1.CheckWaitPromptsAreAskable(wf)
	require.Error(t, err, "a loop's carried state hid a prompt's reach into a sensitive input")
	assert.Contains(t, err.Error(), "token")
}

// TestSubmitRefusesAPromptReachingASensitiveInputThroughAnEnclosingStepsVars is
// the fourth: a container step's `vars:` are bound for everything inside it, so a
// gate in the body reads them exactly as the container's own inputs do.
func TestSubmitRefusesAPromptReachingASensitiveInputThroughAnEnclosingStepsVars(t *testing.T) {
	t.Parallel()

	wf := forEachAsking(v1.NewLiteralList("a"), "cust", v1.NewExpr(`question`),
		&v1.InputDeclaration{Name: "token", Sensitive: true})
	wf.Steps[0].Vars = map[string]*v1.Value{
		"question": v1.NewExpr(`"approve " + inputs.token`),
	}

	err := v1.CheckWaitPromptsAreAskable(wf)
	require.Error(t, err, "an enclosing step's `vars:` hid a prompt's reach into a sensitive input")
	assert.Contains(t, err.Error(), "token")
}

// TestAPromptReachingAnOrdinaryInputThroughALoopBindingIsAccepted is the control
// for all four, and the one that keeps them from being a refusal of every bare
// name: the identical shape over an input nobody declared `sensitive:` is fine,
// in a workflow that does declare one so the rule is actually running.
func TestAPromptReachingAnOrdinaryInputThroughALoopBindingIsAccepted(t *testing.T) {
	t.Parallel()

	require.NoError(t, v1.CheckWaitPromptsAreAskable(forEachAsking(
		v1.NewExpr(`inputs.hosts`), "host", v1.NewExpr(`"deploy to " + host + "?"`),
		&v1.InputDeclaration{Name: "hosts", Type: v1.InputDeclaration_TYPE_LIST},
		&v1.InputDeclaration{Name: "token", Sensitive: true})),
		"a prompt reading an ordinary input through a loop binding was refused because some "+
			"*other* input is sensitive")
}

// TestALoopBindingIsNotInScopeAfterTheLoop pins the extent of the binding rather
// than its existence: a loop's item is bound for the body only, so the same name
// outside the loop is whatever it was outside the loop — here a step of that id,
// which this rule does not follow. Widening the scope would report this file as
// wrong on the strength of a binding that is not in scope where it is read.
func TestALoopBindingIsNotInScopeAfterTheLoop(t *testing.T) {
	t.Parallel()

	inner := forEachAsking(v1.NewExpr(`inputs.customers`), "cust", v1.NewLiteral("approve?"),
		&v1.InputDeclaration{Name: "customers", Sensitive: true})
	// A sibling of the loop, not of the gate inside it: nothing binds `cust` here.
	inner.Steps = append(inner.Steps, gateAsking("after", v1.NewExpr(`cust`)))

	// Both sit inside a container that binds a name of its own, so the scope the
	// loop extends is a live map rather than the empty one at the top of a
	// workflow: a binding written into the enclosing scope in place would be
	// visible to the sibling below, which is the failure this asserts against.
	wf := &v1.Workflow{
		Name:           inner.GetName(),
		DeclaredInputs: inner.GetDeclaredInputs(),
		Steps: []*v1.Node{{
			Id:   "outer",
			Vars: map[string]*v1.Value{"greeting": v1.NewLiteral("hello")},
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items: v1.NewLiteralList("once"),
				Body:  inner.GetSteps(),
			}},
		}},
	}

	require.NoError(t, v1.CheckWaitPromptsAreAskable(wf),
		"a `for_each`'s binding was treated as in scope after the loop, where the engine has "+
			"already dropped it")
}

// TestAStepIdIsNotABindingOfTheSameName is the other half of that: a bare name
// that merely *looks* like a binding is a step reference, which is a value this
// rule does not follow, and reporting it would be a false diagnostic about a file
// that is fine.
func TestAStepIdIsNotABindingOfTheSameName(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{
		Name:           "asking",
		DeclaredInputs: []*v1.InputDeclaration{{Name: "customers", Sensitive: true}},
		Steps: []*v1.Node{
			{Id: "cust", Kind: &v1.Node_Value{Value: v1.NewLiteral("acme")}},
			gateAsking("approve", v1.NewExpr(`cust`)),
		},
	}

	require.NoError(t, v1.CheckWaitPromptsAreAskable(wf),
		"a step whose id happens to match nothing bound here was read as a binding")
}

// TestABindingShadowedByAMacroIsNotFollowed keeps free names free in the CEL
// sense. `list.map(cust, cust)` binds `cust` itself, so the walk must not go
// looking for the enclosing loop's binding of that name — the same rule
// [walkInputReach] already applies to `inputs` and the one `flow fix` learned
// first.
func TestABindingShadowedByAMacroIsNotFollowed(t *testing.T) {
	t.Parallel()

	require.NoError(t, v1.CheckWaitPromptsAreAskable(forEachAsking(
		v1.NewExpr(`inputs.customers`), "cust", v1.NewExpr(`["ok"].map(cust, cust)[0]`),
		&v1.InputDeclaration{Name: "customers", Sensitive: true})),
		"a comprehension's own iteration variable was looked up among the grammar's bindings, "+
			"so an expression that reaches nothing was refused")
}

// TestABindingDoesNotEscapeIntoASiblingBranch pins that an inner scope stays
// inner: a binding made for one `for_each`'s body is not in scope in a branch
// beside it.
func TestABindingDoesNotEscapeIntoASiblingBranch(t *testing.T) {
	t.Parallel()

	sensitive := &v1.InputDeclaration{Name: "customers", Sensitive: true}
	wf := &v1.Workflow{
		Name:           "asking",
		DeclaredInputs: []*v1.InputDeclaration{sensitive},
		Steps: []*v1.Node{{
			Id: "both",
			// A `vars:` block so the branches share a live scope: a binding made
			// in one branch and written into that scope in place would be in the
			// branch beside it, which is what this asserts against.
			Vars: map[string]*v1.Value{"greeting": v1.NewLiteral("hello")},
			Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
				{Steps: []*v1.Node{{
					Id: "review",
					Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
						Items:    v1.NewExpr(`inputs.customers`),
						Iterator: "cust",
						Body:     []*v1.Node{gateAsking("inner", v1.NewLiteral("approve?"))},
					}},
				}}},
				{Steps: []*v1.Node{gateAsking("beside", v1.NewExpr(`cust`))}},
			}}},
		}},
	}

	require.NoError(t, v1.CheckWaitPromptsAreAskable(wf),
		"a binding from one branch's loop leaked into the branch beside it")
}

// TestAnUndecidableReachThroughABindingIsRefused carries the fail-closed answer
// across a binding: `inputs[whicheverKey]` names no key, and a name bound to it
// is no more decidable than the expression itself.
func TestAnUndecidableReachThroughABindingIsRefused(t *testing.T) {
	t.Parallel()

	err := v1.CheckWaitPromptsAreAskable(forEachAsking(
		v1.NewExpr(`inputs[inputs.which]`), "cust", v1.NewExpr(`string(cust)`),
		&v1.InputDeclaration{Name: "salary", Sensitive: true},
		&v1.InputDeclaration{Name: "which", Type: v1.InputDeclaration_TYPE_STRING}))

	require.Error(t, err,
		"a reach this walk cannot name was allowed once it passed through a loop binding, so "+
			"\"could not tell\" answered as silence")
}

// TestACalleesPromptIsCheckedAgainstItsOwnScope pins the one boundary bindings do
// not cross. A callee is inlined into what is submitted, so its prompts are
// checked — but `inputs.` there names the callee's arguments, and no bare name the
// caller bound is in scope inside it.
func TestACalleesPromptIsCheckedAgainstItsOwnScope(t *testing.T) {
	t.Parallel()

	// The callee declares a sensitive input of its own, so the rule is running
	// inside it — and one that happens to share the caller's name, which is what
	// makes the claim testable: if the caller's binding of `cust` crossed the
	// boundary, its reach into the *caller's* `customers` would be matched
	// against the callee's declaration of that name and refused.
	callee := &v1.Workflow{
		Name:           "inner",
		DeclaredInputs: []*v1.InputDeclaration{{Name: "customers", Sensitive: true}},
		Steps:          []*v1.Node{gateAsking("approve", v1.NewExpr(`cust`))},
	}
	wf := forEachAsking(v1.NewExpr(`inputs.customers`), "cust", v1.NewLiteral("approve?"),
		&v1.InputDeclaration{Name: "customers", Sensitive: true})
	wf.Steps[0].GetForEach().Body = append(wf.Steps[0].GetForEach().Body, &v1.Node{
		Id:   "delegate",
		Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}},
	})

	require.NoError(t, v1.CheckWaitPromptsAreAskable(wf),
		"the caller's loop binding was carried into a callee, where the name means whatever that "+
			"file says it means")
}

// TestWaitPromptProblemsReportsEveryRefusedGate is what `flowfile` needs and what
// the first-error spelling cannot give it: a file earning two refusals gets two
// positioned diagnostics rather than one and a surprise on the next run.
func TestWaitPromptProblemsReportsEveryRefusedGate(t *testing.T) {
	t.Parallel()

	wf := forEachAsking(v1.NewExpr(`inputs.customers`), "cust", v1.NewExpr(`cust`),
		&v1.InputDeclaration{Name: "customers", Sensitive: true})
	wf.Steps[0].GetForEach().Body = append(wf.Steps[0].GetForEach().Body,
		gateAsking("second", v1.NewExpr(`"about " + cust`)))

	problems := v1.WaitPromptProblems(wf, v1.DescendCalls)
	require.Len(t, problems, 2, "only one of two refused gates was reported: %v", problems)
	assert.Equal(t, "approve", problems[0].StepID)
	assert.Equal(t, "second", problems[1].StepID)
}

// TestSkipCallsLeavesACalleeToItsOwnValidation is the distinction the two callers
// need: `flow validate` has a separate file to report against, and the submit
// boundary does not.
func TestSkipCallsLeavesACalleeToItsOwnValidation(t *testing.T) {
	t.Parallel()

	callee := &v1.Workflow{
		Name:           "inner",
		DeclaredInputs: []*v1.InputDeclaration{{Name: "salary", Sensitive: true}},
		Steps:          []*v1.Node{gateAsking("approve", v1.NewExpr(`inputs.salary`))},
	}
	wf := &v1.Workflow{
		Name:  "outer",
		Steps: []*v1.Node{{Id: "delegate", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}}},
	}

	assert.Empty(t, v1.WaitPromptProblems(wf, v1.SkipCalls),
		"a callee was checked by the caller's compiler, which reports against the wrong file")
	assert.NotEmpty(t, v1.WaitPromptProblems(wf, v1.DescendCalls),
		"an inlined callee's prompt was not checked at the boundary where no separate file is left")
}
