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
