package flowdebug_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// Completion is printing, and a debugger's printing is behind a redaction seam.
//
// The tests below are the negative direction CLAUDE.md asks for — that A cannot
// reach B, rather than that A can reach A. It is easy to write a test proving
// completion offers `token` and be satisfied; the question that matters is
// whether *anything it prints anywhere* can be the token's value, and whether a
// name the caller's redactor withholds stays withheld.

// theSecret is what a completion must never contain, in any field, at any
// position.
const theSecret = "hunter2-the-value-nothing-may-print"

// disclosureSession is a session over a run whose scope holds theSecret in
// every place a scope can hold a value: a step's output, a workflow var, and a
// run input.
//
// The redactor is the shape a caller installs — flowtest's own posture, applied
// to the rendered line — so this is the real seam rather than a stand-in.
func disclosureSession(t *testing.T, redact func(string) string) (console *asking, printed string) {
	t.Helper()

	var out strings.Builder
	// Asked at the *second* stop, so the leaky step has run and its output is
	// in the scope being completed over. Every position a completion is asked
	// at, in one list, because the claim is about all of them at once.
	console = &asking{steps: []string{"step", "continue"}, ask: [][]string{nil, {
		"inspect ",
		"inspect steps.",
		"inspect steps.leak.",
		"inspect vars.",
		"inspect inputs.",
		"break ",
		"delete ",
		"",
		"inspect s",
	}}}

	session, err := flowdebug.New(flowdebug.Options{
		Console: console,
		Out:     &out,
		Steps:   []string{"leak", "after"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session
	session.SetRedactor(redact)

	registry := debugRegistry(t, &ranSteps{})
	require.NoError(t, registry.Register(v1.TaskDef{Name: "leaky", Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
		return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
			"token": v1.NewLiteral(theSecret),
		}}, nil
	}}))

	ctx := v1.NewContextWithRegistry(t.Context(), registry)
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	workflow := &v1.Workflow{
		Name:           "leaks",
		DeclaredInputs: []*v1.InputDeclaration{{Name: "credential", Type: v1.InputDeclaration_TYPE_STRING}},
		Vars:           map[string]*v1.Value{"carried": v1.NewLiteral(theSecret)},
		Steps: []*v1.Node{
			{Id: "leak", Kind: &v1.Node_Task{Task: &v1.Task{Name: "leaky"}}},
			markStep("after"),
		},
	}

	_, err = v1.RunWithInputs(ctx, workflow, map[string]*v1.Value{"credential": v1.NewLiteral(theSecret)})
	require.NoError(t, err)

	return console, out.String()
}

// TestNoCompletionAnywhereCanBeAValue walks every position a completion is
// asked at, over a scope holding one sensitive value in three places, and
// checks the whole answer rather than the name it was hoping for.
//
// This is the traversal rather than the step (CLAUDE.md): a test asserting that
// `inspect steps.leak.` offers `token` passes just as well against a completer
// that also puts the value in the detail beside it.
func TestNoCompletionAnywhereCanBeAValue(t *testing.T) {
	t.Parallel()

	console, _ := disclosureSession(t, func(text string) string {
		return strings.ReplaceAll(text, theSecret, "[redacted]")
	})

	require.NotEmpty(t, console.answers)
	offered := 0
	for i, answer := range console.answers {
		for _, candidate := range answer.Candidates {
			offered++
			assert.NotContains(t, candidate.Text, theSecret,
				"answer %d offered the value as a name to insert", i)
			assert.NotContains(t, candidate.Detail, theSecret,
				"answer %d described a name with the value beside it", i)
		}
	}
	require.Greater(t, offered, 0, "a test over an empty set of offers proves nothing")

	// And the names themselves are still there, so the check above is not
	// passing because nothing was offered.
	names := texts(console.answers[2])
	assert.Equal(t, []string{"token"}, names,
		"the output's name is the author's and is exactly what completion is for")
}

// TestACompletionTheRedactorWouldChangeIsWithheld is the fail-closed half.
//
// A name is normally the author's rather than the run's, so the ordinary case
// is that a redactor has nothing to say about one. Where it does, there is no
// third answer: an offer rendered as `[redacted]` is a marker typed into
// somebody's expression, so the candidate goes away instead.
func TestACompletionTheRedactorWouldChangeIsWithheld(t *testing.T) {
	t.Parallel()

	// A redactor that withholds the *name* `token`, which is what a caller
	// whose sensitive set happens to contain a short string does.
	console, _ := disclosureSession(t, func(text string) string {
		return strings.ReplaceAll(strings.ReplaceAll(text, theSecret, "[redacted]"), "token", "[redacted]")
	})

	assert.Empty(t, texts(console.answers[2]),
		"`steps.leak.` has one output and its name is withheld, so there is nothing to offer")

	for _, answer := range console.answers {
		for _, candidate := range answer.Candidates {
			assert.NotContains(t, candidate.Text, "token")
			assert.NotContains(t, candidate.Detail, "token")
		}
	}
}

// TestEverythingACandidatePrintsGoesThroughTheRedactor covers the second field.
//
// A detail is a description this package writes, so in practice a redactor has
// nothing to say about one — which is exactly why it needs a test: an
// unreachable check is one somebody deletes as dead, and the rule it states is
// what makes the answer safe to *extend*. The day a detail carries something
// drawn from the run, the seam is already there and already tested.
func TestEverythingACandidatePrintsGoesThroughTheRedactor(t *testing.T) {
	t.Parallel()

	// The word appears only in the detail beside an output's name, never in
	// the name itself.
	const inTheDetailOnly = "produced"

	console, _ := disclosureSession(t, func(text string) string {
		return strings.ReplaceAll(text, inTheDetailOnly, "[redacted]")
	})

	assert.Empty(t, texts(console.answers[2]),
		"the name is safe and its description is not, so the offer goes rather than the description")
}

// TestWithoutARedactorTheSameNamesAreOffered is the control the two tests above
// need: withholding that also happens when nothing is being withheld is not
// withholding, it is a broken completer.
func TestWithoutARedactorTheSameNamesAreOffered(t *testing.T) {
	t.Parallel()

	console, _ := disclosureSession(t, nil)

	assert.Equal(t, []string{"token"}, texts(console.answers[2]))
	assert.Equal(t, []string{"carried"}, texts(console.answers[3]))
	assert.Equal(t, []string{"credential"}, texts(console.answers[4]))
	assert.Equal(t, []string{"after", "leak"}, texts(console.answers[5]))
}

// TestTheSecretIsInTheScopeThisIsCompletingOver checks the premise the two
// negative tests rest on rather than assuming it.
//
// Without it they would pass against a run whose scope never held the value at
// all — a security test that is really a test of nothing, which is the exact
// shape CLAUDE.md's tenancy section describes. The session narrates each step's
// values as they arrive, so its own transcript is the evidence: with no
// redactor the value is there in the clear, and with one it is not.
func TestTheSecretIsInTheScopeThisIsCompletingOver(t *testing.T) {
	t.Parallel()

	_, clear := disclosureSession(t, nil)
	require.Contains(t, clear, theSecret,
		"the run being completed over really does hold the value, and the session prints it")

	_, withheld := disclosureSession(t, func(text string) string {
		return strings.ReplaceAll(text, theSecret, "[redacted]")
	})
	require.NotContains(t, withheld, theSecret,
		"and the redactor installed for the negative tests is a live seam, not a no-op")
	require.Contains(t, withheld, "[redacted]")
}
