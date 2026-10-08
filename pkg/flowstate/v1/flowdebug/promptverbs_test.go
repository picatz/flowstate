package flowdebug_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// TestThePromptAnswersTheVerbsTheDriverAlwaysHad: `expand` and `status` were
// driver-only, so a person at `flow run local --debug` could not read a list a
// structured front could page through, or ask where the run was.
func TestThePromptAnswersTheVerbsTheDriverAlwaysHad(t *testing.T) {
	t.Parallel()

	out, _, err := runDebugged(t, "expand [10, 20]\nstatus\ncontinue\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Contains(t, out, "0  int  10", "expand lists a list's children, one to a line, as the driver does")
	assert.Contains(t, out, "1  int  20")
	assert.NotContains(t, out, "unknown command")
	assert.NotContains(t, out, "driver command", "the prompt no longer refuses either verb as belonging elsewhere")
	assert.Contains(t, out, "held", "status says where the run is, and why")
}

// TestExpandAtThePromptNeedsAnExpressionAndSaysWhenItCannotRead: the negative
// direction. An empty argument is a usage line, and an expression that does not
// evaluate is the evaluator's own sentence, neither of them a listing.
func TestExpandAtThePromptNeedsAnExpressionAndSaysWhenItCannotRead(t *testing.T) {
	t.Parallel()

	out, _, err := runDebugged(t, "expand\nexpand nosuchroot.x\ncontinue\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Contains(t, out, "expand needs an expression: expand steps.build")
	assert.Contains(t, out, "no such attribute(s): nosuchroot.x", "the evaluator's own diagnostic is what a failed read says")
	assert.NotContains(t, out, "has no children", "a failed read is not an empty listing")
}

// TestAPromptThatCannotStepBackSaysSo: `back` and `reverse-continue` are
// accepted verbs at a prompt, so a script written for a session that can step
// back reads the same here, and the capability it lacks is the answer. Neither is
// recorded, because a refused command is not part of the session it would replay.
func TestAPromptThatCannotStepBackSaysSo(t *testing.T) {
	t.Parallel()

	for _, verb := range []string{"back", "reverse-continue", "rc"} {
		t.Run(verb, func(t *testing.T) {
			t.Parallel()

			session, out, _, err := runDebuggedSession(t, "step\n"+verb+"\ncontinue\n", flowdebug.Options{})
			require.NoError(t, err)

			assert.Contains(t, out, "this session cannot step back")
			assert.NotContains(t, out, "unknown command")
			assert.Equal(t, []string{"step", "continue"}, session.Script(),
				"a refused command is not recorded, under any spelling, canonical or not")
		})
	}
}

// TestTheAutopsyDoesNotLeaveOnAVerbThatStepsBack: every other verb that moves the
// run ends the autopsy, because there is nothing left to move. `back` is a
// movement too, but it is the one that goes the other way, and a finished run
// cannot be stepped back from: it is refused where it is typed, and the session
// is still there to ask the next question.
func TestTheAutopsyDoesNotLeaveOnAVerbThatStepsBack(t *testing.T) {
	t.Parallel()

	for _, verb := range []string{"back", "reverse-continue"} {
		t.Run(verb, func(t *testing.T) {
			t.Parallel()

			var out strings.Builder
			session, err := flowdebug.New(flowdebug.Options{In: strings.NewReader(verb + "\nhelp\nquit\n"), Out: &out})
			require.NoError(t, err)
			t.Cleanup(func() { _ = session.Close() })

			session.Autopsy(t.Context(), &v1.Scope{}, nil, []string{"a failure"})

			assert.Contains(t, out.String(), "`"+verb+"` has nothing to act on at the autopsy")
			assert.Contains(t, out.String(), "list what this run can name", "the session was still reading after the refusal, so help answered")
		})
	}
}

// longList is a CEL list literal of n integers, longer than one page of
// children for any n above [flowdebug.DefaultInspectLimit].
func longList(n int) string {
	items := make([]string, n)
	for i := range items {
		items[i] = strconv.Itoa(i)
	}

	return "[" + strings.Join(items, ", ") + "]"
}

// TestExpandPagesThroughAListLongerThanOnePage: the page that was cut off says
// how to ask for the rest, and asking for it lists exactly the rest, at the
// prompt and at the driver alike.
func TestExpandPagesThroughAListLongerThanOnePage(t *testing.T) {
	t.Parallel()

	n := flowdebug.DefaultInspectLimit + 25
	list := longList(n)
	first := strconv.Itoa(flowdebug.DefaultInspectLimit)

	out, _, err := runDebugged(t, "expand "+list+"\nexpand "+list+" from "+first+"\nexpand "+list+" from 999\ncontinue\n",
		flowdebug.Options{})
	require.NoError(t, err)

	assert.Contains(t, out, "… and 25 more (repeat the expand with `from "+first+"`)",
		"a cut-off page does not say how to read the rest:\n"+out)
	assert.Contains(t, out, "debug> "+first+"  int  "+first, "the next page does not start at the child the first left off")
	assert.Contains(t, out, strconv.Itoa(n-1)+"  int  "+strconv.Itoa(n-1), "the last child was never listed")
	assert.Contains(t, out, fmt.Sprintf("has %d children; none from 999", n),
		"a page past the end is neither empty-handed nor silent")
	assert.Equal(t, 1, strings.Count(out, "… and"), "the last page still claims more follows")
}

// TestExpandOnlyReadsAFromSuffixThatIsANumber: " from " is part of an
// expression until a whole number follows it, so the evaluator keeps judging
// anything else.
func TestExpandOnlyReadsAFromSuffixThatIsANumber(t *testing.T) {
	t.Parallel()

	out, _, err := runDebugged(t, "expand [1, 2] from x\nexpand [1, 2] from -1\ncontinue\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.NotContains(t, out, "none from", "a non-number or negative offset paged instead of reaching the evaluator:\n"+out)
	assert.NotContains(t, out, "0  int  1", "the expression was read as a list")
}
