package flowdebug_test

import (
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
			assert.NotContains(t, strings.Join(session.Script(), "\n"), verb, "a refused command is not recorded")
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
