package flowdebug

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestAPendingUntilIsJudgedAgainstTheNextProgram: a conditional `until` a case
// never reached is still pending when the next case's program arrives
// ([Session.Program]). It is judged against that program as a breakpoint is,
// and one the program refuses is dropped for `continue`, with a notice saying
// why, rather than evaluated and declined at every arrival (Codex, #2202). One
// the program admits stays.
func TestAPendingUntilIsJudgedAgainstTheNextProgram(t *testing.T) {
	t.Parallel()

	looping := &v1.Workflow{Name: "looping", Steps: []*v1.Node{
		{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewLiteralList(1, 2), Iterator: "n",
			Body: []*v1.Node{{Id: "body", Kind: &v1.Node_Value{Value: v1.NewExpr("n")}}},
		}}},
	}}
	flat := &v1.Workflow{Name: "flat", Steps: []*v1.Node{
		{Id: "body", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}},
	}}
	pend := func(t *testing.T, step, condition string) (*Session, *strings.Builder) {
		t.Helper()
		var printed strings.Builder
		session, err := New(Options{Emit: func(text string, _ Tone) { printed.WriteString(text) }})
		require.NoError(t, err)
		t.Cleanup(func() { _ = session.Close() })
		session.Program(looping)
		var compiled *v1.Value
		if condition != "" {
			var err error
			compiled, err = v1.CompileDebugCondition(condition, v1.CurrentProfile)
			require.NoError(t, err)
		}
		target, err := v1.ParseDebugTarget(step)
		require.NoError(t, err)
		session.resumeUntil(modeUntil, target, compiled, condition)

		return session, &printed
	}

	t.Run("refused", func(t *testing.T) {
		t.Parallel()

		session, printed := pend(t, "body", "n > 1")
		session.Program(flat)

		session.mu.Lock()
		mode, condition := session.mode, session.untilCondition
		session.mu.Unlock()
		assert.Equal(t, modeRun, mode, "an until the new program refuses is still pending")
		assert.Nil(t, condition)
		assert.Contains(t, printed.String(), "until body if n > 1 no longer applies to this program: condition: `n` is not bound")
		snapshot, err := session.Snapshot(t.Context())
		require.NoError(t, err)
		require.NotEmpty(t, snapshot.GetObservations())
		assert.Contains(t, snapshot.GetObservations()[len(snapshot.GetObservations())-1].GetText(), "until body if n > 1 no longer applies")
	})

	t.Run("refused, applied inside a callee", func(t *testing.T) {
		t.Parallel()

		// The hold the `until` was applied at withheld a callee's input, and
		// the condition quotes it: the notice withholds it too.
		const secret = "hunter2-callee-only-secret"
		session, printed := pend(t, "body", `n > 1 && "`+secret+`" != ""`)
		session.mu.Lock()
		session.untilSensitive = v1.SensitiveInputValues(map[string]*v1.Value{"k": v1.NewLiteral(secret)}, map[string]bool{"k": true})
		session.mu.Unlock()
		session.Program(flat)

		assert.Contains(t, printed.String(), "no longer applies to this program", "the notice was not said, so this proves nothing")
		assert.NotContains(t, printed.String(), secret)
		snapshot, err := session.Snapshot(t.Context())
		require.NoError(t, err)
		require.NotEmpty(t, snapshot.GetObservations())
		assert.NotContains(t, snapshot.GetObservations()[len(snapshot.GetObservations())-1].GetText(), secret)
	})

	t.Run("admitted", func(t *testing.T) {
		t.Parallel()

		session, printed := pend(t, "body", "steps.body != null")
		session.Program(flat)

		session.mu.Lock()
		mode := session.mode
		session.mu.Unlock()
		assert.Equal(t, modeUntil, mode, "an until the new program admits was dropped")
		assert.NotContains(t, printed.String(), "no longer applies")
	})
	t.Run("target gone", func(t *testing.T) {
		t.Parallel()

		session, printed := pend(t, "each", "")
		session.Program(flat)

		session.mu.Lock()
		mode := session.mode
		session.mu.Unlock()
		assert.Equal(t, modeRun, mode, "an until naming a step the new program lacks is still pending")
		assert.Contains(t, printed.String(), "until each no longer applies to this program")
	})
}
