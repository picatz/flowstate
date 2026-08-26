package flowdebug_test

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// Asking a paused session questions without typing at it.
//
// Every test here would previously have had to drive the prompt with a line of
// text and then match on what it printed — which is a test of the rendering,
// not of the answer, and is the only thing a debug adapter could have done
// either.

// TestAPausedSessionAnswersWithoutBeingTypedAt is the seam, end to end.
func TestAPausedSessionAnswersWithoutBeingTypedAt(t *testing.T) {
	t.Parallel()

	// `quit` leaves once the questions are asked; the answers are taken from
	// another goroutine while the autopsy holds, exactly as a console's
	// completer already does.
	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("quit\n"),
		Out: &out,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	// Nothing is paused yet, and that is a different answer from "nothing is
	// in scope".
	_, paused := session.Paused()
	assert.False(t, paused)

	_, _, err = session.Evaluate(t.Context(), "1 + 1")
	assert.ErrorIs(t, err, flowdebug.ErrNotPaused,
		"a question asked of a session holding no run was answered anyway")

	scope := &v1.Scope{Outputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"build": {NamedValues: map[string]*v1.Value{"artifact": v1.NewLiteral("web.tar.gz")}},
	}}}

	answers := make(chan struct{})
	console := &probing{steps: []string{"quit"}, before: func(s *flowdebug.Session) {
		defer close(answers)

		position, ok := s.Paused()
		if assert.True(t, ok, "a session holding a run reported that it was not paused") {
			assert.True(t, position.Autopsy)
		}

		text, value, evalErr := s.Evaluate(t.Context(), "steps.build.artifact")
		if assert.NoError(t, evalErr) {
			assert.Equal(t, `"web.tar.gz"`, text,
				"the rendered answer differs from what `inspect` prints")
			assert.NotNil(t, value, "the value's own shape was not handed back")
		}

		// An expression that does not compile is an ordinary answer, not a
		// session-ending one: somebody probing questions will ask some that do
		// not parse.
		_, _, evalErr = s.Evaluate(t.Context(), "steps.build.")
		assert.Error(t, evalErr)

		groups, scopeErr := s.Scope()
		if assert.NoError(t, scopeErr) {
			assert.Equal(t, []string{"build"}, namesOf(groups, "steps"),
				"the values behind `scope` do not name what the run can reach")
		}
	}}

	session2, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session2.Close() })
	console.session = session2

	session2.Autopsy(t.Context(), scope, nil, []string{"a failure"})
	<-answers

	// And the pause is over, so the same questions stop being answerable —
	// a session that kept the last scope alive would answer about a position
	// the run has left.
	_, stillPaused := session2.Paused()
	assert.False(t, stillPaused)
}

// TestTheValueSurfaceWithholdsWhatThePromptWithholds is the property that makes
// this safe to add.
//
// A caller reaching a session this way is no more entitled to a secret than one
// at a terminal: the front changes and the withholding does not. A redaction
// that applied only to the printed path would be a hole opened by the
// convenience of not typing.
func TestTheValueSurfaceWithholdsWhatThePromptWithholds(t *testing.T) {
	t.Parallel()

	var out strings.Builder

	answers := make(chan struct{})
	console := &probing{steps: []string{"quit"}, before: func(s *flowdebug.Session) {
		defer close(answers)

		text, _, err := s.Evaluate(t.Context(), "steps.deploy.token")
		if assert.NoError(t, err) {
			assert.NotContains(t, text, "hunter2",
				"a secret the prompt redacts came back in the clear to a caller that "+
					"asked for it as a value instead of typing `inspect`")
			assert.Contains(t, text, "redacted")
		}
	}}

	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	session.SetRedactor(func(text string) string {
		if strings.Contains(text, "hunter2") {
			return "[redacted]"
		}

		return text
	})

	session.Autopsy(t.Context(), &v1.Scope{
		Outputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"deploy": {NamedValues: map[string]*v1.Value{"token": v1.NewLiteral("hunter2")}},
		}},
	}, nil, []string{"a failure"})
	<-answers
}

// namesOf is one group's names, or nil.
func namesOf(groups []flowdebug.Names, group string) []string {
	for _, g := range groups {
		if g.Group == group {
			return g.Names
		}
	}

	return nil
}

// probing drives a prompt and runs a callback while the run is held, which is
// the only moment these questions have an answer.
type probing struct {
	session *flowdebug.Session
	steps   []string
	before  func(*flowdebug.Session)
	at      int
	ran     bool
}

func (a *probing) Prompt() (string, error) {
	if !a.ran {
		a.ran = true
		if a.before != nil {
			a.before(a.session)
		}
	}
	if a.at >= len(a.steps) {
		return "", io.EOF
	}
	line := a.steps[a.at]
	a.at++

	return line, nil
}
