package flowdebug_test

import (
	"fmt"
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

		text, value, err := s.Evaluate(t.Context(), "steps.deploy.token")
		if assert.NoError(t, err) {
			assert.NotContains(t, text, "hunter2",
				"a secret the prompt redacts came back in the clear to a caller that "+
					"asked for it as a value instead of typing `inspect`")
			assert.Contains(t, text, "redacted")

			// The half the first draft of this test missed, and the half that
			// matters most: an adapter expanding a variable in a pane reads
			// the *value*, not the rendering. Asserting only on the text is
			// perfectly satisfied by a method that redacts what it prints and
			// hands the secret out beside it (Codex, #1120).
			//
			// This session has a text redactor and no value redactor, which is
			// a reachable configuration — the two are installed independently
			// — and it is the one that must fail closed: told there is
			// something to withhold, with no way to withhold it structurally.
			assert.Nil(t, value,
				"a session that cannot redact a structured value handed one out anyway")
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

// TestEvaluateRefusesAnExpressionTooLargeToParse is the bound a console reader
// owes and a caller reaching past it does not.
//
// `MaxCommandBytes` is enforced on the way in by whatever reads a typed line,
// so the text path was bounded by the surface it arrives on — and this method
// arrives on no surface at all. `DefaultCostLimit` bounds *evaluation*, which
// happens after a parse, so an expression large enough to be a problem is one
// the cost limit never sees (Codex, #1120).
func TestEvaluateRefusesAnExpressionTooLargeToParse(t *testing.T) {
	t.Parallel()

	var out strings.Builder

	done := make(chan struct{})
	console := &probing{steps: []string{"quit"}, before: func(s *flowdebug.Session) {
		defer close(done)

		huge := strings.Repeat("a+", flowdebug.MaxCommandBytes)

		_, _, err := s.Evaluate(t.Context(), huge)
		require.Error(t, err, "an expression past the bound reached the parser")
		assert.ErrorIs(t, err, flowdebug.ErrExpressionTooLarge,
			"the refusal is indistinguishable from an expression that did not compile")

		// And the bound is a bound rather than a wall: an ordinary expression
		// still answers.
		_, _, err = s.Evaluate(t.Context(), "1 + 1")
		assert.NoError(t, err)
	}}

	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	session.Autopsy(t.Context(), v1.NewScope(v1.CurrentProfile, nil), nil, []string{"a failure"})
	<-done
}

// TestScopeNamesTheInputsEvaluateCanResolve keeps the value surface as wide as
// the run.
//
// `inputs.<name>` resolves through the same activation `Evaluate` uses and has
// been offered by completion since it learned the root, so a collector that
// omitted it made the value surface narrower than both — which is the failure
// its own comment warns about (Codex, #1120).
func TestScopeNamesTheInputsEvaluateCanResolve(t *testing.T) {
	t.Parallel()

	var out strings.Builder

	scope := v1.NewScope(v1.CurrentProfile, nil)
	scope.Inputs = map[string]*v1.Value{
		"environment": v1.NewLiteral("staging"),
		"release":     v1.NewLiteral("v2.1.0"),
	}

	done := make(chan struct{})
	console := &probing{steps: []string{"quit"}, before: func(s *flowdebug.Session) {
		defer close(done)

		groups, err := s.Scope()
		require.NoError(t, err)

		assert.Equal(t, []string{"environment", "release"}, namesOf(groups, "inputs"),
			"the arguments the run was started with are resolvable and unlistable")

		// The join that makes it a real claim rather than a listing: every name
		// the scope offers is one Evaluate can actually answer.
		for _, name := range namesOf(groups, "inputs") {
			_, _, evalErr := s.Evaluate(t.Context(), "inputs."+name)
			assert.NoError(t, evalErr, "scope named %q and Evaluate cannot resolve it", name)
		}
	}}

	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	session.Autopsy(t.Context(), scope, nil, []string{"a failure"})
	<-done
}

// TestAValueRedactorIsWhatMakesTheStructuredAnswerAvailable is the other arm of
// the same decision.
//
// Failing closed is only defensible if the open path exists: a session that
// *can* redact a structured value hands back the redacted one, so an adapter
// on a properly configured deployment gets what it needs to expand a variable.
// Without this, "withhold when unsure" would be indistinguishable from "never
// return a value at all".
func TestAValueRedactorIsWhatMakesTheStructuredAnswerAvailable(t *testing.T) {
	t.Parallel()

	var out strings.Builder

	done := make(chan struct{})
	console := &probing{steps: []string{"quit"}, before: func(s *flowdebug.Session) {
		defer close(done)

		text, value, err := s.Evaluate(t.Context(), "steps.deploy.token")
		require.NoError(t, err)

		assert.NotContains(t, text, "hunter2")
		require.NotNil(t, value,
			"a session that can redact a structured value withheld it anyway, so an "+
				"adapter can never expand a variable")
		assert.NotContains(t, fmt.Sprintf("%v", value.Value()), "hunter2",
			"the structured value carried the secret")
		assert.Contains(t, fmt.Sprintf("%v", value.Value()), "redacted")
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
	session.SetValueRedactor(func(value any) any {
		if text, ok := value.(string); ok && strings.Contains(text, "hunter2") {
			return "[redacted]"
		}

		return value
	})

	session.Autopsy(t.Context(), &v1.Scope{
		Outputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"deploy": {NamedValues: map[string]*v1.Value{"token": v1.NewLiteral("hunter2")}},
		}},
	}, nil, []string{"a failure"})
	<-done
}

// TestAStructuredValueIsRedactedThroughItsContainers is the hole a scalar-only
// test cannot see, and it is why the value surface shares the printing path's
// conversion instead of having one of its own.
//
// CEL hands back its own backing representation from [ref.Val.Value] —
// `map[ref.Val]ref.Val` for a map, `[]ref.Val` for a list — and a redactor
// written against native Go walks neither. `flowtest`'s switches on
// `map[string]any` and `[]any` (`flowtest/stub.go:940-957`), so handed CEL's
// map it matches nothing, returns the container unchanged, and every secret
// inside it travels. A test that only ever asks about a string passes the
// whole way through that (Codex, #1120).
func TestAStructuredValueIsRedactedThroughItsContainers(t *testing.T) {
	t.Parallel()

	var out strings.Builder

	done := make(chan struct{})
	console := &probing{steps: []string{"quit"}, before: func(s *flowdebug.Session) {
		defer close(done)

		for _, expression := range []string{"steps.deploy.config", "steps.deploy.hosts"} {
			text, value, err := s.Evaluate(t.Context(), expression)
			if !assert.NoError(t, err, expression) {
				continue
			}

			assert.NotContains(t, text, "hunter2",
				"%s: a secret inside a container came back in the rendered answer", expression)
			require.NotNil(t, value, "%s: the structured answer was withheld", expression)
			assert.NotContains(t, fmt.Sprintf("%v", value.Value()), "hunter2",
				"%s: the container was handed back holding the secret, because the redactor "+
					"never saw a shape it could walk", expression)
			assert.Contains(t, fmt.Sprintf("%v", value.Value()), "redacted", expression)
		}

		// And the walk is a walk rather than a blanket withholding: what was
		// not sensitive is still readable, which is the whole reason a caller
		// asks for the value rather than the rendering.
		_, value, err := s.Evaluate(t.Context(), "steps.deploy.config.region")
		if assert.NoError(t, err) && assert.NotNil(t, value) {
			assert.Equal(t, "us-east-1", value.Value())
		}
	}}

	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	// Deliberately the shape `flowtest` installs: equality at the leaves, and a
	// recursion that knows only about native Go containers. A redactor that
	// happened to handle CEL's own types would test this file's fix against a
	// redactor no caller writes.
	var walk func(any) any
	walk = func(value any) any {
		if text, ok := value.(string); ok && text == "hunter2" {
			return "[redacted]"
		}
		switch container := value.(type) {
		case map[string]any:
			redacted := make(map[string]any, len(container))
			for name, element := range container {
				redacted[name] = walk(element)
			}

			return redacted
		case []any:
			redacted := make([]any, len(container))
			for i, element := range container {
				redacted[i] = walk(element)
			}

			return redacted
		default:
			return value
		}
	}
	session.SetValueRedactor(walk)

	session.Autopsy(t.Context(), &v1.Scope{
		Outputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"deploy": {NamedValues: map[string]*v1.Value{
				"config": v1.NewLiteralMap(map[string]any{
					"token":  "hunter2",
					"region": "us-east-1",
				}),
				"hosts": v1.NewLiteralList("web-1", "hunter2"),
			}},
		}},
	}, nil, []string{"a failure"})
	<-done
}

// TestAnAnswerUsesTheWithholdingThePauseBeganUnder pins the redactors to the
// pause rather than to the session.
//
// `flow test` clears both the moment [flowdebug.Session.Autopsy] returns
// (`flowtest/run.go:768,790`), and a caller asking from its own goroutine
// snapshots the pause and then spends real time evaluating — so redactors read
// at the end of that work can be the cleared ones, and the answer comes back
// carrying what they existed to withhold (Codex, #1120).
//
// The window itself is not observable from outside, so this clears them before
// the call instead, which covers it: an evaluation entered *after* the clear
// must still withhold, and one that entered before it holds a snapshot taken
// earlier still. The pause is what decides, and the pause has not ended.
func TestAnAnswerUsesTheWithholdingThePauseBeganUnder(t *testing.T) {
	t.Parallel()

	var out strings.Builder

	done := make(chan struct{})
	console := &probing{steps: []string{"quit"}, before: func(s *flowdebug.Session) {
		defer close(done)

		// Exactly what leaving the autopsy does, arriving while the autopsy is
		// still held.
		s.SetRedactor(nil)
		s.SetValueRedactor(nil)

		text, value, err := s.Evaluate(t.Context(), "steps.deploy.token")
		require.NoError(t, err)

		assert.NotContains(t, text, "hunter2",
			"the answer was rendered with the session's redactors read after the fact, "+
				"and by then there were none")
		require.NotNil(t, value)
		assert.NotContains(t, fmt.Sprintf("%v", value.Value()), "hunter2",
			"the structured answer outlived the withholding that was in force when the "+
				"run was paused")
	}}

	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	session.SetRedactor(func(text string) string {
		return strings.ReplaceAll(text, "hunter2", "[redacted]")
	})
	session.SetValueRedactor(func(value any) any {
		if text, ok := value.(string); ok && text == "hunter2" {
			return "[redacted]"
		}

		return value
	})

	session.Autopsy(t.Context(), &v1.Scope{
		Outputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"deploy": {NamedValues: map[string]*v1.Value{"token": v1.NewLiteral("hunter2")}},
		}},
	}, nil, []string{"a failure"})
	<-done
}
