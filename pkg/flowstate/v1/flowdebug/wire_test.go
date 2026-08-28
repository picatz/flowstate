package flowdebug_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The session's answers as the schema spells them (#928, stage 1 of the
// durable-debug arc).
//
// What these hold to is that the messages say what the session says, including
// where the session says it does not know: a wire surface that guessed an
// attribution the local pane refuses to guess would be a debugger pointing at
// the wrong step, over a network, durably.

// twiceCalled invokes one callee from two `call:` steps, which is the shape
// where a boundary's (workflow, step id) pair names two rows and can choose
// neither.
func twiceCalled() *v1.Workflow {
	callee := func() *v1.Workflow {
		return &v1.Workflow{Name: "inner", Steps: []*v1.Node{markStep("build")}}
	}

	return &v1.Workflow{Name: "outer", Steps: []*v1.Node{
		{Id: "first_call", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee()}}},
		{Id: "second_call", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee()}}},
	}}
}

// twiceCalledSteps is what the walk produces for it: four rows, two of them
// `inner.build` against their own declaration.
func twiceCalledSteps() []flowdebug.Step {
	return []flowdebug.Step{
		{Workflow: "outer", Declaration: 0, ID: "first_call"},
		{Workflow: "inner", Declaration: 1, Via: "first_call", ID: "build"},
		{Workflow: "outer", Declaration: 0, ID: "second_call"},
		{Workflow: "inner", Declaration: 2, Via: "second_call", ID: "build"},
	}
}

// onceCalled is the same shape with one call site, so the callee's row is the
// only one a boundary can mean.
func onceCalled() *v1.Workflow {
	return &v1.Workflow{Name: "outer", Steps: []*v1.Node{
		{Id: "only_call", Kind: &v1.Node_Call{Call: &v1.Call{
			Workflow: &v1.Workflow{Name: "inner", Steps: []*v1.Node{markStep("build")}},
		}}},
	}}
}

func onceCalledSteps() []flowdebug.Step {
	return []flowdebug.Step{
		{Workflow: "outer", Declaration: 0, ID: "only_call"},
		{Workflow: "inner", Declaration: 1, Via: "only_call", ID: "build"},
	}
}

// wireStop is one stop's messages, taken where a console would paint a frame.
type wireStop struct {
	position *v1.DebugPosition
	window   *v1.DebugStepWindow
}

// wireStops drives a workflow under a scripted session and returns the messages
// built at every stop.
func wireStops(t *testing.T, workflow *v1.Workflow, script string, inventory []flowdebug.Step, offset, limit int) []wireStop {
	t.Helper()

	var (
		session *flowdebug.Session
		stops   []wireStop
	)

	opts := flowdebug.Options{
		In:    strings.NewReader(script),
		Out:   &strings.Builder{},
		Steps: inventory,
		Emit: func(_ string, tone flowdebug.Tone) {
			if tone != flowdebug.ToneBreak {
				return
			}
			position, paused := session.PositionProto()
			if !paused {
				return
			}
			stops = append(stops, wireStop{position: position, window: session.StepWindowProto(offset, limit)})
		},
	}

	var err error
	session, err = flowdebug.New(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, &ranSteps{}))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	_, runErr := v1.Run(ctx, workflow)
	require.NoError(t, runErr)
	require.NotEmpty(t, stops, "the run never stopped, so no message was built")

	return stops
}

// TestThePositionNamesTheDeclarationItResolves is the invocation-identity
// position, in both directions.
//
// The positive direction first, because the refusal below proves nothing until
// the field is known to be fillable at all: with one call site the position
// resolves to exactly one row and says which.
func TestThePositionNamesTheDeclarationItResolves(t *testing.T) {
	t.Parallel()

	stops := wireStops(t, onceCalled(), "step\nstep\ncontinue\n", onceCalledSteps(), 0, -1)
	require.Len(t, stops, 2, "the run should have offered the call and the callee's step")

	callee := stops[1]
	assert.Equal(t, "inner", callee.position.GetWorkflow())
	assert.Equal(t, "build", callee.position.GetStepId())
	require.NotNil(t, callee.position.Declaration,
		"the position resolves to exactly one row and did not say which, so a paging client cannot find it")
	assert.Equal(t, int32(1), callee.position.GetDeclaration(),
		"the position named a declaration other than the row it resolves to")

	// The root's own position names the root declaration, which is zero — the
	// value an absent field would be read as, which is why absence is spelled
	// separately.
	require.NotNil(t, stops[0].position.Declaration)
	assert.Equal(t, int32(0), stops[0].position.GetDeclaration())
}

// TestThePositionSaysNothingItCannotAttribute is the refusal.
//
// One callee invoked twice gives a boundary a (workflow, step id) pair that
// names two rows. The wire says not-attributable — an absent declaration —
// exactly as the local pane marks no row, rather than naming the first match.
func TestThePositionSaysNothingItCannotAttribute(t *testing.T) {
	t.Parallel()

	stops := wireStops(t, twiceCalled(), "step\nstep\nstep\nstep\ncontinue\n", twiceCalledSteps(), 0, -1)
	require.Len(t, stops, 4, "the run should have offered four boundaries")

	for _, i := range []int{1, 3} {
		at := stops[i]
		assert.Equal(t, "inner", at.position.GetWorkflow())
		assert.Nil(t, at.position.Declaration,
			"stop %d named one of two indistinguishable invocations", i)
		assert.Nil(t, at.window.Held,
			"stop %d marked a row the session cannot tell from its twin", i)
	}

	// And the window says how many rows carry an id it cannot attribute, so a
	// renderer can say so rather than drawing a silent gap.
	assert.Equal(t, int32(2), stops[3].window.GetUnattributed())
	for _, i := range []int{1, 3} {
		assert.Equal(t, v1.DebugStepState_DEBUG_STEP_STATE_PENDING, stops[3].window.GetSteps()[i].GetState(),
			"row %d claimed an outcome that names two invocations", i)
	}
}

// TestTheWindowMarksTheHeldRowOnlyWhereItHoldsIt is the pair of absences the
// schema separates and the pane cannot.
//
// A pane always windows around the row it is holding, so "outside the window"
// and "cannot attribute" never differ for it. A client paging the list freely
// does differ, and reading one as the other would tell it a resolvable position
// is unresolvable.
func TestTheWindowMarksTheHeldRowOnlyWhereItHoldsIt(t *testing.T) {
	t.Parallel()

	inventory := declared("wide", "a", "b", "c", "d", "e")
	workflow := &v1.Workflow{Name: "wide", Steps: []*v1.Node{
		markStep("a"), markStep("b"), markStep("c"), markStep("d"), markStep("e"),
	}}

	// Held at `c`, with a window that reaches it.
	reaching := wireStops(t, workflow, "step\nstep\nstep\ncontinue\n", inventory, 1, 3)
	require.Len(t, reaching, 4, "the run should have offered a boundary before each of the first four steps")

	third := reaching[2]
	require.NotNil(t, third.window.Held, "the window holds the row and did not mark it")
	assert.Equal(t, int32(1), third.window.GetHeld(), "the mark is not at the row's index inside the window")
	assert.Equal(t, third.position.GetStepId(), third.window.GetSteps()[third.window.GetHeld()].GetStepId(),
		"the marked row is not the step the position names")

	// The same stop, through a window that does not reach it. The row is real
	// and the position still says which declaration it is; only the mark is
	// absent, and that is the distinction.
	missing := wireStops(t, workflow, "step\nstep\nstep\ncontinue\n", inventory, 3, 2)
	require.Len(t, missing, 4)

	assert.Nil(t, missing[2].window.Held,
		"a window that does not reach the held row marked one of the rows it does hold")
	require.NotNil(t, missing[2].position.Declaration,
		"a position outside the window stopped saying which declaration it is, so the two absences are one")
}

// TestTheWindowIsTheSessionsOwnAnswer pins the window's own facts against the
// accessor they are derived from, which is the drift a bridge can produce.
func TestTheWindowIsTheSessionsOwnAnswer(t *testing.T) {
	t.Parallel()

	inventory := declared("wide", "a", "b", "c", "d", "e")
	workflow := &v1.Workflow{Name: "wide", Steps: []*v1.Node{
		markStep("a"), markStep("b"), markStep("c"), markStep("d"), markStep("e"),
	}}

	stops := wireStops(t, workflow, "step\nstep\ncontinue\n", inventory, 1, 3)
	require.NotEmpty(t, stops)

	window := stops[1].window
	assert.Equal(t, int32(1), window.GetOffset(), "the window reports an offset other than the clamped one")
	assert.Equal(t, int32(5), window.GetTotal(), "the window reports a total other than the whole list's length")
	assert.Len(t, window.GetSteps(), 3, "the window carries a number of rows that is not the number asked for")
	assert.Equal(t, []string{"b", "c", "d"}, []string{
		window.GetSteps()[0].GetStepId(), window.GetSteps()[1].GetStepId(), window.GetSteps()[2].GetStepId(),
	}, "the window starts somewhere other than its offset")
	assert.False(t, window.GetTruncated())
}

// pausedAt holds a session at an autopsy with scope, and hands it to ask.
//
// The autopsy is the cheapest real pause with a scope in it, and it is a pause
// the wire has to describe as much as a breakpoint is.
func pausedAt(t *testing.T, scope *v1.Scope, prepare func(*flowdebug.Session), ask func(*flowdebug.Session)) {
	t.Helper()

	done := make(chan struct{})
	console := &probing{steps: []string{"quit"}, before: func(s *flowdebug.Session) {
		defer close(done)
		ask(s)
	}}

	session, err := flowdebug.New(flowdebug.Options{Console: console, Out: &strings.Builder{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	console.session = session

	if prepare != nil {
		prepare(session)
	}

	session.Autopsy(t.Context(), scope, nil, []string{"a failure"})
	<-done
}

// secretScope is a scope holding one step output that must not travel.
func secretScope() *v1.Scope {
	return &v1.Scope{
		Outputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"deploy": {NamedValues: map[string]*v1.Value{"token": v1.NewLiteral("hunter2")}},
		}},
	}
}

// containers are the shapes a value leaks through when it is printed rather
// than returned: directly, inside a struct, and inside a slice of those.
//
// CLAUDE.md's containment list, because a redacting method protects a value
// printed directly and does nothing for one reached through an unexported
// field — and a wire message holding a rendered scope is exactly where such a
// regression would become durable.
func containers(scope *v1.DebugScope) []string {
	type holder struct {
		Scope *v1.DebugScope
		Note  string
	}

	held := holder{Scope: scope, Note: "a struct holding it"}
	slice := []holder{held}

	var printed []string
	for _, value := range []any{scope, held, slice} {
		printed = append(printed,
			fmt.Sprintf("%v", value),
			fmt.Sprintf("%+v", value),
			fmt.Sprintf("%#v", value),
			// %s on a message that has a String method is the verb an
			// operator's careless log line uses, which is why it is one of the
			// shapes. It carries no `//lint:ignore` because the loop hands
			// `any` to Sprintf and staticcheck's S1025 cannot see the concrete
			// type through it — a directive here would be one that matches
			// nothing, which the analyser reports in its own right.
			fmt.Sprintf("%s", value),
		)
	}

	return printed
}

// TestTheScopeMessageCarriesOnlyWhatWasRedacted is the containment claim, in
// both directions.
//
// The positive direction is not decoration. An assertion that a secret is
// absent from a message is worth nothing until the secret is known to be
// something the message could have carried — so the same scope is rendered
// with no redactor first, and the value is there.
func TestTheScopeMessageCarriesOnlyWhatWasRedacted(t *testing.T) {
	t.Parallel()

	// Could have carried it.
	pausedAt(t, secretScope(), nil, func(s *flowdebug.Session) {
		scope, err := s.ScopeProto(t.Context(), -1)
		require.NoError(t, err)

		var found bool
		for _, text := range containers(scope) {
			found = found || strings.Contains(text, "hunter2")
		}
		require.True(t, found,
			"the unredacted message does not carry the value either, so the assertion below is about nothing")
	})

	// Does not carry it.
	pausedAt(t, secretScope(), func(s *flowdebug.Session) {
		s.SetRedactor(func(text string) string {
			return strings.ReplaceAll(text, "hunter2", "[redacted]")
		})
		s.SetValueRedactor(func(value any) any {
			if text, ok := value.(string); ok && text == "hunter2" {
				return "[redacted]"
			}

			return value
		})
	}, func(s *flowdebug.Session) {
		scope, err := s.ScopeProto(t.Context(), -1)
		require.NoError(t, err)

		for i, text := range containers(scope) {
			assert.NotContains(t, text, "hunter2",
				"containment shape %d printed the value the session was withholding", i)
		}
		assert.Contains(t, strings.Join(containers(scope), "\n"), "[redacted]",
			"nothing was withheld at all, so the message never reached the value it was meant to redact")
	})
}

// TestTheScopeBudgetIsTheCallersAndTheTotalIsNot pins the three answers the
// limit produces.
//
// A zero limit is not an empty scope: it is the names with nothing resolved,
// which is what a debug adapter's `scopes` request wants before anybody has
// expanded a pane. The totals do not move with the budget, because a total that
// reported the budget back would make every elision a lie.
func TestTheScopeBudgetIsTheCallersAndTheTotalIsNot(t *testing.T) {
	t.Parallel()

	pausedAt(t, secretScope(), nil, func(s *flowdebug.Session) {
		names, err := s.Scope()
		require.NoError(t, err)

		total := 0
		for _, group := range names {
			total += len(group.Names)
		}
		require.Positive(t, total, "the scope holds no names, so every claim below is vacuous")

		none, err := s.ScopeProto(t.Context(), 0)
		require.NoError(t, err)
		assert.Equal(t, int32(total), none.GetTotal(),
			"a zero budget reported fewer names than the run can reach")
		assert.Equal(t, total, countBindings(none),
			"a zero budget dropped names rather than leaving their values unresolved")
		for _, binding := range flatten(none) {
			assert.Nil(t, binding.GetAnswer(),
				"a zero budget resolved %q anyway", binding.GetExpression())
		}

		all, err := s.ScopeProto(t.Context(), -1)
		require.NoError(t, err)
		assert.Equal(t, int32(total), all.GetTotal())
		for _, binding := range flatten(all) {
			assert.NotNil(t, binding.GetAnswer(),
				"a negative budget left %q unresolved", binding.GetExpression())
		}

		one, err := s.ScopeProto(t.Context(), 1)
		require.NoError(t, err)
		assert.Equal(t, int32(total), one.GetTotal(),
			"a budget of one reported the budget back as the scope's size")
		assert.Equal(t, total, countBindings(one),
			"a budget of one dropped the names it did not resolve")

		resolved := 0
		for _, binding := range flatten(one) {
			if binding.GetAnswer() != nil {
				resolved++
			}
		}
		assert.Equal(t, 1, resolved, "a budget of one resolved %d values", resolved)
	})
}

// flatten is every binding in a scope, in the order the groups list them.
func flatten(scope *v1.DebugScope) []*v1.DebugBinding {
	var bindings []*v1.DebugBinding
	for _, group := range scope.GetGroups() {
		bindings = append(bindings, group.GetBindings()...)
	}

	return bindings
}

func countBindings(scope *v1.DebugScope) int { return len(flatten(scope)) }

// TestTheScopeCarriesTheExpressionAndNotJustTheName pins the join the producer
// does, because a consumer redoing it is the switch over group names that two
// renderers already kept privately.
func TestTheScopeCarriesTheExpressionAndNotJustTheName(t *testing.T) {
	t.Parallel()

	pausedAt(t, secretScope(), nil, func(s *flowdebug.Session) {
		scope, err := s.ScopeProto(t.Context(), -1)
		require.NoError(t, err)

		groups := scope.GetGroups()
		require.NotEmpty(t, groups, "the scope has no groups, so the claim below is vacuous")

		var rooted, bare int
		for _, group := range groups {
			for _, binding := range group.GetBindings() {
				if group.GetRoot() == "" {
					bare++
					assert.Equal(t, binding.GetName(), binding.GetExpression(),
						"a bare binding was given a root it does not hang from")

					continue
				}
				rooted++
				assert.Equal(t, group.GetRoot()+"."+binding.GetName(), binding.GetExpression(),
					"a rooted binding's expression is not what would be typed to ask for it again")
			}
		}
		assert.Positive(t, rooted+bare, "no binding was checked, so this test asserts nothing")
	})
}

// TestASessionReportsItselfLocal is the one fact this package's sessions can
// state about who is debugging what.
func TestASessionReportsItselfLocal(t *testing.T) {
	t.Parallel()

	session, err := flowdebug.New(flowdebug.Options{Out: &strings.Builder{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	facts := session.SessionProto()
	assert.True(t, facts.GetLocal(),
		"a local session did not say so, so a consumer cannot tell it from an unattested durable one")
	assert.Nil(t, facts.GetRun(), "a local run was given a durable address")
	assert.Nil(t, facts.GetAttachedBy(), "a session nothing attested named a caller")
	assert.Nil(t, facts.LeaseExpiresAt, "a local session was given a lease it does not have")
}

// TestAPositionIsNotAnsweredBetweenStops is the boolean, which is what keeps a
// consumer from drawing a pause the run has left.
func TestAPositionIsNotAnsweredBetweenStops(t *testing.T) {
	t.Parallel()

	session, err := flowdebug.New(flowdebug.Options{Out: &strings.Builder{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	position, paused := session.PositionProto()
	assert.False(t, paused, "a session holding nothing reported a position")
	assert.Nil(t, position)

	_, err = session.ScopeProto(t.Context(), -1)
	assert.ErrorIs(t, err, flowdebug.ErrNotPaused,
		"a scope was answered against a run that is not held")
}

// TestAnAutopsyPositionNamesNoDeclaration is the third state of the position.
//
// An autopsy is a real pause with no step to be at, so there is no row for it
// to be in — and a declaration there would place a finished run at a step.
func TestAnAutopsyPositionNamesNoDeclaration(t *testing.T) {
	t.Parallel()

	pausedAt(t, secretScope(), nil, func(s *flowdebug.Session) {
		position, paused := s.PositionProto()
		require.True(t, paused, "an autopsy is a pause and was not reported as one")
		assert.True(t, position.GetAutopsy())
		assert.Empty(t, position.GetStepId(), "an autopsy named a step the run is at")
		assert.Nil(t, position.Declaration, "an autopsy named a declaration it is not in")
	})
}

// TestACommandRoundTripsThroughTheWire is the pair of conversions, over every
// verb the schema has.
//
// A line goes out as a message and comes back as the same line, because the
// line is the artifact: a session records what it accepted and replays a run
// from it, so a wire command that rendered to something else would replay a
// different run.
func TestACommandRoundTripsThroughTheWire(t *testing.T) {
	t.Parallel()

	for _, line := range []string{
		"step",
		"continue",
		"until build",
		"break build",
		"break build if steps.build.ok == 'build'",
		"delete build",
		"breakpoints",
		"inspect steps.build.ok",
		"scope",
		"complete inspect steps.",
		"info",
		"quit",
		"help",
	} {
		t.Run(line, func(t *testing.T) {
			t.Parallel()

			command, ok := flowdebug.CommandProto(line)
			require.True(t, ok, "the wire does not understand a line the prompt does")

			back, err := flowdebug.CommandLine(command)
			require.NoError(t, err)
			assert.Equal(t, line, back, "a command did not render back to the line it came from")
		})
	}
}

// TestCompleteKeepsWhatTrimmingWouldEat is the one verb whose argument is not
// trimmed, and the reason.
//
// Trailing space is the thing that says the current word is empty. Trimming it
// tells a completer the cursor sits earlier than it does, and a console
// replacing the reported prefix then cuts into the word before the space.
func TestCompleteKeepsWhatTrimmingWouldEat(t *testing.T) {
	t.Parallel()

	command, ok := flowdebug.CommandProto("complete inspect steps. ")
	require.True(t, ok)
	assert.Equal(t, "inspect steps. ", command.GetArgument(),
		"the trailing space was trimmed, so a remote cursor moved left of where it is")

	// Every other verb *is* trimmed, which is what makes the exception an
	// exception rather than an accident.
	trimmed, ok := flowdebug.CommandProto("inspect steps.build.ok   ")
	require.True(t, ok)
	assert.Equal(t, "steps.build.ok", trimmed.GetArgument())
}

// TestTheWireRefusesWhatIsNotACommand covers the lines that are not commands
// and the messages that cannot be sent.
func TestTheWireRefusesWhatIsNotACommand(t *testing.T) {
	t.Parallel()

	t.Run("lines", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name string
			line string
			want v1.DebugCommandVerb
			ok   bool
		}{
			{name: "an alias resolves to its canonical verb", line: "s", want: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_STEP, ok: true},
			{name: "another alias", line: "p steps.build", want: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_INSPECT, ok: true},
			{name: "a bare newline is one step, as at the prompt", line: "", want: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_STEP, ok: true},
			{name: "a comment is not a command", line: "# reproducing #1186"},
			{name: "an indented comment is not either", line: "   # still a comment"},
			{name: "an unknown verb is refused rather than guessed", line: "sudo continue"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				command, ok := flowdebug.CommandProto(tc.line)
				require.Equal(t, tc.ok, ok)
				if tc.ok {
					assert.Equal(t, tc.want, command.GetVerb())

					return
				}
				assert.Nil(t, command)
			})
		}
	})

	t.Run("messages", func(t *testing.T) {
		t.Parallel()

		_, err := flowdebug.CommandLine(&v1.DebugCommand{})
		assert.Error(t, err, "a command with no verb rendered to a line anyway")

		_, err = flowdebug.CommandLine(&v1.DebugCommand{
			Verb:     v1.DebugCommandVerb_DEBUG_COMMAND_VERB_SCOPE,
			Argument: "steps",
		})
		assert.Error(t, err,
			"an argument on a verb that takes none was dropped, which runs a command the caller did not send")

		// A *missing* argument is not refused: `until` with nothing after it is
		// a line the prompt answers with a usage sentence, and a wire client
		// should meet that same answer rather than a different refusal here.
		line, err := flowdebug.CommandLine(&v1.DebugCommand{Verb: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_UNTIL})
		require.NoError(t, err)
		assert.Equal(t, "until", line)
	})
}

// TestEveryWireVerbReachesTheSession is the reachability claim: a vocabulary
// the session does not understand is scaffolding, however green its table is.
//
// Each verb is sent as a message, rendered to a line, and fed to a real
// session; what must not happen is the session answering "unknown command",
// which is what it says about a verb it does not have.
func TestEveryWireVerbReachesTheSession(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		verb     v1.DebugCommandVerb
		argument string
	}{
		{verb: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_BREAKPOINTS},
		{verb: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_SCOPE},
		{verb: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_INFO},
		{verb: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_HELP},
		{verb: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_INSPECT, argument: "1 + 1"},
		{verb: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_COMPLETE, argument: "sco"},
		{verb: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_BREAK, argument: "second"},
		{verb: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_DELETE, argument: "second"},
		{verb: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_UNTIL, argument: "second"},
		{verb: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_STEP},
		{verb: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_CONTINUE},
		{verb: v1.DebugCommandVerb_DEBUG_COMMAND_VERB_QUIT},
	} {
		t.Run(tc.verb.String(), func(t *testing.T) {
			t.Parallel()

			line, err := flowdebug.CommandLine(&v1.DebugCommand{Verb: tc.verb, Argument: tc.argument})
			require.NoError(t, err)

			out, _, _ := runDebugged(t, line+"\ncontinue\n", flowdebug.Options{
				Steps: declared("wire", "first", "second"),
			})
			assert.NotContains(t, out, "unknown command",
				"the session does not understand %v, which the wire advertises", tc.verb)
		})
	}
}
