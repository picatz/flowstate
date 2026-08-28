package flowdebug_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/celcomplete"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The step list a renderer draws, asked of the session rather than reassembled
// from what it printed.
//
// The property under test throughout is the one a pane depends on: a state is
// what the run *did*, arrived at through [v1.RunObserver], and it is the same
// reading the printed account makes. A pane and a transcript that disagreed
// about whether a step failed would be two answers to one question.

// statesWorkflow runs one step of each outcome a step list has a mark for: a
// step that works, one whose failure the run absorbs, one whose `if:` is false,
// and one that fails the run. `never` is declared after the failure so that
// something is left unreached — pending is a state, and a list that could not
// show it would be a list of history rather than of the run.
func statesWorkflow() *v1.Workflow {
	return &v1.Workflow{Name: "states", Steps: []*v1.Node{
		markStep("build"),
		{Id: "flaky", Kind: &v1.Node_Task{Task: &v1.Task{Name: "boom"}},
			Policy: &v1.StepPolicy{ContinueOnError: true, Retry: &v1.RetryPolicy{MaxAttempts: 1}}},
		{Id: "gated", Condition: v1.NewExpr("false"), Kind: &v1.Node_Value{Value: v1.NewExpr("1")}},
		{Id: "fatal", Kind: &v1.Node_Task{Task: &v1.Task{Name: "boom"}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{MaxAttempts: 1}}},
		markStep("never"),
	}}
}

// statesRegistry answers `mark` as the shared helper does and `boom` with a
// failure.
func statesRegistry(t *testing.T) *v1.Registry {
	t.Helper()

	registry := debugRegistry(t, &ranSteps{})
	require.NoError(t, registry.Register(v1.TaskDef{Name: "boom", Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
		return nil, errors.New("deliberate failure")
	}}))

	return registry
}

// runStates drives statesWorkflow to the end and hands back the step list as it
// stood at each stop, in order.
func runStates(t *testing.T, script string, opts flowdebug.Options) (stops [][]flowdebug.Step, out string) {
	t.Helper()

	var console strings.Builder
	opts.In = strings.NewReader(script)
	opts.Out = &console

	var session *flowdebug.Session

	// Read at the break line, which is the moment a pane is painted — see
	// cmd/flow/debugpanes.go. Reading it here rather than after the run is what
	// makes these tests about the list a reader sees rather than about the one
	// left over at the end.
	opts.Emit = func(text string, tone flowdebug.Tone) {
		_, _ = console.WriteString(text)
		if tone == flowdebug.ToneBreak {
			stops = append(stops, allSteps(session))
		}
	}

	session, err := flowdebug.New(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	ctx := v1.NewContextWithRegistry(t.Context(), statesRegistry(t))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	_, _ = v1.Run(ctx, statesWorkflow())

	return stops, console.String()
}

// declared is an inventory of one workflow's steps, which is what a caller
// holding the file hands over.
func declared(workflow string, ids ...string) []flowdebug.Step {
	steps := make([]flowdebug.Step, 0, len(ids))
	for _, id := range ids {
		steps = append(steps, flowdebug.Step{Workflow: workflow, ID: id})
	}

	return steps
}

// allSteps is the whole list through the windowed accessor, for the tests whose
// claim is about the list rather than about the window.
func allSteps(session *flowdebug.Session) []flowdebug.Step {
	return session.Steps(0, -1).Steps
}

// stateOf is one step's state out of a list, and whether the list held it.
func stateOf(steps []flowdebug.Step, id string) (flowdebug.StepState, bool) {
	for _, step := range steps {
		if step.ID == id {
			return step.State, true
		}
	}

	return flowdebug.StepPending, false
}

// idsOf is the list's ids in the order it gave them.
func idsOf(steps []flowdebug.Step) []string {
	ids := make([]string, 0, len(steps))
	for _, step := range steps {
		ids = append(ids, step.ID)
	}

	return ids
}

// TestTheStepListReportsWhatEachStepDid is the surface, end to end.
//
// One assertion per state, at the stop where that state has just become true,
// because a list read only at the end cannot tell a step that is pending from
// one that ran while nobody was looking.
func TestTheStepListReportsWhatEachStepDid(t *testing.T) {
	t.Parallel()

	// A stop at every boundary: build, flaky, fatal. `gated` is skipped by its
	// `if:` and never offered one, which is the case that can only arrive
	// through StepSkipped.
	stops, _ := runStates(t, "step\nstep\nstep\n", flowdebug.Options{
		Steps: declared("states", "build", "flaky", "gated", "fatal", "never"),
	})
	require.Len(t, stops, 3, "the run should have offered three boundaries")

	// At the first stop the run is held before `build` and nothing has
	// finished: the step being held is running, and everything after it is
	// pending.
	first := stops[0]
	assert.Equal(t, []string{"build", "flaky", "gated", "fatal", "never"}, idsOf(first),
		"the list is the workflow's order, not the order things happened in")

	state, held := stateOf(first, "build")
	require.True(t, held)
	assert.Equal(t, flowdebug.StepRunning, state, "the step the run is held before reads as running")

	state, held = stateOf(first, "fatal")
	require.True(t, held)
	assert.Equal(t, flowdebug.StepPending, state, "a step the run has not reached reads as pending")

	// By the last stop every earlier outcome has arrived.
	last := stops[len(stops)-1]

	for _, want := range []struct {
		id    string
		state flowdebug.StepState
	}{
		{"build", flowdebug.StepDone},
		{"flaky", flowdebug.StepTolerated},
		{"gated", flowdebug.StepSkipped},
		{"fatal", flowdebug.StepRunning},
		{"never", flowdebug.StepPending},
	} {
		state, held := stateOf(last, want.id)
		if assert.True(t, held, "%s is missing from the list entirely", want.id) {
			assert.Equal(t, want.state, state, "%s", want.id)
		}
	}

	// The distinction the pane's marks turn on, stated on its own: a run that
	// carried on is not a step that worked, and conflating the two would hide
	// the thing somebody most often opens a debugger to find.
	tolerated, _ := stateOf(last, "flaky")
	done, _ := stateOf(last, "build")
	assert.NotEqual(t, done, tolerated,
		"an absorbed failure and a success read as the same state")
}

// TestAFailedStepIsDistinguishedFromAToleratedOne runs to the end, where the
// fatal step's own outcome has arrived.
//
// Separate from the test above because it needs the run to be over, and the
// list at the last *stop* is by definition taken before the step it is stopped
// at has done anything.
func TestAFailedStepIsDistinguishedFromAToleratedOne(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:    strings.NewReader("continue\n"),
		Out:   &console,
		Steps: declared("states", "build", "flaky", "gated", "fatal", "never"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	ctx := v1.NewContextWithRegistry(t.Context(), statesRegistry(t))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	_, runErr := v1.Run(ctx, statesWorkflow())
	require.Error(t, runErr, "the fourth step fails the run")

	steps := allSteps(session)

	state, held := stateOf(steps, "fatal")
	require.True(t, held)
	assert.Equal(t, flowdebug.StepFailed, state)

	state, held = stateOf(steps, "flaky")
	require.True(t, held)
	assert.Equal(t, flowdebug.StepTolerated, state,
		"the absorbed failure was recorded as the fatal one")

	// And the printed account agrees, which is the property that keeps a pane
	// and a transcript from being two answers to one question.
	assert.Contains(t, console.String(), "gated skipped (`if:` was false)")
}

// TestAStepTheRunEntersAgainReadsAsRunning is the loop case, and the reason
// entering is a fact a callback states rather than one the id cache implies.
//
// A `for_each` body is entered once per iteration. Its outcome arrives after
// each one, so by the second arrival the session has already recorded it as
// done — and a row saying `ok` for the very step the prompt above it is holding
// would be the list disagreeing with the session.
func TestAStepTheRunEntersAgainReadsAsRunning(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &console,
		Steps: declared("looping", "body")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{},
	})

	// Two iterations of one step, driven the way the engine drives a loop body:
	// a boundary, then the outcome, then the same boundary again.
	finished := make(chan error, 1)
	go func() {
		for range 2 {
			if beforeErr := session.BeforeStep(t.Context(), markStep("body"), scope); beforeErr != nil {
				finished <- beforeErr

				return
			}
			session.StepFinished("body", nil, nil, false)
		}
		finished <- nil
	}()

	// First arrival: entered, nothing finished.
	_, err = session.WaitForPause(t.Context())
	require.NoError(t, err)

	state, held := stateOf(allSteps(session), "body")
	require.True(t, held)
	assert.Equal(t, flowdebug.StepRunning, state)

	// Second arrival, after the first iteration's outcome has been recorded.
	_, err = session.Step(t.Context())
	require.NoError(t, err)

	state, held = stateOf(allSteps(session), "body")
	require.True(t, held)
	assert.Equal(t, flowdebug.StepRunning, state,
		"a step the run is held at read as the outcome of its previous iteration")

	// And the id is listed once, not once per iteration.
	assert.Equal(t, []string{"body"}, idsOf(allSteps(session)))

	require.NoError(t, session.Control(t.Context(), "continue"))
	require.NoError(t, <-finished)

	// The last iteration's outcome still lands once the run leaves.
	state, held = stateOf(allSteps(session), "body")
	require.True(t, held)
	assert.Equal(t, flowdebug.StepDone, state)
}

// TestTheStepListFallsBackToArrivalOrder covers the caller that holds no
// workflow.
//
// [flowdebug.Options.Steps] is a caller's answer, and an embedder handed the
// [v1.Debugger] seam and nothing else honestly does not have one. What that
// caller gets is the run so far — in the order it happened, because a map has
// no order and a list drawn in map order is a different list every time.
func TestTheStepListFallsBackToArrivalOrder(t *testing.T) {
	t.Parallel()

	stops, _ := runStates(t, "step\nstep\nstep\n", flowdebug.Options{})
	require.NotEmpty(t, stops)

	// The last stop is before `fatal`, by which point the run has been through
	// build, flaky and gated in that order.
	assert.Equal(t, []string{"build", "flaky", "gated", "fatal"}, idsOf(stops[len(stops)-1]),
		"the fallback list is not the order the run reached the steps in")

	// And it is a *prefix* rather than the workflow: `never` is a step this
	// run will reach and no source available here knows about it yet.
	_, held := stateOf(stops[len(stops)-1], "never")
	assert.False(t, held, "a step nothing has seen was listed as though it had been")
}

// TestTheStepListSaysWhenItStoppedRecording is the bound, and the notice that
// makes it safe.
//
// The cache a state is written into is [Session.sawStep]'s, bounded because the
// number of ids a run produces is the workflow's rather than the session's. Past
// the bound a step's state reads as pending — which is indistinguishable, to a
// reader, from a step the run has not reached. So the list says so.
func TestTheStepListSaysWhenItStoppedRecording(t *testing.T) {
	t.Parallel()

	// One more id than the cache holds, so the last of them is refused.
	ids := make([]string, 0, celcomplete.MaxCandidates+1)
	steps := make([]*v1.Node, 0, celcomplete.MaxCandidates+1)
	for i := range celcomplete.MaxCandidates + 1 {
		id := fmt.Sprintf("s%04d", i)
		ids = append(ids, id)
		steps = append(steps, markStep(id))
	}

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:    strings.NewReader("continue\n"),
		Out:   &console,
		Steps: declared("many", ids...),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	assert.False(t, session.Steps(0, -1).Truncated,
		"a session that has watched nothing reported itself truncated")

	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, &ranSteps{}))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	_, runErr := v1.Run(ctx, &v1.Workflow{Name: "many", Steps: steps})
	require.NoError(t, runErr)

	list := allSteps(session)

	// Every declared id is still listed — the ids come from the caller and are
	// not what the cache bounds — and every one of them but the last carries
	// the state it earned.
	require.Len(t, list, len(ids))

	state, held := stateOf(list, ids[0])
	require.True(t, held)
	assert.Equal(t, flowdebug.StepDone, state)

	state, held = stateOf(list, ids[len(ids)-1])
	require.True(t, held)
	assert.Equal(t, flowdebug.StepPending, state,
		"the id past the bound should have no state recorded, which is what the notice is for")

	assert.True(t, session.Steps(0, -1).Truncated,
		"the session dropped a step's outcome and did not say so, which makes a real step "+
			"read as one the run never reached")
}

// TestTheStepListIsAnsweredBetweenStops is the property a pane depends on that
// the scope surface deliberately does not have.
//
// [Session.Scope] and [Session.Evaluate] answer against a scope that only
// exists while a run is held, and refuse otherwise. A step list is about the
// workflow and about what has happened, both of which stay true after a
// `continue` — and a pane that vanished the moment somebody resumed would be a
// pane nobody could read the result off.
func TestTheStepListIsAnsweredBetweenStops(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:    strings.NewReader("continue\n"),
		Out:   &console,
		Steps: declared("w", "build", "test"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	// Before anything runs: the workflow's steps, none of them anywhere.
	assert.Equal(t, []string{"build", "test"}, idsOf(allSteps(session)))

	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, &ranSteps{}))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	_, runErr := v1.Run(ctx, &v1.Workflow{Name: "w", Steps: []*v1.Node{markStep("build"), markStep("test")}})
	require.NoError(t, runErr)

	// And after the run is over, with nothing paused at all.
	_, paused := session.Paused()
	require.False(t, paused)

	_, scopeErr := session.Scope()
	assert.ErrorIs(t, scopeErr, flowdebug.ErrNotPaused,
		"the scope surface answered a session holding no run")

	after := allSteps(session)
	assert.Equal(t, []string{"build", "test"}, idsOf(after))
	for _, step := range after {
		assert.Equal(t, flowdebug.StepDone, step.State, "%s", step.ID)
	}
}

// TestScopeGroupsCarryTheRootTheirNamesHangFrom is the other half of the
// programmatic surface this slice needed.
//
// A renderer turning a name into a value has to know what to prefix it with,
// and two of them kept their own switch over the group names — `flowdap.rootOf`
// said in as many words that it was "the same fact read for a different
// renderer". The session holds that fact already, beside the `inspect steps.`
// pointer the prompt prints, so both are derived from one value.
func TestScopeGroupsCarryTheRootTheirNamesHangFrom(t *testing.T) {
	t.Parallel()

	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			"build": {NamedValues: map[string]*v1.Value{"artifact": v1.NewLiteral("web.tar.gz")}},
		},
	})
	scope.Vars = map[string]*v1.Value{"item": v1.NewLiteral(1)}
	scope.AmbientVars = map[string]*v1.Value{"region": v1.NewLiteral("eu")}
	scope.Inputs = map[string]*v1.Value{"version": v1.NewLiteral("1.2.3")}

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{Controlled: true, Out: &out})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	finished := make(chan error, 1)
	go func() { finished <- session.BeforeStep(t.Context(), markStep("deploy"), scope) }()

	_, err = session.WaitForPause(t.Context())
	require.NoError(t, err)

	groups, err := session.Scope()
	require.NoError(t, err)
	require.NotEmpty(t, groups, "a paused run named nothing at all")

	roots := map[string]string{}
	for _, group := range groups {
		roots[group.Group] = group.Root
	}

	// The rooted groups, and the two that are deliberately bare: `vars` here is
	// a loop's `as:` and a step's own `vars:`, which resolve under no root at
	// all, while the workflow's declared `vars:` are what `vars.` reaches.
	assert.Equal(t, "steps", roots["steps"])
	assert.Equal(t, "inputs", roots["inputs"])
	assert.Equal(t, "vars", roots["workflow vars"])
	assert.Equal(t, "run", roots["run"])
	assert.Equal(t, "trigger", roots["trigger"])
	assert.Equal(t, "", roots["vars"], "a bare binding was given a root it cannot be reached through")

	// And the root is the prefix that actually resolves, which is the claim a
	// renderer is making when it uses one.
	for _, group := range groups {
		for _, name := range group.Names {
			expression := name
			if group.Root != "" {
				expression = group.Root + "." + name
			}

			_, _, evalErr := session.Evaluate(t.Context(), expression)
			assert.NoError(t, evalErr, "%s.%s does not resolve through the root the group reported",
				group.Group, name)
		}
	}

	require.NoError(t, session.Control(t.Context(), "continue"))
	require.NoError(t, <-finished)
}
