package flowdebug_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// Where the paused position's workflow comes from, and that it comes from there
// on every configuration.
//
// It used to ride on [v1.TaskRuntime], which only exists where secrets or
// workload identity are configured — so the answer was empty on an ordinary
// `flow run local --debug` (`cmd/flow/secrets.go` returns the context untouched
// when neither is configured) and on every `flow test --debug`
// (`flowtest/secrets.go` builds a runtime whose `Step` is the zero value). A
// qualification that only works where a deployment happens to be configured is
// not a capability anybody can reach (Codex, #1186).
//
// The engine stamps it now — [v1.ExecutingWorkflowFromContext] — so these three
// configurations are one run as far as a boundary is concerned.

// TestTheBoundaryReportsTheWorkflowOnEveryConfiguration is the reachability
// claim, stated over the three runtime shapes this repository actually builds.
func TestTheBoundaryReportsTheWorkflowOnEveryConfiguration(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		install func(t *testing.T) (v1.TaskRuntime, bool)
	}{
		{
			// `flow run local --debug` with no secret providers and no
			// identity broker: `cmd/flow/secrets.go` hands the context back
			// untouched, so there is no runtime at all.
			name:    "no task runtime, as an unconfigured local run has",
			install: func(*testing.T) (v1.TaskRuntime, bool) { return v1.TaskRuntime{}, false },
		},
		{
			// `flow test --debug`: a runtime exists for the case's secret
			// bindings, and its Step is the zero value — it names no workflow.
			name: "a runtime whose Step is empty, as every `flow test` has",
			install: func(*testing.T) (v1.TaskRuntime, bool) {
				return v1.TaskRuntime{Identity: auth.WorkloadIdentity{Subject: "test"}}, true
			},
		},
		{
			// `flow run local` with secrets configured: the one shape that
			// used to work.
			name: "a runtime naming the workflow, as a configured local run has",
			install: func(*testing.T) (v1.TaskRuntime, bool) {
				return v1.TaskRuntime{Step: auth.StepRef{Workflow: "release", Run: "run-1"}}, true
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var console strings.Builder
			session, err := flowdebug.New(flowdebug.Options{
				In:    strings.NewReader("step\ncontinue\n"),
				Out:   &console,
				Steps: declared("release", "build"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = session.Close() })

			var seen string
			probe := &positionProbe{session: session, at: &seen}

			ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, &ranSteps{}))
			ctx = v1.NewContextWithDebugger(ctx, probe)
			ctx = v1.NewContextWithRunObserver(ctx, session)
			if runtime, ok := tc.install(t); ok {
				ctx = v1.ContextWithTaskRuntime(ctx, runtime)
			}

			_, runErr := v1.Run(ctx, &v1.Workflow{
				Name:  "release",
				Steps: []*v1.Node{markStep("build")},
			})
			require.NoError(t, runErr)

			assert.Equal(t, "release", seen,
				"the boundary was told no workflow, so nothing downstream can tell two `build` steps apart")
		})
	}
}

// positionProbe reads the workflow the engine reports at a boundary, then hands
// the step to the session so the run still walks.
//
// A wrapper rather than reading [flowdebug.Session.Paused] afterwards, because
// the claim is about what the *boundary* is told: a session that had been told
// nothing would report nothing, and so would one nobody asked.
type positionProbe struct {
	session *flowdebug.Session
	at      *string
}

func (p *positionProbe) BeforeStep(ctx context.Context, node *v1.Node, scope *v1.Scope) error {
	if name, ok := v1.ExecutingWorkflowFromContext(ctx); ok {
		*p.at = name
	}

	return p.session.BeforeStep(ctx, node, scope)
}

// TestACalleeReportsItsOwnWorkflowWithNoRuntimeAtAll is the same claim across a
// `call:`, which is the boundary the whole qualification exists for.
func TestACalleeReportsItsOwnWorkflowWithNoRuntimeAtAll(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("step\nstep\nstep\ncontinue\n"),
		Out: &console,
		Steps: []flowdebug.Step{
			{Workflow: "outer", ID: "build"},
			{Workflow: "outer", ID: "nested"},
			{Workflow: "inner", Declaration: 1, Via: "nested", ID: "build"},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	var seen []string
	probe := &positionTrail{session: session, seen: &seen}

	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, &ranSteps{}))
	ctx = v1.NewContextWithDebugger(ctx, probe)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	// No TaskRuntime, which is the unconfigured local run.
	_, runErr := v1.Run(ctx, &v1.Workflow{Name: "outer", Steps: []*v1.Node{
		markStep("build"),
		{Id: "nested", Kind: &v1.Node_Call{Call: &v1.Call{
			Workflow: &v1.Workflow{Name: "inner", Steps: []*v1.Node{markStep("build")}},
		}}},
	}})
	require.NoError(t, runErr)

	assert.Equal(t, []string{"outer", "outer", "inner"}, seen,
		"the callee's steps did not report the callee, so a caller's `build` and a "+
			"callee's are one name to everything downstream")
}

// positionTrail is [positionProbe] recording every boundary rather than the
// last.
type positionTrail struct {
	session *flowdebug.Session
	seen    *[]string
}

func (p *positionTrail) BeforeStep(ctx context.Context, node *v1.Node, scope *v1.Scope) error {
	name, _ := v1.ExecutingWorkflowFromContext(ctx)
	*p.seen = append(*p.seen, name)

	return p.session.BeforeStep(ctx, node, scope)
}

// TestAnUnnamedWorkflowIsNotSaidRatherThanNamedEmpty is the guard on the
// reader, and the case that makes it reachable.
//
// A workflow with no `name:` stamps an empty one, and an empty name is not a
// name: two unnamed workflows would otherwise be "the same workflow" to
// everything that compares them, which is the collapse this whole family of
// fixes is about. So the reader answers "not said", the same as a context the
// engine never touched — and a consumer that must have a name has to treat
// both the same way, which is what [flowdebug.Session.StepPosition] does by
// refusing to choose.
func TestAnUnnamedWorkflowIsNotSaidRatherThanNamedEmpty(t *testing.T) {
	t.Parallel()

	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:    strings.NewReader("step\ncontinue\n"),
		Out:   &console,
		Steps: []flowdebug.Step{{ID: "build"}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	said := true
	probe := &saidProbe{session: session, said: &said}

	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, &ranSteps{}))
	ctx = v1.NewContextWithDebugger(ctx, probe)
	ctx = v1.NewContextWithRunObserver(ctx, session)

	// No `name:` at all, which the engine still runs.
	_, runErr := v1.Run(ctx, &v1.Workflow{Steps: []*v1.Node{markStep("build")}})
	require.NoError(t, runErr)

	assert.False(t, said,
		"an unnamed workflow was reported as a workflow named \"\", which makes every "+
			"unnamed workflow the same one")
}

// saidProbe records whether the boundary was told a workflow at all, rather
// than which.
type saidProbe struct {
	session *flowdebug.Session
	said    *bool
}

func (p *saidProbe) BeforeStep(ctx context.Context, node *v1.Node, scope *v1.Scope) error {
	_, ok := v1.ExecutingWorkflowFromContext(ctx)
	*p.said = ok

	return p.session.BeforeStep(ctx, node, scope)
}
