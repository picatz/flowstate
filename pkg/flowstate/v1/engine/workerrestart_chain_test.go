package engine_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// Worker restart across Continue-As-New.
//
// A step budget of one makes a run Continue-As-New between every pair of steps,
// so a run is a chain of executions and each later one begins from the carryover
// compaction chose rather than from the submitted workflow. That is the state
// only the durable driver has, and the state a worker lost between executions
// has to resume from. This runs the shared cases that need no trigger or inputs,
// and one with enough steps to cross several boundaries, with that budget, stops the first worker after each completion but the last, and requires the chain to give what the
// undisturbed chain gives, with as many activity completions as the undisturbed chain.
//
// The case set has to reach a restart that lands in a later execution than the
// first: a restart confined to the first would prove nothing about carryover, so
// the test fails when no seed of any case resumed past it.
func TestWorkerRestartAcrossContinueAsNew(t *testing.T) {
	if withoutDevServer() {
		t.Skip("skipping: needs the shared Temporal dev server, which this process did not start (-short, or a fuzzing run); CI runs the full suite")
	}

	base := conformance.NewHTTPServer(t)

	type chainCase struct {
		name     string
		workflow *v1.Workflow
	}
	var cases []chainCase
	for _, outline := range conformance.Workflows(base) {
		if eligibleForRestart(outline) {
			cases = append(cases, chainCase{outline.Name, outline.Workflow})
		}
	}
	// The shared cases are one or two steps, so at most one boundary each. This
	// one has enough executions for a restart to land after the first and still
	// leave work behind it.
	says := func(id, message string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
			Name:   "log",
			Inputs: map[string]*v1.Value{"message": v1.NewLiteral(message)},
		}}}
	}
	cases = append(cases, chainCase{"four steps, an execution each", &v1.Workflow{
		Name:  "four-steps-a-chain",
		Steps: []*v1.Node{says("a", "one"), says("b", "two"), says("c", "three"), says("d", "four")},
	}})

	crossed := 0
	for index, outline := range cases {
		t.Run(outline.name, func(t *testing.T) {
			temporal := newTemporalNamespace(t)
			ctx, cancel := context.WithTimeout(t.Context(), exampleRunTimeout)
			defer cancel()

			state := func() *v1.RunState { return &v1.RunState{Workflow: outline.workflow, StepsBudget: 1} }
			prefix := fmt.Sprintf("restart-chain-%d", index)

			baseline := runWithRestart(ctx, t, temporal, prefix+"-baseline", state(), 0)
			require.NoError(t, baseline.runErr, "the undisturbed chain must succeed before a restart can be judged against it")
			// Compared with the undisturbed chain rather than the case's own
			// outputs: a chain's result is what its last execution returns,
			// which is not the whole run's step outputs the case pins.
			if baseline.chainRuns < 2 {
				t.Skipf("the case ran in %d execution, so there is no Continue-As-New to cross", baseline.chainRuns)
			}

			// Every completion but the last, not a seed's pick: a chain is short,
			// and which execution a seed lands in is luck the crossing claim
			// below cannot rest on. The finding's seed is the stop point itself.
			if baseline.completions < 2 {
				t.Skip(fmt.Errorf("no restart point: the chain completed %d activities", baseline.completions))
			}
			for stopAt := int64(1); stopAt < baseline.completions; stopAt++ {
				seed := uint64(stopAt)
				got := runWithRestart(ctx, t, temporal, fmt.Sprintf("%s-seed-%d", prefix, seed), state(), stopAt)
				requireRestarted(t, outline.name, seed, stopAt, baseline.completions, got)
				require.NoError(t, got.runErr, "the restarted chain failed (seed %d, stop after %d)", seed, stopAt)
				require.Equal(t, baseline.chainRuns, got.chainRuns,
					"the restarted chain has a different number of executions (seed %d, stop after %d)", seed, stopAt)
				requireOutputs(t, baseline.outputs, got.outputs,
					fmt.Sprintf("the chain restarted after completion %d (seed %d)", stopAt, seed))
				if got.resumedRun > 0 {
					crossed++
				}
			}
		})
	}

	require.Positive(t, crossed, "no seed of any case resumed in a later execution than the first, so no restart crossed a Continue-As-New")
}
