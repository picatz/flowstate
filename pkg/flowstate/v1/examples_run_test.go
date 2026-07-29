package flowstatev1_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// CLAUDE.md says a capability is not done until an example exercises it, "those
// run in CI, which is what keeps them honest". They did not run. Every test over
// `examples/` compiled or validated them, and the difference is not academic: an
// example shipped here that `flow validate` called ok and `flow run local`
// refused on its first step, because `expect:` was written as a mapping where the
// http task wants an expression.
//
// It got through because `expect` is a *deferred* input — evaluated by the task
// against a scope the validator cannot see, so the validator correctly declines
// to judge it. Every deferred input has that shape. Validation cannot close this;
// only running can.
//
// So this runs them. It is the cheap half of what the rule already claimed.

// TestEveryOfflineExampleRuns executes each example that needs no network.
//
// Which ones those are is derived rather than listed: a workflow reaching the
// network is one with an `http` step somewhere in it, and asking the compiled
// workflow means a new example is covered the day it is written, without anyone
// remembering to add it here.
func TestEveryOfflineExampleRuns(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no examples found; the glob is wrong")

	var ran int
	for _, path := range paths {
		name := filepath.Base(filepath.Dir(path))

		data, err := os.ReadFile(path)
		require.NoError(t, err)

		wf, err := flowfile.Unmarshal(data)
		require.NoError(t, err, "%s does not compile", name)

		if reachesTheNetwork(wf.GetSteps()) {
			continue
		}
		// A gate is answered from outside the workload, which is the point of it.
		// The local driver takes one through `--signal`; a run without it is
		// refused, correctly, and that refusal is not something to assert here.
		if waitsForASignal(wf.GetSteps()) {
			continue
		}
		ran++

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			// Bounded, because an example that waits is an example that could hang
			// this suite, and a test whose failure mode is "CI times out in ten
			// minutes" is worse than one that says which example stopped.
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()

			outputs, err := v1.Run(ctx, wf)
			require.NoError(t, err, "%s validates but does not run", name)
			require.NotNil(t, outputs)

			// The run reached the end, rather than "succeeding" having done almost
			// nothing — which the error above would not catch.
			//
			// Asserted over unconditional top-level task steps only, and each
			// exclusion is a real rule rather than a way to make this pass. A step
			// behind an `if:` that is false is *meant* to produce nothing. A
			// `parallel:` reports through its branches, whose outputs merge into the
			// enclosing scope under their own ids and not the block's. A loop reports
			// through `results`.
			for _, step := range wf.GetSteps() {
				if step.GetTask() == nil || step.GetCondition() != nil {
					continue
				}
				assert.Contains(t, outputs.GetStepValues(), step.GetId(),
					"step %q produced no outputs", step.GetId())
			}
		})
	}

	// The count is asserted so that a change making every example look like it
	// needs the network — a rename, a broken predicate — fails here rather than
	// silently running nothing and reporting success.
	assert.GreaterOrEqual(t, ran, 8,
		"expected most examples to be runnable offline; only %d were, which suggests the network check is wrong", ran)
}

// waitsForASignal reports whether any step, at any depth, waits to be told
// something from outside the workload.
func waitsForASignal(nodes []*v1.Node) bool {
	return anyStep(nodes, func(node *v1.Node) bool {
		return node.GetWait().GetSignal() != nil
	})
}

// reachesTheNetwork reports whether any step, at any depth, makes a request.
func reachesTheNetwork(nodes []*v1.Node) bool {
	return anyStep(nodes, func(node *v1.Node) bool {
		return node.GetTask().GetName() == "http"
	})
}

// anyStep reports whether pred holds for any step, at any nesting depth.
//
// Both questions above are about the whole workflow rather than its top level: an
// example's only `http` step is as likely to be inside a loop body as beside one.
func anyStep(nodes []*v1.Node, pred func(*v1.Node) bool) bool {
	for _, node := range nodes {
		if pred(node) {
			return true
		}
		switch kind := node.GetKind().(type) {
		case *v1.Node_ForEach:
			if anyStep(kind.ForEach.GetBody(), pred) {
				return true
			}
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				if anyStep(branch.GetSteps(), pred) {
					return true
				}
			}
		}
	}
	return false
}
