package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The inventory behind `break <tab>` and `until <tab>`, which is the one part
// of debug completion this command owns: the session completes over what it is
// given, and this is what gives it.

// callingFixture writes a caller and a callee, and answers with the caller's
// path.
//
// A real pair of files through the real parser, because the claim is about a
// compiled callee: a `*v1.Workflow` assembled in Go would prove the walk
// recurses and say nothing about whether a `call:` an author writes arrives
// with a workflow inside it (CLAUDE.md).
func callingFixture(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()

	callee := filepath.Join(dir, "callee.yaml")
	require.NoError(t, os.WriteFile(callee, []byte(`edition: v2026.3
name: callee
steps:
  - id: inside_the_callee
    log:
      message: from the callee
  - id: also_inside
    for_each:
      items: ${[1, 2]}
      as: n
      steps:
        - id: deep_in_the_callee
          log:
            message: ${"n is " + string(n)}
`), 0o600))

	caller := filepath.Join(dir, "caller.yaml")
	require.NoError(t, os.WriteFile(caller, []byte(`edition: v2026.3
name: caller
steps:
  - id: before
    log:
      message: before
  - id: the_call
    call: ./callee.yaml
  - id: after
    log:
      message: after
`), 0o600))

	return caller
}

// TestTheStepInventoryReachesIntoACall (Codex, #1114).
//
// `v1.WalkWorkflow` does not follow a `call:` into its callee, for a reason
// that is right where its callers report diagnostics: a callee is a different
// author's workflow, and its problems are not the caller's. A breakpoint is the
// other question — `runCall` executes those nodes through the same debugger
// context, so `break inside_the_callee` has always worked. Only the list was
// blind to them, which is worse than a short list: an inventory that looks
// complete with a whole workflow missing from it.
func TestTheStepInventoryReachesIntoACall(t *testing.T) {
	t.Parallel()

	steps := workflowStepList(callingFixture(t))

	ids := make([]string, 0, len(steps))
	for _, step := range steps {
		ids = append(ids, step.ID)
	}

	assert.Contains(t, ids, "the_call", "the calling step itself")
	assert.Contains(t, ids, "inside_the_callee", "and the steps it runs")
	assert.Contains(t, ids, "deep_in_the_callee",
		"including one nested inside the callee's own loop, since that is where "+
			"somebody sets a breakpoint most")

	// The caller's own steps are still all there: descending must add to the
	// inventory rather than replace it.
	assert.Subset(t, ids, []string{"before", "the_call", "after"})
}

// TestTheStepInventoryNamesTheWorkflowThatDeclaresEachStep is what makes the
// flattened list above safe to draw as rows.
//
// `runCall` moves the run's position across a call because "a callee's step ids
// belong to the callee, not to its caller" (`eval.go:1804-1812`). An inventory
// that walks into a callee and keeps only bare ids re-creates exactly the
// confusion that move prevents: two rows called `build`, and a step list that
// points at whichever came first (Codex, #1182).
//
// Asserted through the real parser and a real `call:` for [callingFixture]'s
// own reason — a `*v1.Workflow` assembled in Go would prove the walk carries a
// name and say nothing about whether the compiled callee has one.
func TestTheStepInventoryNamesTheWorkflowThatDeclaresEachStep(t *testing.T) {
	t.Parallel()

	steps := workflowStepList(callingFixture(t))
	require.NotEmpty(t, steps, "the fixture produced no inventory, so nothing below is asserted")

	declaring := map[string]string{}
	for _, step := range steps {
		declaring[step.ID] = step.Workflow
	}

	assert.Equal(t, "caller", declaring["before"], "a caller's own step")
	assert.Equal(t, "caller", declaring["the_call"], "the calling step itself, which is the caller's")
	assert.Equal(t, "callee", declaring["inside_the_callee"],
		"a callee's step was attributed to the workflow that called it")
	assert.Equal(t, "callee", declaring["deep_in_the_callee"],
		"including one nested inside the callee's own loop")

	// The negative direction: nothing in the inventory is left unattributed,
	// because a row carrying no workflow is a row a step list cannot place.
	for _, step := range steps {
		assert.NotEmpty(t, step.Workflow, "%s carries no declaring workflow", step.ID)
	}
}

// TestTheStepInventoryNumbersEveryDescentSeparately is the identity a name
// cannot carry.
//
// One callee invoked from two `call:` steps appears twice under one name, and
// so do two different embedded workflows that share a `name:`. Grouped by name
// those are one declaration, and the session then attributes an outcome that
// names one of two invocations to both rows (Codex, #1186). The walk's descents
// are the engine's own structure read statically — `runNodes` descends into a
// callee once per `call:` node (`eval.go:1734`) — so each gets its own number.
func TestTheStepInventoryNumbersEveryDescentSeparately(t *testing.T) {
	t.Parallel()

	callee := func() *v1.Workflow {
		return &v1.Workflow{Name: "inner", Steps: []*v1.Node{
			{Id: "build", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}},
		}}
	}
	workflow := &v1.Workflow{Name: "outer", Steps: []*v1.Node{
		{Id: "first_call", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee()}}},
		{Id: "second_call", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee()}}},
	}}

	steps := stepList(workflow)
	require.Len(t, steps, 4, "the walk did not reach both callees")

	declarations := map[int]struct{}{}
	reachedBy := map[string]string{}
	for _, step := range steps {
		if step.ID != "build" {
			continue
		}
		declarations[step.Declaration] = struct{}{}
		reachedBy[step.Via] = step.Workflow
	}

	assert.Len(t, declarations, 2,
		"two invocations of one callee were numbered as one declaration")

	// And each row says which `call:` reached it, which is the only thing that
	// differs when the workflow name does not — what the pane draws them
	// against.
	assert.Equal(t, map[string]string{"first_call": "inner", "second_call": "inner"}, reachedBy)

	// The caller's own steps share the root declaration and are reached
	// through no call: the numbering separates descents, not rows.
	for _, step := range steps {
		if step.Workflow == "outer" {
			assert.Zero(t, step.Declaration, "%s", step.ID)
			assert.Empty(t, step.Via, "%s is the caller's own", step.ID)
		}
	}
}

// TestTheStepInventoryStopsAtTheEnginesOwnCallDepth pins the bound to the
// engine's rather than to a number chosen here.
//
// An inventory deeper than the runs it describes offers breakpoints on steps no
// run reaches; a shallower one hides steps that do. A compiled callee is
// embedded whole so a file's call tree is finite anyway — the bound is for the
// workflow built in Go, under nobody's compiler.
func TestTheStepInventoryStopsAtTheEnginesOwnCallDepth(t *testing.T) {
	t.Parallel()

	assert.Equal(t, v1.MaxCallDepth, maxCallInventoryDepth,
		"the inventory follows calls exactly as far as the engine runs them")
}
