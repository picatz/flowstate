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

	ids := workflowStepIDs(callingFixture(t))

	assert.Contains(t, ids, "the_call", "the calling step itself")
	assert.Contains(t, ids, "inside_the_callee", "and the steps it runs")
	assert.Contains(t, ids, "deep_in_the_callee",
		"including one nested inside the callee's own loop, since that is where "+
			"somebody sets a breakpoint most")

	// The caller's own steps are still all there: descending must add to the
	// inventory rather than replace it.
	assert.Subset(t, ids, []string{"before", "the_call", "after"})
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
