package engine

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Version 1 of [workflowSliceCostChange] shipped in #1919 and recorded segments
// that charged a `value:` step's expression and nothing else, and that never
// suspended at a step skipped by a false `if:`. Version 2 charges every other
// workflow-side expression and adds that boundary.
//
// A worker running this build replays a version 1 history and has to reach the
// same commands it recorded. Charging more in a replayed prefix can cross the
// threshold — and emit a Continue-As-New — at a boundary whose recorded history
// holds an activity instead, which wedges the run on a nondeterminism error
// rather than failing it visibly. The replay corpus cannot catch this: every
// recording in `testdata/replay/` predates #1919 and takes the default path,
// where no cost is charged at all.
//
// So the split between the two charge entry points is the guard, and these
// tests are about that split rather than about any one expression's price —
// which is deliberate, and is also the limit of what they prove. A version is a
// set of reasons to emit a continuation; the estimator that prices an expression
// is built once per program and read by every version alike, so re-pricing one
// reaches a replaying version 1 history too. See [workflowSliceCostChange] for
// why that half cannot be gated and is stated as a deploy-window exposure
// instead.

// TestVersionOneChargesAValueStepAndNothingElse pins the accumulator's two
// entry points against the versions they belong to.
func TestVersionOneChargesAValueStepAndNothingElse(t *testing.T) {
	t.Parallel()

	t.Run("version 1", func(t *testing.T) {
		t.Parallel()

		spent := uint64(0)
		e := &executor{sliceCost: &spent}

		e.chargeWorkflowCost(1000)
		require.Zero(t, spent,
			"a version 1 history charged an expression version 2 added, so its replay can suspend where it did not")

		e.chargeValueCost(1000)
		assert.Equal(t, uint64(1000), spent, "a version 1 history stopped charging its `value:` steps")
	})

	t.Run("version 2", func(t *testing.T) {
		t.Parallel()

		spent := uint64(0)
		e := &executor{sliceCost: &spent, everyExpressionCharged: true}

		e.chargeWorkflowCost(1000)
		e.chargeValueCost(1000)
		assert.Equal(t, uint64(2000), spent, "version 2 did not charge both")
	})

	t.Run("before the marker", func(t *testing.T) {
		t.Parallel()

		// No accumulator at all, and neither entry point may invent one.
		e := &executor{}
		e.chargeWorkflowCost(1000)
		e.chargeValueCost(1000)
	})
}

// TestEveryNestedExecutorCarriesTheCostVersion is the structural half, and it
// is the one a later change is most likely to break: a nested executor that
// inherited `sliceCost` but not `everyExpressionCharged` would charge a version
// 2 history as though it were version 1 — silently cheaper, silently later to
// suspend — for every expression inside a call, a loop body, a parallel branch
// or an async step.
//
// Read off the source rather than exercised, because the claim is about every
// executor literal in the package including ones no test reaches.
func TestEveryNestedExecutorCarriesTheCostVersion(t *testing.T) {
	t.Parallel()

	sources, err := filepath.Glob("*.go")
	require.NoError(t, err)

	fset := token.NewFileSet()
	found := 0
	for _, source := range sources {
		// Production files only: a test may legitimately build a version 1
		// executor, which is exactly what the first test above does.
		if strings.HasSuffix(source, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, source, nil, 0)
		require.NoError(t, err)

		ast.Inspect(file, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			ident, ok := lit.Type.(*ast.Ident)
			if !ok || ident.Name != "executor" {
				return true
			}

			var carries, costs bool
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok {
					continue
				}
				switch key.Name {
				case "sliceCost":
					costs = true
				case "everyExpressionCharged":
					carries = true
				}
			}
			if costs {
				found++
				assert.Truef(t, carries,
					"%s: an executor built with sliceCost and without everyExpressionCharged charges a version 2 history at version 1 prices",
					fset.Position(lit.Pos()))
			}

			return true
		})
	}

	require.Positive(t, found, "no executor literal sets sliceCost, so this test proves nothing")
	require.NotEmpty(t, sources, "the package's own sources did not glob")
}
