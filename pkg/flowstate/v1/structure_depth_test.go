package flowstatev1_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// This file is the regression suite for #334: the compiler's document-nesting
// bound and the schema-side walks' structure-nesting bound used to disagree
// (64 versus 32), and the gap between them was a value the compiler admitted
// that a schema-layer walk could not fully inspect — the same fail-open shape
// #329 found in the secret walk, one layer further down the stack. See
// [v1.MaxStructureDepth]'s doc for the decision and the reproduction that
// established the premise.
//
// Every test here pins one of two things: that [v1.MaxStructureDepth] is
// enforced at exactly the boundary in both directions, or that a walk this
// package runs over a [v1.Value_Structure] fails *closed* — answers
// conservatively rather than silently under-reporting — when it hits that
// bound. The second kind is the real deliverable: an assertion that only
// checks the refusal is satisfied by a walk that gives up early and says
// nothing is wrong.

// wrapStructure nests inner in levels of one-entry structure maps, the same
// shape [TestASecretRefBelowTheDepthBoundStillAnswersTrue] already uses to
// probe [v1.ValueHoldsSecretRef].
func wrapStructure(inner *v1.Value, levels int) *v1.Value {
	for range levels {
		inner = v1.NewStructureMap(map[string]*v1.Value{"k": inner})
	}
	return inner
}

// wrapStructureNamed is [wrapStructure] but keeps a distinct field name at
// every level, so a Continue-As-New style walk that reports a dotted path can
// be asked whether it actually reached the bottom.
func wrapStructureNamed(inner *v1.Value, levels int) *v1.Value {
	for range levels {
		inner = v1.NewStructureMap(map[string]*v1.Value{"level": inner})
	}
	return inner
}

func TestCheckStructureDepthBothDirections(t *testing.T) {
	t.Parallel()

	leaf := v1.NewLiteral("x")

	within := &v1.Workflow{
		Name: "within",
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "http",
				Inputs: map[string]*v1.Value{
					"headers": wrapStructure(leaf, v1.MaxStructureDepth),
				},
			}},
		}},
	}
	require.NoError(t, v1.CheckStructureDepth(within),
		"a structure exactly at the bound must be accepted, not refused early")

	over := &v1.Workflow{
		Name: "over",
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "http",
				Inputs: map[string]*v1.Value{
					"headers": wrapStructure(leaf, v1.MaxStructureDepth+1),
				},
			}},
		}},
	}
	err := v1.CheckStructureDepth(over)
	require.Error(t, err, "a structure one level past the bound must be refused")
	require.Contains(t, err.Error(), "32",
		"the refusal must name the bound so an author has a number to act on")
}

// TestCollectNodeRefsFailsClosedPastTheDepthBound is the CAN-compaction half
// of the sweep. Before this, [v1.CollectValueRefs] silently stopped
// descending at the bound and recorded nothing for what lay beneath — an
// expression referencing a prior step, nested past the bound, would have had
// that step's outputs pruned at the next Continue-As-New, and the run would
// fail the first time it tried to read them.
func TestCollectNodeRefsFailsClosedPastTheDepthBound(t *testing.T) {
	t.Parallel()

	prev := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"first":  {},
		"second": {},
	}}

	// Within the bound: an exact answer. Only the step actually referenced is
	// retained, and the other one is not swept in for free.
	within := &v1.Node{
		Id: "a",
		Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
			"headers": wrapStructure(v1.NewExpr("steps.first.value"), v1.MaxStructureDepth-1),
		}}},
	}
	refs := map[string]map[string]struct{}{}
	v1.CollectNodeRefs(within, prev, refs)
	require.Contains(t, refs, "first", "a reference within the walk's reach must be found")
	require.NotContains(t, refs, "second",
		"a walk that cannot tell which step is referenced must not retain everything by default — "+
			"only past its bound may it fall back to that")

	// Past the bound: the walk cannot see the expression at all, so it must
	// retain every step [prev] carries rather than silently keeping neither.
	// Dropping "second" here (which nothing found a reference to, because
	// nothing could look) would be exactly the pruning bug this pins against.
	over := &v1.Node{
		Id: "a",
		Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
			"headers": wrapStructure(v1.NewExpr("steps.first.value"), v1.MaxStructureDepth+8),
		}}},
	}
	refs = map[string]map[string]struct{}{}
	v1.CollectNodeRefs(over, prev, refs)
	require.Contains(t, refs, "first",
		"past the bound every known step must be retained, including the one actually referenced")
	require.Contains(t, refs, "second",
		"past the bound the walk cannot know which step an expression names, so it must keep them all "+
			"rather than pruning one that turns out to be needed")
	_, whole := refs["first"][v1.WholeStep]
	require.True(t, whole, "past the bound a step must be retained whole, not by a field guess")
	_, whole = refs["second"][v1.WholeStep]
	require.True(t, whole, "past the bound a step must be retained whole, not by a field guess")
}

// TestWalkTruncatedFiresAtTheDepthBound pins the shared traversal's own
// signal: a caller that cares about "I stopped looking" versus "there was
// nothing to find" (today, only [v1.CollectNodeRefs]) must be told when
// [v1.Walk.nested] gives up on a structure with more beneath it, rather than
// the walk returning silently as though the value ended there.
func TestWalkTruncatedFiresAtTheDepthBound(t *testing.T) {
	t.Parallel()

	within := wrapStructureNamed(v1.NewLiteral("x"), v1.MaxStructureDepth-1)
	over := wrapStructureNamed(v1.NewLiteral("x"), v1.MaxStructureDepth+8)

	wf := func(headers *v1.Value) *v1.Workflow {
		return &v1.Workflow{
			Name: "t",
			Steps: []*v1.Node{{
				Id: "a",
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
					"headers": headers,
				}}},
			}},
		}
	}

	var truncated int
	v1.WalkWorkflow(wf(within), v1.Walk{
		Value:     func(v1.ValueSite) {},
		Truncated: func(v1.ValueSite) { truncated++ },
	})
	require.Zero(t, truncated, "a structure within the bound must never report truncation")

	truncated = 0
	var sites []string
	v1.WalkWorkflow(wf(over), v1.Walk{
		Value: func(v1.ValueSite) {},
		Truncated: func(site v1.ValueSite) {
			truncated++
			sites = append(sites, site.Field())
		},
	})
	require.NotZero(t, truncated, "a structure past the bound must report that the walk was cut off")
	require.NotEmpty(t, sites, "the truncation must name the position it happened at")
	require.True(t, strings.HasPrefix(sites[0], "headers"),
		"the truncated site should still be addressable by the caller, got %q", sites[0])
}

// TestNestedSecretStructureCompilesOnlyWithinTheBound is the compile-time
// half, exercised from this package via the same value shape a Flowfile
// compiles a nested reference into: the compiler now refuses a structure
// past [v1.MaxStructureDepth] directly (see
// pkg/flowstate/v1/flowfile/structure_depth_test.go for the Flowfile-level
// reproduction), and this pins the schema-side counterpart so a hand-built
// [v1.Workflow] arriving without a compiler in front of it — the RPC path —
// gets refused by the identical bound rather than a looser or absent one.
func TestNestedSecretStructureCompilesOnlyWithinTheBound(t *testing.T) {
	t.Parallel()

	ref := &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "TOKEN"}}}

	within := &v1.Workflow{
		Name: "within",
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "http",
				Inputs: map[string]*v1.Value{"headers": wrapStructure(ref, v1.MaxStructureDepth)},
			}},
		}},
	}
	require.NoError(t, v1.CheckStructureDepth(within))
	require.Equal(t, []string{"env:TOKEN"},
		v1.SecretRefsIn(within.GetSteps()[0].GetTask()),
		"a reference exactly at the bound must still be named, not just detected")

	over := &v1.Workflow{
		Name: "over",
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "http",
				Inputs: map[string]*v1.Value{"headers": wrapStructure(ref, v1.MaxStructureDepth+1)},
			}},
		}},
	}
	require.Error(t, v1.CheckStructureDepth(over),
		"a hand-built specification past the bound must be refused before it reaches any walk that "+
			"cannot fully inspect it")
}
