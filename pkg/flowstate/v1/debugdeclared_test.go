package flowstatev1_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestDebugDeclaredStepsFollowsCallsToTheSameDepthAsTheSites: a callee's steps
// are declared to the depth [v1.DebugStaticSites] follows calls, and no
// further, so the two answers about one program agree on where calls stop.
func TestDebugDeclaredStepsFollowsCallsToTheSameDepthAsTheSites(t *testing.T) {
	t.Parallel()

	// A chain of calls one deeper than MaxCallDepth; level i declares li.
	var build func(level int) *v1.Workflow
	build = func(level int) *v1.Workflow {
		wf := &v1.Workflow{Name: "level", Steps: []*v1.Node{{Id: "l" + string(rune('a'+level)), Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}}}
		if level <= v1.MaxCallDepth {
			wf.Steps = append(wf.Steps, &v1.Node{Id: "down", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: build(level + 1)}}})
		}

		return wf
	}
	spec := build(0)

	sites, _ := v1.DebugStaticSites(spec)
	fromSites := map[string]bool{}
	for _, site := range sites {
		path := site.Site.GetPath()
		fromSites[path[len(path)-1]] = true
	}
	declared := slices.Collect(v1.DebugDeclaredSteps(spec))
	for _, id := range declared {
		assert.True(t, fromSites[id], "%s is declared past the depth the sites reach", id)
	}
	for id := range fromSites {
		assert.Contains(t, declared, id, "%s is a site but not declared", id)
	}

	deepest, err := v1.ParseDebugTarget("l" + string(rune('a'+v1.MaxCallDepth+1)))
	assert.NoError(t, err)
	assert.False(t, deepest.DeclaredIn(spec), "a step past MaxCallDepth was declared")
}

// TestDeclaredInLooksEverywhereAStepCanBeWritten covers each place a step id
// can be declared, and a callee's steps, and holds a qualified target to the
// containers it names, so a truncated program refuses only a target written
// nowhere.
func TestDeclaredInLooksEverywhereAStepCanBeWritten(t *testing.T) {
	t.Parallel()

	step := func(id string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}
	}
	spec := &v1.Workflow{Name: "root", Steps: []*v1.Node{
		{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{Body: []*v1.Node{step("in-each")}}}},
		{Id: "again", Kind: &v1.Node_Loop{Loop: &v1.Loop{Body: []*v1.Node{step("in-loop")}}}},
		{Id: "fan", Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{{Steps: []*v1.Node{step("in-branch")}}}}}},
		{Id: "pick", Kind: &v1.Node_Switch{Switch: &v1.Switch{
			Cases:   []*v1.Switch_Case{{Steps: []*v1.Node{step("in-arm")}}},
			Default: &v1.Switch_Default{Steps: []*v1.Node{step("in-default")}},
		}}},
		{Id: "sub", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: &v1.Workflow{Name: "callee", Steps: []*v1.Node{step("in-callee")}}}}},
	}}
	declared := func(text string) bool {
		t.Helper()
		target, err := v1.ParseDebugTarget(text)
		if err != nil {
			t.Fatalf("parsing %s: %v", text, err)
		}

		return target.DeclaredIn(spec)
	}
	for _, text := range []string{
		"each", "in-each", "in-loop", "in-branch", "in-arm", "in-default", "sub", "in-callee",
		"each/in-each", "each[2]/in-each", "fan/in-branch", "fan#1/in-branch", "pick/in-default",
		"sub/in-callee", "sub(callee)/in-callee",
	} {
		assert.True(t, declared(text), "%s is declared but not found", text)
	}
	for _, text := range []string{
		"nowhere", "bogus/in-each", "fan/in-each", "each/sub/in-callee",
		// The right ids under the wrong kind of container, or another callee.
		"each#0/in-each", "fan[1]/in-branch", "sub(other)/in-callee",
	} {
		assert.False(t, declared(text), "%s is not declared but was found", text)
	}
}

// TestTheDeclarationWalksSurviveASharedCallee: a workflow built in memory may
// share one callee among many calls. Walked once per call, thirty calls a level
// through every call level is 30^8 walks; walked once per callee and depth
// (and, for a target, per chain its qualifiers see), it answers at once.
func TestTheDeclarationWalksSurviveASharedCallee(t *testing.T) {
	t.Parallel()

	leaf := &v1.Workflow{Name: "leaf", Steps: []*v1.Node{{Id: "bottom", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}}}
	level := leaf
	for range v1.MaxCallDepth {
		next := &v1.Workflow{Name: "level"}
		for i := range 30 {
			next.Steps = append(next.Steps, &v1.Node{Id: fmt.Sprintf("c%d", i), Kind: &v1.Node_Call{Call: &v1.Call{Workflow: level}}})
		}
		level = next
	}

	assert.Contains(t, slices.Collect(v1.DebugDeclaredSteps(level)), "bottom")
	target, err := v1.ParseDebugTarget("c7/bottom")
	require.NoError(t, err)
	assert.True(t, target.DeclaredIn(level))
	missing, err := v1.ParseDebugTarget("nowhere")
	require.NoError(t, err)
	assert.False(t, missing.DeclaredIn(level), "the walk answered for a step no workflow declares")

	// As many qualifiers as the chain is deep, none of which match: keyed
	// by the segments around each callee, every path would be its own key.
	long, err := v1.ParseDebugTarget("q1/q2/q3/q4/q5/q6/q7/q8/q9/bottom")
	require.NoError(t, err)
	assert.False(t, long.DeclaredIn(level))
	deep, err := v1.ParseDebugTarget("c1/c2/c3/c4/c5/c6/c7/c8/bottom")
	require.NoError(t, err)
	assert.True(t, deep.DeclaredIn(level), "a fully qualified path the program declares was not found")
}

// TestTheDeclarationWalkRevisitsACalleeFromShallower: a callee first reached at
// the call-depth limit cannot follow its own calls there; reached again from a
// shallower call it can, so the inventory is not settled by the first visit.
func TestTheDeclarationWalkRevisitsACalleeFromShallower(t *testing.T) {
	t.Parallel()

	call := func(id string, wf *v1.Workflow) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Call{Call: &v1.Call{Workflow: wf}}}
	}
	inner := &v1.Workflow{Name: "inner", Steps: []*v1.Node{{Id: "innermost", Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}}}
	shared := &v1.Workflow{Name: "shared", Steps: []*v1.Node{call("down", inner)}}
	// A chain long enough that `shared` is first reached at MaxCallDepth.
	deep := shared
	for range v1.MaxCallDepth - 1 {
		deep = &v1.Workflow{Name: "chain", Steps: []*v1.Node{call("hop", deep)}}
	}
	root := &v1.Workflow{Name: "root", Steps: []*v1.Node{call("far", deep), call("near", shared)}}

	assert.Contains(t, slices.Collect(v1.DebugDeclaredSteps(root)), "innermost",
		"a callee walked first at the depth limit was not walked again from a shallower call")
}
