package flowstatev1_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

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
		"each/in-each", "each[2]/in-each", "fan/in-branch", "pick/in-default", "sub/in-callee",
	} {
		assert.True(t, declared(text), "%s is declared but not found", text)
	}
	for _, text := range []string{"nowhere", "bogus/in-each", "fan/in-each", "each/sub/in-callee"} {
		assert.False(t, declared(text), "%s is not declared but was found", text)
	}
}
