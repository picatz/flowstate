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

	deepest := "l" + string(rune('a'+v1.MaxCallDepth+1))
	assert.False(t, v1.DebugDeclaresStep(spec, deepest), "a step past MaxCallDepth was declared")
}
