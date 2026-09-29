package flowstatev1

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestASitesBindingsCostTheProgramNotItsDepth is the bound on
// [DebugStaticSites]' bindings: each container stores the names it binds and
// no more, and its body's sites share that one node. A flattened list copied
// every enclosing name into each container, so a specification the size bound
// admits (deep nesting, many names per level, many containers at the bottom)
// held sites × depth × names, gigabytes, in one workflow task (exact-head
// review, #2202).
func TestASitesBindingsCostTheProgramNotItsDepth(t *testing.T) {
	t.Parallel()

	const (
		levels   = 40
		perLevel = 16
		siblings = 200
	)
	vars := func(level int) map[string]*Value {
		out := make(map[string]*Value, perLevel)
		for i := range perLevel {
			out[fmt.Sprintf("v%d_%d", level, i)] = NewExpr("1")
		}

		return out
	}
	bottom := make([]*Node, 0, siblings)
	for i := range siblings {
		bottom = append(bottom, &Node{Id: fmt.Sprintf("b%d", i), Vars: vars(levels + i), Kind: &Node_ForEach{ForEach: &ForEach{
			Items: NewLiteralList(1), Iterator: fmt.Sprintf("it%d", i),
			Body: []*Node{{Id: "leaf", Kind: &Node_Value{Value: NewExpr("1")}}},
		}}})
	}
	body := bottom
	for level := levels - 1; level >= 0; level-- {
		body = []*Node{{Id: fmt.Sprintf("l%d", level), Vars: vars(level), Kind: &Node_ForEach{ForEach: &ForEach{
			Items: NewLiteralList(1), Iterator: fmt.Sprintf("i%d", level), Body: body,
		}}}}
	}

	sites, truncated := DebugStaticSites(&Workflow{Name: "deep", Steps: body})
	require.False(t, truncated)

	stored := 0
	for scope := range debugScopesOf(sites) {
		assert.Len(t, scope.names, perLevel+1, "a node holds more than its own container's names")
		stored += len(scope.names)
	}
	assert.Equal(t, (levels+siblings)*(perLevel+1), stored,
		"the names stored are not the program's own: each container's, once")

	// And every one of them is still visible where it is bound.
	var leaf DebugStaticSite
	for _, site := range sites {
		if path := site.Site.GetPath(); path[len(path)-1] == "leaf" {
			leaf = site

			break
		}
	}
	seen := 0
	for range leaf.Locals.All() {
		seen++
	}
	assert.Equal(t, (levels+1)*(perLevel+1), seen, "a leaf does not see every enclosing container's names")
}
