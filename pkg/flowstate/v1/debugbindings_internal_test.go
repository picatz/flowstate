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

// TestAConditionsCheckCostsNotTheProgram: a request may carry many
// conditional breakpoints against one large program, and the whole-program
// half of the check is taken once ([NewDebugProgramNames]) rather than once
// per condition, so one check costs its own sites, not the program's (Codex,
// #2202). Measured as allocations, which a walk of the program's scopes makes
// in proportion to its size.
func TestAConditionsCheckCostsNotTheProgram(t *testing.T) {
	program := func(containers int) (*DebugProgramNames, []DebugStaticSite) {
		steps := make([]*Node, 0, containers)
		for i := range containers {
			steps = append(steps, &Node{Id: fmt.Sprintf("c%d", i), Kind: &Node_ForEach{ForEach: &ForEach{
				Items: NewLiteralList(1), Iterator: fmt.Sprintf("it%d", i),
				Body: []*Node{{Id: fmt.Sprintf("leaf%d", i), Kind: &Node_Value{Value: NewExpr("1")}}},
			}}})
		}
		sites, truncated := DebugStaticSites(&Workflow{Name: "wide", Steps: steps})
		require.False(t, truncated)
		for _, site := range sites {
			if path := site.Site.GetPath(); path[len(path)-1] == "leaf0" {
				return NewDebugProgramNames(sites), []DebugStaticSite{site}
			}
		}
		t.Fatal("no site for leaf0")

		return nil, nil
	}
	condition, err := CompileDebugCondition("it0 > 0 && steps.c0 != null", CurrentProfile)
	require.NoError(t, err)

	allocs := func(containers int) float64 {
		names, at := program(containers)
		require.NoError(t, CheckDebugConditionScope(condition, CurrentProfile, at, names))

		return testing.AllocsPerRun(20, func() {
			_ = CheckDebugConditionScope(condition, CurrentProfile, at, names)
		})
	}
	small, large := allocs(4), allocs(4000)
	assert.Equal(t, small, large, "a check allocated %v against 4 containers and %v against 4000", small, large)
}
