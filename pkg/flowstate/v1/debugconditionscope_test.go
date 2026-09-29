package flowstatev1_test

import (
	"context"
	"maps"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// scopedProgram binds a bare name in every way a container can: a `for_each`
// iterator and the loop step's own `vars:`, a `loop:`'s state, a parallel
// block's `vars:`, and two calls — one inside the loop — whose callees bind
// none of their caller's. The leaf `charge` declares vars of its own, which
// its `if:` does not see.
func scopedProgram() *v1.Workflow {
	return &v1.Workflow{Name: "scoped", Steps: []*v1.Node{
		{Id: "compose", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}},
		{
			Id:   "orders",
			Vars: map[string]*v1.Value{"rate": v1.NewExpr("2")},
			Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
				Items:    v1.NewLiteralList(1, 2),
				Iterator: "order",
				Body: []*v1.Node{
					{
						Id:   "charge",
						Vars: map[string]*v1.Value{"fee": v1.NewExpr("order * rate")},
						Kind: &v1.Node_Value{Value: v1.NewExpr("fee")},
					},
					{
						Id:   "fan",
						Vars: map[string]*v1.Value{"lane": v1.NewExpr("order")},
						Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{Branches: []*v1.Parallel_Branch{
							{Steps: []*v1.Node{{Id: "notify", Kind: &v1.Node_Value{Value: v1.NewExpr("lane")}}}},
						}}},
					},
					{Id: "audit", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: &v1.Workflow{
						Name:  "auditor",
						Steps: []*v1.Node{{Id: "within", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}},
					}}}},
				},
			}},
		},
		{Id: "pages", Kind: &v1.Node_Loop{Loop: &v1.Loop{
			State:         "page",
			Initial:       v1.NewExpr("0"),
			Update:        v1.NewExpr("page + 1"),
			Until:         v1.NewExpr("page >= 1"),
			MaxIterations: 3,
			Body:          []*v1.Node{{Id: "fetch", Kind: &v1.Node_Value{Value: v1.NewExpr("page")}}},
		}}},
		{Id: "sub", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: &v1.Workflow{
			Name:  "callee",
			Steps: []*v1.Node{{Id: "inside", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}},
		}}}},
	}}
}

// localsGate records, at every step boundary, the bare names the local driver
// has bound where the step's `if:` was just evaluated.
type localsGate struct {
	mu   sync.Mutex
	seen map[string][]string
}

func (g *localsGate) BeforeStep(ctx context.Context, node *v1.Node, scope *v1.Scope) error {
	site := v1.DebugSiteKey(v1.ExecutingOccurrenceFromContext(ctx, node).GetSite())
	g.mu.Lock()
	defer g.mu.Unlock()
	g.seen[site] = slices.Sorted(maps.Keys(scope.GetVars()))

	return nil
}

// TestAStaticSitesLocalsAreWhatTheRunBindsThere pins the static derivation to
// the driver it describes: at every site the run reaches, the names
// [v1.DebugStaticSites] says are bound are exactly the ones the local driver
// bound. A derivation that disagreed would refuse a condition that works, or
// admit one that never can.
func TestAStaticSitesLocalsAreWhatTheRunBindsThere(t *testing.T) {
	t.Parallel()

	wf := scopedProgram()
	gate := &localsGate{seen: map[string][]string{}}
	_, err := v1.Run(v1.NewContextWithDebugger(t.Context(), gate), wf)
	require.NoError(t, err)

	sites, truncated := v1.DebugStaticSites(wf)
	require.False(t, truncated)
	require.Len(t, gate.seen, len(sites), "the run reached a site the enumeration does not have, or missed one")
	for _, site := range sites {
		key := v1.DebugSiteKey(site.Site)
		bound, reached := gate.seen[key]
		require.True(t, reached, "the run never reached %s", key)
		assert.Equal(t, slices.Concat([]string{}, bound), slices.Concat([]string{}, slices.Compact(slices.Sorted(site.Locals.All()))), "at %s", key)
	}

	// And the program does bind something, somewhere: an agreement over
	// empty sets would prove nothing.
	byStep := map[string][]string{}
	for _, site := range sites {
		byStep[site.Site.GetPath()[len(site.Site.GetPath())-1]] = slices.Sorted(site.Locals.All())
	}
	assert.Equal(t, []string{"order", "rate"}, byStep["charge"], "the loop's iterator and its own vars; not the step's own")
	assert.Equal(t, []string{"lane", "order", "rate"}, byStep["notify"], "a parallel block's vars reach its branches")
	assert.Equal(t, []string{"page"}, byStep["fetch"])
	assert.Empty(t, byStep["inside"], "a callee sees none of its caller's bare names")
	assert.Empty(t, byStep["within"], "a callee called inside a loop sees none of the loop's bare names")
	assert.Equal(t, []string{"order", "rate"}, byStep["audit"], "the call step itself is in the loop")
	assert.Empty(t, byStep["compose"])
}

// TestAConditionReadingANameNothingBindsIsRefused is #2194: a condition is
// evaluated where the step's `if:` is, so a bare name no site the breakpoint
// fires at binds can never resolve, and is refused when it is set rather than
// declined at every arrival.
func TestAConditionReadingANameNothingBindsIsRefused(t *testing.T) {
	t.Parallel()

	wf := scopedProgram()
	sites, truncated := v1.DebugStaticSites(wf)
	require.False(t, truncated)
	at := func(step string) []v1.DebugStaticSite {
		target, err := v1.ParseDebugTarget(step)
		require.NoError(t, err)

		return target.Resolve(sites)
	}

	for _, test := range []struct {
		step, condition string
		// refused is a fragment of the refusal, or empty for admitted.
		refused string
	}{
		{step: "compose", condition: "nosuch > 1", refused: "`nosuch` is not bound where this breakpoint fires"},
		{step: "compose", condition: "order > 1", refused: "`order` is bound only inside the loops and steps that declare it"},
		{step: "inside", condition: "order > 1", refused: "`order` is bound only inside"},
		{step: "within", condition: "order > 1", refused: "`order` is bound only inside"},
		{step: "compose", condition: "nosuch.exists(x, x > 1)", refused: "`nosuch` is not bound"},
		{step: "compose", condition: "size([1, nosuch]) > 1", refused: "`nosuch` is not bound"},
		{step: "compose", condition: `size({"a": nosuch}) > 0`, refused: "`nosuch` is not bound"},
		{step: "compose", condition: `size({nosuch: 1}) > 0`, refused: "`nosuch` is not bound"},
		{step: "compose", condition: "[1].map(x, x).exists(y, y > nosuch)", refused: "`nosuch` is not bound"},
		{step: "charge", condition: "fee > 1", refused: "`fee` is not bound"},
		{step: "charge", condition: "ordr > 1", refused: "did you mean `order`?"},
		{step: "charge", condition: "nosuch > 1", refused: "and here `order`, `rate`"},
		{step: "compose", condition: "charge.value > 1", refused: "`charge` is a step, and a step's outputs are read as `steps.charge.<output>`"},
		{step: "compose", condition: "now > 1", refused: "`now` is bound only inside a wait's own expressions"},
		{step: "compose", condition: "[1].map(x, x).exists(y, x > y)", refused: "`x` is not bound"},

		{step: "charge", condition: "order > 1 && rate == 2"},
		{step: "notify", condition: "lane == order"},
		{step: "fetch", condition: "page > 0"},
		{step: "compose", condition: `size(steps) > 0 && inputs.a == 1 && vars.b == 1 && run.local && trigger.kind == ""`},
		{step: "compose", condition: "[1, 2].exists(x, x > 1) && [3].all(x, [x].exists(y, y == x))"},
		{step: "compose", condition: "math.greatest(1, 2) > 1"},
		{step: "compose", condition: `math.ceil(1.5) > 1.0 && base64.encode(b"x") != "" && sets.contains([1], [1])`},
		{step: "compose", condition: "nosuch.size() > 1", refused: "`nosuch` is not bound"},
		// Type values parse as identifiers and are resolved by the
		// environment, bare and qualified (Copilot, #2202).
		{step: "charge", condition: "type(order) == int"},
		{step: "compose", condition: `type("x") == string && type([]) == list && type({}) == map && type(null) == null_type`},
		{step: "compose", condition: "type(steps) != google.protobuf.Timestamp"},
		{step: "compose", condition: "type(nosuch) == int", refused: "`nosuch` is not bound"},
	} {
		compiled, err := v1.CompileDebugCondition(test.condition, v1.CurrentProfile)
		require.NoError(t, err, "%s if %s", test.step, test.condition)

		err = v1.CheckDebugConditionScope(compiled, v1.CurrentProfile, at(test.step), sites)
		if test.refused == "" {
			assert.NoError(t, err, "%s if %s", test.step, test.condition)

			continue
		}
		if assert.Error(t, err, "%s if %s", test.step, test.condition) {
			assert.Contains(t, err.Error(), test.refused, "%s if %s", test.step, test.condition)
		}
	}
}

// TestAConditionWhereItFiresIsNotKnownIsAdmitted: with no sites to ask — a
// program whose enumeration was cut short — the names cannot be judged, so
// the condition is admitted as it was before the check, and the caller says so.
func TestAConditionWhereItFiresIsNotKnownIsAdmitted(t *testing.T) {
	t.Parallel()

	compiled, err := v1.CompileDebugCondition("nosuch > 1", v1.CurrentProfile)
	require.NoError(t, err)
	assert.NoError(t, v1.CheckDebugConditionScope(compiled, v1.CurrentProfile, nil, nil))
}

// TestABindingSpelledLikeATypeIsAScopeRead: a name the program binds is read
// from scope wherever it is written, even when it is also a type's name. The
// activation answers a bound name first, so inside the loop that binds
// `string` the condition reads the binding, and outside it the name reads
// nothing: refused, not taken for the type (Codex, #2202). A type name the
// program never binds is still the type.
func TestABindingSpelledLikeATypeIsAScopeRead(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{Name: "typed", Steps: []*v1.Node{
		{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewLiteralList("x", "y"), Iterator: "string",
			Body: []*v1.Node{{Id: "body", Kind: &v1.Node_Value{Value: v1.NewExpr("string")}}},
		}}},
		{Id: "done", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}},
	}}
	sites, truncated := v1.DebugStaticSites(wf)
	require.False(t, truncated)
	check := func(step, condition string) error {
		t.Helper()
		target, err := v1.ParseDebugTarget(step)
		require.NoError(t, err)
		compiled, err := v1.CompileDebugCondition(condition, v1.CurrentProfile)
		require.NoError(t, err)

		return v1.CheckDebugConditionScope(compiled, v1.CurrentProfile, target.Resolve(sites), sites)
	}

	assert.NoError(t, check("body", `string == "x"`), "the binding was refused inside the loop that binds it")
	err := check("done", `string == "x"`)
	if assert.Error(t, err, "a binding named outside its loop was taken for the type") {
		assert.Contains(t, err.Error(), "`string` is bound only inside")
	}
	assert.NoError(t, check("done", "type(1) == int"), "a type name the program never binds was refused")
}

// TestAParseOnlyConditionIsJudgedAlike: a condition [v1.CompileDebugCondition]
// returns has been through cel-go's checker, which rewrites a namespaced call
// to one targetless call and a qualified type name to one identifier. One it
// could not check, when the checking environment cannot be built, arrives as
// parsed: `math` a call's target and `google.protobuf.Timestamp` a chain of
// selects. The walk reads both shapes the same way.
func TestAParseOnlyConditionIsJudgedAlike(t *testing.T) {
	t.Parallel()

	sites, truncated := v1.DebugStaticSites(scopedProgram())
	require.False(t, truncated)
	target, err := v1.ParseDebugTarget("compose")
	require.NoError(t, err)
	at := target.Resolve(sites)

	for condition, refused := range map[string]string{
		`math.ceil(1.5) > 1.0 && base64.encode(b"x") != ""`: "",
		"type(steps) != google.protobuf.Timestamp":          "",
		"nosuch.size() > 1":                                 "`nosuch` is not bound",
	} {
		parsed := v1.NewExpr(condition)
		require.NotNil(t, parsed.GetExpr(), condition)
		err := v1.CheckDebugConditionScope(parsed, v1.CurrentProfile, at, sites)
		if refused == "" {
			assert.NoError(t, err, condition)

			continue
		}
		if assert.Error(t, err, condition) {
			assert.Contains(t, err.Error(), refused, condition)
		}
	}
}
