package flowtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The three decisions computed vars turn on, driven directly rather than
// through a document — because in a real file the answers agree with
// themselves, and a check written where they agree is one no fixture can reach
// (CLAUDE.md, "assert where the answers differ").

// TestVarOrderPutsEveryDependencyFirst drives the sort over graphs a real file
// would rarely have: a diamond, a chain whose name order is the reverse of its
// dependency order, and a var nothing reads.
func TestVarOrderPutsEveryDependencyFirst(t *testing.T) {
	t.Parallel()

	declared := map[string]*varDeclaration{
		"a": {deps: []string{"b", "c"}},
		"b": {deps: []string{"d"}},
		"c": {deps: []string{"d"}},
		"d": {},
		"z": {},
	}

	order, cycles := varOrder(declared)
	require.Empty(t, cycles)
	require.Len(t, order, len(declared), "every declared var is ordered exactly once")

	position := map[string]int{}
	for i, name := range order {
		require.NotContains(t, position, name, "%s was ordered twice", name)
		position[name] = i
	}
	for name, d := range declared {
		for _, dep := range d.deps {
			assert.Less(t, position[dep], position[name], "%s must be evaluated before %s", dep, name)
		}
	}
}

// TestVarOrderNamesTheCycleItFound: the path, in the order the hops close it,
// which is the sentence an author acts on. A set of "these are cyclic" is what
// this test exists to refuse.
func TestVarOrderNamesTheCycleItFound(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		declared map[string]*varDeclaration
		want     []string
	}{
		{
			name:     "a var reading itself",
			declared: map[string]*varDeclaration{"a": {deps: []string{"a"}}},
			want:     []string{"a", "a"},
		},
		{
			name: "a pair",
			declared: map[string]*varDeclaration{
				"a": {deps: []string{"b"}},
				"b": {deps: []string{"a"}},
			},
			want: []string{"a", "b", "a"},
		},
		{
			name: "a cycle reached through an acyclic var",
			declared: map[string]*varDeclaration{
				"entry": {deps: []string{"b"}},
				"b":     {deps: []string{"c"}},
				"c":     {deps: []string{"b"}},
			},
			want: []string{"b", "c", "b"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, cycles := varOrder(tc.declared)
			require.Len(t, cycles, 1)
			assert.Equal(t, tc.want, cycles[0])
		})
	}
}

// TestTaintedVarsReachesSourcesAndReaders drives the closure both ways at
// once, over a graph whose answers differ per direction — which a real file's
// data cannot do on its own, since the seed is a syntactic fact from one walk
// and the closure a fact about another.
//
// Three claims, and the third is the one the union of two one-shot closures
// would get wrong: `sibling` reads a var that is only reachable *backward*
// from a seed, and is tainted because a var reached backward is itself secret
// material and the forward rule then applies to it by its own terms.
//
// The negative direction is asserted too: a var with no path to any seed is
// not tainted, whatever it sits beside.
func TestTaintedVarsReachesSourcesAndReaders(t *testing.T) {
	t.Parallel()

	declared := map[string]*varDeclaration{
		// Forward: reads a seed.
		"reader": {deps: []string{"derived"}},
		// The seed itself, computed from two sources.
		"derived": {deps: []string{"prefix", "token"}},
		// Backward-reached `token`'s other reader, two hops out.
		"sibling": {deps: []string{"token"}},
		// Nothing on any path to a seed.
		"unrelated": {deps: []string{"region"}},
	}

	taint := taintedVars(declared, map[string]string{"derived": `tests[0].secrets["auth"]`})

	assert.Equal(t, []string{"derived", "prefix", "reader", "sibling", "token"}, taint.names())
	assert.True(t, taint.holds("token"), "a source of a secret is secret")
	assert.True(t, taint.holds("prefix"), "so is the benign half of it; the cost is accepted")
	assert.True(t, taint.holds("sibling"),
		"a var reading a backward-reached source holds that source's material too")
	assert.False(t, taint.holds("unrelated"))
	assert.False(t, taint.holds("region"))
}

// TestTaintPathNamesTheChainAndTheSecretsEntry: a refusal claims a var is
// derived from a secret, and an author can only check that claim if they are
// shown the chain.
func TestTaintPathNamesTheChainAndTheSecretsEntry(t *testing.T) {
	t.Parallel()

	taint := taintedVars(
		map[string]*varDeclaration{
			"derived": {deps: []string{"token"}},
			"token":   {deps: []string{"seedmaterial"}},
		},
		map[string]string{"derived": `tests[2].secrets["env:TOKEN"]`})

	assert.Equal(t, `vars.derived, which tests[2].secrets["env:TOKEN"] references`, taint.path("derived"))
	assert.Equal(t, `vars.token → vars.derived, which tests[2].secrets["env:TOKEN"] references`,
		taint.path("token"))
	assert.Equal(t,
		`vars.seedmaterial → vars.token → vars.derived, which tests[2].secrets["env:TOKEN"] references`,
		taint.path("seedmaterial"))
}

// TestUnprotectableValueNamesWhatRedactionCannotMatch is the decision the
// non-string refusal turns on, taken as a value: in a real document a tainted
// var is nearly always a string, so a check written where the values agree is
// one no fixture could reach.
func TestUnprotectableValueNamesWhatRedactionCannotMatch(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		value any
		kind  string
	}{
		"a string":            {value: "s3cr3t"},
		"the empty string":    {value: ""},
		"a map of strings":    {value: map[string]any{"a": "b"}},
		"a list of strings":   {value: []any{"a", "b"}},
		"an empty map":        {value: map[string]any{}},
		"an empty list":       {value: []any{}},
		"an integer":          {value: int64(12), kind: "an integer"},
		"a boolean":           {value: true, kind: "a boolean"},
		"a double":            {value: 1.5, kind: "a number"},
		"null":                {value: nil, kind: "null"},
		"an integer in a map": {value: map[string]any{"len": int64(3)}, kind: "an integer"},
		"a boolean deep in a list": {value: []any{"a", map[string]any{"ok": false}},
			kind: "a boolean"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			kind, unprotectable := unprotectableValue(tc.value, 0)
			assert.Equal(t, tc.kind != "", unprotectable)
			assert.Equal(t, tc.kind, kind)
		})
	}
}

// TestUnprotectableValueNamesTheFirstLeafEveryTime: two unprotectable leaves
// in one map must name the same one on every run, or a diagnostic depends on
// a map's iteration order.
func TestUnprotectableValueNamesTheFirstLeafEveryTime(t *testing.T) {
	t.Parallel()

	value := map[string]any{"b": true, "a": int64(1), "c": 2.5}
	for range 20 {
		kind, unprotectable := unprotectableValue(value, 0)
		require.True(t, unprotectable)
		require.Equal(t, "an integer", kind, "the sorted-first key decides, every time")
	}
}

// TestWithheldMaterialKeepsEveryTaintedVarButALiteralSeed is the one narrowing
// that survives: a literal var named by `secrets:` is already that case's own
// secret, and withholding it file-wide would change what a case that never
// named it redacts. Everything else the closure reaches is withheld, computed
// or not — a literal can only be tainted by standing between expressions.
func TestWithheldMaterialKeepsEveryTaintedVarButALiteralSeed(t *testing.T) {
	t.Parallel()

	declared := map[string]*varDeclaration{"header": {deps: []string{"token", "prefix"}}}
	values := map[string]any{
		"token":  "s3cr3t",
		"prefix": "Bearer",
		"header": map[string]any{"Authorization": "Bearer s3cr3t"},
	}
	resolved := map[string]bool{"token": true, "prefix": true, "header": true}

	p := newProblems(nil)

	// `header` is the seed and is computed; `token` and `prefix` are literals
	// reached backward through it.
	taint := taintedVars(declared, map[string]string{"header": `tests[0].secrets["auth"]`})
	withheld := withheldMaterial(p, at("vars"), declared, taint, resolved, values)

	require.Nil(t, p.err())
	assert.Equal(t, []string{"header", "prefix", "token"}, withheld.names)
	assert.Equal(t, []string{"Authorization", "Bearer", "Bearer s3cr3t", "s3cr3t"}, withheld.text,
		"a map's keys carry material as readily as its values")

	// And the exclusion, driven where it applies: a *literal* seed.
	p = newProblems(nil)
	literalSeed := withheldMaterial(p, at("vars"), map[string]*varDeclaration{},
		taintedVars(nil, map[string]string{"token": `tests[0].secrets["auth"]`}),
		map[string]bool{"token": true}, map[string]any{"token": "s3cr3t"})

	require.Nil(t, p.err())
	assert.Empty(t, literalSeed.names,
		"a literal named straight from `secrets:` is already that case's own secret")
}

// TestWithheldMaterialRefusesAValueItCannotAffordToProtect: over the bound the
// file is refused rather than partly protected. Fail closed.
func TestWithheldMaterialRefusesAValueItCannotAffordToProtect(t *testing.T) {
	t.Parallel()

	sprawl := make([]any, maxWithheldVarStrings+1)
	for i := range sprawl {
		sprawl[i] = string(rune('a' + i%26))
	}

	declared := map[string]*varDeclaration{"sprawl": {deps: []string{"token"}}}
	p := newProblems(nil)
	withheld := withheldMaterial(p, at("vars"), declared,
		taintedVars(declared, map[string]string{"token": `tests[0].secrets["auth"]`}),
		map[string]bool{"sprawl": true},
		map[string]any{"sprawl": sprawl})

	refused := p.err()
	require.NotNil(t, refused)
	assert.Contains(t, refused.Error(), "vars.sprawl is computed from a secret and holds")
	assert.Empty(t, withheld.text, "a value the set cannot hold contributes nothing to it")
	assert.Contains(t, withheld.names, "sprawl",
		"the name is still withheld, because the refusal is about the material and not the var")
}

// TestRedactedVarsWithholdsAWithheldVarWhole drives the autopsy's own view of
// the block, which is otherwise reached only with a debugger attached — and
// where the substring backstop alone gives a *different* answer from the
// withholding, which is what makes the two distinguishable at all.
func TestRedactedVarsWithholdsAWithheldVarWhole(t *testing.T) {
	t.Parallel()

	vars := fileVars{
		values: map[string]any{
			"token":  "s3cr3t",
			"header": "Bearer s3cr3t",
			"region": "eu-west-1",
		},
		withheld: withheldVars{names: []string{"header"}},
	}
	sensitive := sensitiveInputs{}.WithValues("s3cr3t")

	shown := redactedVars(vars, sensitive)

	assert.Equal(t, sensitiveMarker, shown["header"],
		"a withheld var is replaced whole, not merely cleared of the secret inside it")
	assert.Equal(t, sensitiveMarker, shown["token"],
		"the plain set still answers for a value that *is* the secret")
	assert.Equal(t, "eu-west-1", shown["region"],
		"a var the file does not withhold is shown, or the autopsy stops being useful")
}

// TestWithheldCoversAPathAndNotItsNeighbour is the prefix test, written where
// the answers differ: `vars.token` and `vars.tokenish` share a prefix, and a
// naive [strings.HasPrefix] withholds a var the file never said to withhold —
// a false redaction, which reads to an author as a value their check could not
// see.
func TestWithheldCoversAPathAndNotItsNeighbour(t *testing.T) {
	t.Parallel()

	withheld := withheldVars{names: []string{"token"}}

	for path, want := range map[string]bool{
		"vars.token":            true,
		"vars.token.prefix":     true,
		"vars.token['k']":       true,
		"vars.token[0]":         true,
		"vars.tokenish":         false,
		"vars.tokenish.field":   false,
		"vars.other":            false,
		"steps.token.value":     false,
		"run.error":             false,
		"inputs.token":          false,
		"vars":                  false,
		"varstoken":             false,
		"prefix.vars.token.bit": false,
	} {
		assert.Equal(t, want, withheld.covers(path), "covers(%q)", path)
	}
}
