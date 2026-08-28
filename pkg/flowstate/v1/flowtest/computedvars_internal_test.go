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

// TestTaintedVarsFollowsEveryReader is the taint's direction, which a real
// file's data cannot drive on its own: the seed is a syntactic fact from one
// walk and the closure a fact about another, and the two only meet here.
//
// The negative direction is the one that matters, and it is asserted: a var
// that reads nothing tainted is not tainted, whatever it sits beside.
func TestTaintedVarsFollowsEveryReader(t *testing.T) {
	t.Parallel()

	declared := map[string]*varDeclaration{
		"header":     {deps: []string{"token"}},
		"envelope":   {deps: []string{"header", "region"}},
		"unrelated":  {deps: []string{"region"}},
		"selfsecret": {deps: []string{}},
	}

	tainted := taintedVars(declared, map[string]bool{"token": true, "selfsecret": true})

	assert.Equal(t, []string{"envelope", "header", "selfsecret", "token"}, tainted)
	assert.NotContains(t, tainted, "unrelated",
		"a var reading only untainted siblings must not be withheld")
	assert.NotContains(t, tainted, "region")
}

// TestWithheldMaterialKeepsOnlyComputedVars is the narrowing #1072's repair 4
// turns on: a literal var named by `secrets:` is already that case's own
// secret, and withholding it file-wide would change what a case that never
// named it redacts.
func TestWithheldMaterialKeepsOnlyComputedVars(t *testing.T) {
	t.Parallel()

	declared := map[string]*varDeclaration{"header": {deps: []string{"token"}}}
	values := map[string]any{
		"token":  "s3cr3t",
		"header": map[string]any{"Authorization": "Bearer s3cr3t"},
	}
	resolved := map[string]bool{"token": true, "header": true}

	p := newProblems(nil)
	withheld := withheldMaterial(p, at("vars"), declared, []string{"header", "token"}, resolved, values)

	require.Nil(t, p.err())
	assert.Equal(t, []string{"header"}, withheld.names)
	assert.Equal(t, []string{"Authorization", "Bearer s3cr3t"}, withheld.text,
		"a map's keys carry material as readily as its values")
}

// TestWithheldMaterialRefusesAValueItCannotAffordToProtect: over the bound the
// file is refused rather than partly protected. Fail closed.
func TestWithheldMaterialRefusesAValueItCannotAffordToProtect(t *testing.T) {
	t.Parallel()

	sprawl := make([]any, maxWithheldVarStrings+1)
	for i := range sprawl {
		sprawl[i] = string(rune('a' + i%26))
	}

	p := newProblems(nil)
	withheld := withheldMaterial(p, at("vars"),
		map[string]*varDeclaration{"sprawl": {deps: []string{"token"}}},
		[]string{"sprawl"}, map[string]bool{"sprawl": true},
		map[string]any{"sprawl": sprawl})

	refused := p.err()
	require.NotNil(t, refused)
	assert.Contains(t, refused.Error(), "vars.sprawl is computed from a secret and holds")
	assert.Empty(t, withheld.text, "a value the set cannot hold contributes nothing to it")
	assert.Equal(t, []string{"sprawl"}, withheld.names,
		"the name is still withheld, because the refusal is about the material and not the var")
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
