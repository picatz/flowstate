package flowstatev1

import (
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// One walk yields both the reads and the closed principals. The table asserts
// both from the single entry point, then asserts that the public surfaces
// (the compiled predicate's Reads and SignalPolicyClosedPrincipals) report
// exactly what that walk did, so a second traversal reintroduced behind either
// of them disagrees with a row here.
func TestAnalyzeSignalPredicateYieldsReadsAndClosedPrincipalsFromOneWalk(t *testing.T) {
	t.Parallel()

	const a, b = "https://i#a", "https://i#b"
	principalA := `sender.identity.principal == "` + a + `"`
	principalB := `sender.identity.principal == "` + b + `"`

	tests := []struct {
		name       string
		src        string
		inputNames []string
		inputs     bool
		run        bool
		claims     bool
		principals []string // nil means open
	}{
		{name: "principal literal", src: principalA, principals: []string{a}},
		{name: "in list", src: `sender.identity.principal in ["` + a + `", "` + b + `"]`, principals: []string{a, b}},
		{name: "or unions", src: principalA + ` || ` + principalB, principals: []string{a, b}},
		{
			name: "and narrows by claims", src: principalA + ` && sender.identity.claims["team"] == "x"`,
			claims: true, principals: []string{a},
		},
		{
			name: "and intersects", src: `sender.identity.principal in ["` + a + `", "` + b + `"] && ` + principalB,
			principals: []string{b},
		},
		{
			name: "run read does not open a closed set", src: principalA + ` && sender.identity.principal != run.identity.principal`,
			run: true, principals: []string{a},
		},
		{
			name: "inputs named in a closed and", src: principalA + ` && inputs.who == "x"`,
			inputs: true, inputNames: []string{"who"}, principals: []string{a},
		},
		{
			name: "or with an open side is open", src: principalA + ` || sender.identity.claims["team"] == "x"`,
			claims: true,
		},
		{name: "claims only is open", src: `sender.identity.claims["team"] == "x"`, claims: true},
		{name: "negation is open", src: `!(` + principalA + `)`},
		{name: "not-equal to a literal is open", src: `sender.identity.principal != "` + a + `"`},
		{
			name: "computed literal is open", src: `sender.identity.principal == "x#" + inputs.who && run.identity.kind == "k"`,
			inputs: true, inputNames: []string{"who"}, run: true,
		},
		{
			name: "comprehension local named run is not a read", src: `[1].exists(run, run == 1) && ` + principalA,
			principals: []string{a},
		},
		{
			name: "input keys by select, index, and membership", src: `inputs.a == 1 && inputs["b"] == 2 && "c" in inputs && sender.identity.claims.t == "x"`,
			inputs: true, inputNames: []string{"a", "b", "c"}, claims: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env, err := signalPolicyEnv()
			require.NoError(t, err)
			checked, issues := env.Compile(tc.src)
			require.NoError(t, issues.Err())

			got := analyzeSignalPredicate(checked)
			require.Equal(t, tc.inputs, got.reads.inputs, "inputs")
			require.Equal(t, tc.run, got.reads.run, "run")
			require.Equal(t, tc.claims, got.reads.claims, "claims")
			require.False(t, got.reads.opaqueInputs, "opaque inputs")
			require.Equal(t, tc.inputNames, slices.Sorted(maps.Keys(got.reads.inputNames)), "input names")
			require.Equal(t, tc.principals != nil, got.closed, "closed")
			if tc.principals != nil {
				require.Equal(t, tc.principals, slices.Sorted(maps.Keys(got.principals)), "principals")
			}

			// The public answers are the walk's answers.
			principals, closed := SignalPolicyClosedPrincipals(&SignalPolicy{Allow: tc.src})
			require.Equal(t, got.closed, closed)
			if closed {
				require.Equal(t, tc.principals, principals)
			}

			p, err := CompileSignalPolicyPredicate(tc.src)
			if err == nil {
				require.Equal(t, SignalPolicyReads{
					Inputs: got.reads.inputs, InputNames: slices.Sorted(maps.Keys(got.reads.inputNames)),
					Run: got.reads.run, Claims: got.reads.claims,
				}, p.Reads())
			}
		})
	}
}

// A long chain of `||` over principals stays closed through all of it, and each
// principal survives the unions that fold the chain's sets together. (That the
// walk is iterative is by construction, not something a parseable predicate is
// deep enough to prove: the parser's own depth limit is lower than the goroutine
// stack's.)
func TestAnalyzeSignalPredicateKeepsALongOrChainClosed(t *testing.T) {
	t.Parallel()

	const terms = 80
	parts := make([]string, terms)
	for i := range parts {
		parts[i] = `sender.identity.principal == "p` + string(rune('a'+i%26)) + string(rune('a'+i/26)) + `"`
	}

	principals, closed := SignalPolicyClosedPrincipals(&SignalPolicy{Allow: strings.Join(parts, " || ")})
	require.True(t, closed)
	require.Len(t, principals, terms)
}
