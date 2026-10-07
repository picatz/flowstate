package principal_test

import (
	"errors"
	"testing"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// TestClaims_refusedClaimsErrorOnEveryRead pins the fail-closed reading of a
// claim set that was over its bounds: absence tests, presence tests, size and
// comprehensions all error, so no rule can permit against a set that lacks the
// refused claim.
func TestClaims_refusedClaimsErrorOnEveryRead(t *testing.T) {
	env := newEnv(t)
	refused := principal.Caller{Claims: principal.RefusedClaims(
		map[string]any{"repo": "x/y"}, errors.New("claim too deep"))}
	normal := principal.Caller{Claims: principal.NewClaims(map[string]any{"repo": "x/y"})}

	for _, expr := range []string{
		`!("k" in identity.claims)`,
		`"contractors" in identity.claims.groups`,
		`!("contractors" in identity.claims.groups)`,
		`"repo" in identity.claims`,
		`identity.claims.repo == "x/y"`,
		`identity.claims["repo"] == "x/y"`,
		`has(identity.claims.nope)`,
		`size(identity.claims) == 1`,
		`identity.claims.all(k, k != "never")`,
		`identity.claims.exists(k, k == "never")`,
		`identity.claims == {"repo": "x/y"}`,
		// The operator forms the carrier alone could not refuse: equality
		// dispatches on the left operand, and `!=` reads a non-true Equal as true.
		`identity.claims != {}`,
		`identity.claims == {}`,
		`{} == identity.claims`,
		`{} != identity.claims`,
		`{"repo": "x/y"} != identity.claims`,
		`identity.claims != identity.claims`,
		`identity.claims.all(k, true)`,
		`identity.claims.exists(k, false)`,
		`identity.claims.map(k, 1).size() > 0`,
		`identity.claims.filter(k, true).size() == 0`,
		`identity.claims in [{}]`,
		`identity.claims in [{"a": 1}]`,
		`!(identity.claims in [{"a": 1}])`,
		`[{}].exists(e, e == identity.claims)`,
		`identity.claims.size() == 0`,
		`[identity].all(i, i.claims != {})`,
	} {
		_, err := eval(t, env, expr, refused)
		require.Error(t, err, "refused claims, %s", expr)
		require.ErrorContains(t, err, "refused", expr)
	}

	// The same absence test on a normal caller evaluates, which is what makes
	// the refused case's error the difference and not a malformed rule.
	got, err := eval(t, env, `!("k" in identity.claims)`, normal)
	require.NoError(t, err)
	require.Equal(t, true, got)
}

// TestCallerBind_refusedKeepsTheOtherFields pins that only the claims are
// refused: every other field of the stand-in reads as the Caller's own, and
// `has` on the claims field itself answers without reading a claim.
func TestCallerBind_refusedKeepsTheOtherFields(t *testing.T) {
	env := newEnv(t)
	refused := principal.Caller{
		Issuer: "https://i", Subject: "s", Namespace: "team-a", Kind: "agent",
		Principal: "https://i#s", Actions: []string{"run"},
		Actors: []principal.Actor{{Issuer: "https://a", Subject: "bot"}},
		Claims: principal.RefusedClaims(nil, errors.New("over bound")),
	}

	for _, expr := range []string{
		`identity.namespace == "team-a"`,
		`identity.kind == "agent" && identity.principal == "https://i#s"`,
		`"run" in identity.actions`,
		`identity.delegated && identity.actors[0].subject == "bot"`,
		`has(identity.kind) && has(identity.claims)`,
	} {
		got, err := eval(t, env, expr, refused)
		require.NoError(t, err, expr)
		require.Equal(t, true, got, expr)
	}
}

// TestClaims_refusedCarrierErrorsWithoutBind pins the second line: a Caller
// bound as a struct, without [principal.Caller.Bind], still errors on every
// read the carrier itself serves, though it cannot refuse the operator forms.
func TestClaims_refusedCarrierErrorsWithoutBind(t *testing.T) {
	env := newEnv(t)
	ast, iss := env.Compile(`"k" in identity.claims`)
	require.NoError(t, iss.Err())
	prg, err := env.Program(ast)
	require.NoError(t, err)

	refused := principal.Caller{Claims: principal.RefusedClaims(nil, errors.New("over bound"))}
	_, _, err = prg.Eval(map[string]any{"identity": refused.Normalized()})
	require.ErrorContains(t, err, "refused")
}

func TestCallerMap_refusedClaimsRenderAnErrorValue(t *testing.T) {
	refused := principal.Caller{Claims: principal.RefusedClaims(nil, errors.New("over bound"))}
	got, ok := refused.Map()["claims"].(ref.Val)
	require.True(t, ok)
	require.True(t, types.IsError(got))
	require.ErrorContains(t, got.(error), "refused")
	require.Equal(t, map[string]any{}, refused.Claims.Map())

	// A normal caller binds as itself.
	require.Equal(t, principal.Caller{}, principal.Caller{}.Bind())

	normal := principal.Caller{Claims: principal.NewClaims(map[string]any{"k": "v"})}
	require.Equal(t, map[string]any{"k": "v"}, normal.Map()["claims"])
}
