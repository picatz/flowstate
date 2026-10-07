package principal_test

import (
	"errors"
	"testing"

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

func TestCallerMap_refusedClaimsRenderTheErroringCarrier(t *testing.T) {
	refused := principal.Caller{Claims: principal.RefusedClaims(nil, errors.New("over bound"))}
	require.Equal(t, refused.Claims, refused.Map()["claims"])

	normal := principal.Caller{Claims: principal.NewClaims(map[string]any{"k": "v"})}
	require.Equal(t, map[string]any{"k": "v"}, normal.Map()["claims"])
}
