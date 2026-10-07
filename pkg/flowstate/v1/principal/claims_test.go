package principal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

func TestClaims_carrierReadsEveryShape(t *testing.T) {
	env := newEnv(t)
	c := principal.Caller{Claims: principal.NewClaims(map[string]any{
		"repo":   "x/y",
		"groups": []any{"eng", "oncall"},
		"slack":  map[string]any{"user": "U1", "teams": []any{"a"}},
		"admin":  true,
		"level":  float64(3),
	})}

	for expr, want := range map[string]any{
		`identity.claims.repo == "x/y"`:                     true,
		`identity.claims["repo"] == "x/y"`:                  true,
		`"oncall" in identity.claims.groups`:                true,
		`"sales" in identity.claims.groups`:                 false,
		`identity.claims.groups.exists(g, g == "eng")`:      true,
		`identity.claims.slack.user == "U1"`:                true,
		`"U1" == identity.claims.slack["user"]`:             true,
		`"a" in identity.claims.slack.teams`:                true,
		`"slack" in identity.claims`:                        true,
		`"nope" in identity.claims`:                         false,
		`identity.claims.admin`:                             true,
		`identity.claims.level == 3.0`:                      true,
		`size(identity.claims)`:                             int64(5),
		`size(identity.claims.groups)`:                      int64(2),
		`identity.claims.all(k, k != "")`:                   true,
		`has(identity.claims.groups)`:                       true,
		`has(identity.claims.nope)`:                         false,
		`"nope" in identity.claims && identity.claims.nope`: false,
	} {
		got, err := eval(t, env, expr, c)
		require.NoError(t, err, expr)
		require.Equal(t, want, got, expr)
	}
}

func TestClaims_unknownReadsFailClosed(t *testing.T) {
	env := newEnv(t)
	c := principal.Caller{Claims: principal.NewClaims(map[string]any{"slack": map[string]any{"user": "U1"}})}
	for _, expr := range []string{
		`identity.claims.groups == ["x"]`,
		`"x" in identity.claims.groups`,
		`identity.claims.slack.team == "t"`,
		`identity.claims["nope"] == "v"`,
	} {
		_, err := eval(t, env, expr, c)
		require.Error(t, err, expr)
		_, err = eval(t, env, expr, principal.Caller{})
		require.Error(t, err, expr)
	}
}
