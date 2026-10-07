package principal_test

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

func newEnv(t *testing.T) *cel.Env {
	t.Helper()
	env, err := cel.NewEnv(principal.EnvOptions(), principal.Var("identity"))
	require.NoError(t, err)

	return env
}

func eval(t *testing.T, env *cel.Env, expr string, c principal.Caller) (any, error) {
	t.Helper()
	ast, iss := env.Compile(expr)
	require.NoError(t, iss.Err(), expr)
	prg, err := env.Program(ast)
	require.NoError(t, err)
	out, _, err := prg.Eval(map[string]any{"identity": c.Normalized()})
	if err != nil {
		return nil, err
	}

	return out.Value(), nil
}

func TestTypeName_isPinned(t *testing.T) {
	require.Equal(t, "principal.Caller", principal.TypeName)

	// The registered type really carries that name: a mismatch would fail to
	// compile any rule that touches the variable.
	ast, iss := newEnv(t).Compile("identity")
	require.NoError(t, iss.Err())
	require.Equal(t, principal.TypeName, ast.OutputType().TypeName())
}

func TestCaller_Normalized(t *testing.T) {
	n := principal.Caller{}.Normalized()
	require.NotNil(t, n.Claims.Map())
	require.NotNil(t, n.Actions)
	require.Zero(t, n.Claims.Len())
	require.Empty(t, n.Actions)
	require.NotNil(t, n.Actors)
	require.False(t, n.Delegated)

	in := principal.Caller{
		Subject: "s", Claims: principal.StringClaims(map[string]string{"k": "v"}), Actions: []string{"a"},
		Actors: []principal.Actor{{Issuer: "https://agents", Subject: "bot"}}, Delegated: true,
	}
	require.Equal(t, in, in.Normalized(), "populated values are untouched")

	// delegated is derived from the chain, so a hand-built Caller cannot claim
	// one without the other.
	forged := principal.Caller{Delegated: true}.Normalized()
	require.False(t, forged.Delegated, "delegated without actors is not delegated")
	unflagged := principal.Caller{Actors: []principal.Actor{{Issuer: "i", Subject: "s"}}}.Normalized()
	require.True(t, unflagged.Delegated, "actors without the flag is delegated")
}

func TestCaller_actorsReadableFromExpression(t *testing.T) {
	env := newEnv(t)
	c := principal.Caller{
		Issuer: "https://idp", Subject: "alice",
		Actors: []principal.Actor{
			{Issuer: "https://agents", Subject: "triage-bot"},
			{Issuer: "https://platform", Subject: "orchestrator"},
		},
	}

	for expr, want := range map[string]any{
		`identity.delegated`:                                           true,
		`size(identity.actors)`:                                        int64(2),
		`identity.actors[0].subject == "triage-bot"`:                   true,
		`identity.actors[1].issuer == "https://platform"`:              true,
		`!identity.delegated || identity.actors[0].subject == "other"`: false,
		`identity.actors.exists(a, a.subject == "orchestrator")`:       true,
	} {
		got, err := eval(t, env, expr, c)
		require.NoError(t, err, expr)
		require.Equal(t, want, got, expr)
	}

	// An undelegated caller passes the guard without indexing an empty list.
	got, err := eval(t, env, `!identity.delegated || identity.actors[0].subject == "other"`, principal.Caller{Subject: "alice"})
	require.NoError(t, err)
	require.Equal(t, true, got)

	// Indexing past the chain errors, which every surface reads as a denial.
	_, err = eval(t, env, `identity.actors[0].subject == "x"`, principal.Caller{})
	require.Error(t, err)

	// Map renders the same chain for the surfaces that bind a plain map.
	m := c.Map()
	require.Equal(t, true, m["delegated"])
	require.Equal(t, []any{
		map[string]any{"issuer": "https://agents", "subject": "triage-bot"},
		map[string]any{"issuer": "https://platform", "subject": "orchestrator"},
	}, m["actors"])
	require.Equal(t, []any{}, principal.Caller{}.Map()["actors"])
	require.Equal(t, false, principal.Caller{}.Map()["delegated"])
	require.Equal(t, "https://agents#triage-bot", c.Actors[0].String())
}

func TestCaller_readableFromExpression(t *testing.T) {
	env := newEnv(t)
	c := principal.Caller{
		Issuer: "https://idp", Subject: "ci", Namespace: "team-a", Kind: "workload",
		Principal: "https://idp#ci",
		Claims:    principal.StringClaims(map[string]string{"repo": "x/y"}),
		Actions:   []string{"run.start", "run.read"},
	}

	for expr, want := range map[string]any{
		`identity.kind == "workload"`:            true,
		`identity.kind == "human"`:               false,
		`"run.start" in identity.actions`:        true,
		`"run.delete" in identity.actions`:       false,
		`identity.principal == "https://idp#ci"`: true,
		`identity.claims["repo"] == "x/y"`:       true,
		`identity.namespace == "team-a"`:         true,
		`size(identity.actions)`:                 int64(2),
	} {
		got, err := eval(t, env, expr, c)
		require.NoError(t, err, expr)
		require.Equal(t, want, got, expr)
	}
}

func TestCaller_zeroValueDoesNotMatchOrError(t *testing.T) {
	env := newEnv(t)
	for expr, want := range map[string]any{
		`identity.kind == "workload"`: false,
		`"x" in identity.actions`:     false,
		`"k" in identity.claims`:      false,
		`identity.principal == ""`:    true,
		`size(identity.claims) == 0`:  true,
	} {
		got, err := eval(t, env, expr, principal.Caller{})
		require.NoError(t, err, expr)
		require.Equal(t, want, got, expr)
	}

	// An absent key still errors, as documented, so a rule must guard it.
	_, err := eval(t, env, `identity.claims["k"] == "v"`, principal.Caller{})
	require.Error(t, err)
}

func TestCaller_unknownFieldIsACompileError(t *testing.T) {
	env := newEnv(t)
	_, iss := env.Compile(`identity.nonexistent == "x"`)
	require.Error(t, iss.Err())
	_, iss = env.Compile(`identity.kind == 1`)
	require.Error(t, iss.Err(), "kind is a string")
}
