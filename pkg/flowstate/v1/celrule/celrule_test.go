package celrule_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/celrule"
)

func env(t *testing.T) *cel.Env {
	t.Helper()
	e, err := cel.NewEnv(cel.Variable("name", cel.StringType), cel.Variable("n", cel.IntType))
	require.NoError(t, err)
	return e
}

func compile(t *testing.T, e *cel.Env, srcs ...string) []celrule.Rule {
	t.Helper()
	rules, err := celrule.CompileAll(e, celrule.Allow, srcs, 50_000, func(_ celrule.Kind, err error) error { return err })
	require.NoError(t, err)
	return rules
}

// TestCompileRefusesWhatEverySurfaceRefused pins the four refusals at load,
// in the words each surface's start-up refusal is built from: empty, invalid,
// not a bool, and — through CompileAll — wrapped with the kind.
func TestCompileRefusesWhatEverySurfaceRefused(t *testing.T) {
	t.Parallel()
	e := env(t)

	_, err := celrule.Compile(e, "", 50_000)
	assert.EqualError(t, err, "rule must not be empty")

	_, err = celrule.Compile(e, "nonexistent == 1", 50_000)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `rule "nonexistent == 1" is invalid: `)

	_, err = celrule.Compile(e, `name + "x"`, 50_000)
	assert.EqualError(t, err, `rule "name + \"x\"" evaluates to string, want bool`)

	_, err = celrule.CompileAll(e, celrule.Deny, []string{"true", ""}, 50_000, func(kind celrule.Kind, err error) error {
		return errors.New("policy: " + string(kind) + " " + err.Error())
	})
	assert.EqualError(t, err, "policy: deny rule must not be empty")
}

// TestDecideIsDenyFirstAndTheZeroAllowAnswerIsDeclared is the table the
// surfaces used to each be an accident of: deny rules run first and win, allow
// rules gate when configured, and a set of deny rules alone answers what its
// WithoutAllow says — permitting for a task-shape or egress policy, refusing
// for a secret policy (#1634 review, F5).
func TestDecideIsDenyFirstAndTheZeroAllowAnswerIsDeclared(t *testing.T) {
	t.Parallel()
	e := env(t)
	vars := map[string]any{"name": "alice", "n": int64(3)}

	for _, test := range []struct {
		name      string
		set       celrule.Set
		verdict   celrule.Verdict
		decidedBy string
	}{
		{"a matching deny wins over a matching allow",
			celrule.Set{Deny: compile(t, e, `n > 2`), Allow: compile(t, e, `true`)}, celrule.DeniedByRule, "n > 2"},
		{"the second deny is reached when the first misses",
			celrule.Set{Deny: compile(t, e, `n > 5`, `name == "alice"`)}, celrule.DeniedByRule, `name == "alice"`},
		{"an allow that matches permits and is named",
			celrule.Set{Allow: compile(t, e, `n > 5`, `name == "alice"`)}, celrule.Permitted, `name == "alice"`},
		{"allow rules configured and none matched refuses",
			celrule.Set{Allow: compile(t, e, `n > 5`)}, celrule.NoAllowRuleMatched, ""},
		{"deny rules alone permit by default",
			celrule.Set{Deny: compile(t, e, `n > 5`)}, celrule.Permitted, ""},
		{"deny rules alone refuse when the set says so",
			celrule.Set{Deny: compile(t, e, `n > 5`), WithoutAllow: celrule.NoAllowRules}, celrule.NoAllowRules, ""},
		{"an empty set permits",
			celrule.Set{}, celrule.Permitted, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			decision, err := test.set.Decide(context.Background(), vars)
			require.NoError(t, err)
			assert.Equal(t, test.verdict, decision.Verdict)
			assert.Equal(t, test.decidedBy, decision.Rule.Source())
		})
	}
}

// TestAnEvaluationFailureIsARefusalUnlessTheContextEnded pins the fail-closed
// half: a rule that cannot be evaluated is reported with its kind and source,
// and a context that ended is reported as itself, since running out of time
// is not a policy decision.
func TestAnEvaluationFailureIsARefusalUnlessTheContextEnded(t *testing.T) {
	t.Parallel()
	e := env(t)

	// A division by zero is a runtime error the checker cannot see.
	set := celrule.Set{Deny: compile(t, e, `n / (n - 3) > 0`)}
	_, err := set.Decide(context.Background(), map[string]any{"name": "a", "n": int64(3)})
	var failed *celrule.EvalError
	require.ErrorAs(t, err, &failed)
	assert.Equal(t, celrule.Deny, failed.Kind)
	assert.Equal(t, `n / (n - 3) > 0`, failed.Rule.Source())
	assert.Contains(t, err.Error(), `deny rule "n / (n - 3) > 0" could not be evaluated: `)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = set.Decide(ctx, map[string]any{"name": "a", "n": int64(3)})
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, errors.As(err, &failed), "a cancelled context is not a policy decision")
}
