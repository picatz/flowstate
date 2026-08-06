package flowstatev1_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestTaskPolicyZeroCase pins #187's zero case: no policy configured
// restricts nothing, exactly current behavior. [v1.TaskPolicyIn] falling back
// to nil, and nil's [v1.TaskPolicy.Check] permitting everything, is what
// keeps an existing deployment's next run from being denied the day this
// ships.
func TestTaskPolicyZeroCase(t *testing.T) {
	require.Nil(t, v1.DefaultTaskPolicy(), "no policy installed at test start")

	err := v1.CheckTaskPolicy(context.Background(), "codex.exec", &v1.WorkloadIdentity{
		Subject:   "anyone@example.com",
		Namespace: "any-namespace",
	})
	require.NoError(t, err, "no policy configured must permit every dispatch")
}

// TestTaskPolicyDenyRuleDenies is the core fail-closed matrix case: a
// configured deny rule that matches refuses the dispatch, and the error names
// the task, the policy source (the rule text), and the remedy — CLAUDE.md's
// "Diagnostics are a feature" applied to a deployment refusal.
func TestTaskPolicyDenyRuleDenies(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`task == "codex.exec" && identity.namespace != "platform"`},
	}
	policy, err := cfg.Policy()
	require.NoError(t, err)

	err = policy.Check(context.Background(), "codex.exec", &v1.WorkloadIdentity{
		Namespace: "not-platform",
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, v1.ErrTaskPolicyDenied))

	var denied *v1.TaskPolicyDeniedError
	require.True(t, errors.As(err, &denied))
	require.Equal(t, "codex.exec", denied.Task)
	require.Equal(t, v1.TaskPolicyReasonDenyRule, denied.Reason)
	require.Contains(t, denied.Detail, `identity.namespace != "platform"`,
		"the denial must name the rule that fired, so an operator can find it")
	require.Contains(t, err.Error(), "codex.exec")
	require.Contains(t, err.Error(), "task-shape policy",
		"the message must read as a deployment refusal, not a task failure")
	require.Contains(t, err.Error(), "contact the operator",
		"the message must say what to do about the denial, not only that it happened")
}

// TestTaskPolicyDenyRuleAllowsOutsideMatch is the negative-direction pair of
// [TestTaskPolicyDenyRuleDenies], per CLAUDE.md's "test that A cannot reach
// B, not that A can reach A": a deny rule scoped to non-platform callers must
// let a platform caller *through*, proving the rule discriminates rather
// than merely existing.
func TestTaskPolicyDenyRuleAllowsOutsideMatch(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`task == "codex.exec" && identity.namespace != "platform"`},
	}
	policy, err := cfg.Policy()
	require.NoError(t, err)

	err = policy.Check(context.Background(), "codex.exec", &v1.WorkloadIdentity{
		Namespace: "platform",
	})
	require.NoError(t, err, "a platform caller must not be caught by a rule scoped to non-platform ones")

	// And a task the rule never names dispatches regardless of namespace.
	err = policy.Check(context.Background(), "log", &v1.WorkloadIdentity{
		Namespace: "not-platform",
	})
	require.NoError(t, err, "a deny rule scoped to codex.exec must not reach an unrelated task")
}

// TestTaskPolicyAllowlistDeniesNoMatch checks the allowlist half: configuring
// any allow rule turns the policy into an allowlist, and a dispatch matching
// none of them is denied with [v1.TaskPolicyReasonNoAllowRule].
func TestTaskPolicyAllowlistDeniesNoMatch(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Allow: []string{`task == "log"`},
	}
	policy, err := cfg.Policy()
	require.NoError(t, err)

	require.NoError(t, policy.Check(context.Background(), "log", &v1.WorkloadIdentity{}))

	err = policy.Check(context.Background(), "codex.exec", &v1.WorkloadIdentity{})
	require.Error(t, err)
	var denied *v1.TaskPolicyDeniedError
	require.True(t, errors.As(err, &denied))
	require.Equal(t, v1.TaskPolicyReasonNoAllowRule, denied.Reason)
}

// TestTaskPolicyRuleErrorDenies is the "errored rule denies" cell of the
// fail-closed matrix: a rule that type-checks (so it compiles and loads
// successfully) but fails at evaluation time — here, integer division by
// zero, which CEL only catches at runtime — must refuse the dispatch rather
// than let it through because the policy "couldn't decide."
func TestTaskPolicyRuleErrorDenies(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`1 / 0 == 0`},
	}
	policy, err := cfg.Policy()
	require.NoError(t, err, "a rule that only fails at runtime must still compile and load")

	err = policy.Check(context.Background(), "log", &v1.WorkloadIdentity{})
	require.Error(t, err, "a rule that cannot be evaluated must deny, not permit")

	var denied *v1.TaskPolicyDeniedError
	require.True(t, errors.As(err, &denied))
	require.Equal(t, v1.TaskPolicyReasonRuleError, denied.Reason)
}

// TestTaskPolicyLoadRefusesInvalidRule is the "policy load error = startup
// refusal" cell: a rule that does not compile must refuse at
// [v1.TaskPolicyConfig.Policy] — configuration load — never lazily on the
// first dispatch that happens to reach it.
func TestTaskPolicyLoadRefusesInvalidRule(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`task == `}, // syntactically invalid
	}
	_, err := cfg.Policy()
	require.Error(t, err)
	require.True(t, errors.Is(err, v1.ErrInvalidTaskPolicy))
}

// TestTaskPolicyLoadRefusesNonBoolRule checks the same load-time refusal for
// a rule that compiles but does not type-check to bool — a rule that
// evaluates to a string, say, which CEL happily compiles but which a
// deny/allow decision cannot use.
func TestTaskPolicyLoadRefusesNonBoolRule(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`task`}, // evaluates to a string, not a bool
	}
	_, err := cfg.Policy()
	require.Error(t, err)
	require.True(t, errors.Is(err, v1.ErrInvalidTaskPolicy))
}

// TestTaskPolicyLoadRefusesUnknownField checks that a rule naming a field the
// activation does not declare — a misspelling, or `inputs.*` before slice 2
// lands — is refused at load. This is the "a rule referencing a nonexistent
// input fails at load, not silently at runtime" property #187 names,
// narrowed to slice 1's actual surface (identity and task).
func TestTaskPolicyLoadRefusesUnknownField(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`identity.nmaespace == "platform"`}, // misspelled
	}
	_, err := cfg.Policy()
	require.Error(t, err)
	require.True(t, errors.Is(err, v1.ErrInvalidTaskPolicy))
}

// TestTaskPolicyLoadRefusesEmptyConfig checks that a config with no rules at
// all is refused rather than silently accepted as a no-op — an operator who
// pointed --task-policy at a file meant it to restrict something, and a file
// that restricts nothing is almost certainly a mistake, not an intentional
// wide-open policy.
func TestTaskPolicyLoadRefusesEmptyConfig(t *testing.T) {
	_, err := v1.TaskPolicyConfig{}.Policy()
	require.Error(t, err)
	require.True(t, errors.Is(err, v1.ErrInvalidTaskPolicy))
}

// TestTaskPolicyParseConfigRejectsUnknownKeys checks that the YAML decoder
// used by `flow worker --task-policy` is strict, mirroring
// [netpolicy.ParseConfig] and auth's ParsePolicy: a misspelled key in an
// operator's file must fail loudly at load rather than silently drop the
// restriction the operator meant to write.
func TestTaskPolicyParseConfigRejectsUnknownKeys(t *testing.T) {
	_, err := v1.ParseTaskPolicyConfig([]byte("denny:\n  - \"true\"\n"))
	require.Error(t, err)
	require.True(t, errors.Is(err, v1.ErrInvalidTaskPolicy))
}

// TestTaskPolicyContextOverridesDefault checks that a context-scoped policy
// (set by [v1.NewContextWithTaskPolicy]) takes precedence over the
// process-wide default, and that a context explicitly carrying nil forces
// "no policy" for that one call even while a process default is installed —
// [v1.TaskPolicyFromContext]'s own documented behavior.
func TestTaskPolicyContextOverridesDefault(t *testing.T) {
	restrictive, err := v1.TaskPolicyConfig{Deny: []string{"true"}}.Policy()
	require.NoError(t, err)

	v1.SetDefaultTaskPolicy(restrictive)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	// The process default denies everything.
	require.Error(t, v1.CheckTaskPolicy(context.Background(), "log", &v1.WorkloadIdentity{}))

	// A context carrying nil overrides it back to unrestricted for this call.
	ctx := v1.NewContextWithTaskPolicy(context.Background(), nil)
	require.NoError(t, v1.CheckTaskPolicy(ctx, "log", &v1.WorkloadIdentity{}))

	// A context carrying its own permissive policy also overrides the
	// restrictive default.
	permissive, err := v1.TaskPolicyConfig{Allow: []string{"true"}}.Policy()
	require.NoError(t, err)
	ctx = v1.NewContextWithTaskPolicy(context.Background(), permissive)
	require.NoError(t, v1.CheckTaskPolicy(ctx, "log", &v1.WorkloadIdentity{}))
}

// TestTaskPolicyNilIdentityReadsEmpty checks that a nil identity — what a
// local run and a legacy scope both carry — renders as every field empty
// rather than panicking or being treated specially, matching how
// [Scope.identity] itself documents a nil identity.
func TestTaskPolicyNilIdentityReadsEmpty(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`identity.subject == "" && identity.namespace == ""`},
	}
	policy, err := cfg.Policy()
	require.NoError(t, err)

	err = policy.Check(context.Background(), "log", nil)
	require.Error(t, err, "a nil identity must read as empty fields, which the rule above denies")
}

// TestTaskPolicyClassifiedNonRetryable checks that a denial is classified
// [v1.ErrorKindPolicyDenied], the same classification egress and secret
// denials carry — not the [v1.ErrorKindInternal] default an unclassified
// error would fall through to, which [v1.ErrorKind.Retryable] treats as
// worth retrying. Retrying a denied dispatch would repeat a decision that
// cannot change between attempts.
func TestTaskPolicyClassifiedNonRetryable(t *testing.T) {
	cfg := v1.TaskPolicyConfig{Deny: []string{"true"}}
	policy, err := cfg.Policy()
	require.NoError(t, err)
	v1.SetDefaultTaskPolicy(policy)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	err = v1.CheckTaskPolicy(context.Background(), "log", &v1.WorkloadIdentity{})
	require.Error(t, err)
	require.Equal(t, v1.ErrorKindPolicyDenied, v1.ClassifyError(err))
	require.False(t, v1.ClassifyError(err).Retryable())
}
