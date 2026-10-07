package flowstatev1_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestTaskPolicyRulesReadIdentityKind proves a task-shape rule can key on the
// shared principal.Caller's kind, in both the allowing and the denying
// direction, and that an identity with no kind never reads as a workload.
func TestTaskPolicyRulesReadIdentityKind(t *testing.T) {
	workload := &v1.WorkloadIdentity{Subject: "ci", PrincipalKind: v1.PrincipalKind_PRINCIPAL_KIND_WORKLOAD}
	human := &v1.WorkloadIdentity{Subject: "kent", PrincipalKind: v1.PrincipalKind_PRINCIPAL_KIND_HUMAN}
	unassigned := &v1.WorkloadIdentity{Subject: "ci"}

	allow, err := v1.TaskPolicyConfig{Allow: []string{`identity.kind == "workload"`}}.Policy()
	require.NoError(t, err)
	require.NoError(t, allow.Check(context.Background(), "log", workload))
	for name, id := range map[string]*v1.WorkloadIdentity{"human": human, "unassigned": unassigned, "nil": nil} {
		err := allow.Check(context.Background(), "log", id)
		require.ErrorIs(t, err, v1.ErrTaskPolicyDenied, name)
		var denied *v1.TaskPolicyDeniedError
		require.True(t, errors.As(err, &denied), name)
		require.Equal(t, v1.TaskPolicyReasonNoAllowRule, denied.Reason, name)
	}

	deny, err := v1.TaskPolicyConfig{Deny: []string{`task == "log" && identity.kind == "human"`}}.Policy()
	require.NoError(t, err)
	require.ErrorIs(t, deny.Check(context.Background(), "log", human), v1.ErrTaskPolicyDenied)
	require.NoError(t, deny.Check(context.Background(), "log", workload))
	require.NoError(t, deny.Check(context.Background(), "log", unassigned))

	// actions is declared on the shared type: a rule naming it compiles, and an
	// identity carrying none is a non-match rather than an evaluation error.
	byAction, err := v1.TaskPolicyConfig{Allow: []string{`"run.start" in identity.actions`}}.Policy()
	require.NoError(t, err)
	denied := byAction.Check(context.Background(), "log", workload)
	require.ErrorIs(t, denied, v1.ErrTaskPolicyDenied)
	var d *v1.TaskPolicyDeniedError
	require.True(t, errors.As(denied, &d))
	require.Equal(t, v1.TaskPolicyReasonNoAllowRule, d.Reason, "an empty actions list is a non-match, not a rule error")

	// A misspelled field is still a load-time error.
	_, err = v1.TaskPolicyConfig{Allow: []string{`identity.kinds == "workload"`}}.Policy()
	require.Error(t, err)
}
