package flowstatev1_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestTaskPolicyRulesReadIdentityActions covers what the shared
// conformance.TaskPolicyCases do not: the kind allow/deny pairs run there on
// both drivers, so this keeps only the actions field and the load-time check.
func TestTaskPolicyRulesReadIdentityActions(t *testing.T) {
	workload := &v1.WorkloadIdentity{Subject: "ci", PrincipalKind: v1.PrincipalKind_PRINCIPAL_KIND_WORKLOAD}

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
