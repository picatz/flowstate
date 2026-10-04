package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestReportableFailureKind: a server reports a declared name as a run's kind
// only when the run's own specification, or a workflow it calls, declares it.
func TestReportableFailureKind(t *testing.T) {
	t.Parallel()

	callee := &v1.Workflow{Name: "callee", DeclaredErrors: []*v1.ErrorDeclaration{{Name: "QuotaExceeded"}}}
	wf := &v1.Workflow{
		Name:           "caller",
		DeclaredErrors: []*v1.ErrorDeclaration{{Name: "Refused"}},
		Steps: []*v1.Node{{
			Id: "c", Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}},
		}},
	}

	assert.True(t, v1.ReportableFailureKind(wf, "Refused"), "declared by the workflow")
	assert.True(t, v1.ReportableFailureKind(wf, "QuotaExceeded"), "declared by a workflow it calls")
	assert.True(t, v1.ReportableFailureKind(wf, "Upstream"), "built in")
	assert.False(t, v1.ReportableFailureKind(wf, "PathError"), "an arbitrary type a failure carried")
	assert.False(t, v1.ReportableFailureKind(wf, ""))
}
