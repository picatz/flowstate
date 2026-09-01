package engine_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// The durable half of the task-dispatch audit seam (picatz/flowstate#1379),
// and the same argument policy_denial_metric_test.go makes for the denial
// counter: the record is written in [v1.CheckTaskPolicy], a seam both drivers
// execute, and a claim proved only against the local driver is a claim about
// one caller.
//
// The other three seams — secret access, egress, credential assumption — are
// activity-side by construction: they run inside a task's own execution on the
// worker, reached identically from either driver's dispatch, and there is no
// workflow-side direction of them for a test to assert. They are proved at the
// seam in pkg/flowstate/v1/enforcementaudit_test.go.

// recordingSink collects what a recorder emitted, safely across the activity
// goroutines the test environment runs.
type recordingSink struct {
	mu      sync.Mutex
	records []*v1.AuditRecord
}

func (s *recordingSink) Emit(_ context.Context, record *v1.AuditRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.records = append(s.records, record)
	return nil
}

func (s *recordingSink) all() []*v1.AuditRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]*v1.AuditRecord(nil), s.records...)
}

// TestADeniedDurableDispatchIsRecorded runs a workflow the deployment's policy
// refuses through the durable driver and reads the trail.
//
// Through a real durable run rather than by calling the check directly,
// because the process-wide installation is half the claim: `flow worker`
// installs the auditor before it polls, and an activity the Temporal SDK
// invokes carries none of this process's context values — so a seam that
// resolved its auditor only from the context would record nothing here while
// passing every test that calls it directly.
func TestADeniedDurableDispatchIsRecorded(t *testing.T) {
	denying, err := v1.TaskPolicyConfig{Deny: []string{`task == "log"`}}.Policy()
	require.NoError(t, err)
	v1.SetDefaultTaskPolicy(denying)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	sink := &recordingSink{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink))
	require.NoError(t, err)
	v1.SetDefaultEnforcementAuditor(recorder)
	t.Cleanup(func() { v1.SetDefaultEnforcementAuditor(nil) })

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	engine.Register(env, engine.TaskRuntimeConfig{})

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: &v1.Workflow{
			Name:    "denied-durably",
			Profile: v1.CurrentProfile,
			Steps: []*v1.Node{
				{Id: "narrate", Kind: &v1.Node_Task{Task: &v1.Task{
					Name:   "log",
					Inputs: map[string]*v1.Value{"message": v1.NewLiteral("never printed")},
				}}},
			},
		},
		Identity: &v1.WorkloadIdentity{Subject: "deploy-bot", Namespace: "acme"},
	})
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError(), "the policy denies this task")

	records := sink.all()
	require.Len(t, records, 1, "one refused dispatch, recorded once — not once per retry attempt")

	record := records[0]
	require.Equal(t, v1.AuditDecision_AUDIT_DECISION_DENY, record.GetDecision())
	require.Equal(t, v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_TASK_DISPATCH,
		record.GetEnforcementPoint())
	require.Equal(t, v1.AuditDenyCode_AUDIT_DENY_CODE_DENY_RULE, record.GetDenyCode())
	require.Equal(t, `task == "log"`, record.GetRule())
	require.Equal(t, "log", record.GetResourceKey())
	require.Equal(t, "deploy-bot", record.GetIdentity().GetSubject())
}
