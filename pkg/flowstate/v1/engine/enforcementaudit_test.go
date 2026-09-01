package engine_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
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

// TestARetriedDispatchIsRecordedOnceDurable is the second of the two driver
// callers [conformance.AssertOneDispatchAllowPerDispatch] asks for, and the
// one the claim was false for (Codex, picatz/flowstate#1394).
//
// This driver's dispatch check runs *inside* the activity, and Temporal retries
// an activity by invoking it again, so a step attempted twice consulted the
// policy twice and wrote two allows — for a dispatch the local driver recorded
// once. The policy is still consulted on every attempt; what a later attempt no
// longer does is write a second record of one dispatch.
//
// Not parallel, and neither is the test above: the auditor and the task-shape
// policy are process-wide, which is the point of them — see the comment on
// v1.SetDefaultEnforcementAuditor.
func TestARetriedDispatchIsRecordedOnceDurable(t *testing.T) {
	var attempts atomic.Int32

	require.NoError(t, v1.DefaultRegistry().Register(conformance.DispatchAuditTaskDef(&attempts)))

	sink := &recordingSink{}
	recorder, err := audit.NewRecorder(audit.WithoutStderr(), audit.WithEmitter(sink))
	require.NoError(t, err)
	v1.SetDefaultEnforcementAuditor(recorder)
	t.Cleanup(func() { v1.SetDefaultEnforcementAuditor(nil) })

	suite := &testsuite.WorkflowTestSuite{}
	env := suite.NewTestWorkflowEnvironment()
	engine.Register(env, engine.TaskRuntimeConfig{})

	env.ExecuteWorkflow(engine.Run, &v1.RunState{
		Workflow: conformance.DispatchAuditWorkflow(),
		Identity: &v1.WorkloadIdentity{Subject: "deploy-bot", Namespace: "acme"},
	})
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(), "the fixture succeeds on its second attempt")

	conformance.AssertOneDispatchAllowPerDispatch(t, "the durable driver", sink.all(), attempts.Load())
}

// TestADenialOnALaterAttemptIsStillRecorded is the negative direction of the
// same change: recording once per dispatch must not become recording once and
// then never looking again.
//
// Durable-only, and not for want of trying to share it. A denial on a *later*
// attempt requires the policy to be consulted on that attempt, and the local
// driver deliberately consults it once above its retry loop — so it has no
// later attempt to refuse, and a shared case would be asserting a property one
// driver is designed not to have (the shape conformance/callers_test.go's
// oneSidedByDesign records elsewhere). What both drivers do agree on is the
// allow, which [conformance.AssertOneDispatchAllowPerDispatch] pins.
func TestADenialOnALaterAttemptIsStillRecorded(t *testing.T) {
	var attempts atomic.Int32

	// Permits the first attempt, refuses every one after it: the operator who
	// tightens a policy while a run is retrying.
	denying, err := v1.TaskPolicyConfig{Deny: []string{`task == "` + conformance.DispatchAuditTaskName + `"`}}.Policy()
	require.NoError(t, err)

	require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
		Name: conformance.DispatchAuditTaskName,
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			if attempts.Add(1) == 1 {
				v1.SetDefaultTaskPolicy(denying)

				return nil, v1.NewTaskError(conformance.DispatchAuditTaskName, v1.ErrorKindUpstream,
					errors.New("fixture fails once, and the deployment tightens its policy meanwhile"))
			}

			return &v1.Node_Outputs{}, nil
		},
	}))
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
		Workflow: conformance.DispatchAuditWorkflow(),
		Identity: &v1.WorkloadIdentity{Subject: "deploy-bot", Namespace: "acme"},
	})
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError(), "the second attempt is refused by the tightened policy")

	var allows, denies int
	for _, record := range sink.all() {
		if record.GetEnforcementPoint() != v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_TASK_DISPATCH {
			continue
		}
		switch record.GetDecision() {
		case v1.AuditDecision_AUDIT_DECISION_ALLOW:
			allows++
		case v1.AuditDecision_AUDIT_DECISION_DENY:
			denies++
		}
	}

	require.Equal(t, 1, allows, "the first attempt was permitted, once")
	require.Equal(t, 1, denies,
		"the policy is still consulted on every attempt, and a refusal is recorded whenever it happens")
}
