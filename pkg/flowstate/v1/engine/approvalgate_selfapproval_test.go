package engine_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// approvalGateInputs are examples/approval-gate/inputs.json, read in Go so this
// test cannot drift from the file `flow test` and the README both exercise.
func approvalGateInputs() map[string]*v1.Value {
	return map[string]*v1.Value{
		"version":           v1.NewLiteral("v1.4.2"),
		"environment":       v1.NewLiteral("production"),
		"expected_approver": v1.NewLiteral("sre-lead@example.com"),
	}
}

// loadApprovalGate parses the real example file, so this test exercises exactly
// what a Flowfile author would write and `flow validate` accepts — not a Go
// literal standing in for it.
func loadApprovalGate(t *testing.T) *v1.Workflow {
	t.Helper()

	path := filepath.Join("..", "..", "..", "..", "examples", "approval-gate", "workflow.yaml")
	wf, _, err := flowfile.ParseFile(path)
	require.NoError(t, err, "examples/approval-gate/workflow.yaml does not compile")

	return wf
}

// TestApprovalGateRefusesSelfApproval is #206's payoff: a run where the
// attested approver is the run's own attested starter is refused, and a run
// where they differ (and the approver is the one this run declared it
// expects) proceeds. Both are checked against the real example file, on the
// durable driver, with a real attested sender and a real attested run
// identity — the two facts #206's gap made impossible to compare at all.
func TestApprovalGateRefusesSelfApproval(t *testing.T) {
	t.Run("the run's starter approves their own request: refused", func(t *testing.T) {
		suite := &testsuite.WorkflowTestSuite{}
		env := suite.NewTestWorkflowEnvironment()

		env.RegisterWorkflow(engine.Run)
		env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)

		env.RegisterDelayedCallback(func() {
			// The same subject the run itself was started as — a requester who
			// also holds a credential able to send `deploy-approved`, which
			// nothing about signal delivery today prevents (#206's first gap,
			// still open). What this file now refuses is the approval itself.
			env.SignalWorkflow("deploy-approved", &v1.SignalDelivery{
				Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
					"approved": v1.NewLiteral(true),
				}},
				Sender: &v1.SignalSender{
					Identity: &v1.WorkloadIdentity{
						Subject: "sre-lead@example.com",
						Issuer:  "flowstate:test",
					},
				},
			})
		}, time.Minute)

		env.ExecuteWorkflow(engine.Run, &v1.RunState{
			Workflow: loadApprovalGate(t),
			Inputs:   approvalGateInputs(),
			Identity: &v1.WorkloadIdentity{
				// The run's own starter — the same subject that signs the
				// approval below.
				Subject: "sre-lead@example.com",
				Issuer:  "flowstate:test",
			},
		})

		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())

		var outputs v1.Workflow_StepOutputs
		require.NoError(t, env.GetWorkflowResult(&outputs))

		require.Nil(t, outputs.GetStepValues()["deploy"],
			"a run approved its own request and the gate let it through")
		require.NotNil(t, outputs.GetStepValues()["deploy_refused"],
			"self-approval did not take the refusal branch")

		decision := outputs.GetRunOutputs().GetValues()["decision"].GetLiteral().GetStringValue()
		require.Equal(t, "refused_self_approved", decision)

		approver := outputs.GetRunOutputs().GetValues()["approver_subject"].GetLiteral().GetStringValue()
		require.Empty(t, approver, "approver_subject leaked on a run that never deployed")

		t.Logf("decision=%s approver_subject=%q (self-approval correctly refused)", decision, approver)
	})

	// #215's first finding: comparing subject alone would refuse this run too,
	// because the approver's subject is textually identical to the run's
	// starter subject. The two are different principals -- a subject is only
	// unique within its issuer (auth/principal.go), and here the issuers
	// differ -- so this is a genuinely different approver who happens to
	// share a subject with the starter, and the gate must let it through
	// rather than mistake the collision for self-approval.
	t.Run("same subject, different issuer: not self-approval, proceeds", func(t *testing.T) {
		suite := &testsuite.WorkflowTestSuite{}
		env := suite.NewTestWorkflowEnvironment()

		env.RegisterWorkflow(engine.Run)
		env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)

		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("deploy-approved", &v1.SignalDelivery{
				Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
					"approved": v1.NewLiteral(true),
				}},
				Sender: &v1.SignalSender{
					Identity: &v1.WorkloadIdentity{
						// Same subject as the run's own starter, below, but a
						// different issuer -- a different identity provider
						// minted this "sre-lead@example.com", not the one
						// that started the run.
						Subject: "sre-lead@example.com",
						Issuer:  "other-idp",
					},
				},
			})
		}, time.Minute)

		env.ExecuteWorkflow(engine.Run, &v1.RunState{
			Workflow: loadApprovalGate(t),
			Inputs:   approvalGateInputs(),
			Identity: &v1.WorkloadIdentity{
				Subject: "sre-lead@example.com",
				Issuer:  "flowstate:test",
			},
		})

		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())

		var outputs v1.Workflow_StepOutputs
		require.NoError(t, env.GetWorkflowResult(&outputs))

		require.NotNil(t, outputs.GetStepValues()["deploy"],
			"a cross-issuer approver was wrongly refused as a self-approval")
		require.Nil(t, outputs.GetStepValues()["deploy_refused"])

		decision := outputs.GetRunOutputs().GetValues()["decision"].GetLiteral().GetStringValue()
		require.Equal(t, "deployed", decision)

		approver := outputs.GetRunOutputs().GetValues()["approver_subject"].GetLiteral().GetStringValue()
		require.Equal(t, "sre-lead@example.com", approver)

		t.Logf("decision=%s approver_subject=%q (cross-issuer approver correctly allowed)", decision, approver)
	})

	t.Run("a different attested approver: proceeds", func(t *testing.T) {
		suite := &testsuite.WorkflowTestSuite{}
		env := suite.NewTestWorkflowEnvironment()

		env.RegisterWorkflow(engine.Run)
		env.OnActivity(engine.Task, mock.Anything, mock.Anything).Return(engine.Task)

		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("deploy-approved", &v1.SignalDelivery{
				Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
					"approved": v1.NewLiteral(true),
				}},
				Sender: &v1.SignalSender{
					Identity: &v1.WorkloadIdentity{
						Subject: "sre-lead@example.com",
						Issuer:  "flowstate:test",
					},
				},
			})
		}, time.Minute)

		env.ExecuteWorkflow(engine.Run, &v1.RunState{
			Workflow: loadApprovalGate(t),
			Inputs:   approvalGateInputs(),
			Identity: &v1.WorkloadIdentity{
				// A different starter than the one who signs the approval below.
				Subject: "release-requester@example.com",
				Issuer:  "flowstate:test",
			},
		})

		require.True(t, env.IsWorkflowCompleted())
		require.NoError(t, env.GetWorkflowError())

		var outputs v1.Workflow_StepOutputs
		require.NoError(t, env.GetWorkflowResult(&outputs))

		require.NotNil(t, outputs.GetStepValues()["deploy"],
			"a distinct, expected approver did not deploy")
		require.Nil(t, outputs.GetStepValues()["deploy_refused"])

		decision := outputs.GetRunOutputs().GetValues()["decision"].GetLiteral().GetStringValue()
		require.Equal(t, "deployed", decision)

		approver := outputs.GetRunOutputs().GetValues()["approver_subject"].GetLiteral().GetStringValue()
		require.Equal(t, "sre-lead@example.com", approver)

		t.Logf("decision=%s approver_subject=%q (deploy proceeded)", decision, approver)
	})
}
