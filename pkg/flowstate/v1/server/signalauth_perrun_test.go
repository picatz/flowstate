package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// #207 slice 1, end to end: per-run signal authorization. An `allow:`
// predicate may read the run's own bound inputs, which the server records with
// the run, and compare the sender with the run's starter
// (`sender.identity.principal != run.identity.principal`) to separate who
// started a run from who may approve it. These tests exercise both through
// the real RPC surface and through both submit paths
// ([FlowstateServer.Run] and [FlowstateServer.CreateSchedule]), because the
// whole point of sharing one memo helper between them is that a scheduled run
// records and enforces exactly what a direct run does.

// perRunGatedWorkflow is [gatedWorkflow] with a signal policy whose predicate
// reads an input named "expected_approver" — the shape #207's decision record
// calls "per-run signal authorization".
//
// The comparison with the starter is there because a predicate over inputs is
// required to carry something the run's own inputs cannot reach, and this is
// the cheaper of the two such things to express here (claims would need the
// server configured with an identity-claim allowlist). It is not incidental to
// what these tests assert: the input naming the approver is chosen by whoever
// starts the run, so without it the starter could name themselves — see
// [v1.CheckSignalPolicyShape] for why a namespace comparison cannot serve
// instead.
func perRunGatedWorkflow() *v1.Workflow {
	wf := gatedWorkflow()
	wf.DeclaredInputs = []*v1.InputDeclaration{
		{Name: "expected_approver", Type: v1.InputDeclaration_TYPE_STRING, Required: true},
	}
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-approved": {
			Allow: `sender.identity.principal == inputs.expected_approver && sender.identity.principal != run.identity.principal`,
		},
	}
	return wf
}

// TestSignalPolicyReadingInputsIsEvaluatedAgainstTheRunsOwnInputs is the positive-then-negative
// pair for a direct run: the sender named by this particular run's own
// `expected_approver` input is authorized, and a sender who would have
// satisfied a *different* run's input is not — proving the predicate was
// evaluated against this run's own bound inputs rather than against the
// workflow's specification in the abstract.
func TestSignalPolicyReadingInputsIsEvaluatedAgainstTheRunsOwnInputs(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: perRunGatedWorkflow(),
		Inputs: map[string]*v1.Value{
			"expected_approver": v1.NewLiteral(v1.QualifiedSubject("https://issuer.example.com", "release-manager@example.com")),
		},
	}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	// The negative direction first: a sender this run's own input never
	// named is refused, even though they are an ordinary authenticated,
	// in-tenant caller.
	deniedCtx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "some-other-engineer@example.com",
	})
	_, err = fixture.teamA.Signal(deniedCtx, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		},
	}))
	require.Error(t, err, "a sender this run's expected_approver input never named was authorized")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// The positive direction: the sender this run's own input named.
	allowedCtx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "release-manager@example.com",
	})
	_, err = fixture.teamA.Signal(allowedCtx, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		},
	}))
	require.NoError(t, err, "the sender this run's own expected_approver input named was refused")

	require.Eventually(t, func() bool {
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		return err == nil && resp.Msg.GetStatus() == v1.RunResponse_STATUS_COMPLETED
	}, 60*time.Second, 200*time.Millisecond, "the run did not complete after the sender the run's input named approved")
}

// TestSignalPolicyReadingInputsDecidesDifferentlyPerRun proves the decision is
// truly per run and not merely per workflow: two runs of the identical
// specification, started with two different `expected_approver` inputs,
// each authorize only their own approver.
func TestSignalPolicyReadingInputsDecidesDifferentlyPerRun(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)
	wf := perRunGatedWorkflow()

	startAndApprove := func(approver, senderSubject string) error {
		started, err := fixture.teamA.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
			Workflow: wf,
			Inputs: map[string]*v1.Value{
				"expected_approver": v1.NewLiteral(v1.QualifiedSubject("https://issuer.example.com", approver)),
			},
		}))
		require.NoError(t, err)

		workflowID := started.Msg.GetWorkflowId()
		waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

		ctx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
			Issuer:  "https://issuer.example.com",
			Subject: senderSubject,
		})
		_, err = fixture.teamA.Signal(ctx, connect.NewRequest(&v1.SignalRequest{
			WorkflowId: workflowID,
			Name:       "deploy-approved",
			Payload: &v1.Node_Outputs{
				NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
			},
		}))
		return err
	}

	require.NoError(t, startAndApprove("alice@example.com", "alice@example.com"),
		"alice was refused on the run that named her as expected_approver")
	err := startAndApprove("bob@example.com", "alice@example.com")
	require.Error(t, err, "alice was authorized on a run that named bob, not her, as expected_approver")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
}

// scheduledPerRunGatedWorkflow is [perRunGatedWorkflow]
// with a schedule trigger, so its per-run predicate can be exercised
// through [FlowstateServer.CreateSchedule] — the other submit path
// [signalPolicyMemoEntry] serves, and #207's decision record's "both submit
// paths" requirement.
func scheduledPerRunGatedWorkflow(name string) *v1.Workflow {
	wf := perRunGatedWorkflow()
	wf.Name = name
	wf.Triggers = &v1.Triggers{
		Schedule: &v1.ScheduleTrigger{
			Cron:    []string{"0 * * * *"},
			Overlap: v1.ScheduleTrigger_OVERLAP_SKIP,
		},
	}
	return wf
}

// TestScheduledSignalPolicyReadingInputsIsRecordedAtScheduleCreation is
// [TestSignalPolicyReadingInputsIsEvaluatedAgainstTheRunsOwnInputs] for the scheduled path:
// `CreateSchedule` records the policy and the inputs it reads once, against
// the inputs the schedule was created with, and every firing's memo carries
// them — proving [policyMemoEntries]'s "one function, two callers" discipline
// actually holds for a per-run predicate, not only for the zero-case policy
// shape #206/#215 already covered.
func TestScheduledSignalPolicyReadingInputsIsRecordedAtScheduleCreation(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	wf := scheduledPerRunGatedWorkflow("scheduled-per-run-gate")

	_, err := fixture.teamA.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
		Workflow: wf,
		Paused:   true,
		Inputs: map[string]*v1.Value{
			"expected_approver": v1.NewLiteral(v1.QualifiedSubject("https://issuer.example.com", "release-manager@example.com")),
		},
	}))
	require.NoError(t, err)

	_, err = fixture.teamA.TriggerSchedule(t.Context(),
		connect.NewRequest(&v1.TriggerScheduleRequest{Name: "scheduled-per-run-gate"}))
	require.NoError(t, err)

	var workflowID string
	require.Eventually(t, func() bool {
		described, err := fixture.teamA.DescribeSchedule(t.Context(),
			connect.NewRequest(&v1.DescribeScheduleRequest{Name: "scheduled-per-run-gate"}))
		if err != nil || len(described.Msg.GetSchedule().GetRecentRuns()) == 0 {
			return false
		}
		workflowID = described.Msg.GetSchedule().GetRecentRuns()[0].GetWorkflowId()
		return workflowID != ""
	}, 60*time.Second, 200*time.Millisecond, "the schedule never took an action")

	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	// Negative: the schedule's declared input named nobody else.
	deniedCtx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "some-other-engineer@example.com",
	})
	_, err = fixture.teamA.Signal(deniedCtx, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		},
	}))
	require.Error(t, err, "a scheduled run's fired execution authorized a sender the schedule's own input never named")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// Positive: the schedule's own bound input.
	allowedCtx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "release-manager@example.com",
	})
	_, err = fixture.teamA.Signal(allowedCtx, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		},
	}))
	require.NoError(t, err, "the schedule's own expected_approver was refused on its fired execution")

	require.Eventually(t, func() bool {
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		return err == nil && resp.Msg.GetStatus() == v1.RunResponse_STATUS_COMPLETED
	}, 60*time.Second, 200*time.Millisecond, "the scheduled run did not complete after the named sender approved")
}

// TestSignalPolicyComparingWithTheStarterEndToEnd exercises separation of duties
// through the real RPC surface: whoever starts a run may be named by the
// policy's own principal comparison, but the comparison with the starter
// refuses their own signal all the same — the run's starter and one of the
// principals the predicate names are made to be the identical caller on
// purpose, so a pass here could only come from the starter comparison itself,
// not from the principal comparison failing to match.
func TestSignalPolicyComparingWithTheStarterEndToEnd(t *testing.T) {
	t.Parallel()

	fixture := newTenantFixture(t)

	starterCtx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "requester@example.com",
	})

	wf := gatedWorkflow()
	wf.Signals = map[string]*v1.SignalPolicy{
		"deploy-approved": {
			Allow: `((sender.identity.principal == "` + v1.QualifiedSubject("https://issuer.example.com", "requester@example.com") + `") || (sender.identity.principal == "` + v1.QualifiedSubject("https://issuer.example.com", "release-manager@example.com") + `")) && sender.identity.principal != run.identity.principal`,
		},
	}

	started, err := fixture.teamA.Run(starterCtx, connect.NewRequest(&v1.RunRequest{Workflow: wf}))
	require.NoError(t, err)

	workflowID := started.Msg.GetWorkflowId()
	waitUntilParkedAtTheGate(t, fixture.temporal, workflowID)

	// The starter tries to approve their own run.
	_, err = fixture.teamA.Signal(starterCtx, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		},
	}))
	require.Error(t, err, "the run's own starter approved their own run despite the starter comparison")
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	ran, err := stepsScheduled(t.Context(), fixture.temporal, workflowID)
	require.NoError(t, err)
	require.Equal(t, []string{"requesting approval"}, ran,
		"a step ran after a self-approval the starter comparison should have refused")

	// A distinct sender, named by the same rule, succeeds.
	approverCtx := auth.ContextWithPrincipal(t.Context(), auth.Principal{
		Issuer:  "https://issuer.example.com",
		Subject: "release-manager@example.com",
	})
	_, err = fixture.teamA.Signal(approverCtx, connect.NewRequest(&v1.SignalRequest{
		WorkflowId: workflowID,
		Name:       "deploy-approved",
		Payload: &v1.Node_Outputs{
			NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)},
		},
	}))
	require.NoError(t, err, "a sender distinct from the run's starter was refused by the starter comparison")

	require.Eventually(t, func() bool {
		resp, err := fixture.teamA.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		return err == nil && resp.Msg.GetStatus() == v1.RunResponse_STATUS_COMPLETED
	}, 60*time.Second, 200*time.Millisecond, "the run did not complete after the distinct approver approved")
}
