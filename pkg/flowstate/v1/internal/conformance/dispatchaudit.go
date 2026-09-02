package conformance

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Shared cases for the task-dispatch audit seam (picatz/flowstate#1379), run
// against both execution drivers.
//
// # Why a retried dispatch is the case worth sharing
//
// The two drivers reach [v1.CheckTaskPolicy] from different places, and the
// difference only shows on a step that is attempted more than once. The
// durable driver's check is inside the activity, and Temporal retries by
// invoking the activity again; the local driver's is inside its own retry
// loop. Both therefore decide per attempt, and both record what was decided —
// but that is a claim about two code paths meeting, which is exactly the shape
// invariant 3 exists to hold to evidence.
//
// An ordinary success proves nothing here: one attempt is one record on any
// implementation, which is agreement that was never at risk. The case has to
// retry, and the assertion has to count.
//
// # Why per attempt rather than per dispatch
//
// A retried task really was dispatched twice, and a policy consulted twice
// made two decisions. Recording only the first also assumes the first was
// recorded: under a required recorder, an attempt whose record cannot be
// written is precisely the attempt that gets retried, so a first-attempt-only
// rule would let the work run with nothing in the trail at all
// (Codex, picatz/flowstate#1394).

// DispatchAuditTaskName is the fixture whose dispatches both drivers record.
const DispatchAuditTaskName = "test.dispatch_audit_retry"

// DispatchAuditStepID is the step that is attempted twice.
const DispatchAuditStepID = "flaky"

// DispatchAuditDenyRule refuses the fixture, for the tightening case below.
const DispatchAuditDenyRule = `task == "` + DispatchAuditTaskName + `"`

// DispatchAuditTaskDef fails once and then succeeds, so the step is attempted
// twice and dispatched twice.
//
// The counter belongs to the driver's own test rather than to this package, so
// the fixture holds no process-global state — [TaskSpanRetryTaskDef] makes the
// same choice for the same reason.
func DispatchAuditTaskDef(attempts *atomic.Int32) v1.TaskDef {
	return v1.TaskDef{
		Name: DispatchAuditTaskName,
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			if attempts.Add(1) == 1 {
				return nil, v1.NewTaskError(DispatchAuditTaskName, v1.ErrorKindUpstream,
					errors.New("fixture fails on its first attempt so the step is retried"))
			}

			return &v1.Node_Outputs{}, nil
		},
	}
}

// DispatchAuditTighteningTaskDef is the same fixture with the deployment
// changing its mind: the first attempt installs deny as the process-wide task
// policy and then fails retryably, so the second attempt meets a policy that
// refuses it.
//
// It writes the process-wide policy because that is what an operator's change
// actually is, and what both drivers read. A caller must therefore not run it
// in parallel with anything else that reads that policy, and must restore it;
// each driver's test owns that, the way [InstallEgressIdentityPolicy]'s
// callers own theirs.
func DispatchAuditTighteningTaskDef(attempts *atomic.Int32, deny *v1.TaskPolicy) v1.TaskDef {
	return v1.TaskDef{
		Name: DispatchAuditTaskName,
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			if attempts.Add(1) == 1 {
				v1.SetDefaultTaskPolicy(deny)

				return nil, v1.NewTaskError(DispatchAuditTaskName, v1.ErrorKindUpstream,
					errors.New("fixture fails once, and the deployment tightens its policy meanwhile"))
			}

			return &v1.Node_Outputs{}, nil
		},
	}
}

// DispatchAuditWorkflow retries the fixture through each driver's own retry
// mechanism, quickly.
func DispatchAuditWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    "dispatch-audit-retry",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:   DispatchAuditStepID,
			Kind: &v1.Node_Task{Task: &v1.Task{Name: DispatchAuditTaskName}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{
				MaxAttempts:        2,
				InitialInterval:    durationpb.New(time.Millisecond),
				BackoffCoefficient: 1,
				MaxInterval:        durationpb.New(time.Millisecond),
			}},
		}},
	}
}

// AssertADecisionPerDispatchAttempt is the shared claim: a step attempted
// twice is decided twice, recorded twice, and each record says which attempt
// it belongs to — on both drivers.
//
// attempts is asserted too, because the counts below mean nothing if the
// fixture never retried: a driver that ran it once would produce one record
// and prove nothing about the case this set exists for.
func AssertADecisionPerDispatchAttempt(tb testing.TB, driver string, records []*v1.AuditRecord, attempts int32) {
	tb.Helper()

	if attempts != 2 {
		tb.Fatalf("%s ran the fixture %d times, want 2; without a retry this case cannot "+
			"distinguish a record per attempt from a record per dispatch", driver, attempts)
	}

	AssertDispatchAttemptsRecorded(tb, driver, records,
		[]v1.AuditDecision{v1.AuditDecision_AUDIT_DECISION_ALLOW, v1.AuditDecision_AUDIT_DECISION_ALLOW})
}

// AssertDispatchAttemptsRecorded requires exactly one dispatch record for this
// fixture per attempt, in attempt order, each carrying its own attempt number
// and the decision want names for it.
//
// The attempt numbers are asserted rather than only the count, because a seam
// that recorded one attempt twice would otherwise pass — and a trail that
// cannot tell one attempt's decision from another's is back where it started.
func AssertDispatchAttemptsRecorded(tb testing.TB, driver string, records []*v1.AuditRecord, want []v1.AuditDecision) {
	tb.Helper()

	var got []*v1.AuditRecord
	for _, record := range records {
		if record.GetEnforcementPoint() != v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_TASK_DISPATCH {
			continue
		}
		if record.GetResourceKey() == DispatchAuditTaskName {
			got = append(got, record)
		}
	}

	if len(got) != len(want) {
		tb.Fatalf("%s recorded %d dispatch decisions for %q, want %d: %v",
			driver, len(got), DispatchAuditTaskName, len(want), got)
	}

	for i, record := range got {
		if record.GetDecision() != want[i] {
			tb.Errorf("%s recorded %v for attempt %d, want %v", driver, record.GetDecision(), i+1, want[i])
		}
		if record.GetAttempt() != uint32(i+1) {
			tb.Errorf("%s recorded attempt %d in the record for the %s attempt; a trail that cannot "+
				"tell one attempt's decision from another's is not one",
				driver, record.GetAttempt(), dispatchOrdinal(i+1))
		}
	}
}

// dispatchOrdinal renders a small attempt number for a failure message.
func dispatchOrdinal(n int) string {
	switch n {
	case 1:
		return "first"
	case 2:
		return "second"
	default:
		return "later"
	}
}
