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
// difference only shows on a step that is attempted more than once. The local
// driver checks above its retry loop, so one dispatch is one check. The
// durable driver checks inside the activity, and Temporal retries by invoking
// the activity again, so one dispatch was one check *per attempt* — the policy
// correctly enforced each time, and the trail carrying two allows for a
// dispatch the local driver recorded once.
//
// An ordinary success proves nothing here: both drivers write exactly one
// record for it, which is agreement that was never at risk. The case has to
// retry, and the assertion has to count.

// DispatchAuditTaskName is the fixture whose dispatch both drivers record.
const DispatchAuditTaskName = "test.dispatch_audit_retry"

// DispatchAuditStepID is the step that is attempted twice.
const DispatchAuditStepID = "flaky"

// DispatchAuditTaskDef fails once and then succeeds, so the step is dispatched
// once and attempted twice.
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

// AssertOneDispatchAllowPerDispatch is the shared claim: a step attempted twice
// is one dispatch, and one dispatch is one allow, on both drivers.
//
// attempts is asserted too, because the count above means nothing if the
// fixture never retried — a driver that ran it once would produce exactly one
// allow while proving nothing about the case this set exists for.
func AssertOneDispatchAllowPerDispatch(tb testing.TB, driver string, records []*v1.AuditRecord, attempts int32) {
	tb.Helper()

	if attempts != 2 {
		tb.Fatalf("%s ran the fixture %d times, want 2; without a retry this case cannot "+
			"distinguish one record per dispatch from one per attempt", driver, attempts)
	}

	allows := 0
	for _, record := range records {
		if record.GetEnforcementPoint() != v1.AuditEnforcementPoint_AUDIT_ENFORCEMENT_POINT_TASK_DISPATCH {
			continue
		}
		if record.GetResourceKey() != DispatchAuditTaskName {
			continue
		}
		if record.GetDecision() == v1.AuditDecision_AUDIT_DECISION_ALLOW {
			allows++
		}
		if record.GetDecision() == v1.AuditDecision_AUDIT_DECISION_DENY {
			tb.Errorf("%s recorded a denial for a dispatch its policy permits: %v", driver, record)
		}
	}

	if allows != 1 {
		tb.Errorf("%s recorded %d dispatch allows for one dispatch of %q attempted twice, want 1; "+
			"a record per attempt makes one run's trail depend on which driver ran it",
			driver, allows, DispatchAuditTaskName)
	}
}
