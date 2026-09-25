package conformance

import (
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// BoundaryDeadlockDetectionTimeout is the workflow-task deadlock budget the
// at-a-bound tests hand their workers. It reads the production value, because
// a rehearsal budget that differs from the worker's would let a boundary
// input pass tests and fail its workflow task in production (#431, review on
// #470). See [v1.WorkerDeadlockDetectionTimeout] for the reasoning.
//
// Under the race detector it is the production value times
// [raceDetectorSlowdown]. The race build runs the same workflow-side work
// several times slower than the binary a worker ships as — Go documents the
// slowdown as two to twenty times — so holding it to the worker's own budget
// is stricter than production, not equal to it, and CI failed a list built
// exactly at the element bound on a loaded runner with "didn't yield for over
// 5s" in code no diff had touched. The scaled budget compares like with like;
// a workflow goroutine that spends the whole scaled budget under the detector
// is still far outside anything a bound admits.
const BoundaryDeadlockDetectionTimeout time.Duration = v1.WorkerDeadlockDetectionTimeout * raceDetectorSlowdown

// BoundaryWorkflowTaskTimeout is the Temporal server deadline for the same
// at-a-bound tests. It leaves the live worker's race-scaled detector first say
// while preserving the SDK's ten-second default without instrumentation.
const BoundaryWorkflowTaskTimeout time.Duration = 2 * BoundaryDeadlockDetectionTimeout

// BoundaryWorkflowChainTimeout bounds the complete continuation chain rather
// than one workflow task. It is deliberately fixed across build modes so race
// instrumentation cannot silently weaken the five-minute rehearsal contract.
const BoundaryWorkflowChainTimeout = 5 * time.Minute
