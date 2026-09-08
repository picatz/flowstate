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
// at-a-bound tests. Without race instrumentation it remains the SDK's ten-second
// default. Under instrumentation it leaves a second slowdown factor between the
// separately scheduled worker and server clocks, so the worker's detector—not
// CPU contention—still decides whether a workflow goroutine failed to yield.
const BoundaryWorkflowTaskTimeout time.Duration = 2 * v1.WorkerDeadlockDetectionTimeout *
	raceDetectorSlowdown * raceDetectorSlowdown
