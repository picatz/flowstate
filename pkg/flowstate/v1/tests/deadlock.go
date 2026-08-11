package tests

import (
	"time"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// BoundaryDeadlockDetectionTimeout is the workflow-task deadlock budget the
// at-a-bound tests hand their workers. It reads the production value, because
// a rehearsal budget that differs from the worker's would let a boundary
// input pass tests and fail its workflow task in production (#431, review on
// #470). See [v1.WorkerDeadlockDetectionTimeout] for the reasoning.
const BoundaryDeadlockDetectionTimeout time.Duration = v1.WorkerDeadlockDetectionTimeout
