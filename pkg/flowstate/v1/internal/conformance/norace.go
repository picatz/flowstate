//go:build !race

package conformance

// raceDetectorSlowdown is 1 without the race detector: the budget is the
// worker's own. See [BoundaryDeadlockDetectionTimeout].
const raceDetectorSlowdown = 1
