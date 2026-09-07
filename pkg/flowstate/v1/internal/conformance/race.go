//go:build race

package conformance

// raceDetectorSlowdown is the factor the boundary tests' deadlock budget is
// scaled by when the binary was built with the race detector, which runs the
// same instructions several times slower than any binary a worker ships as.
// See [BoundaryDeadlockDetectionTimeout].
const raceDetectorSlowdown = 3
