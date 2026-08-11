package tests

import "time"

// BoundaryDeadlockDetectionTimeout is the workflow-task deadlock budget given to
// a Temporal worker by the tests whose subject is a bound (#431).
//
// The SDK panics a workflow task whose goroutine has not yielded for a second,
// and one second is the right default: outside these tests, a second of
// non-yielding workflow code is a genuine finding about the engine, so nothing
// else raises it. It is not a finding in the handful of cases that deliberately
// run at the largest input a bound admits, because there the second of work is
// the thing under test, and a machine sharing itself with other suites turns
// that second into two.
//
// The reason it is worth a knob rather than a re-run is what the failure
// becomes. When the detector fires, the SDK fails the workflow task and abandons
// the still-running workflow goroutine; that goroutine then reaches its next log
// call and races the SDK's own teardown of the state it is logging through. So a
// starved runner does not merely report a spurious red, it reports a data race
// in SDK internals with no flowstate value anywhere in it. Raising the budget
// removes both at their shared source.
//
// Nothing about what those tests assert changes: the bound is still reached, the
// counts are still exact. The only thing this buys is time for a starved runner
// to get there.
//
// One value, read by both drivers' tests, because a budget written down twice is
// a budget that disagrees with itself.
const BoundaryDeadlockDetectionTimeout = 5 * time.Second
