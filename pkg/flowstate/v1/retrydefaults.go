package flowstatev1

import "time"

// A step that declares no `retry:` was retried five times under durable execution
// and once locally.
//
// Both numbers were written where they were needed, in two packages, and nothing
// compared them. So a step against a dependency that fails once and then succeeds
// failed the run locally and completed it in production — and the reverse, which
// matters more for a rehearsal: a step that always fails, tolerated by
// `continue_on_error:`, issued one request on an author's laptop and five from a
// worker. A local run did not rehearse the four extra requests production makes,
// which is exactly what a local run is for.
//
// `runStepWithPolicy`'s own comment states the contract it was not keeping:
//
//	They exist so that a local run reproduces the same observable outcome — a
//	flaky dependency that succeeds on the second attempt succeeds in both places.
//
// The values below are the durable driver's, unchanged. Raising the local driver
// to meet them is the only direction that can be right: lowering production to one
// attempt would change how every deployed workload behaves against a dependency
// that blips, and the number a rehearsal should reproduce is the one production
// uses rather than the other way round.
//
// They live here, in the package both drivers import, because two constants with
// one meaning is how they came to disagree.
const (
	// DefaultMaxAttempts bounds how many times a retryable failure is attempted
	// when a step declares no `retry:`.
	//
	// Bounded rather than unlimited: zero attempts means "forever" to Temporal,
	// which turns a persistently failing dependency into an indefinitely running
	// workflow — a workload that neither finishes nor fails.
	DefaultMaxAttempts = 5

	// DefaultRetryInitialInterval is how long the first wait between attempts is.
	DefaultRetryInitialInterval = time.Second

	// DefaultRetryBackoff multiplies the wait after each attempt.
	DefaultRetryBackoff = 2.0

	// DefaultRetryMaxInterval caps that growth, so a long attempt budget does not
	// become an arbitrarily long wait between the last two.
	//
	// The local driver had no cap at all, which only stayed invisible because it
	// also stopped after one attempt: with the attempt counts agreeing and this
	// missing, a fourth retry would have waited eight seconds locally and four in
	// production.
	DefaultRetryMaxInterval = 30 * time.Second
)

// RetryAttemptsFor returns how many attempts a step's policy allows.
//
// A policy that does not say takes [DefaultMaxAttempts], which is what makes the
// two drivers agree about a step with no `retry:` block. A policy that does say is
// honoured exactly, including a deliberate 1.
func RetryAttemptsFor(retry *RetryPolicy) int {
	if attempts := retry.GetMaxAttempts(); attempts > 0 {
		return int(attempts)
	}

	return DefaultMaxAttempts
}
