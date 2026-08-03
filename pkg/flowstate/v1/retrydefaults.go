package flowstatev1

import (
	"context"
	"time"
)

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

// The timeouts have the same history as the attempt count, one step later. They
// were written as literals in `engine/policy.go` and nowhere at all in the local
// driver: a step declaring no `timeout:` was bounded at two minutes per attempt
// and ten minutes overall under Temporal, and bounded by nothing whatsoever on an
// author's laptop. A plugin task that hangs therefore *hung* locally, forever,
// while production failed the step and moved on — the rehearsal disagreeing with
// production in the direction that looks like the workload is simply slow.
//
// The values below are the durable driver's, unchanged, for the reason the
// attempt count was raised rather than lowered: production's behavior is the one a
// rehearsal reproduces.
const (
	// DefaultStartToCloseTimeout bounds a single attempt at a step that declares
	// no `timeout:`.
	//
	// It must accommodate the slowest legitimate task — an HTTP request to a slow
	// endpoint — while still being short enough that a hung attempt is detected
	// rather than occupying a worker slot indefinitely. The http task's own client
	// timeout is lower, so a well-behaved task fails first and this acts as a
	// backstop.
	DefaultStartToCloseTimeout = 2 * time.Minute

	// DefaultScheduleToCloseTimeout bounds a step across all of its attempts.
	//
	// Without an overall bound, a step failing with a retryable error consumes its
	// full attempt budget with backoff between each, so the worst case is the sum
	// of every attempt plus every wait. This caps that.
	DefaultScheduleToCloseTimeout = 10 * time.Minute
)

// StepTimeouts is how long one step may take: one attempt, and all of them.
//
// Named after the durable driver's two Temporal options because they are the same
// two questions, and a local run answering them differently is the divergence this
// type exists to remove.
//
// # Two seams that survive the agreement, both worth knowing about
//
// The *sentence* a timeout records cannot match. Temporal times an activity out on
// the server and hands back its own failure, so a tolerated step records something
// like `activity StartToClose timeout (type: StartToClose)` durably, while locally
// the attempt's context deadline reaches the task and the task classifies it —
// `task "http" failed (Upstream): Get "…": context deadline exceeded`. Rendering a
// Temporal-shaped sentence locally would be inventing a transport detail to match
// a value errors.go exists to keep transports out of, so the recorded text is left
// as each driver's own. What does agree is everything an author can act on: that
// the step fails, roughly when it fails, that `continue_on_error:` tolerates it,
// and that the failure is retryable — [ErrorKindUpstream] locally, and a timeout
// is retried by Temporal durably.
//
// And the local bound is cooperative. Temporal fails the activity from the
// server's side whether or not the worker notices; here the deadline arrives on
// the attempt's context, so a task that ignores its context can still run past it.
// Every shipped task passes the context to whatever blocks, which is what makes
// the bound real for them, and it is the requirement any new task inherits.
type StepTimeouts struct {
	// StartToClose bounds a single attempt.
	StartToClose time.Duration

	// ScheduleToClose bounds the step across every attempt and every wait between
	// them.
	ScheduleToClose time.Duration
}

// DefaultStepTimeouts returns the timeouts a step runs under when neither the step
// nor the deployment says otherwise.
func DefaultStepTimeouts() StepTimeouts {
	return StepTimeouts{
		StartToClose:    DefaultStartToCloseTimeout,
		ScheduleToClose: DefaultScheduleToCloseTimeout,
	}
}

// StepTimeoutsFor applies a step's declared `timeout:` over the given defaults.
//
// The precedence is the durable driver's, because it is the durable driver's:
// `engine/policy.go` calls this to build its activity options, and the local
// driver calls it to bound its attempts. A step's declared timeout replaces the
// per-attempt bound, and the overall bound is widened when the attempts that
// timeout allows would not fit inside it — otherwise declaring a long timeout
// would leave a step cut short by a ceiling derived from defaults rather than by
// its own policy.
//
// A step declaring nothing takes the defaults unchanged, which is what makes the
// two drivers agree about the step nobody wrote a `timeout:` for.
func StepTimeoutsFor(policy *StepPolicy, base StepTimeouts) StepTimeouts {
	out := base

	timeout := policy.GetTimeout().AsDuration()
	if timeout <= 0 {
		return out
	}
	out.StartToClose = timeout

	if attempts := RetryAttemptsFor(policy.GetRetry()); attempts > 0 {
		if budget := timeout * time.Duration(attempts); budget > out.ScheduleToClose {
			out.ScheduleToClose = budget
		}
	}

	return out
}

// stepTimeoutsKey is the context key carrying a deployment's step timeouts.
type stepTimeoutsKey struct{}

// ContextWithStepTimeouts sets the timeouts a local run's steps are bounded by
// when they declare none.
//
// The durable driver's defaults are worker configuration — an activity option,
// decided where the worker is built. A local run has no worker, so the same
// decision is carried on the context, next to the signal waiter and the task
// runtime, which are the other two things a local run's host configures.
//
// A zero field means "use the default", so a caller may set one bound without
// having to restate the other.
func ContextWithStepTimeouts(ctx context.Context, timeouts StepTimeouts) context.Context {
	return context.WithValue(ctx, stepTimeoutsKey{}, timeouts)
}

// StepTimeoutsFromContext returns the timeouts configured for this run, falling
// back to [DefaultStepTimeouts] for anything unset.
func StepTimeoutsFromContext(ctx context.Context) StepTimeouts {
	out := DefaultStepTimeouts()

	configured, ok := ctx.Value(stepTimeoutsKey{}).(StepTimeouts)
	if !ok {
		return out
	}
	if configured.StartToClose > 0 {
		out.StartToClose = configured.StartToClose
	}
	if configured.ScheduleToClose > 0 {
		out.ScheduleToClose = configured.ScheduleToClose
	}

	return out
}

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
