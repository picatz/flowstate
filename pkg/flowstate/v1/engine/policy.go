package engine

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Timeout and retry defaults for step execution.
//
// These are the policy every step runs under until the specification can express
// its own per-step timeouts and retries. The values are deliberately explicit:
// Temporal requires a timeout to be set, and leaving retry behavior implicit is
// how a workflow ends up repeating an operation that should have failed once.
const (
	// The timeouts are v1's for the reason the retry defaults below are: they
	// were literals here and *nothing* in the local driver, so a step declaring
	// no `timeout:` was bounded per attempt and overall in production and
	// unbounded on the laptop rehearsing it.
	defaultStartToCloseTimeout    = v1.DefaultStartToCloseTimeout
	defaultScheduleToCloseTimeout = v1.DefaultScheduleToCloseTimeout

	// The retry defaults are v1's, not this package's. They used to be written
	// here as literals and again in the local driver, where the attempt count was
	// one rather than five — so a step with no `retry:` behaved differently in the
	// driver that exists to rehearse this one.
	defaultMaximumAttempts      = v1.DefaultMaxAttempts
	defaultRetryInitialInterval = v1.DefaultRetryInitialInterval
	defaultRetryBackoff         = v1.DefaultRetryBackoff
	defaultRetryMaximumInterval = v1.DefaultRetryMaxInterval
)

// defaultActivityOptions returns the options every step activity runs under.
//
// Notably absent is ScheduleToStartTimeout. Temporal advises against setting it
// in almost all cases: it measures time spent waiting in the task queue, so it
// fires precisely when workers are saturated or briefly unavailable — turning a
// recoverable capacity problem into a failed workflow. The two timeouts set here
// bound the work itself, which is what callers actually care about.
//
// # WaitForCancellation, and why it is not a preference
//
// Temporal's default is false, which means an activity future resolves as soon as
// cancellation is *requested* rather than when the activity has stopped. The
// workflow then believes the step is over while the worker is still running it.
//
// That is wrong on its own terms — a run reporting CANCELED while its side effects
// are still happening is the same class of lie as a run reporting cleanup it never
// did — and it is actively dangerous for a saga. Compensation for a cancelled run
// starts the moment the forward work reports cancellation, so with this false the
// undo races the step it is undoing: `delete` can be issued, complete, and be
// reported as "undid" while the `create` it was undoing is still in flight and
// about to succeed. The summary then says the resource came off and the resource
// is allocated, which is worse than saying nothing at all, because it is the
// sentence that makes somebody stop looking.
//
// So the run waits for what it started. If the activity honours cancellation it
// stops promptly; if it ignores it, the wait is bounded by the timeouts above and
// `flow terminate` is the verb for that case — which is precisely what the two
// verbs have always meant, and what `flow cancel`'s help says.
//
// An activity that finishes *successfully* after the cancellation arrives is
// reported as having succeeded, which is the outcome a saga wants: the step
// registers its compensation, and the compensation then takes it back. The
// alternative — discarding a completion that already happened — would leave the
// effect in the world with nothing registered to undo it.
//
// Like NonRetryableErrorTypes below, this is never overridable by a step's own
// policy. Whether a workload's effects have actually stopped is not a preference.
func defaultActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout:    defaultStartToCloseTimeout,
		ScheduleToCloseTimeout: defaultScheduleToCloseTimeout,
		WaitForCancellation:    true,

		// Set because every task activity heartbeats — see heartbeat.go, where the
		// interval this is derived from lives. It is what makes the wait above
		// short: Temporal delivers a cancellation to a running activity through the
		// response to a heartbeat, so an activity that never heartbeats never
		// learns it was cancelled and runs to its StartToCloseTimeout.
		HeartbeatTimeout: heartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        defaultRetryInitialInterval,
			BackoffCoefficient:     defaultRetryBackoff,
			MaximumInterval:        defaultRetryMaximumInterval,
			MaximumAttempts:        defaultMaximumAttempts,
			NonRetryableErrorTypes: nonRetryableErrorTypes(),
		},
	}
}

// activityOptionsFor returns the options a specific step runs under, applying any
// policy the step declares over the defaults.
//
// `summary` is the history label — see summary.go for what it carries and why
// it is a parameter rather than derived here: a compensation and the step it
// undoes run under the same policy and must not read the same in history.
//
// Only the settings a step actually specifies are overridden, so declaring a
// timeout does not silently reset the retry behavior. The non-retryable error
// list is never overridable: whether a failure *can* succeed on another attempt
// is a property of the failure, not a preference, and letting a workflow declare
// otherwise would mean retrying operations known to be unrepeatable.
//
// `WaitForCancellation` is not overridable either, and for the same shape of
// reason — whether a step's effects have actually stopped when the run says they
// have is not something a file gets an opinion about. Nothing below touches it;
// this says so, because "not overridable" being true only by omission is how it
// stops being true.
//
// `HeartbeatTimeout` is the third. It is derived from the interval the worker
// actually ticks at, so a file lowering it would fail perfectly healthy steps and
// a file raising it would only delay noticing a dead worker. Neither is a decision
// a workflow is in a position to make.
func activityOptionsFor(policy *v1.StepPolicy, summary string) workflow.ActivityOptions {
	opts := defaultActivityOptions()
	opts.Summary = summary
	if policy == nil {
		return opts
	}

	// Including the rule that the overall bound must leave room for the attempts
	// the retry policy allows, or a step would be cut short by a ceiling derived
	// from defaults rather than by its own policy. The rule lives in v1 rather
	// than here because the local driver has to apply the same one, and a
	// precedence written twice is how the defaults themselves came to disagree.
	timeouts := v1.StepTimeoutsFor(policy, v1.StepTimeouts{
		StartToClose:    opts.StartToCloseTimeout,
		ScheduleToClose: opts.ScheduleToCloseTimeout,
	})
	opts.StartToCloseTimeout = timeouts.StartToClose
	opts.ScheduleToCloseTimeout = timeouts.ScheduleToClose

	retry := policy.GetRetry()
	if retry == nil {
		return opts
	}

	// Copy before mutating: defaultActivityOptions returns a fresh policy, but
	// relying on that from here would be a trap for whoever changes it next.
	rp := *opts.RetryPolicy
	if v := retry.GetMaxAttempts(); v > 0 {
		rp.MaximumAttempts = v
	}
	if v := retry.GetInitialInterval().AsDuration(); v > 0 {
		rp.InitialInterval = v
	}
	if v := retry.GetBackoffCoefficient(); v >= 1 {
		rp.BackoffCoefficient = v
	}
	if v := retry.GetMaxInterval().AsDuration(); v > 0 {
		rp.MaximumInterval = v
	}
	opts.RetryPolicy = &rp

	return opts
}

// How many attempts a policy allows is [v1.RetryAttemptsFor], which is what
// [v1.StepTimeoutsFor] reads when it widens the overall bound. The local copy this
// package kept said the same thing in its own words, which is the arrangement the
// shared constants exist to end.

// nonRetryableErrorTypes returns the application error types that must not be
// retried.
//
// These strings must match the types the activity boundary attaches to failures
// (see activityError), because Temporal matches on the application error type
// and silently retries anything it does not recognize. Deriving both from the
// same classification is what keeps them in agreement — the previous policy
// listed a type no activity could ever return, so every deterministic failure
// was retried to exhaustion.
func nonRetryableErrorTypes() []string {
	kinds := v1.PermanentErrorKinds()
	types := make([]string, 0, len(kinds))
	for _, k := range kinds {
		types = append(types, k.String())
	}
	return types
}
