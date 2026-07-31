package engine

import (
	"time"

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
	// defaultStartToCloseTimeout bounds a single attempt at a step.
	//
	// It must accommodate the slowest legitimate task — an HTTP request to a
	// slow endpoint — while still being short enough that a hung attempt is
	// detected rather than occupying a worker slot indefinitely. The http task's
	// own client timeout is lower, so a well-behaved task fails first and this
	// acts as a backstop.
	defaultStartToCloseTimeout = 2 * time.Minute

	// defaultScheduleToCloseTimeout bounds a step across all of its attempts.
	//
	// Without an overall bound, a step failing with a retryable error consumes
	// its full attempt budget with backoff between each, so the worst case is
	// the sum of every attempt plus every wait. This caps that.
	defaultScheduleToCloseTimeout = 10 * time.Minute

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
func defaultActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout:    defaultStartToCloseTimeout,
		ScheduleToCloseTimeout: defaultScheduleToCloseTimeout,
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
// Only the settings a step actually specifies are overridden, so declaring a
// timeout does not silently reset the retry behavior. The non-retryable error
// list is never overridable: whether a failure *can* succeed on another attempt
// is a property of the failure, not a preference, and letting a workflow declare
// otherwise would mean retrying operations known to be unrepeatable.
func activityOptionsFor(policy *v1.StepPolicy) workflow.ActivityOptions {
	opts := defaultActivityOptions()
	if policy == nil {
		return opts
	}

	if timeout := policy.GetTimeout().AsDuration(); timeout > 0 {
		opts.StartToCloseTimeout = timeout

		// The overall bound must leave room for the attempts the retry policy
		// allows, or a step would be cut short by a ceiling derived from
		// defaults rather than by its own policy.
		if attempts := int64(effectiveMaxAttempts(policy.GetRetry())); attempts > 0 {
			if budget := timeout * time.Duration(attempts); budget > opts.ScheduleToCloseTimeout {
				opts.ScheduleToCloseTimeout = budget
			}
		}
	}

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

// effectiveMaxAttempts reports how many attempts a retry policy allows,
// substituting the default when it does not say.
func effectiveMaxAttempts(retry *v1.RetryPolicy) int32 {
	if v := retry.GetMaxAttempts(); v > 0 {
		return v
	}
	return defaultMaximumAttempts
}

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
