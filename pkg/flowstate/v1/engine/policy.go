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
