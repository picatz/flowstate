package execpolicy

import (
	"errors"
	"fmt"
)

// ErrInvalidPolicy is wrapped by every refusal to load a policy: a table entry
// that is not a usable executable, a missing bound, a CEL rule that does not
// compile. It marks operator configuration mistakes, refused at load, never a
// per-invocation outcome.
var ErrInvalidPolicy = errors.New("invalid exec policy")

// ErrDenied is wrapped by every [*DeniedError]. A caller distinguishes a
// deliberate policy decision from the program failing with errors.Is.
var ErrDenied = errors.New("denied by exec policy")

// Reason classifies why a policy denied an invocation.
type Reason string

const (
	// ReasonNoPolicy means no policy is configured, so nothing is permitted.
	ReasonNoPolicy Reason = "no policy"

	// ReasonExecutable means argv[0] is not a name the policy lists, or is
	// written as a path.
	ReasonExecutable Reason = "executable"

	// ReasonArgv means the argument list is outside the bounds or malformed.
	ReasonArgv Reason = "argv"

	// ReasonDir means the working directory is not an existing directory under
	// a configured root.
	ReasonDir Reason = "dir"

	// ReasonEnv means a step environment entry is not one the policy lets a step
	// set, or would override one the operator set.
	ReasonEnv Reason = "env"

	// ReasonDenyRule means a CEL deny rule matched.
	ReasonDenyRule Reason = "deny rule"

	// ReasonNoAllowRule means allow rules are configured and none matched.
	ReasonNoAllowRule Reason = "allow rules"

	// ReasonRuleError means a CEL rule could not be evaluated. Rules fail
	// closed, so that denies.
	ReasonRuleError Reason = "rule error"

	// ReasonPlatform means this platform cannot enforce a guarantee the task
	// depends on (stopping a program together with its descendants), so no
	// program is started rather than started with the guarantee weakened.
	ReasonPlatform Reason = "platform"

	// ReasonIntegrity means the executable on disk is not the file the policy
	// was loaded against: it is no longer a regular executable file, or it no
	// longer matches its pinned SHA-256.
	ReasonIntegrity Reason = "integrity"
)

// DeniedError reports that the policy refused an invocation. It wraps
// [ErrDenied].
//
// The detail names the offending part of the request and, where the operator's
// table is the remedy, what the table does contain. It never carries an
// environment value: a denial travels into logs and durable history.
type DeniedError struct {
	// Reason is the broad category of the denial.
	Reason Reason

	// Detail says which input was refused and why.
	Detail string

	// Err is the underlying cause when a rule failed to evaluate.
	Err error
}

func (e *DeniedError) Error() string {
	return fmt.Sprintf("denied by the exec policy (%s): %s", e.Reason, e.Detail)
}

// Unwrap returns [ErrDenied], and the underlying cause when there is one.
func (e *DeniedError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrDenied}
	}
	return []error{ErrDenied, e.Err}
}

// Outcome says how an invocation that the policy admitted ended.
type Outcome string

const (
	// OutcomeRan means the program started and finished, whatever its exit code.
	OutcomeRan Outcome = "ran"

	// OutcomeDidNotStart means the worker could not start the program.
	OutcomeDidNotStart Outcome = "did_not_start"

	// OutcomeTimedOut means a time bound ended the program: the policy's
	// timeout, or a deadline on the context it ran under.
	OutcomeTimedOut Outcome = "timed_out"

	// OutcomeCancelled means the context was cancelled around the program.
	OutcomeCancelled Outcome = "cancelled"
)

// RunError reports an admitted invocation that did not run to completion. It
// carries the [Outcome] and, where the program had started, the output it had
// produced, so a caller can keep it for a diagnostic.
type RunError struct {
	// Outcome is which of the three failure outcomes this is.
	Outcome Outcome

	// PolicyTimeout is true when the bound that ended the program was the
	// policy's own timeout, as opposed to a deadline the caller's context
	// carried. The distinction is a classification the caller makes: the first
	// is deterministic for the same program, the second is the step's budget.
	PolicyTimeout bool

	// Err is the cause: the start failure, or the context's error.
	Err error

	// Result is what the program had produced when it was ended. Zero when it
	// never started.
	Result Result
}

func (e *RunError) Error() string {
	switch e.Outcome {
	case OutcomeTimedOut:
		if e.PolicyTimeout {
			return fmt.Sprintf("outcome=%s: the program was still running when the policy's timeout ended it", e.Outcome)
		}
		return fmt.Sprintf("outcome=%s: the program was still running when its deadline passed", e.Outcome)
	case OutcomeCancelled:
		return fmt.Sprintf("outcome=%s: the run was cancelled while the program was running", e.Outcome)
	default:
		return fmt.Sprintf("outcome=%s: %v", e.Outcome, e.Err)
	}
}

// Unwrap returns the cause, so errors.Is matches context.Canceled and
// context.DeadlineExceeded for the outcomes they describe.
func (e *RunError) Unwrap() error { return e.Err }
