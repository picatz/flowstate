package netpolicy

import (
	"errors"
	"fmt"
)

// ErrDenied is the sentinel error wrapped by every egress policy denial. Callers
// use errors.Is(err, ErrDenied) to distinguish a deliberate policy decision from
// a transport failure such as a refused connection, a DNS failure, or a timeout.
var ErrDenied = errors.New("denied by egress policy")

// ErrBodyTooLarge is the sentinel error wrapped when a response body exceeds the
// limit configured by [WithMaxResponseBytes]. It is never returned for a body
// that merely ended early, so callers can report truncation distinctly from a
// read failure.
var ErrBodyTooLarge = errors.New("response body exceeds egress policy limit")

// ErrInvalidPolicy is the sentinel error wrapped by [New] when the supplied
// options do not describe a usable policy, such as a CEL rule that does not
// compile. It marks operator configuration mistakes rather than runtime denials.
var ErrInvalidPolicy = errors.New("invalid egress policy")

// Reason classifies why a policy denied a request. It is stable enough to switch
// on, so callers can report categories separately, for example by counting
// address denials apart from rule denials.
type Reason string

const (
	// ReasonScheme indicates the request URL used a scheme that is not in the
	// policy's scheme allowlist.
	ReasonScheme Reason = "scheme"

	// ReasonPort indicates the request targeted a port the policy does not permit.
	ReasonPort Reason = "port"

	// ReasonAddress indicates the resolved IP address fell into a category or
	// network the policy does not permit.
	ReasonAddress Reason = "address"

	// ReasonRedirect indicates a redirect was refused, either because redirects
	// are disabled or because the hop limit was exhausted.
	ReasonRedirect Reason = "redirect"

	// ReasonDenyRule indicates a CEL deny rule matched the request.
	ReasonDenyRule Reason = "deny rule"

	// ReasonNoAllowRule indicates CEL allow rules are configured and none of
	// them matched the request.
	ReasonNoAllowRule Reason = "allow rules"

	// ReasonRuleError indicates a CEL rule could not be evaluated. Rules fail
	// closed, so an evaluation error denies the request.
	ReasonRuleError Reason = "rule error"

	// ReasonRequest indicates the request itself was unusable, for example a
	// request with no URL or an address that never resolved to an IP.
	ReasonRequest Reason = "request"
)

// DenyError reports that an egress policy refused a request. It wraps
// [ErrDenied], names the request attribute that was rejected, and identifies the
// rule or category responsible so an operator can find and change the relevant
// configuration.
type DenyError struct {
	// Reason is the broad category of the denial.
	Reason Reason

	// Target is what was denied: a URL, a resolved address, a port, or a scheme.
	// URLs are redacted, so a password in the userinfo section is not exposed.
	Target string

	// Detail names the specific rule, category, or constraint responsible for
	// the denial, such as "loopback", "cloud metadata", or the source text of a
	// CEL rule.
	Detail string

	// Err is the underlying cause, when the denial came from something failing
	// rather than from a rule matching. A rule that could not be evaluated puts
	// the evaluation error here, so a caller can inspect it with errors.As.
	Err error
}

// Error implements the error interface.
func (e *DenyError) Error() string {
	return fmt.Sprintf("%s: %s (%s: %s)", ErrDenied, e.Target, e.Reason, e.Detail)
}

// Unwrap returns [ErrDenied], and the underlying cause when there is one, so that
// every denial matches a single sentinel without hiding what went wrong.
func (e *DenyError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrDenied}
	}

	return []error{ErrDenied, e.Err}
}

// BodyTooLargeError reports that a response body exceeded the policy's limit. It
// wraps [ErrBodyTooLarge] and records the limit so the caller can report it.
type BodyTooLargeError struct {
	// Limit is the maximum number of body bytes the policy allows.
	Limit int64
}

// Error implements the error interface.
func (e *BodyTooLargeError) Error() string {
	return fmt.Sprintf("%s of %d bytes", ErrBodyTooLarge, e.Limit)
}

// Unwrap returns [ErrBodyTooLarge] so every size denial matches a single sentinel.
func (e *BodyTooLargeError) Unwrap() error { return ErrBodyTooLarge }
