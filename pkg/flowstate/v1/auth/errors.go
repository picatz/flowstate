package auth

import (
	"errors"
	"fmt"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
)

// Sentinel errors returned by this package. Callers distinguish failures with
// [errors.Is], because every error returned by a [Verifier] wraps exactly one
// of these.
//
// These errors describe why a token was rejected, and are safe to log
// server-side. They never contain token signatures or secret material.
var (
	// ErrNoToken is returned when a request carries no bearer token at all.
	ErrNoToken = errors.New("auth: no bearer token provided")

	// ErrMalformedToken is returned when a token is not a well-formed JWT, or
	// its header and claims cannot be decoded.
	ErrMalformedToken = errors.New("auth: malformed token")

	// ErrUntrustedIssuer is returned when a token's "iss" claim does not match
	// any issuer in the configured trust policy.
	ErrUntrustedIssuer = errors.New("auth: token issuer is not trusted")

	// ErrDisallowedAlgorithm is returned when a token is signed with an
	// algorithm outside the issuer's allowlist, with "none", or with an
	// algorithm that does not match the type of the issuer's published key.
	// The last case is an algorithm confusion attempt.
	ErrDisallowedAlgorithm = errors.New("auth: token signing algorithm is not allowed")

	// ErrUnknownKey is returned when a token names a key ("kid") that the
	// issuer does not publish, or when the issuer publishes several keys and
	// the token names none of them.
	ErrUnknownKey = errors.New("auth: token signing key is unknown to the issuer")

	// ErrInvalidSignature is returned when a token's signature does not verify
	// against the issuer's published key.
	ErrInvalidSignature = errors.New("auth: token signature is invalid")

	// ErrMissingClaim is returned when a token omits a claim that Flowstate
	// requires, such as "exp", "iat", "iss", "sub", or "aud".
	ErrMissingClaim = errors.New("auth: token is missing a required claim")

	// ErrTokenExpired is returned when a token's "exp" claim is in the past, or
	// when it is older than the issuer's configured maximum token age.
	ErrTokenExpired = errors.New("auth: token is expired")

	// ErrTokenNotYetValid is returned when a token's "nbf" or "iat" claim is in
	// the future.
	ErrTokenNotYetValid = errors.New("auth: token is not yet valid")

	// ErrInvalidAudience is returned when a token's "aud" claim contains none
	// of the audiences the issuer is configured to accept.
	ErrInvalidAudience = errors.New("auth: token audience is not accepted")

	// ErrClaimMismatch is returned when a verified token does not satisfy the
	// claim rules of the trust policy. See [ClaimMismatchError].
	ErrClaimMismatch = errors.New("auth: token claim does not match the trust policy")

	// ErrIssuerUnavailable is returned when an issuer's OpenID Connect
	// discovery document or JSON Web Key Set cannot be retrieved or parsed.
	ErrIssuerUnavailable = errors.New("auth: issuer metadata or keys are unavailable")

	// ErrInvalidPolicy is returned by [Policy.Validate] and [NewOIDCVerifier]
	// when a trust policy or the settings it is built with are not usable, and
	// by an [Authenticator] with no verifier at all.
	ErrInvalidPolicy = errors.New("auth: invalid authentication configuration")

	// ErrDelegatedToken is returned when a token carries an RFC 8693 delegation
	// claim — "act" or "may_act" — that this deployment has nowhere to map and
	// therefore refuses rather than ignores. See [DelegationClaimError] for the
	// claim key, and delegation.go for why the refusal is the verifier's and
	// not one surface's.
	ErrDelegatedToken = errors.New("auth: token carries an unsupported delegation claim")

	// ErrAmbiguousIdentity is returned when a caller cannot be attributed to
	// exactly one authority. Per CLAUDE.md's "fail closed", every shape of it
	// is a refusal rather than a precedence rule: a control plane that mints
	// workload assertions from whichever identity it decides to trust has no
	// safe default for "two answers, pick one".
	//
	// Two shapes reach it, and they are the same refusal at two depths:
	//
	//   - A request carrying both a verified client certificate and a bearer
	//     token that name different principals — see [connect.go], where
	//     neither "the token wins" nor "the certificate wins" is safe.
	//   - A credential more than one trust policy entry admits — see
	//     [AmbiguousIssuerError], [OIDCVerifier.Verify] and
	//     [MTLSVerifier.VerifyPeer]. Each entry carries its own namespace and
	//     role, so admitting under one of them is a silent choice between two
	//     identities an operator wrote down as different.
	ErrAmbiguousIdentity = errors.New("auth: the caller cannot be attributed to exactly one identity")
)

// Errors returned when Flowstate acts as an identity of its own, minting
// assertions and exchanging them for downstream credentials.
var (
	// ErrInvalidIdentity is returned when a [WorkloadIdentity] or [StepRef] does
	// not describe a workload well enough to mint an assertion for it.
	ErrInvalidIdentity = errors.New("auth: invalid workload identity")

	// ErrUndeclaredClaim is returned when a mint is asked to carry a claim the
	// issuer does not declare. The claim set an assertion may carry is a closed
	// set, and a name absent from it is refused rather than signed: see
	// [Issuer.mintFor] and [WithDeclaredClaims].
	ErrUndeclaredClaim = errors.New("auth: claim is not declared by this issuer")

	// ErrNoSigningKey is returned when an [Issuer] has no key able to sign, which
	// is the fail-closed outcome of a rotation that left none active.
	ErrNoSigningKey = errors.New("auth: no active signing key")

	// ErrAssumeDenied is returned when the assumption policy refuses to let a
	// workload obtain a credential for a target. See [AssumeDeniedError].
	ErrAssumeDenied = errors.New("auth: denied by assumption policy")

	// ErrUnknownTarget is returned when a credential is requested for a target
	// name the broker was not configured with. An unconfigured target is a
	// refusal, never an unrestricted one.
	ErrUnknownTarget = errors.New("auth: unknown credential target")

	// ErrExchangeFailed is returned when a relying party refused to exchange an
	// assertion, or could not be reached.
	ErrExchangeFailed = errors.New("auth: credential exchange failed")

	// ErrExchangeUnavailable marks a transient exchange failure: transport
	// failure, throttling, or a relying-party 5xx. It also wraps
	// [ErrExchangeFailed], so callers can report one exchange category while task
	// retry policy distinguishes an outage from a permanent refusal.
	ErrExchangeUnavailable = errors.New("auth: credential exchange temporarily unavailable")

	// ErrCredentialUnresolved is returned when a [Credential] carries no secret
	// material. It means the credential was serialized somewhere, which strips
	// the secret by design: credentials must be resolved in the activity that
	// uses them, never carried through workflow history.
	ErrCredentialUnresolved = errors.New("auth: credential carries no secret material")
)

// Retryable reports whether another credential exchange attempt may succeed.
func Retryable(err error) bool { return errors.Is(err, ErrExchangeUnavailable) }

// Errors returned when a tenant boundary or a secret access decision is at stake.
var (
	// ErrNoNamespace is returned when a token verifies but the trust policy cannot
	// work out which namespace its runs belong to, which happens when the claim an
	// issuer entry names is missing from the token.
	//
	// It is a rejection rather than a fallback on purpose. Admitting a caller into
	// a shared namespace because its own could not be determined is how a tenant
	// boundary becomes decorative.
	ErrNoNamespace = errors.New("auth: cannot determine the caller's namespace")

	// ErrSecretDenied is returned when no rule permits a workload to read a
	// secret. See [SecretDeniedError].
	ErrSecretDenied = errors.New("auth: denied by secret access policy")

	// ErrNoTemporalNamespace is returned when a Flowstate namespace has no mapping
	// to a Temporal namespace and the deployment has not named a default.
	ErrNoTemporalNamespace = errors.New("auth: no Temporal namespace for this namespace")
)

// SecretDeniedError reports that no rule permits a workload to read a secret.
//
// Its message names the workload and the reference rather than describing the
// secret as missing, because those need different fixes: an operator hunting for a
// secret that exists, because access to it was reported as absence, is the failure
// this wording exists to avoid.
type SecretDeniedError struct {
	// Scheme and Name are the reference that was refused.
	Scheme string
	Name   string

	// Subject is the workload subject that asked for it.
	Subject string

	// Namespace is the namespace that workload belongs to.
	Namespace string

	// Reason is the broad category of the refusal.
	Reason SecretReason

	// Detail names the rule or constraint responsible.
	Detail string

	// Err is the underlying cause when the refusal came from something failing
	// rather than from a rule matching.
	Err error
}

// SecretReason classifies why secret access was refused.
type SecretReason string

const (
	// ReasonSecretNoPolicy indicates no rules are configured at all. A deployment
	// that has not said which secrets a workload may read permits none.
	ReasonSecretNoPolicy SecretReason = "no rules configured"

	// ReasonSecretDenyRule indicates a deny rule matched.
	ReasonSecretDenyRule SecretReason = "deny rule"

	// ReasonSecretNoAllowRule indicates no allow rule matched.
	ReasonSecretNoAllowRule SecretReason = "allow rules"

	// ReasonSecretRuleError indicates a rule could not be evaluated. Rules fail
	// closed, so an evaluation error refuses the request.
	ReasonSecretRuleError SecretReason = "rule error"

	// ReasonSecretMalformed indicates the reference itself was unusable.
	ReasonSecretMalformed SecretReason = "malformed reference"

	// ReasonSecretNoIdentity indicates the workload had no established identity to
	// authorize. It is distinct from a malformed reference because the fix is
	// elsewhere: the run reached the worker without the identity it was submitted
	// with, rather than the reference being wrong.
	ReasonSecretNoIdentity SecretReason = "no workload identity"
)

// Error implements the error interface.
func (e *SecretDeniedError) Error() string {
	reference := e.Scheme + ":" + e.Name
	if e.Scheme == "" && e.Name == "" {
		reference = "an empty reference"
	}

	return fmt.Sprintf("%s: no rule permits workload %q in namespace %q to read %s (%s: %s)",
		ErrSecretDenied, e.Subject, e.Namespace, reference, e.Reason, e.Detail)
}

// Unwrap returns [ErrSecretDenied], and the underlying cause when there is one.
func (e *SecretDeniedError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrSecretDenied}
	}
	return []error{ErrSecretDenied, e.Err}
}

// AssumeDeniedError reports that the assumption policy refused a credential
// request. It wraps [ErrAssumeDenied] and names the rule responsible, so an
// operator can find the configuration that produced the decision.
type AssumeDeniedError struct {
	// Target is the credential target that was refused.
	Target string

	// Subject is the workload subject that asked for it.
	Subject string

	// Reason is the broad category of the refusal.
	Reason AssumeReason

	// Detail names the specific rule or constraint responsible, such as the
	// source text of a CEL rule.
	Detail string

	// Err is the underlying cause when the refusal came from something failing
	// rather than from a rule matching.
	Err error
}

// AssumeReason classifies why the assumption policy refused a request. It is
// stable enough to switch on, so a caller can report rule denials separately
// from evaluation failures.
type AssumeReason string

const (
	// ReasonAssumeDenyRule indicates a deny rule matched.
	ReasonAssumeDenyRule AssumeReason = "deny rule"

	// ReasonAssumeNoAllowRule indicates allow rules are configured and none of
	// them matched.
	ReasonAssumeNoAllowRule AssumeReason = "allow rules"

	// ReasonAssumeRuleError indicates a rule could not be evaluated. Rules fail
	// closed, so an evaluation error refuses the request.
	ReasonAssumeRuleError AssumeReason = "rule error"
)

// Error implements the error interface.
func (e *AssumeDeniedError) Error() string {
	return fmt.Sprintf("%s: %q may not assume %q (%s: %s)",
		ErrAssumeDenied, e.Subject, e.Target, e.Reason, e.Detail)
}

// Unwrap returns [ErrAssumeDenied], and the underlying cause when there is one,
// so every refusal matches a single sentinel without hiding what went wrong.
func (e *AssumeDeniedError) Unwrap() []error {
	if e.Err == nil {
		return []error{ErrAssumeDenied}
	}
	return []error{ErrAssumeDenied, e.Err}
}

// ClaimMismatchError reports a claim in a verified token that does not satisfy
// a [ClaimRule]. It wraps [ErrClaimMismatch].
//
// The reported values come from a token whose signature has already been
// verified, so they are authentic assertions of a trusted issuer and are safe
// to record in an audit log. They are never returned to the caller over the
// wire.
type ClaimMismatchError struct {
	// Claim is the name of the claim that did not match.
	Claim string

	// Want is the set of values the trust policy accepts.
	Want []string

	// Got is the value the token asserted, truncated if unreasonably long. It
	// is empty when the token omitted the claim entirely.
	Got string

	// RefusedValue is the one value that matched the rule's
	// [ClaimRule.NoneOf], and is empty unless that exclusion is why the rule
	// failed. It names the value rather than the whole refused list because
	// the operator's question is which of their exclusions fired.
	RefusedValue string
}

// Error implements the error interface.
func (e *ClaimMismatchError) Error() string {
	switch {
	case e.RefusedValue != "":
		return fmt.Sprintf("auth: token claim %q is %q, which this entry refuses", e.Claim, e.RefusedValue)
	case e.Got == "" && len(e.Want) == 0:
		// A none_of-only rule still requires its claim: see ClaimRule.check,
		// where the absent-claim reading is decided. Saying "one of []" here
		// would print an empty list as though the entry accepted nothing.
		return fmt.Sprintf("auth: token is missing claim %q, which this entry requires it to assert", e.Claim)
	case e.Got == "":
		return fmt.Sprintf("auth: token is missing claim %q required to be one of [%s]", e.Claim, strings.Join(e.Want, ", "))
	default:
		return fmt.Sprintf("auth: token claim %q is %q, want one of [%s]", e.Claim, e.Got, strings.Join(e.Want, ", "))
	}
}

// Unwrap returns [ErrClaimMismatch] so callers can match this error with
// [errors.Is].
func (e *ClaimMismatchError) Unwrap() error {
	return ErrClaimMismatch
}

// IssuerBlockedError reports that fetching an issuer's OpenID Connect
// discovery document or JSON Web Key Set was refused by the identity egress
// policy rather than the issuer failing to answer. It wraps
// [ErrIssuerUnavailable] so existing errors.Is checks still match, but names
// the denied hop (already redacted by [netpolicy.DenyError]), the rule
// responsible, and the trust policy's egress: section as the remedy.
type IssuerBlockedError struct {
	Issuer string
	Deny   *netpolicy.DenyError
}

func (e *IssuerBlockedError) Error() string {
	hop := e.Deny.Hop
	if hop == "" {
		hop = e.Deny.Target
	}
	return fmt.Sprintf("%v: issuer %q fetch of %s blocked by identity egress policy: %v; "+
		"configure the trust policy's egress: section to allow this fetch",
		ErrIssuerUnavailable, e.Issuer, hop, e.Deny)
}

func (e *IssuerBlockedError) Unwrap() []error {
	return []error{ErrIssuerUnavailable, e.Deny}
}

// AmbiguousIssuerError reports a credential that more than one trust policy
// entry admits. It wraps [ErrAmbiguousIdentity].
//
// Each entry carries its own [TrustedIssuer.Role] and its own tenant, so there
// is no such thing as admitting under "either" of them: the caller would run as
// one of two identities the operator wrote down as different. Both
// [OIDCVerifier.Verify] and [MTLSVerifier.VerifyPeer] refuse instead, and this
// is what they refuse with.
//
// # What it names, and what it does not
//
// It names the entries, by [TrustedIssuer.Name] and by position, because that
// is the operator's own vocabulary for the thing they have to change — the same
// pair [UnreachableIssuer] reports, so the load-time lint and the verification
// refusal point at a policy the same way.
//
// It carries nothing from the credential: no claim value, no subject, no
// certificate field, and obviously no token. A mismatch is a statement about
// one entry's expectations and reports what it saw ([ClaimMismatchError]);
// this is a statement about the *policy*, and the values that reached it add
// nothing an operator needs while widening what a log holds. See
// [PublicReason], which narrows it further for anything shipped off the box.
type AmbiguousIssuerError struct {
	// Issuer is the issuer identifier every matched entry names: an "iss"
	// claim for kind: oidc, and the operator's own label for the CA for
	// kind: mtls.
	//
	// Empty when they do not all name the same one, which only kind: mtls can
	// produce — see sharedIssuer. The message then says nothing about the
	// issuer rather than picking one of the labels.
	Issuer string

	// Entries are the names of every entry that admitted the credential, in
	// policy order. There are always at least two — one is not ambiguous.
	Entries []string

	// Indexes are those entries' positions in [Policy.Issuers], paired with
	// Entries by position. Names are unique within a policy, and the index is
	// still reported because it is what a file's reader counts down to.
	Indexes []int
}

// Error implements the error interface.
func (e *AmbiguousIssuerError) Error() string {
	named := make([]string, 0, len(e.Entries))
	for i, name := range e.Entries {
		if i < len(e.Indexes) {
			named = append(named, fmt.Sprintf("issuers[%d] (%q)", e.Indexes[i], name))
			continue
		}
		named = append(named, fmt.Sprintf("%q", name))
	}
	subject := fmt.Sprintf("%d trust policy entries", len(e.Entries))
	if e.Issuer != "" {
		subject = fmt.Sprintf("%d trust policy entries for issuer %q", len(e.Entries), e.Issuer)
	}
	return fmt.Sprintf("auth: %s admit this caller — %s — and each grants its own namespace and role, so "+
		"admitting under one of them would be a silent choice between them. Make the entries disjoint: narrow "+
		"one, or exclude the other's callers from it with a require rule using none_of",
		subject, strings.Join(named, ", "))
}

// Unwrap returns [ErrAmbiguousIdentity] so callers can match this error with
// [errors.Is], alongside the certificate-versus-token shape of the same
// refusal.
func (e *AmbiguousIssuerError) Unwrap() error {
	return ErrAmbiguousIdentity
}

// PublicReason returns the short, fixed description of an authentication
// failure that [Authenticator.Authenticate] returns to the caller, such as
// "token is expired".
//
// It exists for a [WithFailureObserver] callback that logs rejections. The full
// error is safe to log server-side — it never contains token signatures or
// secret material — but it can carry verified claim values and the wrapped text
// of a parse failure, which a log shipped somewhere less trusted should not.
// This is the classification with none of that: it names no configured value and
// nothing taken from the token.
func PublicReason(err error) string {
	return publicReason(err)
}

// publicReason returns a short description of an authentication failure to
// return to an unauthenticated caller.
//
// It names no configured value: never an expected audience, a claim name, a
// claim value, an issuer, or a key id.
//
// The one claim key it does name is a delegation claim the caller's own token
// carried ("act" or "may_act", and never the object either one holds). That is
// not a configured value: it is one of two constants, chosen by the token in
// the caller's hand, and it tells a client which claim to stop sending rather
// than anything about this deployment. Without it a delegated token and a
// wrong-audience token refuse identically, which is a client that cannot fix
// either — see [ErrDelegatedToken].
//
// Nor does it distinguish "signed by the
// wrong key" from "names a key I do not know", which would let a caller map an
// issuer's key set.
//
// It does reveal the class of failure, which is deliberate: a caller that cannot
// tell an expired token from a rejected one cannot fix either. The one thing
// that leaks is a single bit per guess about the issuer in the caller's own
// token, since an untrusted issuer is reported differently from a bad signature.
// That is accepted, because operators need to be able to tell a misconfigured
// issuer from a broken one, and the issuer list is not a secret: holding it
// grants nothing without a token that also satisfies the audience and claim
// rules.
func publicReason(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrNoToken):
		return "missing bearer token"
	case errors.Is(err, ErrMalformedToken):
		return "malformed token"
	case errors.Is(err, ErrUntrustedIssuer):
		return "untrusted token issuer"
	case errors.Is(err, ErrTokenExpired):
		return "token is expired"
	case errors.Is(err, ErrTokenNotYetValid):
		return "token is not yet valid"
	case errors.Is(err, ErrMissingClaim), errors.Is(err, ErrNoNamespace):
		// A caller whose namespace claim is absent is missing a claim its issuer
		// was expected to assert, which is the same thing from the caller's side
		// and says nothing about how the policy uses it.
		return "token is missing a required claim"
	case errors.Is(err, ErrInvalidAudience):
		return "token audience is not accepted"
	case errors.Is(err, ErrDelegatedToken):
		// Placed before the claim cases below because a delegation refusal is
		// not "your issuer forgot something": the claim is present and this
		// deployment will not act on it.
		if claim := delegationClaimOf(err); claim != "" {
			return fmt.Sprintf("token carries an unsupported %q delegation claim", claim)
		}
		return "token carries an unsupported delegation claim"
	case errors.Is(err, ErrClaimMismatch):
		return "token is not accepted by the trust policy"
	case errors.Is(err, ErrIssuerUnavailable):
		var blocked *IssuerBlockedError
		if errors.As(err, &blocked) {
			return "issuer keys are blocked by the identity egress policy"
		}
		return "issuer keys are temporarily unavailable"
	case errors.Is(err, ErrInvalidSignature),
		errors.Is(err, ErrUnknownKey),
		errors.Is(err, ErrDisallowedAlgorithm):
		return "invalid token signature"
	case errors.Is(err, ErrAmbiguousIdentity):
		// Two shapes, told apart by type rather than by sentinel, because
		// [ErrAmbiguousIdentity] covers both and a caller reading "certificate
		// and token disagree" about a policy misconfiguration would go looking
		// at their own credentials for a problem that is not there. Neither
		// answer names an entry: entry names are configured values, and this
		// string is the one that leaves the box.
		if _, ok := errors.AsType[*AmbiguousIssuerError](err); ok {
			return "more than one trust policy entry admits this caller"
		}
		return "client certificate and bearer token identify different callers"
	default:
		return "unauthenticated"
	}
}

// truncate bounds a value taken from a token before it is placed in an error
// message, so that a trusted-but-hostile issuer cannot flood an operator's
// logs with a single claim.
func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return strings.ToValidUTF8(s[:limit], "") + "..."
}

// AssumptionFailedError reports that the assumption policy permitted a target
// and obtaining or applying the credential then failed: the assertion could not
// be minted, the relying party refused or could not be reached, or the
// credential could not be presented on the request.
//
// It exists to separate "the policy said no" from "the policy said yes and the
// rest went wrong", which are the same error to a caller reading only the
// message. A caller that records what the policy decided
// (picatz/flowstate#1379) needs the second case to be recognizable: the
// decision happened, so it belongs in the trail, and an IdP outage would
// otherwise erase exactly the allows an operator investigating that outage came
// to read.
//
// It deliberately does not wrap [ErrAssumeDenied]: nothing was refused. It
// unwraps to the original failure, so [Retryable] and every errors.Is check
// against [ErrExchangeFailed], [ErrExchangeUnavailable] and the rest answer for
// it exactly as they answered for the bare value it replaced.
type AssumptionFailedError struct {
	// Target is the credential target the policy permitted.
	Target string

	// Err is the failure that followed: a mint, an exchange, or applying the
	// credential to the request.
	Err error
}

// Error implements the error interface.
func (e *AssumptionFailedError) Error() string {
	return fmt.Sprintf("auth: the assumption policy permitted %q and obtaining the credential then failed: %v",
		truncate(e.Target, 128), e.Err)
}

// Unwrap returns the original failure.
func (e *AssumptionFailedError) Unwrap() error { return e.Err }

// assumptionFailed marks a failure that happened after the assumption policy
// allowed the request, and passes a success (nil) through.
//
// Every exit from [Broker.Credential] below the policy call, and the apply in
// [Broker.Authorize], go through this: those are exactly the paths on which a
// decision was made and something else then failed.
func assumptionFailed(target string, err error) error {
	if err == nil {
		return nil
	}

	return &AssumptionFailedError{Target: target, Err: err}
}
