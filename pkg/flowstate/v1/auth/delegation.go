package auth

import (
	"errors"
	"fmt"
)

// Delegation claims an inbound token may carry that this deployment refuses
// rather than interprets, and the one place the refusal is spelled.
//
// Both are RFC 8693 (OAuth 2.0 Token Exchange) delegation claims: "act"
// records that the bearer is an agent exercising some other party's
// authority, and "may_act" records that the token's subject permits some
// other party to do so in a future token. Flowstate has nowhere to map
// either yet — a trust policy cannot express which claim carries a
// delegator, and picatz/flowstate#567's D2 leaves that naming decision open —
// so a token carrying one is refused outright.
//
// Refused, specifically, rather than stripped. Admitting the request as the
// bare subject named by "sub" would let it proceed while the audit record
// said nothing was delegated: the token itself claims an agent is acting for
// somebody, and a deployment that cannot yet read that claim must not answer
// as though it were never made. That is #567's S7a amendment, "deferred
// fail-closed", and it is the confused-deputy shape stated one layer earlier
// than the mapping that would otherwise get it wrong.
//
// # Why this lives beside the verifier rather than in one surface's adapter
//
// It shipped inside [MCPTokenVerifier], which meant exactly one of this
// repository's two bearer surfaces refused a delegated token: the Connect RPC
// surface ([Authenticator]) admitted the identical token, so a token denied at
// MCP walked in through RPC. A refusal that one surface performs and another
// does not is not a policy, it is an accident of which file it was written in
// — and CLAUDE.md's "fail closed" wants the answer to be structural rather
// than remembered at each new surface.
//
// So the refusal is performed by [OIDCVerifier.Verify], the admission path the
// repository's own verifier runs for every request, *and* re-checked by each
// surface on the [Principal] its [Verifier] returned. That belt-and-braces is
// not redundancy for its own sake: a [Verifier] is an interface, and an
// [Authenticator] or an [MCPTokenVerifier] will accept any implementation of
// it. [OIDCVerifier] is the only one in this repository that turns an
// externally minted token into a delegation-bearing [Principal] — but a custom
// or test [Verifier] that returned one would sail past a surface that trusted
// the verifier to have refused it. So the surface refuses too, on the value it
// is about to admit, which is the only thing that makes the guarantee hold for
// every [Verifier] rather than only for the default one.
//
// Both [Authenticator.Authenticate] and [MCPTokenVerifier] therefore call this
// helper on the returned [Principal]'s claims. Three call sites, one spelling:
// the thing CLAUDE.md's "prefer deriving to duplicating" is actually about is
// the *decision* being written down more than once, and it is not — every one
// of them calls [refuseDelegationClaims]. The verifier-internal check is then
// defense in depth: behind an [OIDCVerifier] the token never decodes to an
// admitted [Principal] in the first place.
//
// These are deliberately not [ClaimOnBehalfOf]: that one is a claim
// Flowstate's own issuer *mints* into an assertion it is vouching for, where
// these two are claims an external identity provider mints into a token
// Flowstate only verifies. See pkg/flowstate/v1/authtest/negative.go, whose
// WithDelegation and WithMayAct mint exactly the tokens this refuses.
const (
	// ClaimActor is RFC 8693 section 4.1's "act".
	ClaimActor = "act"

	// ClaimMayAct is RFC 8693 section 4.4's "may_act".
	ClaimMayAct = "may_act"
)

// DelegationClaimError reports which delegation claim a token carried. It
// wraps [ErrDelegatedToken], and names the claim's *key* and never its value:
// the key is one of two constants above, while the value is an object the
// issuer filled in and the thing a future mapping would have to interpret.
type DelegationClaimError struct {
	// Claim is [ClaimActor] or [ClaimMayAct].
	Claim string
}

// Error implements error.
func (e *DelegationClaimError) Error() string {
	return fmt.Sprintf("%s: the token carries a %q delegation claim, which this deployment "+
		"does not interpret and will not ignore", ErrDelegatedToken.Error(), e.Claim)
}

// Unwrap reports [ErrDelegatedToken], so callers match the class with
// [errors.Is] and read the claim key with [errors.As].
func (e *DelegationClaimError) Unwrap() error { return ErrDelegatedToken }

// refuseDelegationClaims returns a [DelegationClaimError] when a verified
// claims set carries a delegation claim, and nil when it does not.
//
// Presence alone is enough — the value is never read, because there is
// nothing here yet that could read it correctly.
func refuseDelegationClaims(claims map[string]any) error {
	// Checked in a fixed order rather than by ranging the map, so a token
	// carrying both is always refused by naming the same one and a test can
	// assert the message.
	for _, name := range []string{ClaimActor, ClaimMayAct} {
		if _, present := claims[name]; present {
			return &DelegationClaimError{Claim: name}
		}
	}

	return nil
}

// delegationClaimOf reports the claim key a delegation refusal named, for
// [publicReason]. It reports "" for any other error.
func delegationClaimOf(err error) string {
	var delegation *DelegationClaimError
	if errors.As(err, &delegation) {
		return delegation.Claim
	}

	return ""
}
