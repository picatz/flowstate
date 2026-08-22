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
// So the refusal is performed by [OIDCVerifier.Verify], the admission path
// both surfaces run for every request: an [Authenticator] and an
// [MCPTokenVerifier] each hold a [Verifier] and neither shares any other code,
// while [OIDCVerifier] is the only implementation in this repository that
// turns an externally minted token into a [Principal] — the other two
// ([InsecureAnonymousVerifier] and the unconfigured stand-in) never read a
// token at all. A surface added tomorrow inherits the refusal by verifying
// tokens, which is the only thing a bearer surface cannot skip.
//
// [MCPTokenVerifier] keeps its own call to the same helper, because it accepts
// any [Verifier] and a deployment (or a test) that hands it something other
// than an [OIDCVerifier] must not thereby lose the refusal. Two call sites,
// one spelling: the thing CLAUDE.md's "prefer deriving to duplicating" is
// actually about is the *decision* being written down twice, and it is not.
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
