package auth

import (
	"context"
	"fmt"
)

// admitBearer is the transport-neutral bearer admission sequence shared by
// the Connect and MCP surfaces. A new bearer refusal belongs here unless its
// transport has a reason the other surface cannot share.
//
// The order is part of the contract: verify, refuse an empty identity, refuse
// anonymous access where the surface cannot represent it, refuse delegation
// claims nobody interprets, then bind the token to this surface's resource.
// Transport adapters add only their own aftermath, such as Connect's mTLS
// agreement check or MCP's session binding.
func admitBearer(
	ctx context.Context,
	verifier Verifier,
	token string,
	resource string,
	refuseAnonymous bool,
) (Principal, error) {
	if verifier == nil {
		verifier = unconfiguredVerifier{}
	}

	principal, err := verifier.Verify(ctx, token)
	if err != nil {
		return Principal{}, err
	}
	if principal.IsZero() {
		return Principal{}, fmt.Errorf("%w: verifier returned no identity", ErrNoToken)
	}
	if refuseAnonymous && principal.IsAnonymous() {
		return Principal{}, fmt.Errorf("%w: anonymous access is not available on this surface", ErrNoToken)
	}
	if err := refuseDelegationClaims(principal.Claims); err != nil {
		return Principal{}, err
	}
	if resource != "" && !principal.HasAudience(resource) {
		// The resource is not named in the error: a caller holding a token for
		// some other service learns its audience was wrong, and does not learn
		// this deployment's resource identifier from a failure.
		return Principal{}, fmt.Errorf("%w: the token's audience does not name this resource", ErrInvalidAudience)
	}

	return principal, nil
}
