package main

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// secretScheme is what a Flowfile writes: `${secret('oidc:billing-api')}`.
//
// The engine refuses two plugins claiming one scheme, so this name is a
// deployment-wide statement: whatever answers for "oidc:" is this plugin.
const secretScheme = "oidc"

// resolveSecret mints a token for the provider a reference names.
//
// The host has already decided that this workload may ask for this reference -
// that is the secret access policy's job, and it happens before a plugin is
// consulted. What this function decides is narrower and its own: whether the
// operator granted *this namespace* this provider, and what the authorization
// server says.
func resolveSecret(ctx context.Context, req sdk.SecretRequest) (sdk.SecretResponse, error) {
	if operatorProviders == nil {
		return sdk.SecretResponse{}, sdk.Failed("%v", providersRefusal)
	}

	name := req.Name
	if !referenceNamePattern.MatchString(name) {
		return sdk.SecretResponse{}, sdk.InvalidInput(
			"%q is not a provider name; a name is lower-case letters, digits and interior hyphens", truncate(name, 64))
	}

	configured, ok := operatorProviders.Providers[name]
	if !ok {
		return sdk.SecretResponse{}, sdk.NotFound(
			"no provider named %q; this worker configures %s", truncate(name, 64), configuredNames())
	}

	// The namespace the host established for the calling workload, never one
	// the workload declared. A provider naming namespaces is one another
	// tenant's workflows cannot mint from.
	if !configured.reachableFrom(req.Namespace) {
		return sdk.SecretResponse{}, sdk.PermissionDenied(
			"the provider %q is not configured for this workload's namespace", truncate(name, 64))
	}

	secret, err := configured.secret(name)
	if err != nil {
		// The secret's own bytes are never in a message; this says which file
		// could not be used, not what was in it.
		return sdk.SecretResponse{}, sdk.Failed("%v", err)
	}

	exchanger, err := auth.NewClientCredentialsExchanger(auth.ClientCredentialsConfig{
		Name:                  "oidc:" + name,
		TokenURL:              configured.TokenURL,
		ClientID:              configured.ClientID,
		ClientSecret:          secret,
		Scopes:                configured.Scopes,
		MaxCredentialLifetime: configured.MaxLifetime.duration(defaultCredentialLifetime),
		Timeout:               configured.Timeout.duration(defaultExchangeTimeout),
		EgressPolicy:          egressPolicy,
	})
	if err != nil {
		return sdk.SecretResponse{}, sdk.Failed("provider %q cannot be used: %v", truncate(name, 64), err)
	}

	// The client credentials grant authenticates with the client secret above,
	// so the assertion is unused - it is the parameter the interface carries for
	// the assertion-based grants this plugin does not implement.
	credential, err := exchanger.Exchange(ctx, auth.Assertion{})
	if err != nil {
		return sdk.SecretResponse{}, classifyExchange(name, err)
	}

	token, ok := credential.Bearer()
	if !ok || token == "" {
		return sdk.SecretResponse{}, sdk.Failed(
			"the authorization server for %q returned no access token", truncate(name, 64))
	}

	// The lifetime the authorization server reported, so the engine caches this
	// no longer than the issuer considers it valid. Nothing here caches a
	// credential of its own: a second cache would be a second answer about when
	// a token stops being usable.
	return sdk.SecretResponse{Value: []byte(token), ExpiresIn: lifetime(credential)}, nil
}

// lifetime is how long the engine may keep this credential, from the expiry the
// authorization server reported.
//
// A margin is taken off, for the reason auth.DefaultRefreshMargin exists: a
// credential handed to a caller with a second left is a credential that expires
// mid-request. A credential already inside the margin still travels - it is
// what the issuer minted, and refusing it would fail a call that would have
// worked - but the engine is told not to keep it.
func lifetime(credential auth.Credential) time.Duration {
	remaining := time.Until(credential.ExpiresAt) - auth.DefaultRefreshMargin
	if remaining <= 0 {
		return 0
	}
	return remaining
}

// classifyExchange turns an exchange failure into the SDK's classification.
//
// The distinction that matters is between an authorization server that refused
// this client - which no retry fixes, because the same secret is sent again -
// and one that could not be reached, which is the one retryable case.
func classifyExchange(name string, err error) error {
	var denied *netpolicy.DenyError
	if errors.As(err, &denied) {
		return sdk.PermissionDenied(
			"the deployment's egress policy does not permit reaching the token endpoint for %q", truncate(name, 64))
	}
	// The exchanger separates the two cases itself, which is the whole reason
	// it has two sentinels: a transient transport failure wraps both, so the
	// transient one is checked first.
	if errors.Is(err, auth.ErrExchangeUnavailable) {
		return sdk.Unavailable("the token endpoint for %q could not be reached: %v", truncate(name, 64), err)
	}
	if errors.Is(err, auth.ErrExchangeFailed) {
		return sdk.PermissionDenied("the authorization server refused this client for %q: %v", truncate(name, 64), err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return sdk.Unavailable("the token endpoint for %q did not answer in time", truncate(name, 64))
	}
	return sdk.Unavailable("minting a token for %q failed: %v", truncate(name, 64), err)
}

// configuredNames renders what this worker does configure, so an author who
// mistyped a reference learns the names rather than searching a file they may
// not be able to read.
func configuredNames() string {
	names := slices.Sorted(maps.Keys(operatorProviders.Providers))
	if len(names) == 0 {
		return "none"
	}
	if len(names) > 20 {
		names = names[:20]
	}
	return truncate(strings.Join(names, ", "), 512)
}
