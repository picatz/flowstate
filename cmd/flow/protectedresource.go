package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// RFC 9728 protected resource metadata, an operator's flags for it, and how
// they turn into an [auth.ProtectedResource]. picatz/flowstate#558's slice
// one: publish the document and challenge for it, and — since
// picatz/flowstate#567's D1 answered the vocabulary question — publish the
// schema's action list as "scopes_supported". Still no authorization
// endpoints; see [addProtectedResourceFlags] for what remains deferred.

// addProtectedResourceFlags declares the protected-resource surface on the
// server command.
//
// Flags, matching TLS's own pattern (cmd/flow/tls.go), not schema: this is a
// serving-surface concern an operator sets per deployment, and #567's review
// of #558 endorses flags over proto config for exactly that kind of setting.
//
// unsetBehavior is the one sentence that differs between the two commands
// that declare these, and it is not cosmetic: on `flow server` an unset
// --protected-resource means the route simply does not exist, and on
// `flow mcp serve` it means the command refuses to start, because that
// surface *is* the protected resource. Help text that described one while
// the command did the other would be a diagnostic that lies, which this
// repository counts as worse than a missing one. Passed rather than derived,
// so adding a third caller is a moment somebody has to decide what it says.
// Reported by Codex on picatz/flowstate#807.
func addProtectedResourceFlags(cmd *cobra.Command, unsetBehavior string) {
	cmd.Flags().String("protected-resource", os.Getenv("FLOWSTATE_PROTECTED_RESOURCE"),
		"the canonical resource URI (RFC 8707 section 2) this deployment's MCP surface "+
			"identifies as (overrides FLOWSTATE_PROTECTED_RESOURCE). No fragment, no trailing "+
			"slash. Given together with one or more --authorization-server, this deployment "+
			"serves RFC 9728 protected resource metadata at "+auth.ProtectedResourceMetadataPath+
			", plus this resource's own path if it has one (RFC 9728 section 3.1's well-known-URI "+
			"construction — a resource ending in /mcp serves its document at "+
			auth.ProtectedResourceMetadataPath+"/mcp, not at the bare prefix), and every 401 "+
			"challenge names that exact document. "+unsetBehavior)

	cmd.Flags().StringArray("authorization-server", nil,
		"an authorization server this deployment advertises as able to mint tokens for "+
			"--protected-resource. Repeatable; RFC 9728 requires at least one when "+
			"--protected-resource is given. Each one must already be a kind: oidc issuer in "+
			"--auth-policy — an authorization server this deployment's own verifier would "+
			"reject is refused at start-up rather than advertised")
}

// protectedResourceFlags is what an operator asked for, read once before
// anything is validated.
type protectedResourceFlags struct {
	resource             string
	authorizationServers []string
}

// protectedResourceFlagsOf reads them off the command being run.
func protectedResourceFlagsOf(cmd *cobra.Command) protectedResourceFlags {
	resource, _ := cmd.Flags().GetString("protected-resource")
	authorizationServers, _ := cmd.Flags().GetStringArray("authorization-server")

	return protectedResourceFlags{
		resource:             resource,
		authorizationServers: authorizationServers,
	}
}

// resolveProtectedResource turns flags into an [auth.ProtectedResource], or
// reports that none was configured.
//
// A nil, nil return means --protected-resource was never given: the route
// this slice adds does not exist at all on that deployment, rather than
// existing and answering an empty document — the same "absence is the whole
// answer" shape [tlsFlagsOf]/[serverTLSConfig] already use for TLS. Every
// other outcome that is not a valid, policy-consistent configuration is a
// start-up error, per CLAUDE.md's "fail closed": an authorization server this
// deployment would advertise but its own trust policy would refuse a token
// from is reported once, here, rather than trusted by a client that then
// gets nothing but 401s.
func resolveProtectedResource(flags protectedResourceFlags, policy *auth.Policy) (*auth.ProtectedResource, error) {
	if flags.resource == "" {
		if len(flags.authorizationServers) > 0 {
			return nil, fmt.Errorf("--authorization-server given without --protected-resource: " +
				"there is no resource identifier to advertise it for")
		}
		return nil, nil
	}

	// No flag name prefixed here: auth.NewProtectedResource's own errors
	// already name the field at fault ("resource" or "authorization_servers"),
	// which is --protected-resource for one and --authorization-server for
	// the other — a blanket "--protected-resource: " prefix would misname the
	// second.
	// The scope vocabulary is not a flag and is deliberately not one: which
	// actions exist is the schema's answer (#567's D1, answered in
	// proto/flowstate/v1/authorization.proto), and an operator who could
	// narrow or extend it here would be publishing a spelling policy does not
	// read. This is the one place it is supplied, which is why
	// TestProtectedResourceDocumentPublishesTheActionVocabulary pins the
	// document this function produces rather than a hand-written list.
	return auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             flags.resource,
		AuthorizationServers: flags.authorizationServers,
	}, policy, auth.WithScopesSupported(v1.AuthorizationActionScopes()))
}

// checkProtectedResourceRouteCollision refuses a configuration where the
// computed protected-resource mount path (see [auth.ProtectedResource.Path])
// is identical to another route serverHandler registers unconditionally at a
// fixed pattern: the OIDC discovery document, or the issuer's own key-set
// path — which, unlike the discovery path, an operator can move with
// federation's own `jwks_path` (see [auth.FederationPolicy.JWKSPath]),
// making the collision reachable from ordinary configuration rather than
// only from a resource whose path happens to spell a well-known suffix.
//
// http.ServeMux panics on a second registration of an identical pattern
// (verified: "pattern %q ... conflicts with pattern %q"), which without this
// check would turn two independently valid flags into a crash at start-up
// instead of a diagnosis. broker nil (no federation configured) skips the
// JWKS half; there is nothing to collide with.
func checkProtectedResourceRouteCollision(pr *auth.ProtectedResource, broker *auth.Broker) error {
	if pr == nil {
		return nil
	}

	if pr.Path() == auth.DiscoveryPath {
		return fmt.Errorf("--protected-resource: the computed metadata path %q is identical to the "+
			"OIDC discovery path this deployment already serves at a fixed location; choose a resource "+
			"whose path does not end in %q", pr.Path(), auth.DiscoveryPath)
	}

	if broker != nil {
		if jwksPath := broker.Issuer().JWKSPath(); pr.Path() == jwksPath {
			return fmt.Errorf("--protected-resource: the computed metadata path %q is identical to "+
				"this deployment's JWKS path (%q, from --auth-policy's federation.jwks_path); the "+
				"two would register the same route and this server would panic at start-up rather "+
				"than serve either one — change one of the two paths", pr.Path(), jwksPath)
		}
	}

	return nil
}
