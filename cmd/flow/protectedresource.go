package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// RFC 9728 protected resource metadata, an operator's flags for it, and how
// they turn into an [auth.ProtectedResource]. picatz/flowstate#558's slice
// one: publish the document and challenge for it; no scope vocabulary, no
// authorization endpoints — see [addProtectedResourceFlags] for the deferral.

// addProtectedResourceFlags declares the protected-resource surface on the
// server command.
//
// Flags, matching TLS's own pattern (cmd/flow/tls.go), not schema: this is a
// serving-surface concern an operator sets per deployment, and #567's review
// of #558 endorses flags over proto config for exactly that kind of setting.
func addProtectedResourceFlags(cmd *cobra.Command) {
	cmd.Flags().String("protected-resource", os.Getenv("FLOWSTATE_PROTECTED_RESOURCE"),
		"the canonical resource URI (RFC 8707 section 2) this deployment's MCP surface "+
			"identifies as (overrides FLOWSTATE_PROTECTED_RESOURCE). No fragment, no trailing "+
			"slash. Given together with one or more --authorization-server, this deployment "+
			"serves RFC 9728 protected resource metadata at "+auth.ProtectedResourceMetadataPath+
			" and every 401 challenge names that document. Unset (the default): the route does "+
			"not exist and every challenge reads exactly as it does today")

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
	return auth.NewProtectedResource(auth.ProtectedResourceConfig{
		Resource:             flags.resource,
		AuthorizationServers: flags.authorizationServers,
	}, policy)
}
