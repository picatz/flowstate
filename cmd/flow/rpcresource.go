package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// The canonical resource identifier `flow server`'s Connect RPC surface
// answers as, and the `aud` claim it therefore requires of every bearer token
// spent there.
//
// rpcResourceFlags are deliberately separate from --protected-resource. The
// latter identifies the remote MCP surface; this value identifies Connect RPC.
// A future ordinary HTTP API must get a third identifier rather than borrowing
// either one and reopening cross-surface replay.
type rpcResourceFlags struct {
	resource                 string
	allowIssuerWideAudiences bool
}

func addRPCResourceFlags(cmd *cobra.Command) {
	cmd.Flags().String("rpc-resource", os.Getenv("FLOWSTATE_RPC_RESOURCE"),
		"canonical resource URI required in the aud claim of every bearer token spent on the Connect RPC surface (default $FLOWSTATE_RPC_RESOURCE); must be an absolute HTTPS URI with no fragment or trailing slash and appear in at least one kind: oidc issuer's audiences. Required whenever --auth-policy trusts an issuer that mints bearer tokens")
	cmd.Flags().Bool("allow-issuer-wide-audiences", false,
		"migration-only: accept any audience listed for a token's trusted issuer on Connect RPC; explicitly restores the pre-resource behavior and cannot be combined with --rpc-resource")
}

func rpcResourceFlagsOf(cmd *cobra.Command) rpcResourceFlags {
	resource, _ := cmd.Flags().GetString("rpc-resource")
	legacy, _ := cmd.Flags().GetBool("allow-issuer-wide-audiences")
	return rpcResourceFlags{resource: resource, allowIssuerWideAudiences: legacy}
}

// resolveRPCResource turns the flags into the resource
// [auth.WithExpectedResource] narrows the Connect RPC surface to, or the empty
// string for a deployment that has no bearer audience to narrow.
//
// # Required, but only where there is something to require it of
//
// Binding tokens to a per-surface audience is the secure default, so a
// deployment that could name its RPC resource and has not is refused here
// rather than started with the check missing — that is the case where the
// absence is a real hole, since a trust policy entry listing both this
// deployment's RPC and MCP audiences otherwise lets a token minted for one be
// spent at the other.
//
// The requirement is conditional on there being a bearer issuer to bind to,
// which is the whole of the difference between a fail-closed default and an
// unstartable one. Three deployments have no such issuer:
//
//   - --insecure-no-auth, which authenticates nobody at all;
//   - a trust policy of nothing but kind: mtls entries, which admits callers by
//     client certificate — [auth.TrustedIssuer]'s own validation refuses one of
//     those an `audiences` list, so no resource an operator could name would
//     ever validate and the requirement could only be satisfied by reaching for
//     the migration flag (reported by Copilot on picatz/flowstate#1007);
//   - and, transitively, `flow server --dev`, which builds its handler from
//     [auth.InsecureAnonymousVerifier] without passing through here.
//
// None of those has a token whose "aud" any surface would check, so demanding
// a resource of them refuses a deployment for failing to name something
// nothing would look at. Both flags are refused there too, rather than
// ignored: a value that cannot take effect is a misconfiguration its author
// should hear about, and silence would leave an operator believing an audience
// is being enforced on a surface that has no audiences.
func resolveRPCResource(flags rpcResourceFlags, authCfg authFlags, policy *auth.Policy) (string, error) {
	if flags.resource != "" && flags.allowIssuerWideAudiences {
		return "", errors.New("--rpc-resource and --allow-issuer-wide-audiences are mutually exclusive: " +
			"one binds Connect RPC to a single audience and the other deliberately stops it doing so, " +
			"so a deployment giving both has not said which it wants")
	}

	if authCfg.insecure {
		if flags.resource != "" || flags.allowIssuerWideAudiences {
			return "", errors.New("--rpc-resource and --allow-issuer-wide-audiences configure bearer " +
				"authentication and cannot be used with --insecure-no-auth: that flag authenticates " +
				"every caller as anonymous without a token, so there is no audience to bind")
		}
		return "", nil
	}

	// Asked of the policy rather than of the flags, because "can a token be
	// minted for this deployment at all" is a property of the trust policy and
	// nothing else — see [auth.AdmitsBearerTokens]. A nil policy cannot reach
	// here (authVerifier has already refused a non-insecure server without
	// one), and reads as "no bearer issuers" if it ever did, which is the same
	// answer for the same reason.
	if !auth.AdmitsBearerTokens(policy) {
		if flags.resource != "" || flags.allowIssuerWideAudiences {
			return "", fmt.Errorf("--rpc-resource and --allow-issuer-wide-audiences bind bearer tokens "+
				"to an audience, and the trust policy in %s has no kind: oidc issuer to mint one: every "+
				"entry admits a caller by client certificate, which carries no audience claim. Remove "+
				"the flag, or add a kind: oidc entry listing the RPC resource among its audiences",
				authCfg.policyPath)
		}
		return "", nil
	}

	if flags.resource == "" {
		if flags.allowIssuerWideAudiences {
			return "", nil
		}
		return "", fmt.Errorf("--rpc-resource (or FLOWSTATE_RPC_RESOURCE) is required with --auth-policy: "+
			"Connect RPC binds tokens to its own audience by default, and the trust policy in %s trusts "+
			"an issuer that mints bearer tokens — without a resource, a token minted for any audience "+
			"that issuer is trusted for is spendable here, including one minted for this deployment's "+
			"MCP surface. Name the RPC audience, or, during migration only, pass "+
			"--allow-issuer-wide-audiences to restore the previous issuer-wide audience behavior",
			authCfg.policyPath)
	}

	if err := auth.ValidateResourceAudience(flags.resource, policy); err != nil {
		return "", fmt.Errorf("--rpc-resource: %w", err)
	}

	return flags.resource, nil
}
