package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

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
		"canonical resource URI required in the aud claim of every bearer token spent on the Connect RPC surface (default $FLOWSTATE_RPC_RESOURCE); must be an absolute HTTPS URI with no fragment or trailing slash and appear in at least one trusted issuer's audiences")
	cmd.Flags().Bool("allow-issuer-wide-audiences", false,
		"migration-only: accept any audience listed for a token's trusted issuer on Connect RPC; explicitly restores the pre-resource behavior and cannot be combined with --rpc-resource")
}

func rpcResourceFlagsOf(cmd *cobra.Command) rpcResourceFlags {
	resource, _ := cmd.Flags().GetString("rpc-resource")
	legacy, _ := cmd.Flags().GetBool("allow-issuer-wide-audiences")
	return rpcResourceFlags{resource: resource, allowIssuerWideAudiences: legacy}
}

func resolveRPCResource(flags rpcResourceFlags, authCfg authFlags, policy *auth.Policy) (string, error) {
	if flags.resource != "" && flags.allowIssuerWideAudiences {
		return "", errors.New("--rpc-resource and --allow-issuer-wide-audiences are mutually exclusive")
	}
	if authCfg.insecure {
		if flags.resource != "" || flags.allowIssuerWideAudiences {
			return "", errors.New("--rpc-resource and --allow-issuer-wide-audiences configure bearer authentication and cannot be used with --insecure-no-auth")
		}
		return "", nil
	}
	if flags.resource == "" {
		if flags.allowIssuerWideAudiences {
			return "", nil
		}
		return "", errors.New("--rpc-resource (or FLOWSTATE_RPC_RESOURCE) is required with --auth-policy: Connect RPC binds tokens to its own audience by default; during migration only, pass --allow-issuer-wide-audiences to restore the previous issuer-wide audience behavior")
	}
	if err := auth.ValidateResourceAudience(flags.resource, policy); err != nil {
		return "", fmt.Errorf("--rpc-resource: %w", err)
	}
	return flags.resource, nil
}
