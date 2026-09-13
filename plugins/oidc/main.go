package main

import (
	"context"
	"fmt"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// operatorProviders is what this plugin was configured to mint for, read once
// at startup. Nil means the file was missing or unusable.
var (
	operatorProviders *providers
	providersRefusal  error

	// egressPolicy is the deployment's grant, which governs reaching a token
	// endpoint exactly as it governs every other outbound request.
	egressPolicy *netpolicy.Policy
)

func main() {
	installEgressPolicy()
	loadOperatorProviders()

	sdk.Main(sdk.Plugin{
		Name:        "oidc",
		Version:     "0.1.0",
		Description: "Mints short-lived access tokens through the OAuth 2.0 client credentials grant and resolves them as ${secret('oidc:<provider>')} at the point of use.",
		Secrets: &sdk.Secrets{
			Schemes: []string{secretScheme},
			Resolve: resolveSecret,
		},
		Health: checkHealth,
	})
}

// installEgressPolicy takes the deployment's grant.
//
// The deployment default is accepted: a token endpoint is an HTTPS POST to the
// organization's authorization server, which is what the default permits. An
// operator who wants these exchanges confined to one host writes the policy,
// and this plugin obeys it because the exchanger is built with it.
//
// A grant this process cannot use leaves the exchanger with a deny-by-default
// policy rather than none, so a misconfigured worker mints nothing instead of
// reaching anywhere.
func installEgressPolicy() {
	policy, err := sdk.EgressPolicy()
	if err == nil {
		egressPolicy = policy
		return
	}

	denying, buildErr := netpolicy.New()
	if buildErr != nil {
		// Unreachable in practice, and fatal if it happened: with no policy at
		// all the exchanger would be unbounded.
		fmt.Fprintf(os.Stderr, "oidc: no usable egress policy (%v) and no deny-by-default to fall back on: %v\n", err, buildErr)
		os.Exit(1)
	}
	egressPolicy = denying
	fmt.Fprintf(os.Stderr, "oidc: no usable egress policy (%v); every exchange will be denied\n", err)
}

// loadOperatorProviders reads the providers file, keeping the reason it could
// not. A missing file does not stop the process: a worker discovering an
// unconfigured plugin should still see it, and every resolution says what is
// missing.
func loadOperatorProviders() {
	parsed, err := loadProviders()
	if err != nil {
		providersRefusal = err
		fmt.Fprintf(os.Stderr, "oidc: no usable providers: %v\n", err)
		return
	}
	operatorProviders = parsed
}

// checkHealth reports whether this plugin could mint anything at all.
func checkHealth(_ context.Context) error {
	if operatorProviders == nil {
		return fmt.Errorf("no usable providers: %v", providersRefusal)
	}
	return nil
}
