package main

import (
	"context"
	"fmt"
	"os"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/netpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	josev1 "github.com/picatz/flowstate/plugins/jose/gen/jose/v1"
)

// verifier is the engine's own OIDC verifier, built once from the operator's
// trust policy. Nil means there is no usable trust and [trustRefusal] says why.
var (
	verifier     auth.Verifier
	trustRefusal error
)

func main() {
	installVerifier()

	sdk.Main(sdk.Plugin{
		Name:        "jose",
		Version:     "0.1.0",
		Description: "Verifies a JWT against the issuers an operator trusts, and returns the verified claims - trust roots are configuration, never workflow input.",
		Tasks: []sdk.Task{{
			Name:    "verify",
			Summary: "Verify one JWT against the operator's trust policy and return its verified subject, audience and claims.",
			Input:   &josev1.VerifyInputs{},
			Output:  &josev1.VerifyOutputs{},
			// Accepted as a secret reference, not required as one: a token to
			// be verified usually arrives in the run rather than from the
			// deployment's secret store.
			SecretInputs: []string{"token"},
			Fn:           joseVerify,
		}},
		Health: checkHealth,
	})
}

// installVerifier builds the verifier from the operator's policy.
//
// The deployment's egress policy is handed to it, so fetching an issuer's key
// set is governed exactly like every other outbound request this worker makes -
// a JWKS URL is a destination, and an issuer this deployment does not permit
// reaching is one whose tokens cannot be verified here.
func installVerifier() {
	policy, err := loadTrust()
	if err != nil {
		trustRefusal = err
		fmt.Fprintf(os.Stderr, "jose: no usable trust policy: %v\n", err)
		return
	}

	// The trust policy's own egress section is the file spelling of
	// auth.WithEgressPolicy, and auth.NewOIDCVerifier refuses both at once. An
	// operator who pointed this plugin at the same complete document
	// `flow server --auth-policy` reads has therefore already decided where
	// identity fetches may go, and the deployment's grant does not get a
	// second, contradictory say.
	var options []auth.Option
	if policy.Egress == nil {
		egress, err := verifierEgress()
		if err != nil {
			trustRefusal = err
			fmt.Fprintf(os.Stderr, "jose: %v\n", err)
			return
		}
		options = append(options, auth.WithEgressPolicy(egress))
	}

	built, err := auth.NewOIDCVerifier(policy, options...)
	if err != nil {
		trustRefusal = fmt.Errorf("the trust policy could not be turned into a verifier: %w", err)
		fmt.Fprintf(os.Stderr, "jose: %v\n", trustRefusal)
		return
	}
	verifier = built
}

// verifierEgress is the policy an issuer's key set is fetched under.
//
// A grant this process cannot use is not a reason to verify nothing: a policy
// whose issuers are all JWKSFile entries needs no network at all. What it must
// not do is fetch under no policy - and an empty netpolicy.New() is not that,
// because its defaults permit public HTTP and HTTPS, which is every JWKS URL on
// the internet. A deny rule matching every request is what actually denies one,
// which is what a missing grant has to mean here.
func verifierEgress() (*netpolicy.Policy, error) {
	granted, err := sdk.EgressPolicy()
	if err == nil {
		return granted, nil
	}

	denying, buildErr := netpolicy.New(netpolicy.WithDenyRules("true"))
	if buildErr != nil {
		return nil, fmt.Errorf("no usable egress policy (%v) and no deny-by-default to fall back on: %w", err, buildErr)
	}
	return denying, nil
}

// checkHealth reports whether this plugin could verify anything at all. Without
// a trust policy it cannot, and an operator who fixes the file and restarts
// should see that here rather than in the first workflow that fails.
func checkHealth(_ context.Context) error {
	if verifier == nil {
		return fmt.Errorf("no usable trust policy: %v", trustRefusal)
	}
	return nil
}
