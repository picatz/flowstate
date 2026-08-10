package auth_test

import (
	"errors"
	"fmt"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// ExampleNewOIDCVerifier builds a verifier from a trust policy: which issuer to
// trust, the audience a token must carry, the claim a caller must prove, and
// the role and namespace this deployment grants callers the entry admits. The
// role and namespace come from the policy and never from the token, which is
// what stops a caller choosing its own.
//
// Construction validates the policy up front and fetches no keys; a live verify
// needs a real issuer and clock, so it is shown in the authtest package's own
// example rather than here where the output must stay deterministic. What this
// example does show, offline, is a property the construction guarantees: the
// policy is well-formed and the verifier is ready.
func ExampleNewOIDCVerifier() {
	verifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:      "ci",
			Issuer:    "https://token.actions.githubusercontent.com",
			Audiences: []string{"flowstate"},
			Require: []auth.ClaimRule{
				auth.RequireClaim("repository", "acme/app"),
			},
			Role:      "deployer",
			Namespace: "acme",
		}},
	})
	if err != nil {
		fmt.Println("policy:", err)
		return
	}

	fmt.Println("verifier built:", verifier != nil)

	// Output:
	// verifier built: true
}

// ExampleNewOIDCVerifier_failClosed shows the half of a policy that is easy to
// leave unproven: a malformed one is refused at construction, not at the first
// request. Here one issuer determines a namespace and another does not, which
// would admit some callers into a shared namespace alongside tenants meant to
// be separated, so the whole policy is rejected as [auth.ErrInvalidPolicy].
func ExampleNewOIDCVerifier_failClosed() {
	_, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{
			{
				Name:      "tenant-a",
				Issuer:    "https://issuer-a.example.com",
				Audiences: []string{"flowstate"},
				Namespace: "tenant-a",
			},
			{
				Name:      "shared",
				Issuer:    "https://issuer-b.example.com",
				Audiences: []string{"flowstate"},
				// No namespace: this entry would admit callers into a shared tenant.
			},
		},
	})

	fmt.Println("refused:", err != nil)
	fmt.Println("invalid policy:", errors.Is(err, auth.ErrInvalidPolicy))

	// Output:
	// refused: true
	// invalid policy: true
}
