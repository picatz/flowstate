package authtest_test

import (
	"context"
	"fmt"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
)

// Example proves a trust policy end to end: an issuer this process controls, a
// token it mints, and the principal a verifier built from the policy derives
// from it.
//
// Everything the policy says is exercised at once. The issuer is discovered,
// its key set is fetched, the signature is checked against a published key, the
// claim rule is applied, and the namespace the caller lands in is read off the
// policy rather than off the token.
func Example() {
	issuer := authtest.NewIssuer()
	defer func() { _ = issuer.Close() }()

	verifier, err := auth.NewOIDCVerifier(auth.Policy{
		Issuers: []auth.TrustedIssuer{{
			Name:      "ci",
			Issuer:    issuer.URL(),
			Audiences: []string{"flowstate"},
			Require: []auth.ClaimRule{
				auth.RequireClaim("team", "platform"),
			},
			Role:      "deployer",
			Namespace: "acme",
		}},
	}, auth.WithEgressPolicy(authtest.EgressPolicy()))
	if err != nil {
		fmt.Println("policy:", err)
		return
	}

	token := issuer.MintToken(
		map[string]any{"team": "platform"},
		authtest.WithSubject("build-runner"),
		authtest.WithAudience("flowstate"),
	)

	principal, err := verifier.Verify(context.Background(), token)
	if err != nil {
		fmt.Println("verify:", err)
		return
	}

	fmt.Println("subject:  ", principal.Subject)
	fmt.Println("role:     ", principal.Role)
	fmt.Println("namespace:", principal.Namespace)

	// A token from another team is refused by the same policy, which is the
	// half of a configuration that is easy to leave unproven.
	_, err = verifier.Verify(context.Background(), issuer.MintToken(
		map[string]any{"team": "someone-else"},
		authtest.WithSubject("build-runner"),
		authtest.WithAudience("flowstate"),
	))
	fmt.Println("other team:", err != nil)

	// So is a token addressed to somebody else, whatever its claims say.
	_, err = verifier.Verify(context.Background(), issuer.MintToken(
		map[string]any{"team": "platform"},
		authtest.WithSubject("build-runner"),
		authtest.WithAudience("some-other-service"),
	))
	fmt.Println("other audience:", err != nil)

	// Output:
	// subject:   build-runner
	// role:      deployer
	// namespace: acme
	// other team: true
	// other audience: true
}
