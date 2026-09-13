package main

import (
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/authtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	josev1 "github.com/picatz/flowstate/plugins/jose/gen/jose/v1"
)

// The tests here never reach the network beyond loopback: the issuer is an
// authtest.Issuer in this process, and the verifier is built over the same
// loopback-permitting egress policy the engine's own auth tests use, so a fetch
// to anywhere else would be denied rather than attempted.

// trustedIssuer starts a stand-in identity provider and installs a verifier
// that trusts it, returning the issuer so a test can mint tokens.
func trustedIssuer(t *testing.T, entries ...auth.TrustedIssuer) *authtest.Issuer {
	t.Helper()

	issuer := authtest.NewIssuer()
	t.Cleanup(func() { _ = issuer.Close() })

	if len(entries) == 0 {
		entries = []auth.TrustedIssuer{{
			Name:      "build-system",
			Issuer:    issuer.URL(),
			JWKSURL:   issuer.JWKSURL(),
			Audiences: []string{"flowstate"},
		}}
	}
	for i := range entries {
		if entries[i].Issuer == "" {
			entries[i].Issuer = issuer.URL()
		}
		if entries[i].JWKSURL == "" {
			entries[i].JWKSURL = issuer.JWKSURL()
		}
	}

	built, err := auth.NewOIDCVerifier(auth.Policy{Issuers: entries}, auth.WithEgressPolicy(authtest.EgressPolicy()))
	if err != nil {
		t.Fatalf("building the verifier: %v", err)
	}

	previous, previousRefusal := verifier, trustRefusal
	verifier, trustRefusal = built, nil
	t.Cleanup(func() { verifier, trustRefusal = previous, previousRefusal })

	return issuer
}

// mint signs a token as the issuer, with the claims a workload token carries.
func mint(t *testing.T, issuer *authtest.Issuer, claims map[string]any) string {
	t.Helper()

	key := issuer.Key()
	now := time.Now()
	full := map[string]any{
		"iss": issuer.URL(),
		"sub": "repo:acme/api:ref:refs/heads/main",
		"aud": "flowstate",
		"iat": now.Add(-time.Minute).Unix(),
		"exp": now.Add(time.Hour).Unix(),
	}
	for name, value := range claims {
		if value == nil {
			delete(full, name)
			continue
		}
		full[name] = value
	}

	return key.Sign(map[string]any{"typ": "JWT", "alg": string(key.Algorithm()), "kid": key.ID()}, full)
}

// verify runs the task the way the host does.
func verify(t *testing.T, in *josev1.VerifyInputs) (*josev1.VerifyOutputs, error) {
	t.Helper()

	encoded, err := sdk.EncodeOutputs(in)
	if err != nil {
		t.Fatalf("encoding inputs: %v", err)
	}

	outputs, err := joseVerify(t.Context(), encoded.GetNamedValues(), nil)
	if err != nil {
		return nil, err
	}

	var out josev1.VerifyOutputs
	if err := sdk.DecodeInputs(outputs.GetNamedValues(), &out); err != nil {
		t.Fatalf("decoding outputs: %v", err)
	}
	return &out, nil
}

// literal renders a token the way a Flowfile does when it is not a secret.
func literal(token string) *flowstatev1.Value { return flowstatev1.NewValue(token) }

// TestAVerifiedTokenBecomesClaimsAWorkflowCanActOn is the ordinary path, and
// what it returns is the point: not "valid", but who, from where, and what the
// issuer said about them.
func TestAVerifiedTokenBecomesClaimsAWorkflowCanActOn(t *testing.T) {
	issuer := trustedIssuer(t)
	token := mint(t, issuer, map[string]any{
		"repository": "acme/api",
		"ref":        "refs/heads/main",
	})

	out, err := verify(t, &josev1.VerifyInputs{Token: literal(token)})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}

	if out.GetSubject() != "repo:acme/api:ref:refs/heads/main" {
		t.Errorf("subject = %q", out.GetSubject())
	}
	if out.GetIssuer() != issuer.URL() {
		t.Errorf("issuer = %q", out.GetIssuer())
	}
	if out.GetTrustName() != "build-system" {
		t.Errorf("trust_name = %q, want the operator's name for the rule that admitted it", out.GetTrustName())
	}
	if len(out.GetAudience()) != 1 || out.GetAudience()[0] != "flowstate" {
		t.Errorf("audience = %v", out.GetAudience())
	}
	if out.GetExpiresAt() == "" || out.GetIssuedAt() == "" {
		t.Errorf("time claims = %q, %q", out.GetIssuedAt(), out.GetExpiresAt())
	}

	claims := out.GetClaims().GetMapValue().GetEntries()
	if len(claims) == 0 {
		t.Fatal("no claims reached the workflow, so nothing issuer-specific can be decided on")
	}
	var sawRepository bool
	for _, entry := range claims {
		if entry.GetKey().GetStringValue() == "repository" && entry.GetValue().GetStringValue() == "acme/api" {
			sawRepository = true
		}
	}
	if !sawRepository {
		t.Error("the issuer-specific claim a policy decision would read did not survive")
	}
}

// TestATokenThatIsNotVerifiedIsRefusedPermanently covers the refusals that
// matter, and their classification: none of these becomes valid by waiting, so
// none of them may be retryable.
func TestATokenThatIsNotVerifiedIsRefusedPermanently(t *testing.T) {
	issuer := trustedIssuer(t)
	other := authtest.NewIssuer()
	t.Cleanup(func() { _ = other.Close() })

	now := time.Now()
	for name, token := range map[string]string{
		"an expired token":             mint(t, issuer, map[string]any{"exp": now.Add(-time.Hour).Unix()}),
		"a token not yet valid":        mint(t, issuer, map[string]any{"nbf": now.Add(time.Hour).Unix()}),
		"the wrong audience":           mint(t, issuer, map[string]any{"aud": "somebody-else"}),
		"an untrusted issuer":          mint(t, other, map[string]any{"iss": other.URL()}),
		"a signature from another key": mint(t, other, map[string]any{"iss": issuer.URL()}),
	} {
		out, err := verify(t, &josev1.VerifyInputs{Token: literal(token)})
		if err == nil {
			t.Errorf("%s was verified: %+v", name, out)
			continue
		}
		if !sdk.IsPermissionDenied(err) {
			t.Errorf("%s: error is %v, want permission denied", name, err)
		}
		if sdk.IsUnavailable(err) {
			t.Errorf("%s was classified as retryable; it will fail the same way next time", name)
		}
	}
}

// TestAMalformedTokenIsInvalidInputRatherThanARefusal: one is fixed by sending a
// token, the other by being trusted, and a workflow dispatching on the
// difference is dispatching on something real.
func TestAMalformedTokenIsInvalidInputRatherThanARefusal(t *testing.T) {
	trustedIssuer(t)

	for _, token := range []string{"not-a-token", "a.b", strings.Repeat("x", 100)} {
		_, err := verify(t, &josev1.VerifyInputs{Token: literal(token)})
		if err == nil {
			t.Errorf("%q was verified", token)
			continue
		}
		if !sdk.IsInvalidInput(err) {
			t.Errorf("%q: error is %v, want invalid input", token, err)
		}
	}
}

// TestTheTrustInputNarrowsAndNeverWidens is what makes "this must be the build
// system's token" expressible: the policy decides what is valid, and the step
// decides which of the valid issuers it will act on.
func TestTheTrustInputNarrowsAndNeverWidens(t *testing.T) {
	issuer := trustedIssuer(t)
	token := mint(t, issuer, nil)

	if _, err := verify(t, &josev1.VerifyInputs{Token: literal(token), Trust: "build-system"}); err != nil {
		t.Fatalf("a token admitted by the named entry was refused: %v", err)
	}

	_, err := verify(t, &josev1.VerifyInputs{Token: literal(token), Trust: "some-other-entry"})
	if err == nil {
		t.Fatal("a token admitted by a different entry satisfied a step that required another")
	}
	if !sdk.IsPermissionDenied(err) {
		t.Errorf("error is %v, want permission denied", err)
	}
}

// TestTheAudienceInputNarrowsAndNeverWidens, and its refusal does not echo the
// token's own audiences: they are a fact about somebody else's credential.
func TestTheAudienceInputNarrowsAndNeverWidens(t *testing.T) {
	issuer := trustedIssuer(t)
	token := mint(t, issuer, nil)

	if _, err := verify(t, &josev1.VerifyInputs{Token: literal(token), Audience: "flowstate"}); err != nil {
		t.Fatalf("a token carrying the required audience was refused: %v", err)
	}

	_, err := verify(t, &josev1.VerifyInputs{Token: literal(token), Audience: "deploy-service"})
	if err == nil {
		t.Fatal("a token without the required audience was accepted")
	}
	if strings.Contains(err.Error(), "flowstate") {
		t.Errorf("the refusal echoes the token's own audience: %v", err)
	}
}

// TestWithoutATrustPolicyNothingIsVerified is the fail-closed direction: the
// only available default would be "whatever the token says about itself".
func TestWithoutATrustPolicyNothingIsVerified(t *testing.T) {
	previous, previousRefusal := verifier, trustRefusal
	verifier, trustRefusal = nil, errNoTrustForTest
	t.Cleanup(func() { verifier, trustRefusal = previous, previousRefusal })

	_, err := verify(t, &josev1.VerifyInputs{Token: literal("a.b.c")})
	if err == nil {
		t.Fatal("a plugin with no trust policy verified something")
	}
	if !strings.Contains(err.Error(), "trusts no issuer") {
		t.Errorf("the refusal does not say what is missing: %v", err)
	}
}

// TestAnOversizedTokenIsRefusedBeforeAnySignatureCheck bounds what a workflow
// can hand a verifier.
func TestAnOversizedTokenIsRefusedBeforeAnySignatureCheck(t *testing.T) {
	trustedIssuer(t)

	_, err := verify(t, &josev1.VerifyInputs{Token: literal(strings.Repeat("x", maxTokenBytes+1))})
	if !sdk.IsInvalidInput(err) {
		t.Errorf("error is %v, want invalid input naming the limit", err)
	}
}

// errNoTrustForTest stands in for the reason loadTrust would have recorded.
var errNoTrustForTest = errTest{}

type errTest struct{}

func (errTest) Error() string {
	return "FLOWSTATE_JOSE_TRUST is not set, so this plugin trusts no issuer and can verify nothing"
}
