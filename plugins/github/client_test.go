package main

import (
	"context"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestTokenFromValueRefusesALiteral(t *testing.T) {
	v := flowstatev1.NewValue("a-literal-token")
	if _, err := tokenFromValue(context.Background(), v); err == nil {
		t.Fatal("tokenFromValue with a literal: got no error, want one")
	}
}

func TestTokenFromValueRefusesAForeignScheme(t *testing.T) {
	v := &flowstatev1.Value{Kind: &flowstatev1.Value_SecretRef{
		SecretRef: &flowstatev1.SecretRef{Scheme: "env", Name: "GITHUB_TOKEN"},
	}}
	if _, err := tokenFromValue(context.Background(), v); err == nil {
		t.Fatal("tokenFromValue with a foreign scheme: got no error, want one")
	}
}

func TestTokenFromValueTreatsUnsetAsUnauthenticated(t *testing.T) {
	token, err := tokenFromValue(context.Background(), nil)
	if err != nil || token != "" {
		t.Fatalf("tokenFromValue(nil): got (%q, %v), want (\"\", nil)", token, err)
	}
}

func TestTokenFromValueResolvesAConfiguredPAT(t *testing.T) {
	t.Setenv(envAppID, "")
	t.Setenv(envAppPrivateKey, "")
	t.Setenv(envAppInstallID, "")
	t.Setenv(envPAT, containmentSecret)

	v := &flowstatev1.Value{Kind: &flowstatev1.Value_SecretRef{
		SecretRef: &flowstatev1.SecretRef{Scheme: secretScheme, Name: "token"},
	}}
	token, err := tokenFromValue(context.Background(), v)
	if err != nil {
		t.Fatalf("tokenFromValue: unexpected error: %v", err)
	}
	if token != containmentSecret {
		t.Fatalf("token: got %q, want %q", token, containmentSecret)
	}
}

func TestResolveSecretRefusesWhenNothingConfigured(t *testing.T) {
	t.Setenv(envAppID, "")
	t.Setenv(envAppPrivateKey, "")
	t.Setenv(envAppInstallID, "")
	t.Setenv(envPAT, "")

	if _, err := tokenFromValue(context.Background(), &flowstatev1.Value{Kind: &flowstatev1.Value_SecretRef{
		SecretRef: &flowstatev1.SecretRef{Scheme: secretScheme, Name: "token"},
	}}); err == nil {
		t.Fatal("resolving a credential with nothing configured: got no error, want one")
	}
}

func TestAuthenticatedClientRefusesWorkflowSelectedBaseURL(t *testing.T) {
	for _, configured := range []string{"", "https://github.example.com/api/v3"} {
		t.Run(configured, func(t *testing.T) {
			t.Setenv(envAPIBaseURL, configured)
			if _, err := newClient("credential", "https://attacker.example/api/v3"); err == nil {
				t.Fatal("newClient with credential and unconfigured base URL: got no error, want one")
			}
		})
	}
}

func TestAuthenticatedClientUsesOperatorSelectedBaseURL(t *testing.T) {
	t.Setenv(envAPIBaseURL, "https://github.example.com/api/v3")
	if err := installEgressPolicy(); err != nil {
		t.Fatalf("installEgressPolicy: %v", err)
	}

	client, err := newClient("credential", "")
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	if got, want := client.BaseURL.String(), "https://github.example.com/api/v3/"; got != want {
		t.Fatalf("BaseURL: got %q, want %q", got, want)
	}
}

// TestAuthenticatedClientKeepsTheGitHubComDefault covers the path the change
// above alters for every deployment that has not set GITHUB_API_BASE_URL: a
// credential now forces the base to the operator-selected one, and with nothing
// configured that is github.com's.
//
// Worth asserting because forcing it makes baseURL non-empty, which sends this
// path through go-github's WithEnterpriseURLs — the call that appends "/api/v3/"
// to a Enterprise Server base. It does not append here, because it skips hosts
// beginning "api.", so the default stays exactly what an unauthenticated client
// uses. Both are checked, so a regression shows up as the two disagreeing rather
// than as every authenticated github.com call 404ing in production.
func TestAuthenticatedClientKeepsTheGitHubComDefault(t *testing.T) {
	t.Setenv(envAPIBaseURL, "")
	if err := installEgressPolicy(); err != nil {
		t.Fatalf("installEgressPolicy: %v", err)
	}

	authenticated, err := newClient("credential", "")
	if err != nil {
		t.Fatalf("newClient with a credential: %v", err)
	}
	if got, want := authenticated.BaseURL.String(), defaultAPIBaseURL+"/"; got != want {
		t.Fatalf("authenticated BaseURL: got %q, want %q", got, want)
	}

	unauthenticated, err := newClient("", "")
	if err != nil {
		t.Fatalf("newClient without a credential: %v", err)
	}
	if got, want := authenticated.BaseURL.String(), unauthenticated.BaseURL.String(); got != want {
		t.Fatalf("authenticated BaseURL %q, unauthenticated %q: a credential must not move the base", got, want)
	}
}
