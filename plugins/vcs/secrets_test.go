package main

import (
	"context"
	"os"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// The task receives the literal wire shape only after the host resolved the
// whole secret reference required by the manifest.
func TestTokenFromValueAcceptsAHostResolvedString(t *testing.T) {
	v := flowstatev1.NewValue("a-literal-token")
	token, err := tokenFromValue(context.Background(), v)
	if err != nil || token != "a-literal-token" {
		t.Fatalf("tokenFromValue with a resolved string: got (%q, %v)", token, err)
	}
}

func TestTokenFromValueRefusesAnUnresolvedReference(t *testing.T) {
	v := &flowstatev1.Value{Kind: &flowstatev1.Value_SecretRef{
		SecretRef: &flowstatev1.SecretRef{Scheme: "env", Name: "GITHUB_TOKEN"},
	}}
	if _, err := tokenFromValue(context.Background(), v); err == nil {
		t.Fatal("tokenFromValue with an unresolved reference: got no error, want one")
	}
}

// An unset token means a public repository, not an error - most of the
// repositories a vcs.log or vcs.diff step reads are public, and requiring a
// token for those would make the common case the awkward one.
func TestTokenFromValueTreatsUnsetAsPublic(t *testing.T) {
	token, err := tokenFromValue(context.Background(), nil)
	if err != nil || token != "" {
		t.Fatalf("tokenFromValue(nil): got (%q, %v), want (\"\", nil)", token, err)
	}
}

// The compatibility provider remains available to the host for existing
// vcs: references, but the task never calls it directly.
func TestCompatibilityProviderResolvesItsOwnScheme(t *testing.T) {
	t.Setenv("VCS_SECRET_0__TEST_TOKEN", containmentSecret)

	resp, err := resolveSecret(context.Background(), sdk.SecretRequest{Scheme: secretScheme, Name: "test-token"})
	if err != nil {
		t.Fatalf("resolveSecret: unexpected error: %v", err)
	}
	if string(resp.Value) != containmentSecret {
		t.Fatalf("token: got %q, want %q", resp.Value, containmentSecret)
	}
}

func TestResolveSecretRefusesWhenUnset(t *testing.T) {
	os.Unsetenv("VCS_SECRET_0__DOES_NOT_EXIST")
	_, err := resolveSecret(context.Background(), sdk.SecretRequest{Scheme: secretScheme, Name: "does-not-exist"})
	if err == nil {
		t.Fatal("resolveSecret for an unset variable: got no error, want one")
	}
}

// TestResolveSecretRefusesAliasingNamesAndNamespaces is the negative
// direction #693 asks for: not that a tenant reads its own secret (the
// colliding encoding satisfied that too), but that a differently-spelled or
// differently-segmented reference cannot resolve another tenant's variable.
// Before the fix, namespace "team-a" + name "prod-token", namespace "team" +
// name "a-prod-token", and the default namespace + name
// "team-a-prod-token" all concatenated to the same VCS_SECRET_TEAM_A_PROD_TOKEN.
func TestResolveSecretRefusesAliasingNamesAndNamespaces(t *testing.T) {
	t.Setenv("VCS_SECRET_6_TEAM_A_PROD_TOKEN", containmentSecret)

	tests := []sdk.SecretRequest{
		{Scheme: secretScheme, Name: "prod_token", Namespace: "team-a"},
		{Scheme: secretScheme, Name: "prod/token", Namespace: "team-a"},
		{Scheme: secretScheme, Name: "prod-token", Namespace: "team_a"},
		{Scheme: secretScheme, Name: "PROD-TOKEN", Namespace: "team-a"},
	}
	for _, req := range tests {
		if _, err := resolveSecret(context.Background(), req); err == nil {
			t.Errorf("resolveSecret(%q, %q): got no error, want invalid input", req.Namespace, req.Name)
		}
	}

	// All three references used to concatenate to TEAM_A_PROD_TOKEN. Only the
	// reference whose length-prefixed variable is configured may resolve it.
	aliases := []sdk.SecretRequest{
		{Scheme: secretScheme, Name: "a-prod-token", Namespace: "team"},
		{Scheme: secretScheme, Name: "team-a-prod-token"},
	}
	for _, req := range aliases {
		if _, err := resolveSecret(context.Background(), req); err == nil {
			t.Errorf("resolveSecret(%q, %q): got no error, want not found", req.Namespace, req.Name)
		}
	}

	resp, err := resolveSecret(context.Background(), sdk.SecretRequest{
		Scheme: secretScheme, Name: "prod-token", Namespace: "team-a",
	})
	if err != nil {
		t.Fatalf("resolveSecret for configured reference: %v", err)
	}
	if got := string(resp.Value); got != containmentSecret {
		t.Fatalf("resolved secret: got %q, want containment canary", got)
	}
}
