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
