package main

import (
	"context"
	"os"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// A literal token in a Flowfile is exactly what CLAUDE.md forbids: it would
// put a credential in the file itself and in workflow history. This must be
// refused, not merely discouraged by a comment.
func TestTokenFromValueRefusesALiteral(t *testing.T) {
	v := flowstatev1.NewValue("a-literal-token")
	if _, err := tokenFromValue(context.Background(), v); err == nil {
		t.Fatal("tokenFromValue with a literal: got no error, want one")
	}
}

// A secret reference naming a scheme this plugin does not own must be
// refused rather than guessed at - resolving it would mean either silently
// answering nothing, or (worse) treating a differently-scoped reference as
// this plugin's own.
func TestTokenFromValueRefusesAForeignScheme(t *testing.T) {
	v := &flowstatev1.Value{Kind: &flowstatev1.Value_SecretRef{
		SecretRef: &flowstatev1.SecretRef{Scheme: "env", Name: "GITHUB_TOKEN"},
	}}
	if _, err := tokenFromValue(context.Background(), v); err == nil {
		t.Fatal("tokenFromValue with a foreign scheme: got no error, want one")
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

// The one path that resolves successfully: this plugin's own scheme, backed
// by the environment variable secrets.go derives from the reference name.
func TestTokenFromValueResolvesItsOwnScheme(t *testing.T) {
	t.Setenv("VCS_SECRET_TEST_TOKEN", containmentSecret)

	v := &flowstatev1.Value{Kind: &flowstatev1.Value_SecretRef{
		SecretRef: &flowstatev1.SecretRef{Scheme: secretScheme, Name: "test-token"},
	}}
	token, err := tokenFromValue(context.Background(), v)
	if err != nil {
		t.Fatalf("tokenFromValue: unexpected error: %v", err)
	}
	if token != containmentSecret {
		t.Fatalf("token: got %q, want %q", token, containmentSecret)
	}
}

func TestResolveSecretRefusesWhenUnset(t *testing.T) {
	os.Unsetenv("VCS_SECRET_DOES_NOT_EXIST")
	_, err := resolveSecret(context.Background(), sdk.SecretRequest{Scheme: secretScheme, Name: "does-not-exist"})
	if err == nil {
		t.Fatal("resolveSecret for an unset variable: got no error, want one")
	}
}
