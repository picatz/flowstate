package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

const containmentSecret = "ghp_git_plugin_containment_canary_do_not_print_me"

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

func TestTokenFromValueTreatsUnsetAsPublic(t *testing.T) {
	token, err := tokenFromValue(context.Background(), nil)
	if err != nil || token != "" {
		t.Fatalf("tokenFromValue(nil): got (%q, %v), want (\"\", nil)", token, err)
	}
}

func TestCompatibilityProviderResolvesItsOwnScheme(t *testing.T) {
	t.Setenv("GIT_SECRET_0__TEST_TOKEN", containmentSecret)

	resp, err := resolveSecret(context.Background(), sdk.SecretRequest{Scheme: secretScheme, Name: "test-token"})
	if err != nil {
		t.Fatalf("resolveSecret: unexpected error: %v", err)
	}
	if string(resp.Value) != containmentSecret {
		t.Fatalf("token: got %q, want %q", resp.Value, containmentSecret)
	}
}

func TestResolveSecretRefusesWhenUnset(t *testing.T) {
	os.Unsetenv("GIT_SECRET_0__DOES_NOT_EXIST")
	_, err := resolveSecret(context.Background(), sdk.SecretRequest{Scheme: secretScheme, Name: "does-not-exist"})
	if err == nil {
		t.Fatal("resolveSecret for an unset variable: got no error, want one")
	}
}

func TestResolveSecretRefusesAliasingNamesAndNamespaces(t *testing.T) {
	t.Setenv("GIT_SECRET_6_TEAM_A_PROD_TOKEN", containmentSecret)

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

// TestCloneOptionsNeverPrintsItsToken is the containment-shape test
// CLAUDE.md requires: %v, %+v, %#v, and %s, on the value itself, on a
// struct holding it, and on a slice of those - the token lives in a closure
// specifically because fmt's reflection cannot reach a captured variable,
// unlike an ordinary struct field.
func TestCloneOptionsNeverPrintsItsToken(t *testing.T) {
	opts := cloneOptions{
		depth: 1,
		token: func() string { return containmentSecret },
	}

	type holder struct {
		Options cloneOptions
		Label   string
	}
	wrapped := holder{Options: opts, Label: "clone request"}

	rendered := []string{
		fmt.Sprintf("%v", opts),
		fmt.Sprintf("%+v", opts),
		fmt.Sprintf("%#v", opts),
		//lint:ignore S1025 the point of this line is the %s verb itself - an
		// operator's log line spells %s, not .String(), and this containment
		// test exists to prove that exact verb never leaks the token; calling
		// String() directly would stop testing the case that matters.
		fmt.Sprintf("%s", containedStringer{opts}),
		fmt.Sprintf("%v", wrapped),
		fmt.Sprintf("%+v", wrapped),
		fmt.Sprintf("%#v", wrapped),
		fmt.Sprintf("%v", []cloneOptions{opts, opts}),
		fmt.Sprintf("%+v", []cloneOptions{opts, opts}),
		fmt.Sprintf("%#v", []cloneOptions{opts, opts}),
		fmt.Sprintf("%v", []holder{wrapped}),
	}

	for _, r := range rendered {
		if strings.Contains(r, containmentSecret) {
			t.Fatalf("token leaked through fmt reflection: %q", r)
		}
	}
}

type containedStringer struct{ opts cloneOptions }

func (c containedStringer) String() string { return fmt.Sprintf("%v", c.opts) }

// TestCommitPushParamsNeverPrintsItsToken is the same containment shape,
// for commitPushParams - the write path's own token-carrying struct, which
// doc.go's whole "secrets never enter workflow history" argument depends on
// holding just as tightly as cloneOptions does.
func TestCommitPushParamsNeverPrintsItsToken(t *testing.T) {
	p := commitPushParams{
		branch: "main",
		token:  func() string { return containmentSecret },
	}

	type holder struct{ Params commitPushParams }
	wrapped := holder{Params: p}

	rendered := []string{
		fmt.Sprintf("%v", p),
		fmt.Sprintf("%+v", p),
		fmt.Sprintf("%#v", p),
		fmt.Sprintf("%v", wrapped),
		fmt.Sprintf("%+v", wrapped),
		fmt.Sprintf("%#v", wrapped),
		fmt.Sprintf("%v", []commitPushParams{p, p}),
	}
	for _, r := range rendered {
		if strings.Contains(r, containmentSecret) {
			t.Fatalf("token leaked through fmt reflection: %q", r)
		}
	}
}
