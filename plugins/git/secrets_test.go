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

func TestTokenFromValueTreatsUnsetAsPublic(t *testing.T) {
	token, err := tokenFromValue(context.Background(), nil)
	if err != nil || token != "" {
		t.Fatalf("tokenFromValue(nil): got (%q, %v), want (\"\", nil)", token, err)
	}
}

func TestTokenFromValueResolvesItsOwnScheme(t *testing.T) {
	t.Setenv("GIT_SECRET_TEST_TOKEN", containmentSecret)

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
	os.Unsetenv("GIT_SECRET_DOES_NOT_EXIST")
	_, err := resolveSecret(context.Background(), sdk.SecretRequest{Scheme: secretScheme, Name: "does-not-exist"})
	if err == nil {
		t.Fatal("resolveSecret for an unset variable: got no error, want one")
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
