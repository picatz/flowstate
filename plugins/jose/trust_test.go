package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTrust writes a policy file and points the environment at it, the way a
// worker's --plugin-env does.
func writeTrust(t *testing.T, document string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "trust.yaml")
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatalf("writing the trust policy: %v", err)
	}
	t.Setenv(trustEnv, path)
}

// validTrust is the same document shape `flow server --auth-policy` reads,
// which is the point: one spelling, not two.
const validTrust = `
issuers:
  - name: build-system
    issuer: https://token.actions.githubusercontent.com
    audiences: [flowstate]
    require:
      - claim: repository
        any_of: [acme/api]
`

// TestAWellFormedTrustPolicyLoads is the premise the refusals are measured
// against, and it also records that this plugin reads the engine's own policy
// document rather than one of its own invention.
func TestAWellFormedTrustPolicyLoads(t *testing.T) {
	writeTrust(t, validTrust)

	policy, err := loadTrust()
	if err != nil {
		t.Fatalf("loadTrust: %v", err)
	}
	if len(policy.Issuers) != 1 || policy.Issuers[0].Name != "build-system" {
		t.Fatalf("the policy did not load: %+v", policy)
	}
}

// TestNoTrustPolicyMeansNoVerification: the only default available would be
// "whatever the token says about itself", so there is none.
func TestNoTrustPolicyMeansNoVerification(t *testing.T) {
	t.Setenv(trustEnv, "")

	_, err := loadTrust()
	if err == nil {
		t.Fatal("a plugin with no trust policy claimed to verify tokens")
	}
	for _, want := range []string{"--plugin-env", "--auth-policy"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %s, so an operator is not told how to grant trust: %v", want, err)
		}
	}
}

// TestATrustPolicyIsRefusedForWhatAnOperatorCanGetWrong: the engine's own
// validation runs here, at startup, rather than at the first token.
func TestATrustPolicyIsRefusedForWhatAnOperatorCanGetWrong(t *testing.T) {
	for name, document := range map[string]string{
		"an issuer that is not https":  strings.Replace(validTrust, "https://token.actions", "http://token.actions", 1),
		"an entry with no name":        strings.Replace(validTrust, "  - name: build-system\n", "  - \n", 1),
		"an entry with no issuer":      strings.Replace(validTrust, "    issuer: https://token.actions.githubusercontent.com\n", "", 1),
		"a misspelled key":             strings.Replace(validTrust, "    audiences:", "    audienses:", 1),
		"no issuers at all":            "issuers: []\n",
		"a document that is not a map": "- just a list\n",
	} {
		writeTrust(t, document)
		if _, err := loadTrust(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}
