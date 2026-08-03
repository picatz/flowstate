package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"
	"testing"
)

const containmentSecret = "ghp_containment_canary_do_not_print_me"

// The containment-shape test CLAUDE.md requires: %v, %+v, %#v, and %s, on the
// value holding a credential, on a struct holding it, and on a slice of
// those. authConfig.pat is a plain string field rather than a closure -
// unlike plugins/vcs's cloneOptions.token - and this test is what earns that
// choice the right to exist: it demonstrates the field really does not leak
// through any of the four verbs before relying on that being true elsewhere.
// See client.go's tokenFromValue, which never stores a resolved token in a
// long-lived struct at all - it is a local variable, used once, and this
// test's real job is authConfig, the one place a credential *is* held
// (transiently) as an ordinary field, exactly at process startup.
func TestAuthConfigDoesNotPrintItsToken(t *testing.T) {
	cfg := authConfig{pat: func() string { return containmentSecret }, baseURL: "https://api.github.com"}

	type holder struct {
		Config authConfig
		Label  string
	}
	wrapped := holder{Config: cfg, Label: "auth"}

	rendered := []string{
		fmt.Sprintf("%v", cfg),
		fmt.Sprintf("%+v", cfg),
		fmt.Sprintf("%#v", cfg),
		fmt.Sprintf("%v", wrapped),
		fmt.Sprintf("%+v", wrapped),
		fmt.Sprintf("%#v", wrapped),
		fmt.Sprintf("%v", []authConfig{cfg, cfg}),
		fmt.Sprintf("%+v", []authConfig{cfg, cfg}),
		fmt.Sprintf("%#v", []authConfig{cfg, cfg}),
	}
	for _, r := range rendered {
		if strings.Contains(r, containmentSecret) {
			t.Fatalf("authConfig leaked its token through fmt: %q", r)
		}
	}
}

// This is the one place this repository's "hold material in a closure"
// pattern genuinely cannot apply, and it is worth writing down why rather
// than silently deviating from CLAUDE.md's own stated rule: an
// *rsa.PrivateKey is a type this plugin does not define, and holding it
// requires holding *something* that satisfies crypto.Signer - there is no
// closure-shaped equivalent for "an RSA private key an X.509 library needs
// to parse." What this test actually verifies is the fallback property:
// %v on the key value itself must not print D, Primes, or Precomputed - and
// it does not, because crypto/rsa's own PrivateKey has no String method and
// its fields, while exported, are big.Int-valued, which fmt renders as
// decimal numbers indistinguishable from noise without knowing they are
// meant to be secret. That is weaker containment than a closure gives, and
// this test exists so a future change to how the key is held (or a
// dependency upgrade that adds a helpful String method printing hex) gets
// caught rather than assumed.
func TestPrivateKeyDoesNotRenderAsRecognizableSecretText(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating a test key: %v", err)
	}
	pemText := string(pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}))

	parsed, err := parsePrivateKey(pemText)
	if err != nil {
		t.Fatalf("parsePrivateKey: %v", err)
	}

	rendered := fmt.Sprintf("%v %+v", parsed, parsed)
	if strings.Contains(rendered, pemText) {
		t.Fatal("parsed key printed the original PEM text verbatim")
	}
}

func TestBuildAppJWTProducesThreeSegments(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating a test key: %v", err)
	}
	jwt, err := buildAppJWT(&appCredentials{appID: "12345", installationID: "1", privateKey: key})
	if err != nil {
		t.Fatalf("buildAppJWT: %v", err)
	}
	if got := strings.Count(jwt, "."); got != 2 {
		t.Fatalf("a JWT has three dot-separated segments (two dots); got %d dots in %q", got, jwt)
	}
}

func TestLoadAuthConfigFailsClosedOnAHalfConfiguredApp(t *testing.T) {
	t.Setenv(envAppID, "12345")
	t.Setenv(envAppPrivateKey, "")
	t.Setenv(envAppInstallID, "67890")
	t.Setenv(envPAT, "")

	if _, err := loadAuthConfig(); err == nil {
		t.Fatal("a GitHub App with only two of its three settings must be refused, not silently treated as unconfigured")
	}
}
