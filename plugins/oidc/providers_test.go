package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeProviders writes a providers file and points the environment at it, the
// way a worker's --plugin-env does.
func writeProviders(t *testing.T, document string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "providers.yaml")
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatalf("writing providers: %v", err)
	}
	t.Setenv(providersEnv, path)
}

// validDocument is a providers file that should load.
const validDocument = `
providers:
  billing-api:
    token_url: https://idp.example.com/oauth2/token
    client_id: flowstate-worker
    client_secret_file: /etc/flowstate/secrets/billing-client
    scopes: [invoices.read, invoices.write]
    max_lifetime: 1h
    timeout: 10s
`

// TestAWellFormedProvidersFileLoads is the premise the refusals are measured
// against.
func TestAWellFormedProvidersFileLoads(t *testing.T) {
	writeProviders(t, validDocument)

	parsed, err := loadProviders()
	if err != nil {
		t.Fatalf("loadProviders: %v", err)
	}
	configured, ok := parsed.Providers["billing-api"]
	if !ok {
		t.Fatal("the provider did not load")
	}
	if configured.ClientID != "flowstate-worker" || len(configured.Scopes) != 2 {
		t.Errorf("provider = %+v", configured)
	}
	if !configured.reachableFrom("any-namespace") {
		t.Error("a provider naming no namespaces should be reachable from every namespace")
	}
}

// TestNoProvidersFileMeansNothingCanBeMinted is the fail-closed direction.
func TestNoProvidersFileMeansNothingCanBeMinted(t *testing.T) {
	t.Setenv(providersEnv, "")

	_, err := loadProviders()
	if err == nil {
		t.Fatal("a plugin with no providers file claimed it could mint tokens")
	}
	if !strings.Contains(err.Error(), "--plugin-env") {
		t.Errorf("the refusal does not tell an operator how to configure one: %v", err)
	}
}

// TestAProvidersFileIsRefusedForWhatAnOperatorCanGetWrong. Each case is a file
// somebody could plausibly write, refused at startup rather than at the first
// credential a workflow needs.
func TestAProvidersFileIsRefusedForWhatAnOperatorCanGetWrong(t *testing.T) {
	for name, document := range map[string]string{
		"a token_url that is not https":   strings.Replace(validDocument, "https://idp.example.com", "http://idp.example.com", 1),
		"no client_id":                    strings.Replace(validDocument, "    client_id: flowstate-worker\n", "", 1),
		"a secret in the document":        strings.Replace(validDocument, "    client_secret_file: /etc/flowstate/secrets/billing-client", "    client_secret: hunter2", 1),
		"a relative secret path":          strings.Replace(validDocument, "    client_secret_file: /etc", "    client_secret_file: etc", 1),
		"a lifetime over the ceiling":     strings.Replace(validDocument, "    max_lifetime: 1h", "    max_lifetime: 24h", 1),
		"a timeout over the ceiling":      strings.Replace(validDocument, "    timeout: 10s", "    timeout: 5m", 1),
		"a scope holding whitespace":      strings.Replace(validDocument, "scopes: [invoices.read, invoices.write]", `scopes: ["invoices.read invoices.write"]`, 1),
		"a name no reference could carry": strings.Replace(validDocument, "  billing-api:", "  Billing_API:", 1),
		"a misspelled key":                strings.Replace(validDocument, "    client_id:", "    clientid:", 1),
		"no providers at all":             "providers: {}\n",
	} {
		writeProviders(t, document)
		if _, err := loadProviders(); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

// TestTheClientSecretIsReadFromItsFileWhenItIsNeeded: not held in memory for
// the life of the process, and a trailing newline - which every editor writes
// and no authorization server expects - is trimmed.
func TestTheClientSecretIsReadFromItsFileWhenItIsNeeded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client-secret")
	if err := os.WriteFile(path, []byte("not-a-real-client-secret\n"), 0o600); err != nil {
		t.Fatalf("writing the secret: %v", err)
	}

	configured := provider{ClientSecretFile: path}
	secret, err := configured.secret("billing-api")
	if err != nil {
		t.Fatalf("secret: %v", err)
	}
	if secret != "not-a-real-client-secret" {
		t.Errorf("the secret is %q, want the file's contents without the trailing newline", secret)
	}

	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("emptying the secret: %v", err)
	}
	if _, err := configured.secret("billing-api"); err == nil {
		t.Error("an empty client_secret_file was accepted")
	}
}
