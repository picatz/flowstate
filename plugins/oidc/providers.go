package main

import (
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/picatz/flowstate/internal/strictyaml"
)

// providersEnv names the operator's provider file. It reaches this process
// through the worker's per-plugin environment:
//
//	flow worker --plugin-env oidc=FLOWSTATE_OIDC_PROVIDERS=/etc/flowstate/oidc.yaml
//
// Unset means this plugin can mint nothing: every reference is refused naming
// the variable.
const providersEnv = "FLOWSTATE_OIDC_PROVIDERS"

// maxProvidersBytes bounds the operator's own file.
const maxProvidersBytes = 1 << 20

// maxSecretBytes bounds a client secret file. A secret is a credential, not a
// document.
const maxSecretBytes = 8192

// Ceilings a provider may narrow within.
const (
	maxCredentialLifetime = 12 * time.Hour
	maxExchangeTimeout    = 30 * time.Second

	defaultCredentialLifetime = time.Hour
	defaultExchangeTimeout    = 20 * time.Second
)

// providers is the operator's whole statement about what may be minted.
type providers struct {
	// Providers are the systems a workflow may obtain a token for, by the name
	// a `${secret('oidc:<name>')}` reference uses.
	Providers map[string]provider `json:"providers" yaml:"providers"`
}

// provider is one authorization server and one client this worker is.
type provider struct {
	// TokenURL is the authorization server's token endpoint. Required, and
	// https: a client secret is not sent in cleartext.
	TokenURL string `json:"token_url" yaml:"token_url"`

	// ClientID is the client identifier the authorization server knows this
	// deployment by. Required, and not a secret - it is in the request either
	// way, and naming it here is how a reviewer sees which client a workflow
	// acts as.
	ClientID string `json:"client_id" yaml:"client_id"`

	// ClientSecretFile is a file holding the client secret. A path rather than
	// a value, so the document an operator diffs in review is not one they have
	// to redact.
	ClientSecretFile string `json:"client_secret_file" yaml:"client_secret_file"`

	// Scopes are the scopes to request. What a token may do is the
	// authorization server's decision; asking for less than everything is this
	// deployment's.
	Scopes []string `json:"scopes,omitempty" yaml:"scopes,omitempty"`

	// MaxLifetime caps what this plugin will accept from the authorization
	// server. A server reporting a longer lifetime has its answer shortened -
	// which is a ceiling on caching, never a claim that the token stops working
	// earlier.
	MaxLifetime Duration `json:"max_lifetime,omitempty" yaml:"max_lifetime,omitempty"`

	// Timeout bounds one exchange.
	Timeout Duration `json:"timeout,omitempty" yaml:"timeout,omitempty"`

	// Namespaces, when non-empty, are the tenant namespaces whose workflows may
	// resolve this provider. The namespace compared is the one the host
	// established for the calling workload, never one the workload declared -
	// which is what makes this a tenant boundary rather than a convention.
	Namespaces []string `json:"namespaces,omitempty" yaml:"namespaces,omitempty"`
}

// Duration is a YAML duration - "30m", "1h".
type Duration time.Duration

// UnmarshalText parses the duration.
func (d *Duration) UnmarshalText(text []byte) error {
	parsed, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("%q is not a duration such as 30m or 1h: %w", string(text), err)
	}
	*d = Duration(parsed)
	return nil
}

// duration renders it, applying a default for zero.
func (d Duration) duration(fallback time.Duration) time.Duration {
	if d == 0 {
		return fallback
	}
	return time.Duration(d)
}

// referenceNamePattern is what a `${secret('oidc:<name>')}` reference may name.
var referenceNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,62}[a-z0-9])?$`)

// loadProviders reads and checks the operator's file at startup.
func loadProviders() (*providers, error) {
	path := os.Getenv(providersEnv)
	if path == "" {
		return nil, fmt.Errorf(
			"%s is not set, so this plugin has no providers and can mint nothing. An operator configures them with "+
				"`flow worker --plugin-env oidc=%s=/path/to/providers.yaml`", providersEnv, providersEnv)
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("%s (%q): %w", providersEnv, truncate(path, 256), err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s (%q) is a directory, not a providers file", providersEnv, truncate(path, 256))
	}
	if info.Size() > maxProvidersBytes {
		return nil, fmt.Errorf("%s (%q) is %d bytes, over the %d-byte limit this plugin reads",
			providersEnv, truncate(path, 256), info.Size(), maxProvidersBytes)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%s (%q): %w", providersEnv, truncate(path, 256), err)
	}

	var parsed providers
	if err := strictyaml.UnmarshalStrict(raw, &parsed); err != nil {
		return nil, fmt.Errorf("%s (%q): %w", providersEnv, truncate(path, 256), err)
	}
	if err := parsed.check(); err != nil {
		return nil, fmt.Errorf("%s (%q): %w", providersEnv, truncate(path, 256), err)
	}
	return &parsed, nil
}

// check validates the whole file.
func (p providers) check() error {
	if len(p.Providers) == 0 {
		return fmt.Errorf("no providers are configured, so no reference could resolve")
	}

	for _, name := range slices.Sorted(maps.Keys(p.Providers)) {
		if !referenceNamePattern.MatchString(name) {
			return fmt.Errorf(
				"the provider %q cannot be named by a reference; a name is lower-case letters, digits and interior hyphens",
				truncate(name, 64))
		}
		if err := p.Providers[name].check(name); err != nil {
			return err
		}
	}
	return nil
}

// check validates one provider.
func (p provider) check(name string) error {
	if !strings.HasPrefix(p.TokenURL, "https://") {
		return fmt.Errorf(
			"provider %q has a token_url that is not https; a client secret is not sent in cleartext", name)
	}
	if p.ClientID == "" {
		return fmt.Errorf("provider %q names no client_id", name)
	}
	if p.ClientSecretFile == "" {
		return fmt.Errorf(
			"provider %q names no client_secret_file; this plugin reads the secret from a file so the document you review is not one you have to redact", name)
	}
	if !strings.HasPrefix(p.ClientSecretFile, "/") {
		return fmt.Errorf("provider %q has a client_secret_file that is not an absolute path", name)
	}
	if time.Duration(p.MaxLifetime) < 0 || p.MaxLifetime.duration(defaultCredentialLifetime) > maxCredentialLifetime {
		return fmt.Errorf("provider %q has a max_lifetime over this plugin's ceiling of %s", name, maxCredentialLifetime)
	}
	if time.Duration(p.Timeout) < 0 || p.Timeout.duration(defaultExchangeTimeout) > maxExchangeTimeout {
		return fmt.Errorf("provider %q has a timeout over this plugin's ceiling of %s", name, maxExchangeTimeout)
	}
	for _, scope := range p.Scopes {
		if scope == "" || strings.ContainsAny(scope, " \t\r\n") {
			return fmt.Errorf("provider %q has a scope holding whitespace; scopes are space-separated on the wire", name)
		}
	}
	return nil
}

// reachableFrom reports whether a caller's namespace may resolve this provider.
func (p provider) reachableFrom(namespace string) bool {
	if len(p.Namespaces) == 0 {
		return true
	}
	return slices.Contains(p.Namespaces, namespace)
}

// secret reads the client secret the provider names, bounded, at the moment it
// is needed rather than at startup: a secret held in memory for the life of a
// process is a secret in a core dump.
func (p provider) secret(name string) (string, error) {
	info, err := os.Stat(p.ClientSecretFile)
	if err != nil {
		return "", fmt.Errorf("provider %q: client_secret_file cannot be read: %w", name, err)
	}
	if info.Size() > maxSecretBytes {
		return "", fmt.Errorf("provider %q: client_secret_file is %d bytes, over the %d-byte limit", name, info.Size(), maxSecretBytes)
	}

	raw, err := os.ReadFile(p.ClientSecretFile)
	if err != nil {
		return "", fmt.Errorf("provider %q: client_secret_file cannot be read: %w", name, err)
	}

	// A trailing newline is what every editor writes and no authorization
	// server expects.
	value := strings.TrimRight(string(raw), "\r\n")
	if value == "" {
		return "", fmt.Errorf("provider %q: client_secret_file is empty", name)
	}
	return value, nil
}

// truncate bounds a value before it is interpolated into a message.
func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "…"
}
