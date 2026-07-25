package secrets

import (
	"context"
	"fmt"
	"strings"
)

// Defaults for the 1Password provider.
const (
	// OnePasswordCommand is the 1Password CLI.
	OnePasswordCommand = "op"

	// DefaultOnePasswordVault is the vault secrets are read from when a namespace
	// does not name its own.
	DefaultOnePasswordVault = "flowstate"

	// DefaultOnePasswordField is the field read from an item when a reference does
	// not name one. It is 1Password's own default field for a password item.
	DefaultOnePasswordField = "password"
)

// OnePasswordProvider resolves secrets from 1Password through its CLI. It handles
// the "op" scheme.
//
// It exists for the same reason as [KeychainProvider]: a developer already keeps
// credentials in a password manager, and reading them from there is better than
// copying them into a file. It works on any platform the CLI runs on, and unlike
// the keychain it is shared across a team, which makes it a reasonable way to give
// several developers the same development credentials without passing them around.
//
// A reference names an item and optionally a field:
//
//	op:github          reads the "password" field of the item "github"
//	op:github#token    reads the "token" field of that item
//
// The namespace selects the vault: a namespaced run reads from the vault named
// after it, and an unnamespaced one reads from the configured default. Give each
// tenant its own 1Password vault and the boundary is 1Password's to enforce as
// well as ours.
//
// Authentication is the CLI's business, not this provider's. `op` must already be
// signed in — through the desktop app's integration, a service account token in the
// environment, or `op signin` — and a provider that finds it is not reports that
// rather than prompting, since a worker has no terminal to prompt on.
//
// It is safe for concurrent use.
type OnePasswordProvider struct {
	vault  string
	runner commandRunner
}

// OnePasswordOption configures an [OnePasswordProvider].
type OnePasswordOption func(*OnePasswordProvider)

// WithOnePasswordVault replaces the vault read when a run has no namespace.
func WithOnePasswordVault(vault string) OnePasswordOption {
	return func(p *OnePasswordProvider) {
		p.vault = vault
	}
}

// withOnePasswordRunner replaces the subprocess runner, for tests.
func withOnePasswordRunner(runner commandRunner) OnePasswordOption {
	return func(p *OnePasswordProvider) {
		p.runner = runner
	}
}

// NewOnePasswordProvider returns a provider reading from 1Password.
//
// It fails on a machine without the CLI, so a worker configured for 1Password
// refuses to start rather than failing the first workflow that needs a secret.
func NewOnePasswordProvider(opts ...OnePasswordOption) (*OnePasswordProvider, error) {
	provider := &OnePasswordProvider{
		vault:  DefaultOnePasswordVault,
		runner: execRunner{timeout: DefaultCommandTimeout, maxBytes: DefaultCommandMaxBytes},
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(provider)
	}

	if provider.vault == "" {
		return nil, fmt.Errorf("secrets: 1Password vault must not be empty")
	}

	if _, real := provider.runner.(execRunner); real {
		if err := hasCommand(OnePasswordCommand); err != nil {
			return nil, fmt.Errorf("secrets: 1Password provider needs the %s CLI: %w", OnePasswordCommand, err)
		}
	}

	return provider, nil
}

// Scheme implements [Provider].
func (p *OnePasswordProvider) Scheme() string {
	return "op"
}

// Resolve implements [Provider].
func (p *OnePasswordProvider) Resolve(ctx context.Context, req Request) (Secret, error) {
	ref := req.Ref

	item, field, err := parseOnePasswordName(ref.GetName())
	if err != nil {
		return Secret{}, &ResolveError{Ref: ref, Err: err}
	}

	vault := p.vaultFor(req.Namespace)

	// The reference is built here rather than taken from the workflow, and each
	// segment is validated above, so a name cannot reach outside its vault by
	// spelling extra path segments.
	uri := fmt.Sprintf("op://%s/%s/%s", vault, item, field)

	out, err := p.runner.run(ctx, OnePasswordCommand, "read", "--no-newline", "--", uri)
	if err != nil {
		return Secret{}, &ResolveError{Ref: ref, Err: err}
	}

	value := strings.TrimSuffix(string(out), "\n")
	if value == "" {
		return Secret{}, &ResolveError{
			Ref: ref,
			Err: fmt.Errorf("%w: 1Password item %q field %q holds nothing", ErrEmpty, item, field),
		}
	}

	return NewSecret(ref, value), nil
}

// vaultFor returns the vault a namespace's secrets live in.
func (p *OnePasswordProvider) vaultFor(namespace string) string {
	if namespace == "" {
		return p.vault
	}

	return namespace
}

// Vault returns the configured default vault. It is safe to log.
func (p *OnePasswordProvider) Vault() string {
	return p.vault
}

// parseOnePasswordName splits a reference name into an item and a field.
//
// Each segment is validated because both become path segments of an op:// URI: a
// slash would let a name address a different vault, and the "#" separator has to be
// unambiguous or an item name containing one would silently select a field.
func parseOnePasswordName(name string) (item, field string, err error) {
	item, field, found := strings.Cut(name, "#")
	if !found {
		field = DefaultOnePasswordField
	}

	for label, segment := range map[string]string{"item": item, "field": field} {
		switch {
		case segment == "":
			return "", "", fmt.Errorf("%w: 1Password %s must not be empty", ErrInvalidRef, label)
		case strings.ContainsAny(segment, "/#"):
			return "", "", fmt.Errorf(
				"%w: 1Password %s %q may not contain a slash or a hash",
				ErrInvalidRef, label, segment,
			)
		case strings.HasPrefix(segment, "-"):
			return "", "", fmt.Errorf("%w: 1Password %s %q may not start with a dash", ErrInvalidRef, label, segment)
		}

		if i := strings.IndexFunc(segment, isControl); i >= 0 {
			return "", "", fmt.Errorf(
				"%w: 1Password %s contains a control character at offset %d",
				ErrInvalidRef, label, i,
			)
		}
	}

	return item, field, nil
}
