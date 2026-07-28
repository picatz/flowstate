package secrets

import (
	"context"
	"fmt"
	"strings"
)

// Defaults for the macOS keychain provider.
const (
	// KeychainCommand is the macOS tool that reads the keychain.
	KeychainCommand = "security"

	// DefaultKeychainService is the keychain service name secrets are stored
	// under. A service groups items, which is what keeps Flowstate's entries
	// distinguishable from everything else in a developer's keychain.
	DefaultKeychainService = "flowstate"
)

// KeychainProvider resolves secrets from the macOS keychain. It handles the
// "keychain" scheme.
//
// It exists for local development, where the alternative is a developer putting
// real credentials in a shell profile or a checked-out file. The keychain already
// holds them, already requires the user's authorization to read, and is already
// backed up and locked with the machine.
//
// A reference names a keychain account within the configured service:
// "keychain:github-token" reads the generic password whose service is
// "flowstate/<namespace>" and whose account is "github-token". Store one with:
//
//	security add-generic-password -s flowstate -a github-token -w
//
// The namespace becomes part of the service name, so two tenants on one machine do
// not share entries. A single-tenant developer machine uses the bare service name.
//
// This is a development convenience and not a production backend: reads may prompt
// for authorization, the keychain is per-user and per-machine, and there is nothing
// to rotate centrally. Use a vault or a cloud manager on a worker.
//
// It is safe for concurrent use.
type KeychainProvider struct {
	service string
	runner  commandRunner

	// namespaced opts this provider into tenancy, and is off by default for the
	// same reason it is off elsewhere: a worker must not become multi-tenant
	// because an identity happened to carry a namespace.
	namespaced bool
}

// WithKeychainNamespaced gives each tenant its own keychain service.
//
// With it, a run in namespace "team-a" reads service "<service>/team-a" and the
// unnamespaced tenant reads "<service>/[DefaultNamespaceDir]". Every tenant gets a
// segment, including the default one.
func WithKeychainNamespaced() KeychainOption {
	return func(p *KeychainProvider) {
		p.namespaced = true
	}
}

// KeychainOption configures a [KeychainProvider].
type KeychainOption func(*KeychainProvider)

// WithKeychainService replaces the keychain service name entries are stored under.
func WithKeychainService(service string) KeychainOption {
	return func(p *KeychainProvider) {
		p.service = service
	}
}

// withKeychainRunner replaces the subprocess runner, for tests.
func withKeychainRunner(runner commandRunner) KeychainOption {
	return func(p *KeychainProvider) {
		p.runner = runner
	}
}

// NewKeychainProvider returns a provider reading from the macOS keychain.
//
// It fails on a machine without the "security" tool — every machine that is not a
// Mac — so a worker configured for a keychain it cannot reach refuses to start
// rather than failing the first workflow that needs a secret.
func NewKeychainProvider(opts ...KeychainOption) (*KeychainProvider, error) {
	provider := &KeychainProvider{
		service: DefaultKeychainService,
		runner:  execRunner{timeout: DefaultCommandTimeout, maxBytes: DefaultCommandMaxBytes},
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(provider)
	}

	if provider.service == "" {
		return nil, fmt.Errorf("secrets: keychain service must not be empty")
	}

	// Only check for the tool when the real runner is in use; a test supplying its
	// own runner is not exercising the tool.
	if _, real := provider.runner.(execRunner); real {
		if err := hasCommand(KeychainCommand); err != nil {
			return nil, fmt.Errorf("secrets: keychain provider needs the macOS %s tool: %w", KeychainCommand, err)
		}
	}

	return provider, nil
}

// Scheme implements [Provider].
func (p *KeychainProvider) Scheme() string {
	return "keychain"
}

// Resolve implements [Provider].
func (p *KeychainProvider) Resolve(ctx context.Context, req Request) (Secret, error) {
	ref := req.Ref

	account := ref.GetName()
	if err := validateKeychainAccount(account); err != nil {
		return Secret{}, &ResolveError{Ref: ref, Err: err}
	}

	service, err := p.serviceFor(req.Namespace)
	if err != nil {
		return Secret{}, &ResolveError{Ref: ref, Err: err}
	}

	// The tool takes the service and account as separate arguments, so neither can
	// be read as an option or a shell construct however it is spelled.
	out, err := p.runner.run(ctx, KeychainCommand,
		"find-generic-password",
		"-s", service,
		"-a", account,
		"-w",
	)
	if err != nil {
		return Secret{}, &ResolveError{Ref: ref, Err: err}
	}

	// The tool prints the password followed by a newline.
	value := strings.TrimSuffix(string(out), "\n")
	if value == "" {
		return Secret{}, &ResolveError{
			Ref: ref,
			Err: fmt.Errorf("%w: keychain item %q holds nothing", ErrEmpty, account),
		}
	}

	return NewSecret(ref, value), nil
}

// serviceFor returns the keychain service a namespace's secrets live under, or
// refuses.
//
// The separator is safe here in a way it is not for a vault name or an
// environment variable: ValidateNamespace forbids "/", so no namespace can forge
// the service of another. That is worth stating, because the same shape is a
// collision in the providers where the separator is legal in a name, and the
// difference is not visible from the code alone.
//
// What it did not have is the opt-in, and that is the half that matters here: a
// worker configured for one tenant would silently start serving per-tenant
// services the moment a namespaced identity arrived, reading somewhere the
// operator never provisioned and reporting "not found" rather than "not
// configured". Refusing says which of those it is.
func (p *KeychainProvider) serviceFor(namespace string) (string, error) {
	switch {
	case p.namespaced:
		if namespace == "" {
			return p.service + "/" + DefaultNamespaceDir, nil
		}
		return p.service + "/" + namespace, nil

	case namespace != "":
		return "", fmt.Errorf(
			"%w: this worker's keychain provider is not namespaced, so it cannot resolve secrets "+
				"for namespace %q; configure it with WithKeychainNamespaced",
			ErrNamespace, namespace)
	}

	return p.service, nil
}

// Service returns the configured keychain service name. It is safe to log.
func (p *KeychainProvider) Service() string {
	return p.service
}

// validateKeychainAccount rejects a name the tool would misread.
//
// A leading dash would be taken as an option however it reached the tool, which is
// the one way an argument can change what the command does without a shell being
// involved.
func validateKeychainAccount(account string) error {
	switch {
	case account == "":
		return fmt.Errorf("%w: keychain account must not be empty", ErrInvalidRef)
	case strings.HasPrefix(account, "-"):
		return fmt.Errorf("%w: keychain account %q may not start with a dash", ErrInvalidRef, account)
	}

	if i := strings.IndexFunc(account, isControl); i >= 0 {
		return fmt.Errorf("%w: keychain account contains a control character at offset %d", ErrInvalidRef, i)
	}

	return nil
}
