package secrets

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
)

// DefaultEnvPrefix is the environment variable prefix [NewEnvProvider] requires by
// default. A reference of "env:API_KEY" resolves to $FLOWSTATE_SECRET_API_KEY.
//
// The prefix is what keeps the provider from being a way to read the whole
// process environment. Without it, a Flowfile could name env:AWS_SECRET_ACCESS_KEY
// or env:TEMPORAL_API_KEY and exfiltrate the worker's own credentials — the
// workflow chooses the name, so the worker has to choose the namespace.
const DefaultEnvPrefix = "FLOWSTATE_SECRET_"

// EnvProvider resolves secrets from the process environment. It handles the "env"
// scheme.
//
// Only variables under a fixed prefix are visible, so the set of secrets a
// workflow can name is bounded by how the worker was launched. An optional
// allowlist narrows it further, to exactly the names a deployment intends.
//
// It is safe for concurrent use.
type EnvProvider struct {
	prefix string
	allow  []string
}

// EnvOption configures an [EnvProvider].
type EnvOption func(*EnvProvider) error

// WithEnvPrefix replaces the required environment variable prefix.
//
// The prefix must be reserved for secrets alone. Pointing it at a namespace the
// worker also reads its own configuration from — "FLOWSTATE_", say — makes that
// configuration workflow-readable, which defeats the point of having a prefix.
//
// An empty prefix exposes the whole environment and is rejected unless
// [WithEnvAllow] also names what may be read.
func WithEnvPrefix(prefix string) EnvOption {
	return func(p *EnvProvider) error {
		p.prefix = prefix
		return nil
	}
}

// WithEnvAllow restricts the provider to the given reference names, which are the
// names as a Flowfile writes them, without the prefix. When set, a reference to
// anything else is refused even if the variable exists.
func WithEnvAllow(names ...string) EnvOption {
	return func(p *EnvProvider) error {
		for _, name := range names {
			if !validEnvName(name) {
				return fmt.Errorf("secrets: %q is not a valid environment variable name", name)
			}
		}

		p.allow = append(p.allow, names...)

		return nil
	}
}

// NewEnvProvider returns a provider reading from the process environment under
// [DefaultEnvPrefix].
//
// It fails when the options would expose the entire environment, so that a
// misconfiguration cannot quietly hand a workflow the worker's own credentials.
func NewEnvProvider(opts ...EnvOption) (*EnvProvider, error) {
	provider := &EnvProvider{prefix: DefaultEnvPrefix}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(provider); err != nil {
			return nil, err
		}
	}

	if provider.prefix == "" && len(provider.allow) == 0 {
		return nil, fmt.Errorf(
			"secrets: an empty environment prefix exposes every variable in the worker's environment; " +
				"set a prefix or name the permitted secrets with WithEnvAllow",
		)
	}

	return provider, nil
}

// Scheme implements [Provider].
func (p *EnvProvider) Scheme() string {
	return "env"
}

// Resolve implements [Provider], reading $<prefix><name>.
//
// Environment lookups are not cached. The environment is already an in-memory map,
// so a cache would add staleness and an invalidation problem in exchange for
// nothing.
func (p *EnvProvider) Resolve(_ context.Context, req Request) (Secret, error) {
	ref := req.Ref

	if err := p.permitted(ref); err != nil {
		return Secret{}, &ResolveError{Ref: ref, Err: err}
	}

	if len(p.allow) > 0 && !slices.Contains(p.allow, ref.GetName()) {
		return Secret{}, notConfigured(ref, p.prefix)
	}

	variable := p.variable(req.Namespace, ref.GetName())

	value, found := os.LookupEnv(variable)
	switch {
	case !found:
		return Secret{}, &ResolveError{
			Ref: ref,
			Err: fmt.Errorf("%w: $%s is not configured on this worker", ErrNotFound, variable),
		}
	case value == "":
		return Secret{}, &ResolveError{
			Ref: ref,
			Err: fmt.Errorf("%w: $%s is set but empty", ErrEmpty, variable),
		}
	}

	return NewSecret(ref, value), nil
}

// variable returns the environment variable a reference reads, within a namespace.
//
// A namespace becomes part of the name — "env:API_KEY" in namespace "team-a" reads
// $FLOWSTATE_SECRET_TEAM_A_API_KEY — so two tenants sharing a worker cannot read
// each other's secrets even though their workflows name the same reference. The
// empty namespace reads the unprefixed form, which is what a single-tenant
// deployment sets.
//
// The namespace is uppercased and its dashes become underscores, because those are
// not legal in a variable name. It is validated before it reaches here, so the
// mapping cannot produce anything a shell would treat oddly.
func (p *EnvProvider) variable(namespace, name string) string {
	if namespace == "" {
		return p.prefix + name
	}

	return p.prefix + strings.ToUpper(strings.ReplaceAll(namespace, "-", "_")) + "_" + name
}

// permitted reports whether the reference names a variable this provider may read.
func (p *EnvProvider) permitted(ref Ref) error {
	if !validEnvName(ref.GetName()) {
		return fmt.Errorf(
			"%w: %q is not a valid environment variable name",
			ErrInvalidRef, ref.GetName(),
		)
	}

	return nil
}

// notConfigured reports a name this provider will not resolve.
//
// A name absent from the allowlist and a variable that is not set produce the same
// error deliberately, so that a workflow cannot use the difference to enumerate
// which secrets a worker is configured with.
func notConfigured(ref Ref, prefix string) error {
	return &ResolveError{
		Ref: ref,
		Err: fmt.Errorf("%w: $%s%s is not configured on this worker", ErrNotFound, prefix, ref.GetName()),
	}
}

// Names returns the reference names resolvable in a namespace, sorted, for
// reporting a worker's configuration. It returns names, never values.
func (p *EnvProvider) NamesIn(namespace string) []string {
	prefix := p.variable(namespace, "")

	var names []string

	for _, entry := range os.Environ() {
		name, _, found := strings.Cut(entry, "=")
		if !found || !strings.HasPrefix(name, prefix) {
			continue
		}

		name = strings.TrimPrefix(name, prefix)
		if len(p.allow) > 0 && !slices.Contains(p.allow, name) {
			continue
		}

		names = append(names, name)
	}

	slices.Sort(names)

	return names
}

// validEnvName reports whether name is a usable environment variable name. It
// rejects the empty name, a leading digit, and anything outside the portable
// character set — including "=" and NUL, which cannot appear in a variable name at
// all.
func validEnvName(name string) bool {
	if name == "" {
		return false
	}

	for i, c := range name {
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c == '_':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}

	return true
}

// Names returns the reference names this provider can resolve right now, in sorted
// order, for reporting a worker's configuration. It returns names, never values.
func (p *EnvProvider) Names() []string {
	var names []string

	for _, entry := range os.Environ() {
		name, _, found := strings.Cut(entry, "=")
		if !found || !strings.HasPrefix(name, p.prefix) {
			continue
		}

		name = strings.TrimPrefix(name, p.prefix)
		if len(p.allow) > 0 && !slices.Contains(p.allow, name) {
			continue
		}

		names = append(names, name)
	}

	slices.Sort(names)

	return names
}
