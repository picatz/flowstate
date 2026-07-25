package secrets

import (
	"context"
	"fmt"
	"maps"
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

	// namespacePrefixes maps a namespace to the variable prefix its secrets live
	// under. It is configured rather than derived; see WithEnvNamespaces.
	namespacePrefixes map[string]string
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

// WithEnvNamespaces maps each namespace to the environment variable prefix its
// secrets live under, which is what makes this provider usable by more than one
// tenant.
//
// The mapping is configured rather than derived from the namespace, because an
// environment is flat and a derived prefix collides. Deriving
// $FLOWSTATE_SECRET_TEAM_A_API_KEY from namespace "team-a" and name "API_KEY"
// produces exactly what namespace "team" and name "A_API_KEY" produce, and what
// the unnamespaced tenant produces from the name "TEAM_A_API_KEY" — three tenants
// reading one variable. There is no separator that fixes it, because every
// character legal in a prefix is also legal in a name.
//
// So an operator states the prefixes, and they are checked for the overlap that
// would reintroduce the problem: no prefix may be a prefix of another.
//
// Without this, a non-empty namespace is refused. That is the fail-closed choice:
// an environment is a reasonable single-tenant backend and a poor multi-tenant one,
// and a deployment that needs real separation should use files or a vault, whose
// hierarchies can express it.
func WithEnvNamespaces(prefixes map[string]string) EnvOption {
	return func(p *EnvProvider) error {
		for namespace, prefix := range prefixes {
			if err := ValidateNamespace(namespace); err != nil {
				return fmt.Errorf("secrets: env namespace mapping: %w", err)
			}
			if prefix == "" {
				return fmt.Errorf("secrets: env prefix for namespace %q must not be empty", namespace)
			}
		}

		if p.namespacePrefixes == nil {
			p.namespacePrefixes = make(map[string]string, len(prefixes))
		}
		maps.Copy(p.namespacePrefixes, prefixes)

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

	// Every prefix in play must be distinguishable from every other, or two
	// tenants can name one variable.
	if err := checkDisjointPrefixes(provider.prefix, provider.namespacePrefixes); err != nil {
		return nil, err
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

	variable, err := p.variable(req.Namespace, ref.GetName())
	if err != nil {
		return Secret{}, &ResolveError{Ref: ref, Err: err}
	}

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
// The empty namespace reads the configured prefix, which is what a single-tenant
// deployment sets. A named namespace reads the prefix an operator mapped to it, and
// a namespace with no mapping is refused rather than guessed at — see
// [WithEnvNamespaces] for why the prefix cannot be derived.
func (p *EnvProvider) variable(namespace, name string) (string, error) {
	if namespace == "" {
		return p.prefix + name, nil
	}

	prefix, ok := p.namespacePrefixes[namespace]
	if !ok {
		return "", fmt.Errorf(
			"%w: this worker's environment provider has no prefix configured for namespace %q, "+
				"so it cannot resolve secrets for it",
			ErrNamespace, namespace,
		)
	}

	return prefix + name, nil
}

// checkDisjointPrefixes reports whether any configured prefix is a prefix of
// another, which would let one tenant name another's variable.
func checkDisjointPrefixes(base string, namespaced map[string]string) error {
	all := map[string]string{"": base}
	maps.Copy(all, namespaced)

	for aNS, a := range all {
		for bNS, b := range all {
			if aNS == bNS {
				continue
			}
			if strings.HasPrefix(b, a) {
				return fmt.Errorf(
					"secrets: env prefix %q (namespace %q) starts with %q (namespace %q), "+
						"so one tenant could name the other's variables",
					b, bNS, a, aNS,
				)
			}
		}
	}

	return nil
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
	prefix, err := p.variable(namespace, "")
	if err != nil {
		return nil
	}

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
