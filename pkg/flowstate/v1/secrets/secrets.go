// Package secrets resolves secret references to secret values, worker-side and
// as late as possible.
//
// # The invariant
//
// A workflow that needs an API key must never contain one. A literal pasted into
// a Flowfile is compiled into the protobuf specification, submitted to the API
// server, handed to the durable execution backend, and written to workflow
// history, where it is replayable and visible in tooling. Nothing about that path
// is reversible, so the value must not enter it.
//
// This package exists to keep a reference a reference for the whole of that path:
//
//	Flowfile -> compile -> submit -> schedule -> workflow-side evaluation
//	                                                     |
//	                                        (still only a reference)
//	                                                     v
//	                                             activity, worker-side
//	                                                     |
//	                                            Resolver.Resolve
//	                                                     v
//	                                                  Secret
//
// A [Ref] is inert: it names a secret and carries no way to obtain one. There is
// no method on it that returns a value, so no amount of workflow-side code can
// turn one into a value. Resolution requires a [Resolver], which requires
// [Provider] implementations, which are constructed from worker-side
// configuration such as the process environment or a mounted directory. The
// workflow side of the engine cannot resolve a secret because it has nothing to
// resolve it with, and that is a property of the types rather than a rule someone
// has to remember.
//
// A [Ref] is safe to log, store, and put in an error message. It names a location,
// not a credential. Because a reference travels through the compiled workflow, the
// schema defines it and this package accepts what the schema produces; see [Ref].
//
// # Handling a resolved value
//
// [Secret] is the only thing a resolver returns, and it is built to survive the
// paths a value should never travel:
//
//   - Every formatting verb prints [Redacted], including %v, %s, %q, and %#v,
//     because it implements [fmt.Formatter] rather than only [fmt.Stringer].
//   - Structured logging prints [Redacted], via [log/slog.LogValuer].
//   - Marshaling to JSON or text yields [Redacted], never the value.
//   - Unmarshaling fails: a value comes from a resolver, never from data.
//   - It cannot be compared with ==, so a comparison has to be constant-time.
//
// Reaching the value takes [Secret.Reveal], which is named to read as the
// deliberate act it is at a call site. Everything else about the type is designed
// to make that the only way it can happen.
//
// Because a revealed value can end up inside an error produced by code that knows
// nothing about any of this — a client library echoing a header, a server
// reflecting a query parameter — use a [Scrubber] on the way out. Register the
// secrets an activity resolved, then pass anything the activity returns or logs
// through it.
//
// # Usage
//
// Build a store once, at worker startup:
//
//	files, err := secrets.NewFileProvider("/var/run/secrets/flowstate")
//	if err != nil {
//		return err
//	}
//
//	store, err := secrets.NewStore(
//		secrets.NewEnvProvider(),
//		secrets.NewCache(files),
//	)
//
// Then resolve inside the activity that needs the value, and nowhere else:
//
//	secret, err := store.Resolve(ctx, value.GetSecretRef())
//	if err != nil {
//		return nil, err // safe to surface: it names the ref, never the value
//	}
//
//	scrubber := secrets.NewScrubber(secret)
//	req.Header.Set("Authorization", "Bearer "+secret.Reveal())
//
//	resp, err := client.Do(req)
//	if err != nil {
//		return nil, scrubber.ScrubError(err)
//	}
package secrets

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// Limits on the parts of a reference, applied by [ValidateRef]. They exist so a
// malformed or hostile reference is rejected while it is still just text.
const (
	// MaxSchemeLen is the longest permitted provider scheme.
	MaxSchemeLen = 32

	// MaxNameLen is the longest permitted secret name.
	MaxNameLen = 1024
)

// Ref identifies a secret without containing it. It is the only form a secret takes
// anywhere outside the activity that needs the value: in a Flowfile, in the compiled
// specification, on the wire, and in workflow history.
//
// Ref is an interface, and its methods are the accessors protoc generates, so the
// schema's own secret reference message satisfies it with no conversion:
//
//	secret, err := store.Resolve(ctx, value.GetSecretRef())
//
// That shape is deliberate. A reference has to survive compilation, submission, and
// workflow-side evaluation untouched, which makes it part of the schema rather than
// an implementation detail — so the schema defines it, and this package accepts what
// the schema produces instead of declaring a second Go struct with the same two
// fields for callers to convert between. Declaring the interface here rather than
// importing the generated package is also what keeps this package free of any
// dependency on the engine, which is what lets the engine depend on it.
//
// Use [NewRef] where no compiled reference exists, such as in a test or a CLI.
//
// A reference is safe to log: it names where a secret lives, which is what makes a
// misconfiguration diagnosable, and it carries no way to obtain the secret itself.
// Render it with [RefString], and validate or quote one that came from outside,
// since an unvalidated name may hold a control character.
type Ref interface {
	// GetScheme returns the provider that resolves the reference, such as "env" or
	// "file". It matches the [Provider.Scheme] of a registered provider.
	GetScheme() string

	// GetName returns the secret's location within that provider. Its meaning is
	// the provider's: an environment variable suffix, a path relative to a mounted
	// directory, a vault path. A provider that supports versions expresses them
	// here, in whatever form it already uses.
	GetName() string
}

// ref is the minimal [Ref] returned by [NewRef] and [ParseRef], for callers that do
// not have a compiled reference to hand.
type ref struct {
	scheme string
	name   string
}

// GetScheme implements [Ref].
func (r ref) GetScheme() string { return r.scheme }

// GetName implements [Ref].
func (r ref) GetName() string { return r.name }

// NewRef returns a reference to the named secret.
//
// Prefer passing the compiled reference straight through where there is one; this
// exists for tests, command-line tools, and anything else holding two strings
// rather than a message.
func NewRef(scheme, name string) Ref {
	return ref{scheme: scheme, name: name}
}

// ParseRef parses the text form of a reference, "scheme:name", as a Flowfile writes
// it.
//
// It is the check to run while a reference is still text, so that a Flowfile naming
// a malformed secret fails when it is compiled rather than when it runs. The
// returned error wraps [ErrInvalidRef].
func ParseRef(s string) (Ref, error) {
	scheme, name, found := strings.Cut(s, ":")
	if !found {
		return nil, fmt.Errorf(
			"%w: %q has no provider, want a reference of the form \"scheme:name\" such as \"env:API_KEY\"",
			ErrInvalidRef, s,
		)
	}

	parsed := ref{scheme: scheme, name: name}
	if err := ValidateRef(parsed); err != nil {
		return nil, err
	}

	return parsed, nil
}

// RefString renders a reference as "scheme:name". It is safe to log and round-trips
// through [ParseRef].
//
// It is a function rather than a method because [Ref] cannot declare one: the
// generated message already has a String method, and it emits protobuf text format
// rather than this form. So format a reference with this, never with %s or %v.
func RefString(r Ref) string {
	if r == nil {
		return ""
	}

	return r.GetScheme() + ":" + r.GetName()
}

// ValidateRef reports whether a reference is well formed. It is applied by
// [ParseRef] and again by [Store.Resolve], so a reference decoded from a message is
// checked too, whether or not it ever existed as text.
//
// The returned error wraps [ErrInvalidRef].
func ValidateRef(r Ref) error {
	if r == nil {
		return fmt.Errorf("%w: reference is missing", ErrInvalidRef)
	}

	scheme, name := r.GetScheme(), r.GetName()

	switch {
	case scheme == "":
		return fmt.Errorf("%w: provider must not be empty", ErrInvalidRef)
	case len(scheme) > MaxSchemeLen:
		return fmt.Errorf("%w: provider is longer than %d characters", ErrInvalidRef, MaxSchemeLen)
	case !validScheme(scheme):
		return fmt.Errorf(
			"%w: provider %q may only contain lowercase letters, digits, and dashes",
			ErrInvalidRef, scheme,
		)
	case name == "":
		return fmt.Errorf("%w: name must not be empty in %q", ErrInvalidRef, RefString(r))
	case len(name) > MaxNameLen:
		return fmt.Errorf("%w: name is longer than %d characters", ErrInvalidRef, MaxNameLen)
	}

	// A control character in a name has no legitimate use and would let a
	// reference forge lines in a log that records it.
	if i := strings.IndexFunc(name, isControl); i >= 0 {
		return fmt.Errorf(
			"%w: name contains a control character at offset %d",
			ErrInvalidRef, i,
		)
	}

	return nil
}

// ValidateScheme reports whether a provider scheme is well formed: one to
// [MaxSchemeLen] lowercase letters, digits, and dashes.
//
// It is exported for providers implemented outside this package, which otherwise
// have to reimplement the same check to validate a configurable scheme — and a
// provider whose idea of a valid scheme differs from the registry's is one that
// fails at registration for reasons its own configuration never explained.
func ValidateScheme(scheme string) error {
	switch {
	case scheme == "":
		return fmt.Errorf("%w: scheme must not be empty", ErrInvalidRef)
	case len(scheme) > MaxSchemeLen:
		return fmt.Errorf("%w: scheme is longer than %d characters", ErrInvalidRef, MaxSchemeLen)
	case !validScheme(scheme):
		return fmt.Errorf(
			"%w: scheme %q may only contain lowercase letters, digits, and dashes",
			ErrInvalidRef, scheme,
		)
	}

	return nil
}

// validScheme reports whether s is a well-formed provider scheme.
func validScheme(s string) bool {
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '-':
		default:
			return false
		}
	}

	return true
}

// isControl reports whether r is a control character, which includes the newlines
// and escapes that would let text forge log output.
func isControl(r rune) bool {
	return r == unicode.ReplacementChar || unicode.IsControl(r)
}

// Namespace limits, applied by [ValidateNamespace].
const (
	// MaxNamespaceLen is the longest permitted namespace.
	MaxNamespaceLen = 63
)

// Request is what a [Provider] is asked to resolve.
//
// It is a struct rather than a parameter list so that a provider implemented
// outside this package keeps compiling when a field is added.
type Request struct {
	// Namespace is the tenant the run belongs to, and it scopes what the
	// reference means: the same reference resolves to a different secret in two
	// namespaces, and neither can reach the other's.
	//
	// It comes from the authenticated caller recorded in the run's identity, never
	// from the workflow, which is what makes the boundary real rather than a
	// convention — a Flowfile that could name its own tenant could name someone
	// else's. It is validated before a provider sees it, so it is safe to use in a
	// path or a variable name.
	//
	// The empty namespace is a namespace like any other, not a wildcard: it is
	// what a single-tenant, self-hosted deployment with no identity provider
	// resolves in, and it cannot reach a named namespace's secrets either.
	Namespace string

	// Ref is the reference to resolve.
	Ref Ref
}

// Resolver turns a reference into a value. It is the interface an activity should
// depend on, since it is the whole of what an activity needs.
//
// A Resolver is already scoped to one namespace — [Store.For] is how one is
// obtained — so an activity cannot resolve outside its own tenant, and cannot
// forget to say which tenant it is in.
//
// Implementations must be safe for concurrent use: one resolver is shared by every
// task execution on a worker.
type Resolver interface {
	// Resolve returns the secret named by ref, within the resolver's namespace.
	//
	// It reports an error wrapping [ErrNotFound] when no such secret exists, and
	// one wrapping [ErrEmpty] when the secret exists but holds nothing. Errors
	// name the reference and never the value.
	Resolve(ctx context.Context, ref Ref) (Secret, error)
}

// Provider resolves the references of one scheme. Adding a source of secrets — a
// cloud KMS, a vault, an OS keychain, a password manager — means implementing this
// interface and registering it, not changing anything else.
//
// # The contract
//
// An implementation must hold to all of the following. The engine's behavior
// depends on it, and the parts about errors are load-bearing: the retry
// classification decides whether a failed step is attempted again based on which
// error came back, so returning the wrong one either retries something that can
// never succeed or gives up on something transient.
//
//   - **Errors must be classified.** Wrap [ErrNotFound] when the secret does not
//     exist, [ErrEmpty] when it exists and holds nothing, [ErrPermission] when the
//     backend refused, and [ErrUnavailable] when the backend could not be reached
//     or timed out. The first three are permanent and must not be retried; the
//     last is transient and will be. Anything unclassified is treated as
//     permanent, since guessing that a failure is retryable is the more expensive
//     mistake. Wrap them in a [*ResolveError] carrying the request's reference.
//   - **Never put the value in an error, a log, or a metric.** Not truncated, not
//     hashed, not "just the first few characters".
//   - **Never retain the value beyond the call.** Do not cache it: expiry,
//     bounding, and stampede collapse are [Cache]'s job, and a second cache inside
//     a provider is a second place a value lives and a second thing to invalidate.
//   - **Must be safe for concurrent use.** One provider serves every task
//     execution on a worker.
//   - **Must honor the context.** Resolve may block — a network round trip, a
//     subprocess — but must return when the context is done, and must not outlive
//     it. A provider with its own timeout should still respect an earlier deadline.
//   - **Must scope by namespace.** Two namespaces asking for the same reference
//     must get different secrets. Ignoring [Request.Namespace] is a tenancy
//     breach, not a missing feature.
//   - **Must return a value only through [NewSecret].** That is what keeps the
//     value out of history, out of logs, and out of anything that formats it.
//
// A provider that cannot work in its environment should fail when it is
// constructed, not when it is first used: a worker whose keychain tool is missing
// should refuse to start rather than failing the first workflow that needs a
// secret.
type Provider interface {
	// Scheme returns the reference scheme this provider handles, such as "env".
	Scheme() string

	// Resolve returns the secret the request names.
	Resolve(ctx context.Context, req Request) (Secret, error)
}

// Store resolves references by dispatching each one to the provider registered for
// its scheme.
//
// A Store is not itself a [Resolver]: it has no namespace, and resolving without
// one would be resolving outside any tenant. [Store.For] binds a namespace and
// returns a Resolver, which is the only way to reach a value. That is deliberate —
// it makes the tenant boundary something a caller cannot forget rather than a
// parameter they might leave empty.
//
// A Store is immutable once built and safe for concurrent use.
type Store struct {
	registry *Registry
	strictNS bool
}

// StoreOption configures a [Store].
type StoreOption func(*Store)

// WithRequiredNamespace refuses to resolve anything in the empty namespace.
//
// Use it in a multi-tenant deployment, where a resolution with no namespace means
// the identity was lost somewhere upstream and the request should fail rather than
// fall back to a shared tenant. A single-tenant deployment leaves it off, since
// invariant 8 requires the engine to work with no identity provider at all.
func WithRequiredNamespace() StoreOption {
	return func(s *Store) {
		s.strictNS = true
	}
}

// NewStore builds a store from the given providers, which must have distinct
// schemes.
//
// A store with no providers resolves nothing, which is the correct configuration
// for a deployment where workflows are not permitted to use secrets at all: every
// reference is refused rather than quietly resolving from an unexpected source.
func NewStore(providers ...Provider) (*Store, error) {
	registry := NewRegistry()

	for _, provider := range providers {
		if err := registry.Register(provider); err != nil {
			return nil, err
		}
	}

	return NewStoreFromRegistry(registry)
}

// NewStoreFromRegistry builds a store over an existing [Registry], for a
// deployment that assembles its providers separately — from a configuration file,
// or from plugins registered at startup.
//
// The store takes a snapshot: providers registered afterwards are not visible to
// it, so what a store resolves cannot change under a running worker.
func NewStoreFromRegistry(registry *Registry, opts ...StoreOption) (*Store, error) {
	if registry == nil {
		return nil, fmt.Errorf("secrets: a store needs a registry")
	}

	store := &Store{registry: registry.clone()}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(store)
	}

	return store, nil
}

// Schemes returns the registered provider schemes in sorted order, for reporting
// what a worker is configured to resolve.
func (s *Store) Schemes() []string {
	return s.registry.Schemes()
}

// For returns a [Resolver] scoped to a namespace, which is how a value is reached.
//
// Pass the run's identity rather than a bare string: the namespace must come from
// the authenticated caller, and taking it from something that reports its own
// namespace is what keeps a workflow from choosing its tenant. A nil identity
// yields the empty namespace, which is a tenant of its own and is what a
// single-tenant deployment uses.
//
// It reports an error for a namespace that is malformed, or for the empty
// namespace when the store was built with [WithRequiredNamespace].
func (s *Store) For(identity NamespaceProvider) (Resolver, error) {
	var namespace string
	if identity != nil {
		namespace = identity.GetNamespace()
	}

	if namespace == "" && s.strictNS {
		return nil, fmt.Errorf(
			"%w: this worker requires a namespace and the run's identity carries none",
			ErrNamespace,
		)
	}

	if err := ValidateNamespace(namespace); err != nil {
		return nil, err
	}

	return &scopedResolver{store: s, namespace: namespace}, nil
}

// NamespaceProvider reports the tenant a run belongs to.
//
// Its method is the accessor protoc generates, so the identity message recorded on
// a run satisfies it directly, and this package needs no dependency on the schema
// to accept one. Use [Namespace] where no identity exists, such as in a test.
type NamespaceProvider interface {
	GetNamespace() string
}

// Namespace is a bare namespace, for a caller that has one without an identity to
// read it from. Prefer passing the run's identity.
type Namespace string

// GetNamespace implements [NamespaceProvider].
func (n Namespace) GetNamespace() string { return string(n) }

// ValidateNamespace reports whether a namespace is usable.
//
// It is applied before any provider sees the namespace, because a provider puts it
// into a filesystem path or an environment variable name: an unconstrained
// namespace would be a path traversal, and one containing a control character
// would forge log lines. The namespace comes from an authenticated identity and
// should already be well formed, so this is the second line rather than the first.
func ValidateNamespace(namespace string) error {
	if namespace == "" {
		return nil
	}

	if len(namespace) > MaxNamespaceLen {
		return fmt.Errorf("%w: namespace is longer than %d characters", ErrNamespace, MaxNamespaceLen)
	}

	for i, c := range namespace {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-' && i > 0:
		default:
			return fmt.Errorf(
				"%w: namespace %q may only contain lowercase letters, digits, and dashes, and may not start with a dash",
				ErrNamespace, namespace,
			)
		}
	}

	return nil
}

// scopedResolver is a [Store] bound to one namespace.
type scopedResolver struct {
	store     *Store
	namespace string
}

// Resolve implements [Resolver].
//
// It validates the reference, dispatches to the registered provider, and checks
// what comes back: a provider must return a non-empty secret carrying the
// reference that was asked for. Those last checks cost nothing and hold for
// provider implementations that live outside this package.
func (r *scopedResolver) Resolve(ctx context.Context, ref Ref) (Secret, error) {
	if ctx == nil {
		return Secret{}, fmt.Errorf("secrets: Resolve requires a context")
	}
	if err := ctx.Err(); err != nil {
		// A cancelled activity should not start a lookup that may block on a
		// network round trip or a subprocess.
		return Secret{}, err
	}

	if err := ValidateRef(ref); err != nil {
		return Secret{}, err
	}

	provider, ok := r.store.registry.Lookup(ref.GetScheme())
	if !ok {
		return Secret{}, &ResolveError{
			Ref: ref,
			Err: fmt.Errorf("%w: %q is not configured on this worker (configured: %s)",
				ErrUnknownScheme, ref.GetScheme(), schemeList(r.store.Schemes())),
		}
	}

	secret, err := provider.Resolve(ctx, Request{Namespace: r.namespace, Ref: ref})
	if err != nil {
		// A provider written elsewhere may not name the reference, which every
		// resolution failure is documented to do.
		var resolveErr *ResolveError
		if !errors.As(err, &resolveErr) {
			err = &ResolveError{Ref: ref, Err: err}
		}
		return Secret{}, err
	}

	switch {
	case secret.IsZero():
		return Secret{}, &ResolveError{
			Ref: ref,
			Err: fmt.Errorf("%w: provider %T returned no value and no error", ErrEmpty, provider),
		}
	case RefString(secret.ref) != RefString(ref):
		return Secret{}, &ResolveError{
			Ref: ref,
			Err: fmt.Errorf("provider %T returned a secret for %q", provider, RefString(secret.ref)),
		}
	}

	return secret, nil
}

// schemeList renders the configured schemes for an error message.
func schemeList(schemes []string) string {
	if len(schemes) == 0 {
		return "none"
	}

	return strings.Join(schemes, ", ")
}
