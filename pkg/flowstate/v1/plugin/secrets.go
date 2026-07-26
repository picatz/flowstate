package plugin

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"connectrpc.com/connect"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// secretProvider resolves one scheme by asking a plugin.
//
// It is a [secrets.Provider] like the environment and file providers, which is
// the whole of what a secrets backend is: a scheme and one method. Nothing else
// in the engine knows that a reference is answered by another process.
//
// # Keeping the value contained
//
// The value crosses a process boundary in a response, and everything this type
// does after that is about where it goes next:
//
//   - It is turned into a [secrets.Secret] immediately, by [secrets.NewSecret],
//     which is the only construction that keeps it out of anything that formats
//     it. It is never assigned to a variable that outlives that statement.
//   - No error from here quotes a response. A plugin's error message is
//     forwarded, because a plugin is required by the schema not to put a value
//     in one and a resolution failure is useless without it — but nothing this
//     code writes reads the response body it just received.
//   - Nothing is logged on the success path. A log line saying which reference
//     resolved would be safe; one saying anything about the value would not, and
//     the reliable way to never write the second is to not write log lines about
//     values at all.
type secretProvider struct {
	plugin *Plugin
	scheme string
	cfg    Config

	// warnOnce keeps the unimplemented-TTL warning to one line per plugin
	// rather than one per resolution.
	warnOnce sync.Once
}

// newSecretProvider returns the provider for one of a plugin's schemes.
//
// One provider per scheme rather than one per plugin, because [secrets.Provider]
// answers for exactly one scheme and the registry keys on it. A plugin claiming
// three schemes yields three providers over one process.
func newSecretProvider(p *Plugin, scheme string, cfg Config) secrets.Provider {
	return &secretProvider{plugin: p, scheme: scheme, cfg: cfg}
}

// Scheme implements [secrets.Provider].
func (s *secretProvider) Scheme() string { return s.scheme }

// String reports which plugin answers this scheme, without saying anything about
// what it has resolved. The registry puts %T of a provider into its
// duplicate-scheme error, and a bare type name there would not say which plugin
// was involved.
func (s *secretProvider) String() string {
	return fmt.Sprintf("plugin %q (scheme %q)", s.plugin.Name(), s.scheme)
}

// Resolve implements [secrets.Provider].
//
// It holds to the contract that interface documents: the namespace scopes the
// lookup and is sent to the plugin so a plugin serving several tenants can scope
// it too, errors are classified so the retry decision is right, nothing is
// retained beyond the call, and the value comes back only through
// [secrets.NewSecret].
func (s *secretProvider) Resolve(ctx context.Context, req secrets.Request) (secrets.Secret, error) {
	ref := req.Ref

	if ref.GetScheme() != s.scheme {
		// The store dispatches by scheme, so this cannot happen through it; it
		// can through a caller holding a provider directly, and answering a
		// reference for another scheme would be answering a question nobody
		// asked this backend.
		return secrets.Secret{}, &secrets.ResolveError{
			Ref: ref,
			Err: fmt.Errorf("%w: %q is answered by %q, not by this provider",
				secrets.ErrUnknownScheme, ref.GetScheme(), s.scheme),
		}
	}

	inst, err := s.plugin.ready()
	if err != nil {
		// A plugin that is restarting is a backend that cannot be reached, which
		// is the one transient classification: the next attempt may well find it
		// serving again.
		return secrets.Secret{}, &secrets.ResolveError{
			Ref: ref,
			Err: fmt.Errorf("%w: %w", secrets.ErrUnavailable, err),
		}
	}

	callCtx, cancel := s.plugin.callContext(ctx)
	defer cancel()

	resp, err := inst.clients.secret.Resolve(callCtx, connect.NewRequest(&flowstatev1.ResolveSecretRequest{
		Ref: &flowstatev1.SecretRef{
			Scheme: ref.GetScheme(),
			Name:   ref.GetName(),
		},
		Namespace: req.Namespace,
		Identity:  identityForNamespace(ctx, req.Namespace),
	}))
	if err != nil {
		return secrets.Secret{}, &secrets.ResolveError{
			Ref: ref,
			Err: s.classify(err),
		}
	}

	value := resp.Msg.GetValue()
	if len(value) == 0 {
		return secrets.Secret{}, &secrets.ResolveError{
			Ref: ref,
			Err: fmt.Errorf("%w: plugin %q returned no value", secrets.ErrEmpty, s.plugin.Name()),
		}
	}

	// The response's expires_in is a per-value hint that the engine should cache
	// this no longer than the plugin considers safe, and it cannot be honored
	// yet: secrets.Provider returns (Secret, error), with nowhere to carry a
	// TTL, so secrets.Cache applies its own default regardless. A plugin vending
	// a short-lived lease therefore has it cached for the cache's default
	// instead. Saying so once per resolution would be noise, so it is said once
	// per plugin and only when a plugin actually asks for something shorter.
	if expires := resp.Msg.GetExpiresIn().AsDuration(); expires > 0 {
		s.warnOnce.Do(func() {
			s.plugin.log.Warn(
				"plugin asks that resolved secrets not be cached beyond a limit, which this engine cannot honor yet; the configured cache TTL applies instead",
				"scheme", s.scheme, "requested", expires,
			)
		})
	}

	// The one statement the value appears in. NewSecret closes over it, so from
	// here it is reachable only through Reveal.
	return secrets.NewSecret(ref, string(value)), nil
}

// classify maps a plugin's failure onto the secrets package's classification,
// which decides whether the step that needed the secret is attempted again.
//
// Getting this wrong is expensive in both directions: a transient failure
// classified as permanent fails a run that would have succeeded, and a permanent
// one classified as transient spends a step's whole retry budget re-asking a
// question whose answer cannot change.
func (s *secretProvider) classify(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", secrets.ErrUnavailable, err)
	}

	code, ok := connectError(err)
	if !ok {
		// Not an answer from the plugin at all — the socket went away, or the
		// response could not be read. Either is the backend being unreachable.
		return fmt.Errorf("%w: plugin %q: %w", secrets.ErrUnavailable, s.plugin.Name(), err)
	}

	switch code {
	case connect.CodeNotFound:
		return fmt.Errorf("%w: %w", secrets.ErrNotFound, err)
	case connect.CodePermissionDenied, connect.CodeUnauthenticated:
		return fmt.Errorf("%w: %w", secrets.ErrPermission, err)
	case connect.CodeInvalidArgument, connect.CodeFailedPrecondition, connect.CodeOutOfRange:
		return fmt.Errorf("%w: %w", secrets.ErrInvalidRef, err)
	case connect.CodeUnimplemented:
		return fmt.Errorf("%w: plugin %q does not serve secret resolution: %w",
			secrets.ErrUnknownScheme, s.plugin.Name(), err)
	case connect.CodeResourceExhausted:
		return fmt.Errorf("%w: %w", secrets.ErrTooLarge, err)
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeAborted, connect.CodeCanceled:
		// Cancellation the plugin reported rather than the caller's own, which
		// the check above catches. It says nothing is wrong with the reference,
		// so another attempt is worth making.
		return fmt.Errorf("%w: %w", secrets.ErrUnavailable, err)
	default:
		// Unclassified is permanent. Guessing that a failure is retryable is the
		// more expensive mistake, and it is the rule the rest of the package
		// applies.
		return fmt.Errorf("plugin %q: %w", s.plugin.Name(), err)
	}
}
