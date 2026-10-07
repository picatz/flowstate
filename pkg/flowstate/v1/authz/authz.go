// Package authz answers one question for every enforcement point that guards a
// Flowstate operation: does this caller hold this authorization action?
//
// The action vocabulary is owned by the schema ([v1.AuthorizationAction]), the
// set a caller holds is decided at admission ([auth.Principal.Actions], the
// trusted issuer entry narrowed by the token's own scopes), and the decision
// lives here, once. The RPC handlers, the debug endpoints, the MCP tools and
// the codec server all ask [Decide] rather than comparing the list themselves,
// so a rule about what an absent list means cannot drift between them.
//
// This package decides deployment-level authority. It does not evaluate the
// author-owned `allow:` predicates in a Flowfile, which decide who may answer
// one particular gate and are evaluated by [v1.SignalPolicyCheck] against the
// run's own scope.
package authz

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"connectrpc.com/connect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// Mode says how an action comes to be held.
type Mode int

const (
	// Implied actions are the ordinary operations. A caller holds what its
	// trusted issuer entry lists and nothing else; only a context with no
	// authentication at all, or the anonymous caller of an explicitly insecure
	// server, is unrestricted.
	Implied Mode = iota

	// Explicit actions are held only when named. They gate what an operator
	// must grant on purpose (reading sensitive values in the clear, using the
	// codec server), so neither an absent list nor anonymity implies them.
	Explicit
)

// Decision is the outcome of one authorization check.
type Decision struct {
	// Allowed reports whether the caller holds the action.
	Allowed bool

	// Scope is the wire spelling of the action, for a refusal to name.
	Scope string

	// Embedder is true when an embedder's [Decider], not the caller's action
	// list, refused the request. A refusal then names no scope, because the
	// caller cannot fix it by asking for one.
	Embedder bool
}

// Refusal returns the error that tells a caller it lacks the action, or nil
// when the decision allowed it. The error carries the RFC 6750 section 3.1
// insufficient_scope challenge, so a client can tell a missing scope from a
// failure of authentication and request the one it needs.
func (d Decision) Refusal() *connect.Error {
	if d.Allowed {
		return nil
	}

	if d.Embedder {
		return connect.NewError(connect.CodePermissionDenied,
			errors.New("the request was refused by this deployment's authorization rules"))
	}

	refusal := connect.NewError(connect.CodePermissionDenied,
		fmt.Errorf("the caller is not authorized for required action %q", d.Scope))
	refusal.Meta().Set("WWW-Authenticate", fmt.Sprintf(`Bearer error="insufficient_scope", scope=%q`, d.Scope))

	return refusal
}

// Request is one authorization question: may this caller hold this action.
type Request struct {
	// Principal is the verified caller. It is meaningful only when
	// Authenticated is true.
	Principal auth.Principal

	// Authenticated is false when no principal was established: a deployment
	// with no authentication configured, or a transport whose trust is the
	// process itself.
	Authenticated bool

	// Action is the action the caller needs.
	Action v1.AuthorizationAction

	// Mode says whether the action is implied for an unrestricted caller.
	Mode Mode
}

// A Decider answers a [Request]. It may be asked more than once for one
// request, so it must be cheap and its answer must not depend on how often it
// has been asked. [PolicyDecider] is the built-in answer, the
// trusted issuer entry's list; an embedder adds its own with [Restrict].
type Decider interface {
	Decide(ctx context.Context, req Request) Decision
}

// DeciderFunc adapts a function to a [Decider].
type DeciderFunc func(ctx context.Context, req Request) Decision

// Decide calls f.
func (f DeciderFunc) Decide(ctx context.Context, req Request) Decision { return f(ctx, req) }

// PolicyDecider is the built-in [Decider]: a verified caller holds what its
// trusted issuer entry lists, narrowed by its token's scopes, and nothing else.
type PolicyDecider struct{}

// Decide answers req from the caller's action list.
func (PolicyDecider) Decide(_ context.Context, req Request) Decision {
	return DecidePrincipal(req.Principal, req.Authenticated, req.Action, req.Mode)
}

// Restrict returns a Decider that allows a request only when base and extra
// both allow it. An embedder's extra check can therefore refuse what the trust
// policy grants, such as a maintenance freeze or a per-tenant allowlist, but can
// never grant what the trust policy withholds: the deployment's policy stays the
// outer bound, and a bug in the extra check fails closed. extra is consulted
// only once base has allowed, and a panic in it is a refusal. Its Allowed is the
// only part of its answer that is used: a refusal reports the action the caller
// needed and says an embedder refused, not what the extra decider put in Scope.
// A nil extra returns base, and a nil base is [PolicyDecider].
func Restrict(base, extra Decider) Decider {
	if base == nil {
		base = PolicyDecider{}
	}
	if extra == nil {
		return base
	}

	return DeciderFunc(func(ctx context.Context, req Request) (decision Decision) {
		decision = base.Decide(ctx, req)
		if !decision.Allowed {
			return decision
		}

		defer func() {
			if recover() != nil {
				decision = Decision{Scope: decision.Scope, Embedder: true}
			}
		}()

		// The extra decider gets its own copy of the action list, so a decider
		// that writes to it cannot change what a later check on this request
		// finds in the caller's principal.
		isolated := req
		isolated.Principal.Actions = slices.Clone(req.Principal.Actions)
		if extra := extra.Decide(ctx, isolated); !extra.Allowed {
			return Decision{Scope: decision.Scope, Embedder: true}
		}

		return decision
	})
}

// DecideWith asks d whether the caller authenticated on ctx holds action. A nil
// d is [PolicyDecider].
func DecideWith(ctx context.Context, d Decider, action v1.AuthorizationAction, mode Mode) Decision {
	if d == nil {
		d = PolicyDecider{}
	}
	principal, ok := auth.PrincipalFromContext(ctx)

	return d.Decide(ctx, Request{Principal: principal, Authenticated: ok, Action: action, Mode: mode})
}

// Decide reports whether the caller authenticated on ctx holds action.
//
// A context with no principal belongs to a deployment that configured no
// authentication, or to a transport whose trust is the process itself such as
// the MCP stdio server; there is no caller to restrict, so an [Implied] action
// is held and an [Explicit] one is not. A verified caller whose entry lists no
// action holds none.
func Decide(ctx context.Context, action v1.AuthorizationAction, mode Mode) Decision {
	return DecideWith(ctx, nil, action, mode)
}

// Restricted reports whether the caller's trusted issuer entry decides what it
// may do. An unrestricted caller holds every [Implied] action, so a check that
// must look the action up first, such as an MCP tool that maps to none, can skip
// the lookup for it and refuse only a caller the list could have refused.
//
// A verified caller is always restricted, and a policy entry cannot omit its
// list, so an absent list never grants anything. The two unrestricted callers
// are deliberate: no authentication configured at all (and so no principal), and
// the anonymous principal that [auth.InsecureAnonymousVerifier] admits, whose
// server an operator had to ask by name to leave open.
func Restricted(principal auth.Principal, authenticated bool) bool {
	return authenticated && !principal.IsAnonymous()
}

// DecidePrincipal is [Decide] for a caller that has already been extracted from
// its context. authenticated is false when no principal was established.
func DecidePrincipal(principal auth.Principal, authenticated bool, action v1.AuthorizationAction, mode Mode) Decision {
	scope := v1.AuthorizationActionScope(action)

	if mode == Implied && !Restricted(principal, authenticated) {
		return Decision{Allowed: true, Scope: scope}
	}

	return Decision{
		Allowed: authenticated && slices.Contains(principal.Actions, scope),
		Scope:   scope,
	}
}
