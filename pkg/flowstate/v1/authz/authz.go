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
}

// Refusal returns the error that tells a caller it lacks the action, or nil
// when the decision allowed it. The error carries the RFC 6750 section 3.1
// insufficient_scope challenge, so a client can tell a missing scope from a
// failure of authentication and request the one it needs.
func (d Decision) Refusal() *connect.Error {
	if d.Allowed {
		return nil
	}

	refusal := connect.NewError(connect.CodePermissionDenied,
		fmt.Errorf("the caller is not authorized for required action %q", d.Scope))
	refusal.Meta().Set("WWW-Authenticate", fmt.Sprintf(`Bearer error="insufficient_scope", scope=%q`, d.Scope))

	return refusal
}

// Decide reports whether the caller authenticated on ctx holds action.
//
// A context with no principal belongs to a deployment that configured no
// authentication, or to a transport whose trust is the process itself such as
// the MCP stdio server; there is no caller to restrict, so an [Implied] action
// is held and an [Explicit] one is not. A verified caller whose entry lists no
// action holds none.
func Decide(ctx context.Context, action v1.AuthorizationAction, mode Mode) Decision {
	principal, ok := auth.PrincipalFromContext(ctx)

	return DecidePrincipal(principal, ok, action, mode)
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
