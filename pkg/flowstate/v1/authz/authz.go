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
	// Implied actions are held by a caller whose entry lists no actions at
	// all. That is the posture of every ordinary operation: an entry that
	// restricts nothing grants everything, and one that lists actions grants
	// what it lists.
	Implied Mode = iota

	// Explicit actions are held only when named. They gate what an operator
	// must grant on purpose (reading sensitive values in the clear, using the
	// codec server), so no absent list and no unrestricted entry implies them.
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
// is held and an [Explicit] one is not.
func Decide(ctx context.Context, action v1.AuthorizationAction, mode Mode) Decision {
	principal, ok := auth.PrincipalFromContext(ctx)

	return DecidePrincipal(principal, ok, action, mode)
}

// Restricted reports whether the caller's entry lists the actions it grants.
// An unrestricted caller holds every [Implied] action, so a check that must look
// the action up first, such as an MCP tool that maps to none, can skip the lookup
// for it and refuse only a caller the list could have refused.
func Restricted(principal auth.Principal, authenticated bool) bool {
	return authenticated && principal.Actions != nil
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
