package flowstatev1_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
)

// TestEveryRPCHasExactlyOneAuthorizationAction is what keeps the vocabulary
// attached to the surface it is a vocabulary of.
//
// It walks flowstate.v1.WorkflowService's own descriptor rather than any list
// of RPC names, in both directions: an RPC the schema declares and no binding
// names fails, and a binding naming an RPC the schema no longer declares fails.
// That is the whole reason the bindings may be hand-written — the judgement
// about which RPCs share an action is recorded once, and the thing it is a
// judgement about is read from the source of truth.
//
// It also pins the *audited* surface (#1018), which is derived from the same
// bindings: every RPC the schema declares reaches an action that
// [audit.AuditedActions] includes, so an RPC cannot arrive unaudited without
// this failing. The seam that does the emitting is held honest separately, by
// TestEveryRPCReachesTheAuditSeam in the server package — this end owns the
// vocabulary, that end owns the call sites.
func TestEveryRPCHasExactlyOneAuthorizationAction(t *testing.T) {
	t.Parallel()

	services := v1.File_flowstate_v1_service_proto.Services()
	require.Equal(t, 1, services.Len(), "the schema declares more than one service; this test names one")

	declared := map[string]bool{}
	methods := services.Get(0).Methods()
	require.NotZero(t, methods.Len(), "the service declares no methods; the lookup is broken")

	for i := range methods.Len() {
		declared[string(methods.Get(i).Name())] = true
	}

	bound := map[string]v1.AuthorizationAction{}
	for _, binding := range v1.AuthorizationActionBindings() {
		for _, rpc := range binding.GetRpcs() {
			previous, seen := bound[rpc]
			require.False(t, seen, "rpc %s is bound to both %s and %s; an operation has one action",
				rpc, previous, binding.GetAction())
			bound[rpc] = binding.GetAction()

			require.True(t, declared[rpc],
				"a binding names the rpc %s, which flowstate.v1.WorkflowService no longer declares", rpc)
		}
	}

	audited := map[v1.AuthorizationAction]bool{}
	for _, action := range audit.AuditedActions() {
		audited[action] = true
	}

	for rpc := range declared {
		action, err := v1.AuthorizationActionForRPC(rpc)
		require.NoError(t, err,
			"the schema declares rpc %s and no authorization action names it", rpc)
		require.Equal(t, bound[rpc], action)
		require.NotEqual(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED, action)

		require.True(t, audited[action],
			"the schema declares rpc %s, whose action %s is outside the audited surface; "+
				"an authorization decision nobody records is one nobody can review",
			rpc, action)
	}
}

// TestEveryAuthorizationActionIsBoundExactlyOnce closes the other direction:
// an action in the schema that no binding mentions is a scope this deployment
// publishes and no operation reaches.
func TestEveryAuthorizationActionIsBoundExactlyOnce(t *testing.T) {
	t.Parallel()

	bound := map[v1.AuthorizationAction]bool{}
	for _, binding := range v1.AuthorizationActionBindings() {
		require.NoError(t, v1.Validate(binding),
			"binding for %s does not satisfy the schema's own bounds", binding.GetAction())

		require.False(t, bound[binding.GetAction()], "%s is bound twice", binding.GetAction())
		bound[binding.GetAction()] = true

		require.NotEmpty(t, append(binding.GetRpcs(), binding.GetMcpTools()...),
			"%s names no operation at all, so nothing can ever be authorized as it", binding.GetAction())

		if parent := binding.GetParent(); parent != v1.AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED {
			require.NotEqual(t, binding.GetAction(), parent, "%s is its own parent", binding.GetAction())
		}
	}

	values := v1.AuthorizationAction(0).Descriptor().Values()
	for i := range values.Len() {
		action := v1.AuthorizationAction(values.Get(i).Number())
		if action == v1.AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED {
			continue
		}

		require.True(t, bound[action],
			"the schema declares %s and no binding names an operation for it", action)
	}
}

// TestAuthorizationActionScopesAreDerivedFromTheSchema pins the derivation the
// schema's comment states, because it is what lets the metadata document
// publish this list without a second copy of it existing.
func TestAuthorizationActionScopesAreDerivedFromTheSchema(t *testing.T) {
	t.Parallel()

	require.Equal(t, "workload.run",
		v1.AuthorizationActionScope(v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN))
	require.Equal(t, "mcp.run_local",
		v1.AuthorizationActionScope(v1.AuthorizationAction_AUTHORIZATION_ACTION_MCP_RUN_LOCAL))
	require.Empty(t,
		v1.AuthorizationActionScope(v1.AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED),
		"the absence of an action is not an operation and has no scope")

	scopes := v1.AuthorizationActionScopes()
	require.Len(t, scopes, v1.AuthorizationAction(0).Descriptor().Values().Len()-1,
		"every action but UNSPECIFIED is a scope")
	require.Equal(t, "workload.run", scopes[0], "the schema's order is the published order")

	seen := map[string]bool{}
	for _, scope := range scopes {
		require.False(t, seen[scope], "%q is published twice", scope)
		seen[scope] = true

		// RFC 6749 section 3.3: a scope value is space-delimited, so a space
		// in one would silently become two scopes. The quoting characters go
		// with it because a scope reaches a client through a JSON document and
		// may yet reach a WWW-Authenticate header.
		require.False(t, strings.ContainsAny(scope, " \"\\,"),
			"%q cannot be spelled as an OAuth scope value", scope)
		require.Equal(t, 1, strings.Count(scope, "."),
			"%q is not group.verb; the derivation turns exactly the first underscore into a dot", scope)
	}
}

// TestAuthorizationActionLookupsFailClosed pins that an unknown operation is
// an error rather than a permissive zero value, since a caller's next move is
// a decision about authority.
func TestAuthorizationActionLookupsFailClosed(t *testing.T) {
	t.Parallel()

	action, err := v1.AuthorizationActionForRPC("Undeclare")
	require.Error(t, err)
	require.ErrorContains(t, err, "Undeclare")
	require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED, action)

	action, err = v1.AuthorizationActionForMCPTool("flowstate_not_a_tool")
	require.Error(t, err)
	require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED, action)

	// A projected RPC tool and a tool no RPC projects both resolve through the
	// one lookup the audit seam uses.
	action, err = v1.AuthorizationActionForMCPTool("flowstate_get_catalog")
	require.NoError(t, err)
	require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_CATALOG_READ, action)
	require.Equal(t, "flowstate_get_catalog", v1.MCPToolNameForRPC("GetCatalog"))

	action, err = v1.AuthorizationActionForMCPTool("flowstate_run_local")
	require.NoError(t, err)
	require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_MCP_RUN_LOCAL, action)
}

// TestAuthorizationActionBindingsAreCopied keeps a caller ranging over the
// vocabulary from editing it.
func TestAuthorizationActionBindingsAreCopied(t *testing.T) {
	t.Parallel()

	bindings := v1.AuthorizationActionBindings()
	require.NotEmpty(t, bindings)
	bindings[0].Rpcs = []string{"Tampered"}

	action, err := v1.AuthorizationActionForRPC("Run")
	require.NoError(t, err)
	require.Equal(t, v1.AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN, action)
}
