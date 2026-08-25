package flowstatev1

import (
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
)

// authorizationActionBindings attaches every action in the schema's closed
// vocabulary to the operations it covers.
//
// Written out rather than derived, for the same reason
// mcp.WorkflowServiceMethods is: nothing in the descriptor says which RPCs
// share an authorization action, and grouping them is the judgement this list
// exists to record. What keeps a hand-written list honest is the test beside
// it — TestEveryRPCHasExactlyOneAuthorizationAction walks the service
// descriptor in both directions, so an RPC added to the schema without a
// binding fails, and a binding naming an RPC the service dropped fails too.
//
// The order is the enum's, and [AuthorizationActionScopes] publishes it, so a
// reader comparing the metadata document to this file sees the same sequence.
var authorizationActionBindings = []*AuthorizationActionBinding{
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN,
		Rpcs:   []string{"Run", "SignalWithStart"},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_READ,
		Rpcs:   []string{"Get", "List"},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_SIGNAL,
		Parent: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN,
		Rpcs:   []string{"Signal"},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_CANCEL,
		Parent: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN,
		Rpcs:   []string{"Cancel"},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_TERMINATE,
		Parent: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_CANCEL,
		Rpcs:   []string{"Terminate"},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_VALIDATE,
		Rpcs:   []string{"Validate"},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_COMPILE,
		Parent: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_VALIDATE,
		Rpcs:   []string{"Compile"},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_CATALOG_READ,
		Rpcs:   []string{"GetCatalog"},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_SCHEDULE_CREATE,
		Rpcs:   []string{"CreateSchedule"},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_SCHEDULE_READ,
		Rpcs:   []string{"ListSchedules", "DescribeSchedule"},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_SCHEDULE_DELETE,
		Rpcs:   []string{"DeleteSchedule"},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_SCHEDULE_PAUSE,
		Rpcs:   []string{"PauseSchedule"},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_SCHEDULE_RESUME,
		Rpcs:   []string{"ResumeSchedule"},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_SCHEDULE_TRIGGER,
		Parent: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN,
		Rpcs:   []string{"TriggerSchedule"},
	},
	{
		Action:   AuthorizationAction_AUTHORIZATION_ACTION_MCP_RUN_LOCAL,
		Parent:   AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN,
		McpTools: []string{"flowstate_run_local"},
	},
	{
		Action:   AuthorizationAction_AUTHORIZATION_ACTION_MCP_TEST,
		Parent:   AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN,
		McpTools: []string{"flowstate_test"},
	},
	{
		Action:   AuthorizationAction_AUTHORIZATION_ACTION_MCP_DEBUG,
		Parent:   AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN,
		McpTools: []string{"flowstate_debug"},
	},
}

// authorizationActionScopePrefix is what an enum value name carries in front
// of the scope it spells. See [AuthorizationActionScope].
const authorizationActionScopePrefix = "AUTHORIZATION_ACTION_"

// AuthorizationActionScope renders an action as the OAuth scope value that
// names it, by the rule the schema's own comment states: strip the enum's
// prefix, lowercase, and turn the first underscore into a dot.
//
// Derived rather than tabulated so that the scope a client requests and the
// action a policy names cannot become two spellings — the failure #567's D1
// exists to prevent. AUTHORIZATION_ACTION_UNSPECIFIED has no scope: it is the
// absence of an action, not an operation, and it answers "".
func AuthorizationActionScope(action AuthorizationAction) string {
	if action == AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED {
		return ""
	}

	name := strings.TrimPrefix(action.String(), authorizationActionScopePrefix)
	if name == action.String() {
		// An enum value whose name does not carry the prefix cannot be spelled
		// by this rule, and guessing would publish a scope nothing agrees on.
		return ""
	}

	return strings.ToLower(strings.Replace(name, "_", ".", 1))
}

// AuthorizationActionScopes is the whole vocabulary as scope values, in the
// schema's own order.
//
// This is what RFC 9728 protected-resource metadata publishes — see
// auth.WithScopesSupported, and pkg/flowstate/v1/auth/protectedresource.go for
// why the auth package is handed the list rather than reading it (that package
// sits below this one in the import graph).
func AuthorizationActionScopes() []string {
	values := AuthorizationAction(0).Descriptor().Values()

	scopes := make([]string, 0, values.Len())
	for i := range values.Len() {
		if scope := AuthorizationActionScope(AuthorizationAction(values.Get(i).Number())); scope != "" {
			scopes = append(scopes, scope)
		}
	}

	return scopes
}

// AuthorizationActionBindings returns a defensive copy of the bindings, so a
// caller ranging over the vocabulary cannot edit it.
func AuthorizationActionBindings() []*AuthorizationActionBinding {
	bindings := make([]*AuthorizationActionBinding, 0, len(authorizationActionBindings))
	for _, binding := range authorizationActionBindings {
		bindings = append(bindings, proto.CloneOf(binding))
	}

	return bindings
}

// AuthorizationActionForRPC answers which action authorizes one
// flowstate.v1.WorkflowService method.
//
// Fails closed: an RPC no binding names is an error rather than a permissive
// default, because the caller's next move is a decision about authority and
// "no action" is not an answer it can act on. The descriptor-walking test
// makes reaching this branch a build-time failure rather than a runtime one.
func AuthorizationActionForRPC(rpc string) (AuthorizationAction, error) {
	for _, binding := range authorizationActionBindings {
		if slices.Contains(binding.GetRpcs(), rpc) {
			return binding.GetAction(), nil
		}
	}

	return AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED,
		fmt.Errorf("no authorization action names the rpc %q; add it to a binding in "+
			"pkg/flowstate/v1/authorization.go, or add an action to "+
			"proto/flowstate/v1/authorization.proto when none of them fits", rpc)
}

// AuthorizationActionForMCPTool answers which action authorizes one MCP tool,
// whether the tool is an RPC's projection or one of the two that are not.
//
// The projection rule lives with the surface that owns it (mcp.ToolName), so
// what this takes is the RPC name a caller already resolved plus the tool name
// itself; see cmd/flow's TestEveryRegisteredMCPToolHasExactlyOneAuthorizationAction,
// which is where registration can be seen and therefore where the pairing is
// held honest.
func AuthorizationActionForMCPTool(tool string) (AuthorizationAction, error) {
	for _, binding := range authorizationActionBindings {
		if slices.Contains(binding.GetMcpTools(), tool) {
			return binding.GetAction(), nil
		}
	}

	return AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED,
		fmt.Errorf("no authorization action names the mcp tool %q", tool)
}
