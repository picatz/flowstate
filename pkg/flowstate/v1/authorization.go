package flowstatev1

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// authorizationActionBindings attaches every action in the schema's closed
// vocabulary to the operations it covers.
//
// The RPCs each action covers are not written here: they are read from the
// (flowstate.v1.authorization_action) option on each WorkflowService method
// (see [boundActions]), so the binding sits on the RPC and cannot drift from it.
// What this list still records is everything the descriptor cannot say — the
// escalation lineage, and the MCP-only tools, HTTP endpoints, and request
// fields an action covers. TestEveryRPCHasExactlyOneAuthorizationAction
// still fails for a method that carries no option.
//
// The order is the enum's, and [AuthorizationActionScopes] publishes it, so a
// reader comparing the metadata document to this file sees the same sequence.
var authorizationActionBindings = []*AuthorizationActionBinding{
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN,
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_READ,
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_SIGNAL,
		Parent: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN,
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_CANCEL,
		Parent: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN,
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_TERMINATE,
		Parent: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_CANCEL,
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_VALIDATE,
		// A static policy check compiles the caller's own source and decides
		// its predicates: it runs no step and reads nothing the caller did
		// not submit, so it asks no more than validating that source does.
		McpTools: []string{"flowstate_check_policy"},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_COMPILE,
		Parent: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_VALIDATE,
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_CATALOG_READ,
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_SCHEDULE_CREATE,
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_SCHEDULE_READ,
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_SCHEDULE_DELETE,
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_SCHEDULE_PAUSE,
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_SCHEDULE_RESUME,
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_SCHEDULE_TRIGGER,
		Parent: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN,
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
		Action: AuthorizationAction_AUTHORIZATION_ACTION_MCP_DEBUG,
		Parent: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_RUN,
		McpTools: []string{
			"flowstate_debug",
			// The retained sessions over the same stubbed run, and over a
			// durable run the server's own debug RPCs authorize.
			"flowstate_debug_session_start",
			"flowstate_debug_session_attach",
			"flowstate_debug_session_observe",
			"flowstate_debug_session_command",
			"flowstate_debug_session_end",
		},
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_DEBUG,
		Parent: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_SIGNAL,
	},
	{
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_DEBUG_INSPECT,
		Parent: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_DEBUG,
		// DebugHistory keeps workload.debug; asking it to evaluate expressions
		// at a recorded point additionally needs this.
		RequestFields: []string{"flowstate.v1.DebugHistoryRequest.inspections"},
	},
	{
		// The codec server's two routes, whose suffixes Temporal's remote
		// codec protocol fixes (go.temporal.io/sdk@v1.48.0 converter/codec.go).
		// Neither has a parent: reading a run through Get and reading its raw
		// history through a decoder are different disclosures to different
		// audiences, and neither implies the other.
		Action:        AuthorizationAction_AUTHORIZATION_ACTION_PAYLOAD_DECODE,
		HttpEndpoints: []string{"/decode"},
	},
	{
		Action:        AuthorizationAction_AUTHORIZATION_ACTION_PAYLOAD_ENCODE,
		HttpEndpoints: []string{"/encode"},
	},
	{
		// A modifier on two reads rather than an operation of its own: the
		// RPCs keep workload.read, and asking for declared-sensitive values in
		// the clear additionally needs this. Its parent is the read it widens.
		Action: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_REVEAL_SENSITIVE,
		Parent: AuthorizationAction_AUTHORIZATION_ACTION_WORKLOAD_READ,
		RequestFields: []string{
			"flowstate.v1.GetRequest.reveal_sensitive",
			"flowstate.v1.GetTimelineRequest.reveal_sensitive",
		},
	},
}

// boundActions fills each binding's rpcs from the authorization_action option
// on the WorkflowService methods, in the schema's declaration order, once. A
// method with no option is left in no binding, which [AuthorizationActionForRPC]
// then refuses and the descriptor-walking test reports. It is lazy rather than
// an init function because this package's generated descriptors initialize in
// file order, and service.proto's comes after this file's.
var boundActions = sync.OnceValue(func() []*AuthorizationActionBinding {
	methods := File_flowstate_v1_service_proto.Services().ByName("WorkflowService").Methods()
	for i := range methods.Len() {
		method := methods.Get(i)
		options, _ := method.Options().(*descriptorpb.MethodOptions)
		if !proto.HasExtension(options, E_AuthorizationAction) {
			continue
		}
		action, _ := proto.GetExtension(options, E_AuthorizationAction).(AuthorizationAction)
		for _, binding := range authorizationActionBindings {
			if binding.GetAction() == action {
				binding.Rpcs = append(binding.Rpcs, string(method.Name()))
			}
		}
	}

	return authorizationActionBindings
})

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
	bindings := make([]*AuthorizationActionBinding, 0, len(boundActions()))
	for _, binding := range boundActions() {
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
	for _, binding := range boundActions() {
		if slices.Contains(binding.GetRpcs(), rpc) {
			return binding.GetAction(), nil
		}
	}

	return AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED,
		fmt.Errorf("no authorization action names the rpc %q; set the method's "+
			"(flowstate.v1.authorization_action) option in proto/flowstate/v1/service.proto, "+
			"or add an action to proto/flowstate/v1/authorization.proto when none of them fits", rpc)
}

// MCPToolPrefix namespaces Flowstate tools when a client aggregates MCP
// servers. Registration and authorization projection share this spelling.
const MCPToolPrefix = "flowstate_"

// MCPToolNameForRPC projects one WorkflowService method name onto the MCP tool
// name that serves it. It is the one projection used by registration,
// authorization lookup, audit mapping, and their conformance tests.
func MCPToolNameForRPC(rpc string) string {
	var b strings.Builder
	b.WriteString(MCPToolPrefix)
	for i, r := range rpc {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			r += 'a' - 'A'
		}
		b.WriteRune(r)
	}

	return b.String()
}

// AuthorizationActionForMCPTool answers which action authorizes one MCP tool,
// whether the tool is an RPC's projection or one that exists only on MCP.
//
// RPC projections are derived through [MCPToolNameForRPC] rather than copied
// into AuthorizationActionBinding.mcp_tools. The latter remains only the
// explicit list of tools no RPC projects, while callers get one total lookup
// that cannot accidentally audit a tool under a different action than the one
// authorization assigns it.
func AuthorizationActionForMCPTool(tool string) (AuthorizationAction, error) {
	for _, binding := range boundActions() {
		if slices.Contains(binding.GetMcpTools(), tool) {
			return binding.GetAction(), nil
		}
		for _, rpc := range binding.GetRpcs() {
			if MCPToolNameForRPC(rpc) == tool {
				return binding.GetAction(), nil
			}
		}
	}

	return AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED,
		fmt.Errorf("no authorization action names the mcp tool %q", tool)
}

// AuthorizationActionForHTTPEndpoint maps an HTTP endpoint outside the RPC
// service, by its bound path suffix, to the action it requires. Unknown
// endpoints fail closed.
func AuthorizationActionForHTTPEndpoint(endpoint string) (AuthorizationAction, error) {
	for _, binding := range boundActions() {
		if slices.Contains(binding.GetHttpEndpoints(), endpoint) {
			return binding.GetAction(), nil
		}
	}

	return AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED,
		fmt.Errorf("no authorization action names the http endpoint %q", endpoint)
}

// AuthorizationActionForRequestField maps a request field that widens an RPC,
// by its full name, to the action it additionally requires. Unknown fields
// fail closed.
func AuthorizationActionForRequestField(field string) (AuthorizationAction, error) {
	for _, binding := range boundActions() {
		if slices.Contains(binding.GetRequestFields(), field) {
			return binding.GetAction(), nil
		}
	}

	return AuthorizationAction_AUTHORIZATION_ACTION_UNSPECIFIED,
		fmt.Errorf("no authorization action names the request field %q", field)
}
