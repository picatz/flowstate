package flowstatev1

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
)

// AuthorizationRootNames is the complete authorization CEL activation. Keep
// this list closed: boundary-specific data must first be normalized into one
// of these typed protobuf messages.
var AuthorizationRootNames = []string{"principal", "action", "resource", "context"}

// ActionDefinition is one entry in the central authorization vocabulary.
// Every surface mapping, audit name, CEL action value, and generated policy
// reference reads this registry rather than maintaining another string list.
type ActionDefinition struct {
	Name, Group, Parent, ResourceType                                     string
	RPCs, MCPTools, PluginOperations, LSPCapabilities, InternalOperations []string
}

var actionRegistry = []ActionDefinition{
	{Name: "workload.run", Group: "workload", ResourceType: "workload", RPCs: []string{"Run", "SignalWithStart"}},
	{Name: "workload.read", Group: "workload", ResourceType: "run", RPCs: []string{"Get", "List"}},
	{Name: "workload.signal", Group: "workload", Parent: "workload.run", ResourceType: "run", RPCs: []string{"Signal"}},
	{Name: "workload.cancel", Group: "workload", Parent: "workload.run", ResourceType: "run", RPCs: []string{"Cancel"}},
	{Name: "workload.terminate", Group: "workload", Parent: "workload.cancel", ResourceType: "run", RPCs: []string{"Terminate"}},
	{Name: "workload.validate", Group: "workload", ResourceType: "flowfile", RPCs: []string{"Validate"}},
	{Name: "workload.compile", Group: "workload", Parent: "workload.validate", ResourceType: "flowfile", RPCs: []string{"Compile"}},
	{Name: "catalog.read", Group: "catalog", ResourceType: "catalog", RPCs: []string{"GetCatalog"}},
	{Name: "schedule.create", Group: "schedule", ResourceType: "schedule", RPCs: []string{"CreateSchedule"}},
	{Name: "schedule.read", Group: "schedule", ResourceType: "schedule", RPCs: []string{"ListSchedules", "DescribeSchedule"}},
	{Name: "schedule.delete", Group: "schedule", ResourceType: "schedule", RPCs: []string{"DeleteSchedule"}},
	{Name: "schedule.pause", Group: "schedule", ResourceType: "schedule", RPCs: []string{"PauseSchedule"}},
	{Name: "schedule.resume", Group: "schedule", ResourceType: "schedule", RPCs: []string{"ResumeSchedule"}},
	{Name: "schedule.trigger", Group: "schedule", Parent: "workload.run", ResourceType: "schedule", RPCs: []string{"TriggerSchedule"}},
	{Name: "mcp.run_local", Group: "mcp", Parent: "workload.run", ResourceType: "flowfile", MCPTools: []string{"flowstate_run_local"}},
	{Name: "mcp.test", Group: "mcp", Parent: "workload.run", ResourceType: "flowfile", MCPTools: []string{"flowstate_test"}},
	{Name: "plugin.execute", Group: "plugin", ResourceType: "plugin_operation", PluginOperations: []string{"execute", "check", "describe"}},
	{Name: "lsp.read", Group: "lsp", ResourceType: "document", LSPCapabilities: []string{"initialize", "textDocument/hover", "textDocument/completion", "textDocument/definition", "textDocument/documentSymbol"}},
	{Name: "lsp.validate", Group: "lsp", Parent: "workload.validate", ResourceType: "document", LSPCapabilities: []string{"textDocument/didOpen", "textDocument/didChange", "textDocument/didSave"}},
	{Name: "credential.assume", Group: "credential", ResourceType: "credential_target", InternalOperations: []string{"credential.assume"}},
	{Name: "secret.read", Group: "secret", ResourceType: "secret", InternalOperations: []string{"secret.read"}},
	{Name: "egress.connect", Group: "egress", ResourceType: "network_endpoint", InternalOperations: []string{"egress.connect"}},
	{Name: "compute.execute", Group: "compute", ResourceType: "compute", InternalOperations: []string{"task.dispatch"}},
	{Name: "storage.read", Group: "storage", ResourceType: "storage_object", InternalOperations: []string{"storage.read"}},
	{Name: "storage.write", Group: "storage", ResourceType: "storage_object", InternalOperations: []string{"storage.write"}},
	{Name: "federation.call", Group: "federation", ResourceType: "flowstate_peer", InternalOperations: []string{"federation.call"}},
}

// ActionRegistry returns a defensive copy of the canonical registry.
func ActionRegistry() []ActionDefinition { return slices.Clone(actionRegistry) }

// LookupAction returns the registered action named name.
func LookupAction(name string) (ActionDefinition, bool) {
	for _, action := range actionRegistry {
		if action.Name == name {
			return action, true
		}
	}
	return ActionDefinition{}, false
}

// ActionForSurface maps one boundary operation to exactly one action.
func ActionForSurface(surface, operation string) (ActionDefinition, error) {
	for _, action := range actionRegistry {
		var names []string
		switch surface {
		case "rpc":
			names = action.RPCs
		case "mcp":
			names = action.MCPTools
		case "plugin":
			names = action.PluginOperations
		case "lsp":
			names = action.LSPCapabilities
		case "internal":
			names = action.InternalOperations
		default:
			return ActionDefinition{}, fmt.Errorf("unknown authorization surface %q", surface)
		}
		if slices.Contains(names, operation) {
			return action, nil
		}
		// Ordinary MCP tools are descriptor-derived RPC mirrors. Derive their
		// scope here too instead of copying all RPC names into a second list.
		if surface == "mcp" && strings.HasPrefix(operation, "flowstate_") {
			for _, rpc := range action.RPCs {
				if operation == "flowstate_"+camelToSnake(rpc) {
					return action, nil
				}
			}
		}
	}
	return ActionDefinition{}, fmt.Errorf("no authorization action for %s operation %q", surface, operation)
}

func camelToSnake(name string) string {
	var b strings.Builder
	for i, r := range name {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}
	return strings.ToLower(b.String())
}

// AuthorizationActionFor returns the wire value used by CEL and audit output.
func AuthorizationActionFor(surface, operation string) (*AuthorizationAction, error) {
	entry, err := ActionForSurface(surface, operation)
	if err != nil {
		return nil, err
	}
	return &AuthorizationAction{Name: entry.Name, Group: entry.Group, Parent: entry.Parent}, nil
}

// AuthorizationPolicyReference renders registry-derived policy documentation.
func AuthorizationPolicyReference() string {
	var b strings.Builder
	for _, action := range actionRegistry {
		fmt.Fprintf(&b, "- `%s` (%s; resource `%s`)\n", action.Name, action.Group, action.ResourceType)
	}
	return b.String()
}

var authorizationEnv = sync.OnceValues(func() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Types(&AuthorizationPrincipal{}, &AuthorizationAction{}, &AuthorizationResource{}, &AuthorizationContext{}),
		cel.Variable("principal", cel.ObjectType("flowstate.v1.AuthorizationPrincipal")),
		cel.Variable("action", cel.ObjectType("flowstate.v1.AuthorizationAction")),
		cel.Variable("resource", cel.ObjectType("flowstate.v1.AuthorizationResource")),
		cel.Variable("context", cel.ObjectType("flowstate.v1.AuthorizationContext")),
	)
})

// CompileAuthorizationPolicy type-checks a boolean rule against exactly the
// four typed roots. Requests are checked against the registry before evaluation.
func CompileAuthorizationPolicy(expression string) (*cel.Env, *cel.Ast, error) {
	env, err := authorizationEnv()
	if err != nil {
		return nil, nil, err
	}
	ast, issues := env.Compile(expression)
	if issues.Err() != nil {
		return nil, nil, issues.Err()
	}
	if !ast.OutputType().IsExactType(cel.BoolType) {
		return nil, nil, fmt.Errorf("authorization policy evaluates to %s, want bool", ast.OutputType())
	}
	return env, ast, nil
}

// EvaluateAuthorizationPolicy uses the repository's single cost-bounded CEL
// evaluator. Evaluation errors are denials, never permission.
func EvaluateAuthorizationPolicy(ctx context.Context, expression string, request *AuthorizationRequest) (*AuthorizationDecision, error) {
	if request == nil || request.Principal == nil || request.Action == nil || request.Resource == nil || request.Context == nil {
		return &AuthorizationDecision{Reason: "incomplete authorization request"}, nil
	}
	entry, ok := LookupAction(request.Action.Name)
	if !ok || entry.ResourceType != request.Resource.Type {
		return &AuthorizationDecision{Action: request.Action.Name, Resource: request.Resource.Id, Reason: "unregistered action or resource type"}, nil
	}
	env, ast, err := CompileAuthorizationPolicy(expression)
	if err != nil {
		return nil, err
	}
	activation := map[string]any{"principal": request.Principal, "action": request.Action, "resource": request.Resource, "context": request.Context}
	value, err := DefaultEvaluator().Eval(ctx, env, ast, activation)
	if err != nil {
		return &AuthorizationDecision{Action: request.Action.Name, Resource: request.Resource.Id, Reason: "policy evaluation failed"}, nil
	}
	allowed, ok := value.Value().(bool)
	if !ok {
		return nil, fmt.Errorf("authorization policy returned %T", value.Value())
	}
	return &AuthorizationDecision{Allowed: allowed, Action: request.Action.Name, Resource: request.Resource.Id}, nil
}
