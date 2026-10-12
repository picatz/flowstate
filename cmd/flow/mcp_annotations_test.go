package main

import (
	"slices"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// rpcsWithoutEffect names the service methods that declare no effect in their
// method_rules option, sorted.
func rpcsWithoutEffect(rpcs []string) []string {
	var missing []string
	for _, rpc := range rpcs {
		rules, ok := v1.MethodRulesForRPC(rpc)
		if !ok || rules.GetEffect() == v1.MethodEffect_METHOD_EFFECT_UNSPECIFIED {
			missing = append(missing, rpc)
		}
	}
	slices.Sort(missing)

	return missing
}

// TestEveryRPCDeclaresAnEffect is the sibling of TestToolsMatchTheServiceDescriptor
// for what a tool does to the world: every RPC the service declares says so in
// its method_rules option, so a client's hint is never a default.
func TestEveryRPCDeclaresAnEffect(t *testing.T) {
	t.Parallel()

	var rpcs []string
	for name := range serviceMethodNames(t) {
		rpcs = append(rpcs, name)
	}
	require.NotEmpty(t, rpcs, "the service declares no methods; the lookup is broken")

	assert.Empty(t, rpcsWithoutEffect(rpcs),
		"these rpcs declare no `option (flowstate.v1.method_rules) = {effect: ...}` in service.proto")

	// The check can fail: an RPC the schema does not declare has no effect.
	assert.Equal(t, []string{"NotAnRPC"}, rpcsWithoutEffect(append(slices.Clone(rpcs), "NotAnRPC")))
}

// TestEveryToolAdvertisesAnnotations holds every tool a client lists, RPC or
// hand-written, to a complete set of hints.
func TestEveryToolAdvertisesAnnotations(t *testing.T) {
	t.Parallel()

	tools := registeredTools(t)
	require.NotEmpty(t, tools)
	for _, tool := range tools {
		if assert.NotNil(t, tool.Annotations, "%s advertises no annotations", tool.Name) {
			assert.NotNil(t, tool.Annotations.DestructiveHint, "%s leaves destructiveHint to its default of true", tool.Name)
			assert.NotNil(t, tool.Annotations.OpenWorldHint, "%s leaves openWorldHint to its default of true", tool.Name)
		}
	}
}

// TestToolAnnotationsFollowTheDeclaredEffect pins the hints a client reads for
// the cases the issue names, over tools/list.
func TestToolAnnotationsFollowTheDeclaredEffect(t *testing.T) {
	t.Parallel()

	byName := map[string]*mcp.ToolAnnotations{}
	for _, tool := range registeredTools(t) {
		byName[tool.Name] = tool.Annotations
	}

	type hints struct{ readOnly, idempotent, destructive, openWorld bool }
	want := map[string]hints{
		"flowstate_validate":              {readOnly: true, idempotent: true},
		"flowstate_get":                   {readOnly: true, idempotent: true},
		"flowstate_terminate":             {idempotent: true, destructive: true},
		"flowstate_delete_schedule":       {idempotent: true, destructive: true},
		"flowstate_pause_schedule":        {idempotent: true},
		"flowstate_signal":                {},
		"flowstate_run":                   {openWorld: true},
		"flowstate_run_local":             {destructive: true, openWorld: true},
		"flowstate_debug_session_command": {destructive: true, openWorld: true},
		"flowstate_debug_session_observe": {readOnly: true, idempotent: true, openWorld: true},
		"flowstate_debug_session_start":   {},
		"flowstate_check_policy":          {readOnly: true, idempotent: true},
		"flowstate_test":                  {readOnly: true, idempotent: true},
	}
	for name, h := range want {
		a := byName[name]
		if !assert.NotNil(t, a, "%s is not served", name) {
			continue
		}
		got := hints{a.ReadOnlyHint, a.IdempotentHint, a.DestructiveHint != nil && *a.DestructiveHint, a.OpenWorldHint != nil && *a.OpenWorldHint}
		assert.Equal(t, h, got, name)
	}
}
