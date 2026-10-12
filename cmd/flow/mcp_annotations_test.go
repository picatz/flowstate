package main

import (
	"slices"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
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

type toolHints struct{ readOnly, idempotent, destructive, openWorld bool }

func hintsOf(a *mcp.ToolAnnotations) toolHints {
	return toolHints{a.ReadOnlyHint, a.IdempotentHint, a.DestructiveHint != nil && *a.DestructiveHint, a.OpenWorldHint != nil && *a.OpenWorldHint}
}

// TestEveryRPCToolAnnotationsArePinned pins the hints of every RPC's tool over
// tools/list, so flipping any one declaration in service.proto fails here.
func TestEveryRPCToolAnnotationsArePinned(t *testing.T) {
	t.Parallel()

	r := toolHints{readOnly: true, idempotent: true}
	want := map[string]toolHints{
		"Run":                 {destructive: true, openWorld: true},
		"Get":                 r,
		"Signal":              {destructive: true, openWorld: true},
		"GetGate":             r,
		"ListGates":           r,
		"SignalWithStart":     {destructive: true, openWorld: true},
		"List":                r,
		"GetTimeline":         r,
		"GetCheckpoint":       r,
		"ResumeRun":           {destructive: true, openWorld: true},
		"Cancel":              {idempotent: true, destructive: true, openWorld: true},
		"Terminate":           {idempotent: true, destructive: true},
		"DebugAttach":         {idempotent: true, openWorld: true},
		"DebugGet":            r,
		"DebugHistory":        r,
		"DebugResume":         {destructive: true, openWorld: true},
		"DebugSetBreakpoints": {idempotent: true},
		"DebugInspect":        r,
		"Validate":            r,
		"Compile":             r,
		"GetCatalog":          r,
		"CreateSchedule":      {destructive: true, openWorld: true},
		"ListSchedules":       r,
		"DescribeSchedule":    r,
		"DeleteSchedule":      {idempotent: true, destructive: true},
		"PauseSchedule":       {idempotent: true},
		"ResumeSchedule":      {idempotent: true, destructive: true, openWorld: true},
		"TriggerSchedule":     {destructive: true, openWorld: true},
		"Whoami":              r,
	}
	require.Len(t, want, len(serviceMethodNames(t)), "the table must cover every RPC")

	byName := map[string]*mcp.ToolAnnotations{}
	for _, tool := range registeredTools(t) {
		byName[tool.Name] = tool.Annotations
	}
	for rpc, h := range want {
		a := byName[flowmcp.ToolName(rpc)]
		if assert.NotNil(t, a, "%s is not served", rpc) {
			assert.Equal(t, h, hintsOf(a), rpc)
		}
	}
}

// TestTaskDispatchingRPCsAreDestructiveAndOpenWorld holds the one rule the
// MethodRules comment states: a call that starts, releases or resumes task
// execution is destructive and open-world.
func TestTaskDispatchingRPCsAreDestructiveAndOpenWorld(t *testing.T) {
	t.Parallel()

	dispatching := []string{
		"Run", "SignalWithStart", "ResumeRun", "TriggerSchedule", "Cancel",
		"Signal", "DebugResume", "CreateSchedule", "ResumeSchedule",
	}
	for _, rpc := range dispatching {
		rules, ok := v1.MethodRulesForRPC(rpc)
		require.True(t, ok, rpc)
		assert.True(t, rules.GetDestructive() && rules.GetOpenWorld(), "%s dispatches task execution", rpc)
	}
}

// TestUndeclaredRPCIsAnnotatedCautiously: a method with no declared effect is
// presented as the worst case, so a missing declaration can only make a client ask more.
func TestUndeclaredRPCIsAnnotatedCautiously(t *testing.T) {
	t.Parallel()

	a := flowmcp.ToolAnnotationsForRPC("NotAnRPC")
	assert.Equal(t, toolHints{destructive: true, openWorld: true}, hintsOf(a))
}

// TestHandWrittenToolAnnotations pins the hints of the tools that are not RPCs.
func TestHandWrittenToolAnnotations(t *testing.T) {
	t.Parallel()

	byName := map[string]*mcp.ToolAnnotations{}
	for _, tool := range registeredTools(t) {
		byName[tool.Name] = tool.Annotations
	}

	want := map[string]toolHints{
		"flowstate_run_local":             {destructive: true, openWorld: true},
		"flowstate_debug_session_command": {destructive: true, openWorld: true},
		"flowstate_debug_session_observe": {readOnly: true, idempotent: true, openWorld: true},
		"flowstate_debug_session_start":   {},
		"flowstate_check_policy":          {readOnly: true, idempotent: true},
		"flowstate_test":                  {readOnly: true, idempotent: true},
		"flowstate_debug":                 {readOnly: true, idempotent: true},
		"flowstate_debug_session_attach":  {idempotent: true, openWorld: true},
		"flowstate_debug_session_end":     {idempotent: true, destructive: true, openWorld: true},
	}
	for name, h := range want {
		a := byName[name]
		if assert.NotNil(t, a, "%s is not served", name) {
			assert.Equal(t, h, hintsOf(a), name)
		}
	}
}
