package plugin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A plugin could say who evaluates an input and not what it has to be.
//
// `deferred_inputs` says the task evaluates this itself. `expression_inputs` says an
// author must write `${...}` rather than a literal, and the manifest had no way to
// say it — so a plugin's deferred input accepted `expect: {status: 200}`, compiled,
// passed `flow validate`, and failed inside the plugin on a workload somebody was
// running for real.
//
// That is the exact failure the engine's own ExpressionInputs was added to move back
// to the author's terminal, for the built-in http task. Every plugin was still in the
// position http had been in.

// TestAPluginCanRequireAnInputBeAnExpression is the claim, checked where it has an
// effect rather than where it is stored.
//
// Through MustBeExpression, which is what the validator asks. A test asserting the
// TaskDef carries the field would pass on a field nothing reads — and a declaration
// nothing reads is the thing this is fixing.
func TestAPluginCanRequireAnInputBeAnExpression(t *testing.T) {
	t.Parallel()

	const task = "example_check"

	def, err := (&Plugin{name: "example"}).taskDef(&pluginv1.TaskManifest{
		Name:             task,
		Summary:          "checks something",
		InputMessage:     "flowstate.v1.Task.Log.Inputs",
		OutputMessage:    "flowstate.v1.Task.Log.Outputs",
		DeferredInputs:   []string{"expect"},
		ExpressionInputs: []string{"expect"},
		NeedsScope:       true,
	}, Config{})
	require.NoError(t, err)

	require.Equal(t, []string{"expect"}, def.ExpressionInputs,
		"the manifest declared it and the task definition does not carry it")

	// Registered into the *default* registry, because that is the one
	// MustBeExpression asks — and this is where the test stops proving as much as
	// it looks like it does, which review caught and which is worth writing down
	// rather than quietly relying on.
	//
	// `Host.Register` writes to whichever registry it is handed, and the host's own
	// documented usage hands it a fresh one. So a deployment following the host API
	// gets a registry the validator never reads, and this declaration reaches
	// nothing. Nor is that specific to expression inputs: `flow validate` on a
	// plugin's task answers `unknown task`, because no binary in this repository
	// connects the host to the registry the validator uses.
	//
	// What this test therefore proves is the mapping and the lookup — the manifest
	// reaches TaskDef, and a registry holding that TaskDef answers correctly. It
	// does not prove `flow validate` enforces it, because today it does not.
	//
	// Not restored afterwards, because a registry has no way to remove a task. Safe
	// here for a stated reason rather than an assumed one: the name is a plugin's,
	// nothing in this package enumerates the registry, and a Go test binary is one
	// process per package.
	require.NoError(t, flowstatev1.DefaultRegistry().Register(def))

	assert.True(t, flowstatev1.MustBeExpression(task, "expect"),
		"a plugin declared `expect` must be an expression and a registry holding it says otherwise")

	// The negative direction, so this is not simply answering true.
	assert.False(t, flowstatev1.MustBeExpression(task, "message"),
		"an input the plugin said nothing about is being required to be an expression")
}

// TestAPluginThatSaysNothingRequiresNothing is the other direction, and the one that
// would make this a breaking change if it were wrong.
//
// Every plugin written before this field existed omits it, and omitting it has to go
// on meaning "any value is fine". A default that required expressions would refuse
// workloads that run today.
func TestAPluginThatSaysNothingRequiresNothing(t *testing.T) {
	t.Parallel()

	def, err := (&Plugin{name: "example"}).taskDef(&pluginv1.TaskManifest{
		Name:           "example_quiet",
		Summary:        "says nothing about its inputs",
		InputMessage:   "flowstate.v1.Task.Log.Inputs",
		OutputMessage:  "flowstate.v1.Task.Log.Outputs",
		DeferredInputs: []string{"message"},
	}, Config{})
	require.NoError(t, err)

	assert.Empty(t, def.ExpressionInputs,
		"a manifest that declared nothing produced a requirement, so existing plugins would break")
}
