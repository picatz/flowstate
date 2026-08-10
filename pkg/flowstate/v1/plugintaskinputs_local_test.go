package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// TestPluginTaskInputsLocal is one of the two driver callers #436 asks for:
// what a plugin task is handed, asserted against the local driver.
//
// It became a question worth asking of this driver only when `flow run local`
// gained plugins. Before that the local driver could not dispatch a plugin task
// at all, so every claim about deferred inputs and secret-reference containment
// at a plugin task's boundary rested on the durable driver alone.
// TestPluginTaskInputsDurable in engine/plugintaskinputs_test.go is the other
// caller, running the identical [tests.PluginTaskInputCases] through
// [runAuthorityCase]'s durable twin.
//
// The fixture goes into [v1.DefaultRegistry] and not onto a private registry,
// unlike runPluginIdentityLocal beside it, and the difference is load-bearing
// rather than stylistic. [v1.ResolvableInputs] is the function that decides
// which inputs the engine evaluates and which the task does, and it is a
// package-level function over the default registry alone: a fixture on a
// context-scoped registry is dispatched correctly and then partitioned as
// though it declared nothing, so its deferred input is evaluated and the case
// fails for a reason that has nothing to do with either driver. A real plugin
// is never in that position, because plugin.Host.Register registers into the
// default registry and says in its own doc that anything else "holds tasks
// nothing will ever look up".
func TestPluginTaskInputsLocal(t *testing.T) {
	require.NoError(t, v1.DefaultRegistry().Register(tests.PluginTaskInputsTaskDef()))

	for _, test := range tests.PluginTaskInputCases() {
		t.Run(test.Name, func(t *testing.T) {
			runAuthorityCase(t, test)
		})
	}
}
