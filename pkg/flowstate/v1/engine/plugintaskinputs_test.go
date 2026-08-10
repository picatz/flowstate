package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// TestPluginTaskInputsDurable is the second of #436's two driver callers: the
// identical [tests.PluginTaskInputCases], run through worker registration and a
// Temporal test environment instead of a context value.
//
// The pairing is the point. An input evaluated a step too early, a deferred
// input the engine resolved anyway, or a secret reference turned into a value
// before dispatch would each still produce a run that works, and a secret
// resolved early is a secret in workflow history. Nothing but the two drivers
// answering the same question can see that they answered it differently.
// TestPluginTaskInputsLocal in pkg/flowstate/v1/plugintaskinputs_local_test.go
// is the first caller.
func TestPluginTaskInputsDurable(t *testing.T) {
	require.NoError(t, v1.DefaultRegistry().Register(tests.PluginTaskInputsTaskDef()))

	for _, test := range tests.PluginTaskInputCases() {
		t.Run(test.Name, func(t *testing.T) {
			runAuthorityCase(t, test)
		})
	}
}
