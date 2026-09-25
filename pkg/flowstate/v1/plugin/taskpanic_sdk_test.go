package plugin

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

const panicFixtureTask = "panic_once"

// runPanicPlugin is a real SDK plugin whose first task call panics after
// receiving its input and whose next call succeeds. It is intentionally a
// subprocess fixture: only the real HTTP boundary can prove net/http did not
// turn the panic into a connection loss and that the same process kept serving.
func runPanicPlugin() int {
	var calls atomic.Int32
	err := sdk.Run(context.Background(), sdk.Plugin{
		Name:        "panic",
		Version:     "0.0.1",
		Description: "a fixture plugin that panics once inside a task",
		Tasks: []sdk.Task{{
			Name:         panicFixtureTask,
			Input:        &flowstatev1.Task_Log_Inputs{},
			Output:       &flowstatev1.Task_Log_Outputs{},
			SecretInputs: []string{"message"},
			Fn: func(_ context.Context, inputs map[string]*flowstatev1.Value, _ *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
				if calls.Add(1) == 1 {
					panic("task side effect may have happened for " + inputs["message"].GetLiteral().GetStringValue())
				}
				return &flowstatev1.Node_Outputs{}, nil
			},
		}},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "panic fixture: %v\n", err)
		return 1
	}
	return 0
}

func TestSDKTaskPanicIsPermanentAndProcessSurvives(t *testing.T) {
	t.Parallel()

	// Longer than a tempting SDK-side diagnostic bound. Truncating this before
	// the host sees it would leave a prefix the scrubber cannot match to the
	// complete delivered secret.
	material := "host-secret-in-plugin-panic-" + strings.Repeat("x", 1500) + "-end"
	secretPrefix := material[:64]
	var logged capturedLogs
	cfg := testConfig(t, pluginDir(t, "panic"))
	cfg.Logger = newCapturingLogger(t, &logged)
	host := openHost(t, cfg)

	pluginProcess, ok := host.Lookup("panic")
	require.True(t, ok)
	pid := pluginProcess.PID()
	require.NotZero(t, pid)

	defs := host.TaskDefs()
	require.Len(t, defs, 1)
	require.Equal(t, "panic."+panicFixtureTask, defs[0].Name)

	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), hostSecretRuntime(t, "TOKEN", material))
	inputs := map[string]*flowstatev1.Value{
		"message": {Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{
			Scheme: "env", Name: "TOKEN",
		}}},
	}

	_, err := defs[0].Fn(ctx, inputs, nil)
	require.Error(t, err)
	var taskErr *flowstatev1.TaskError
	require.ErrorAs(t, err, &taskErr)
	assert.Equal(t, flowstatev1.ErrorKindUpstreamUnknown, taskErr.Kind)
	assert.False(t, taskErr.Retryable(), "an unknown side-effect outcome must not be retried")
	assert.NotContains(t, taskErr.Error(), material)
	assert.NotContains(t, taskErr.Error(), "side effect may have happened")

	require.True(t, waitFor(t, time.Second, func() bool {
		return strings.Contains(logged.String(), "plugin task panicked; outcome unknown")
	}), "the SDK panic diagnostic was not relayed from stderr")

	_, err = defs[0].Fn(ctx, inputs, nil)
	require.NoError(t, err, "the call after the panic did not succeed")
	assert.Equal(t, pid, pluginProcess.PID(), "the host replaced the plugin process after a recovered task panic")
	assert.True(t, processAlive(pid), "the plugin process died after a recovered task panic")

	output := logged.String()
	assert.Equal(t, 1, strings.Count(output, "plugin task panicked; outcome unknown"))
	assert.NotContains(t, output, "http: panic serving")
	assert.NotContains(t, output, material)
	assert.NotContains(t, output, secretPrefix)
	assert.Contains(t, output, secrets.Redacted)
	assert.Contains(t, output, metricschema.PluginName+"=panic")
	assert.Contains(t, output, metricschema.TaskName+"="+panicFixtureTask)
	assert.Contains(t, output, "stack=")
}
