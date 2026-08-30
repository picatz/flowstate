package temporaltest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLauncherArgsIgnoreOrdinaryTestInvocation(t *testing.T) {
	_, _, _, handled, err := launcherArgs([]string{"-test.run=TestSomething"})
	require.NoError(t, err)
	require.False(t, handled)
}

func TestLauncherArgsRequireCompleteOwnedInvocation(t *testing.T) {
	_, _, _, handled, err := launcherArgs([]string{
		"server", "start-dev", "--ip", "127.0.0.1", "--port", "7233", "--namespace", "default",
	})
	require.ErrorContains(t, err, "incomplete Temporal supervisor arguments")
	require.True(t, handled)
}

func TestLauncherArgsRecoverParentAndServerIdentity(t *testing.T) {
	parentPID, hostPort, namespace, handled, err := launcherArgs([]string{
		"server", "start-dev",
		"--ip", "127.0.0.1",
		"--port", "8123",
		"--namespace", "isolated",
		"--headless",
		parentPIDFlag, "42",
	})
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, 42, parentPID)
	require.Equal(t, "127.0.0.1:8123", hostPort)
	require.Equal(t, "isolated", namespace)
}
