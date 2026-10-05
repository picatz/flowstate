package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A check interrupted before it answers is an error with exit status 1, not a
// `refused` that would satisfy --expect refused.
func TestSignalsCheckInterruptedIsNotARefusal(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	args := []string{"signals", "check", gateWorkflow, "--input-file", gateInputs, "--expect", "refused"}

	res := runFlowUnder(t, ctx, args...)
	require.Equal(t, exitCodeFailure, res.ExitCode, res.Output())
	require.NotContains(t, res.Stdout, "refused")

	matrix := writeFile(t, "m.yaml", "identities:\n  - name: nobody\n    starter: {}\n")
	res = runFlowUnder(t, ctx, append(args, "--matrix", matrix)...)
	require.Equal(t, exitCodeFailure, res.ExitCode, res.Output())
	require.NotContains(t, res.Stdout, "refused")
}
