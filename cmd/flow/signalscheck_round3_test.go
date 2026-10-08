package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A row that asserts one gate still takes --expect for the rest: naming `debug`
// must not silently assert nothing about the signal.
func TestSignalsCheckMatrixByGateRowStillTakesTheDefaultExpectation(t *testing.T) {
	t.Parallel()

	row := func(name string) string {
		return "identities:\n  - name: " + name + "\n" +
			"    principal:\n      subject: sre-lead@example.com\n      issuer: " + gateIssuer + "\n" +
			"      claims: {team: release-managers}\n" +
			"    starter: {principal: {subject: dev@example.com, issuer: " + gateIssuer + "}}\n" +
			"    expect_by_gate: {debug: refused}\n"
	}

	path := writeFile(t, "m.yaml", row("lead"))
	args := []string{"--signal", "deploy-approved", "--debug", "--matrix", path}

	// The signal is admitted for this row, so a default of refused is a mismatch
	// on the gate the row did not name.
	res := checkGate(t, append(args, "--expect", "refused")...)
	require.Equal(t, exitCodeFailure, res.ExitCode, res.Output())
	require.Contains(t, res.Stdout, "admitted (expected refused)")

	// The gate it did name keeps its own expectation: debug is refused, as the
	// row says, whatever the default is, and the other gate takes the default.
	res = checkGate(t, append(args, "--expect", "admitted")...)
	require.Zero(t, res.ExitCode, res.Output())
	require.NotContains(t, res.Stdout, "(expected")

	require.Zero(t, checkGate(t, args...).ExitCode, "with no default the row asserts only what it names")
}
