package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Nothing a matrix or a claim flag holds is written by an error about it, on
// any stream: the matrix's decode errors used to print the annotated source
// line, and a malformed claim flag its own entry.
func TestSignalsCheckErrorsNeverEchoWhatWasTyped(t *testing.T) {
	t.Parallel()

	const secret = "SECRET-typed-value-77"

	matrices := map[string]string{
		"an unknown key beside a claim": "identities:\n  - name: a\n    expct: admitted\n    principal: {claims: {team: " + secret + "}}\n",
		"a type error":                  "identities:\n  - name: a\n    principal:\n      subject: [" + secret + "]\n      claims: {team: " + secret + "}\n",
		"a bad outcome":                 "identities:\n  - name: a\n    inputs: {pin: " + secret + "}\n    expect: " + secret + "\n",
		"an alias":                      "x: &c {team: " + secret + "}\nidentities:\n  - name: a\n    principal: {claims: *c}\n",
	}

	for name, doc := range matrices {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			res := checkGate(t, "--matrix", writeFile(t, "m.yaml", doc))
			require.Equal(t, exitCodeFailure, res.ExitCode, res.Output())
			require.NotContains(t, res.Stdout+res.Stderr, secret)
			require.False(t, strings.Contains(res.Output(), "claims: {"), "the source line was echoed:\n%s", res.Output())
		})
	}

	for _, flag := range []string{"--signal-as-claim", "--starter-claim"} {
		res := checkGate(t, flag, secret)
		require.Equal(t, exitCodeUsage, res.ExitCode, res.Output())
		require.Contains(t, res.Output(), "want NAME=VALUE")
		require.NotContains(t, res.Stdout+res.Stderr, secret, "%s echoed its entry", flag)

		res = checkGate(t, flag, "team=")
		require.Equal(t, exitCodeUsage, res.ExitCode, res.Output())
		require.NotContains(t, res.Output(), "team=")
	}
}
