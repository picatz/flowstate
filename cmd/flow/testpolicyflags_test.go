package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// #652 item 2, made executable.
//
// The decision — `flow test` does not grow deployment-policy flags, and a case
// that wants to exercise an identity-dependent policy denial writes it against
// the policy's own package instead — was taken on #652 and written down in
// `flowtest`'s package doc, which #877 landed. Nothing enforced it. A prose
// decision beside a flag registration is the same value written down twice, and
// CLAUDE.md is explicit about which of the two drifts: `addTaskPolicyFlag(testCmd)`
// is one line, reads like an obvious convenience, and would silently reverse a
// design decision three files away from where it is recorded.
//
// The reason the decision is worth a mechanism at all is that reversing it is not
// merely inconsistent, it changes what a *case file* means: with the flag, a
// suite's verdict depends on which policy file happened to be passed on that
// invocation — deployment configuration the workflow file does not carry and the
// case's author may not control. That is CLAUDE.md's "report what is a property of
// the file, and stay silent about what a deployment decides", applied one level up
// from a diagnostic to a test verdict.
//
// This is the negative direction CLAUDE.md asks for: not "the verbs that should
// take a policy flag do", which passes for a tree where every verb takes one, but
// "`flow test` cannot reach one".
func TestFlowTestTakesNoDeploymentPolicyFlags(t *testing.T) {
	root := newRootCommand()

	testCmd, _, err := root.Find([]string{"test"})
	require.NoError(t, err)
	require.Equal(t, "test", testCmd.Name(), "root.Find must not have fallen back to the root command")

	for _, flag := range []string{"task-policy", "egress-policy"} {
		require.Nil(t, testCmd.Flags().Lookup(flag),
			"`flow test` must not declare --%s: a case's verdict must not depend on which "+
				"policy file was passed to the command that ran it (#652 item 2). A case "+
				"exercising a policy denial is written against that policy's own package "+
				"— v1.TaskPolicy.Check and auth.SecretPolicy are pure functions of "+
				"(thing, identity) and need no workflow at all.", flag)
	}

	// The positive half, kept only because it is what makes the assertion above
	// mean "deliberately absent here" rather than "absent everywhere". A tree
	// that had dropped --task-policy entirely would pass the loop above while
	// having deleted the feature.
	for _, path := range [][]string{
		{"worker"},
		{"run", "local"},
		{"mcp"},
		{"server", "dev"},
		{"task", "run"},
	} {
		cmd, _, err := root.Find(path)
		require.NoError(t, err)
		require.NotNil(t, cmd.Flags().Lookup("task-policy"),
			"%v must still declare --task-policy; the flag is deliberately absent from "+
				"`flow test` and deliberately present on every verb that runs a real dispatch", path)
	}
}

// TestFlowTestPolicyFlagsAreAbsentFromItsHelp is the other half of the surface a
// person actually meets. A flag can be absent from a command and still be
// promised by its help text, which is the same false diagnostic in a different
// venue: an author who reads `--task-policy` in `flow test --help` and types it
// gets an error naming a flag the documentation offered them.
func TestFlowTestPolicyFlagsAreAbsentFromItsHelp(t *testing.T) {
	root := newRootCommand()

	testCmd, _, err := root.Find([]string{"test"})
	require.NoError(t, err)

	usage := testCmd.UsageString()
	require.NotContains(t, usage, "--task-policy",
		"`flow test`'s help must not offer a flag it does not accept")
	require.NotContains(t, usage, "--egress-policy",
		"`flow test`'s help must not offer a flag it does not accept")
}
