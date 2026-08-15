package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServerHasAListenFlag is the property CLAUDE.md asks for: not that the
// flag parses, but that `flow server`'s own listen address is discoverable
// and settable from the command line at all. Before this, FLOWSTATE_ADDRESS
// was the only way to point `flow server` anywhere — an operator reading
// `flow server --help` had no flag to find, and every sibling setting
// (--internal-listen, --auth-policy, --identity-key, ...) had one beside its
// environment variable.
func TestServerHasAListenFlag(t *testing.T) {
	server := findCommand(t, "server")

	flag := server.Flags().Lookup("listen")
	require.NotNil(t, flag, "`flow server` has no --listen flag; FLOWSTATE_ADDRESS is once again "+
		"the only way to set the address this server binds")

	assert.Contains(t, flag.Usage, "FLOWSTATE_ADDRESS",
		"--listen's help text should say it defaults from FLOWSTATE_ADDRESS, so an operator who "+
			"already set the variable understands why nothing changed")
	assert.NotContains(t, flag.Usage, "http://", "--listen documents a bind address for net.Listen, "+
		"not a URL — a scheme belongs to the client's --address, not this flag")
}

// TestServerListenPrecedence pins the precedence --listen's help text claims:
// an explicit flag beats the environment, and the environment beats the
// built-in default when the flag is not given — the same rule every other
// env-defaulted flag in this tree follows (--address on the client verbs,
// --internal-listen, --auth-policy, --identity-key, --task-queue-prefix, and
// `flow server dev`'s own --listen).
//
// A test that only asserted the flag parses would pass just as happily if
// --listen silently ignored FLOWSTATE_ADDRESS, or if the environment
// silently overrode an explicit flag — both are the defect returning in a
// different spelling, and CLAUDE.md's "A capability is not done until it is
// reachable" asks for exactly this: the path from what an operator sets to
// what the server binds.
func TestServerListenPrecedence(t *testing.T) {
	t.Run("no flag, no environment: the built-in default", func(t *testing.T) {
		server := findCommand(t, "server")
		listen, err := server.Flags().GetString("listen")
		require.NoError(t, err)
		assert.Equal(t, defaultServerAddress, listen)
	})

	t.Run("no flag, environment set: the environment wins", func(t *testing.T) {
		t.Setenv("FLOWSTATE_ADDRESS", "10.0.0.4:9233")

		// The default is composed once, when the flag is registered, so the
		// environment has to be set before the command tree is built — exactly
		// the constraint TestGeneratingRestoresTheEnvironment documents for the
		// docs generator.
		server := findCommand(t, "server")
		listen, err := server.Flags().GetString("listen")
		require.NoError(t, err)
		assert.Equal(t, "10.0.0.4:9233", listen,
			"--listen should default from $FLOWSTATE_ADDRESS when no flag is given")
	})

	t.Run("flag and environment both set: the explicit flag wins", func(t *testing.T) {
		t.Setenv("FLOWSTATE_ADDRESS", "10.0.0.4:9233")

		server := findCommand(t, "server")
		require.NoError(t, server.Flags().Parse([]string{"--listen", "0.0.0.0:8443"}))

		listen, err := server.Flags().GetString("listen")
		require.NoError(t, err)
		assert.Equal(t, "0.0.0.0:8443", listen,
			"an explicit --listen must override $FLOWSTATE_ADDRESS, not the other way round")
	})
}
