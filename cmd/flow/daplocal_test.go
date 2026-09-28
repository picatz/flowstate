package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAFailedLaunchReleasesWhatItOpened is a launch whose plugins cannot
// start after its secret providers have: the providers are released at once
// rather than held until the adapter exits, so a client retrying the launch
// does not accumulate a set per attempt.
func TestAFailedLaunchReleasesWhatItOpened(t *testing.T) {
	cmd := newDAPCommand()
	require.NoError(t, cmd.ParseFlags([]string{"--plugin", filepath.Join(t.TempDir(), "no-such-plugin")}))
	cmd.SetContext(t.Context())

	var local localRunResources
	for range 3 {
		_, _, err := local.open(cmd)
		require.Error(t, err, "a plugin that does not exist started")
		assert.Empty(t, local.closers, "a failed launch kept resources until the adapter exits")
		assert.False(t, local.opened)
	}
}
