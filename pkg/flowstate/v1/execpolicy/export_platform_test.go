package execpolicy

import "testing"

// PretendProcessGroupsAreUnenforced makes Check behave as it does on a
// platform that cannot stop a program with its descendants, for one test.
func PretendProcessGroupsAreUnenforced(t *testing.T) {
	t.Helper()
	was := processGroupsEnforced
	processGroupsEnforced = false
	t.Cleanup(func() { processGroupsEnforced = was })
}
