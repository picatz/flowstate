package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRunLocalRefusesWhatTheServerRefusesAtSubmit is #1757 at the door an author
// uses most. A 101-step file is refused by the server with "workflow.steps: must
// contain no more than 100 item(s)"; `flow run local` used to run it.
func TestRunLocalRefusesWhatTheServerRefusesAtSubmit(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: chain\nsteps:\n")
	for i := range 101 {
		fmt.Fprintf(&b, "  - id: s%d\n    log:\n      message: hi\n", i)
	}
	path := filepath.Join(t.TempDir(), "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o600))

	_, err := loadWorkflow(path)
	require.Error(t, err, "a file the server refuses at submit was loaded to run locally")
	require.ErrorContains(t, err, "must contain no more than 100 item(s)")
	require.ErrorContains(t, err, "workflow.yaml:304", "the refusal is not positioned on the 101st step")
}
