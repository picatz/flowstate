package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// TestDebugAttachFailsOnAScriptLineItCannotRead: a line longer than a command
// may be stops the reader mid-script. That is not the end of the script, so
// the command fails instead of releasing the run and exiting as though every
// command had been driven.
func TestDebugAttachFailsOnAScriptLineItCannotRead(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(heldRun{}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	script := filepath.Join(t.TempDir(), "session.script")
	long := "inspect " + strings.Repeat("x", flowdebug.MaxCommandBytes+1)
	require.NoError(t, os.WriteFile(script, []byte("status\n"+long+"\ncontinue\n"), 0o600))

	res := runFlow(t, "debug", "attach", "w", "--session", "held-1", "--script", script, "--address", srv.URL)
	require.Error(t, res.Err, "an unread script line was reported as a completed session")
	assert.Contains(t, res.Err.Error(), "at most")
	assert.Contains(t, res.Err.Error(), "held-1")
}
