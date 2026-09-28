package main

import (
	"encoding/json"
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

// TestDebugAttachJSONIsOneDocument: `-o json` is one document per invocation,
// so an attach that answers several commands writes one array of them, which
// a JSON parser reads whole; `-o jsonl` streams one object per line.
func TestDebugAttachJSONIsOneDocument(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(heldRun{}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	script := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(script, []byte("status\nbreakpoints\ndisconnect\n"), 0o600))

	res := runFlow(t, "debug", "attach", "w", "--session", "held-1", "--script", script, "--address", srv.URL, "-o", "json")
	require.NoError(t, res.Err)
	var answers []struct {
		Command string `json:"command"`
	}
	require.NoError(t, json.Unmarshal([]byte(res.Stdout), &answers), "not one JSON document: %s", res.Stdout)
	commands := make([]string, 0, len(answers))
	for _, answer := range answers {
		commands = append(commands, answer.Command)
	}
	assert.Equal(t, []string{"status", "status", "breakpoints"}, commands)

	res = runFlow(t, "debug", "attach", "w", "--session", "held-1", "--script", script, "--address", srv.URL, "-o", "jsonl")
	require.NoError(t, res.Err)
	lines := strings.Split(strings.TrimSpace(res.Stdout), "\n")
	require.Len(t, lines, 3, res.Stdout)
	for _, line := range lines {
		assert.True(t, json.Valid([]byte(line)), "a JSON line is not an object: %s", line)
	}
}
