package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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

// TestDebugAttachReleasesTheRunWhenItCannotAnswer: an attach whose output
// cannot be written ends in an error, and the session it attached is detached
// on the way out rather than left holding the run until its lease lapses.
func TestDebugAttachReleasesTheRunWhenItCannotAnswer(t *testing.T) {
	recorder := &detachRecorder{}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(recorder))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	script := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(script, []byte("status\n"), 0o600))

	root := newRootCommand()
	root.SetOut(failingWriter{})
	root.SetErr(io.Discard)
	root.SetIn(strings.NewReader(""))
	root.SetArgs([]string{"debug", "attach", "w", "--script", script, "--address", srv.URL, "-o", "jsonl"})
	require.Error(t, root.ExecuteContext(t.Context()), "an answer that could not be written was reported as delivered")
	assert.Equal(t, 1, recorder.detaches(), "the attach left the run held when it could not answer")
}

// TestDebugAttachJSONIsBounded: the `-o json` document is held until the
// session ends, so it is bounded, and a session past the bound is told to
// stream instead.
func TestDebugAttachJSONIsBounded(t *testing.T) {
	t.Parallel()

	answers := &driveAnswers{out: io.Discard, format: FormatJSON}
	result := &flowdebug.DriveResult{Snapshot: &v1.DebugSnapshot{Message: strings.Repeat("x", 1<<20)}}
	var err error
	for range maxAttachJSONBytes>>20 + 1 {
		if err = answers.add("status", result); err != nil {
			break
		}
	}
	require.Error(t, err, "the held document grew past its bound")
	assert.Contains(t, err.Error(), "-o jsonl")
	assert.LessOrEqual(t, answers.heldBytes, maxAttachJSONBytes)
}

// TestDebugAttachReleasesTheRunWhenItsDetachIsRefused: `detach` (or `quit`)
// whose detach the run did not take has not released it, so the attach
// detaches again on the way out instead of only disconnecting and leaving the
// run held by a session nobody drives.
func TestDebugAttachReleasesTheRunWhenItsDetachIsRefused(t *testing.T) {
	recorder := &detachRecorder{refuseFirst: true}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(recorder))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	script := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(script, []byte("quit\n"), 0o600))

	res := runFlow(t, "debug", "attach", "w", "--script", script, "--address", srv.URL)
	require.NoError(t, res.Err)
	assert.Equal(t, 2, recorder.detaches(), "a refused detach was walked away from, leaving the run held")
}

// TestDebugAttachReleasesTheRunWhenItCannotReadIt: an attach whose first read
// of the run fails has told nobody the session's id, so there is nobody to
// rejoin it; the session is detached rather than left holding the run.
func TestDebugAttachReleasesTheRunWhenItCannotReadIt(t *testing.T) {
	recorder := &detachRecorder{unreadable: true}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(recorder))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	res := runFlow(t, "debug", "attach", "w", "--address", srv.URL, "-o", "jsonl")
	require.Error(t, res.Err)
	assert.Equal(t, 1, recorder.detaches(), "an attach that could not read the run left it held")
}

// TestAScriptedAttachFailsOnACommandThatFails: a script's later lines assume
// its earlier ones ran, so a line that fails fails the attach — releasing the
// run — instead of the script exiting 0 having run in part.
func TestAScriptedAttachFailsOnACommandThatFails(t *testing.T) {
	recorder := &detachRecorder{}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(recorder))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	script := filepath.Join(t.TempDir(), "session.script")
	require.NoError(t, os.WriteFile(script, []byte("frobnicate\nstatus\n"), 0o600))

	res := runFlow(t, "debug", "attach", "w", "--script", script, "--address", srv.URL)
	require.Error(t, res.Err, "a script with a failing line exited 0")
	assert.Contains(t, res.Err.Error(), "frobnicate")
	assert.Equal(t, 1, recorder.detaches(), "the failed script left the run held")
}
