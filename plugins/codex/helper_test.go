package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

// containmentSecret is a value that would be obviously wrong to find in any
// output or error this plugin produces - the same convention
// plugins/vcs/clone_test.go and plugins/github/auth_test.go use.
const containmentSecret = "sk-containment_canary_do_not_print_me"

var (
	fakeCodexOnce sync.Once
	fakeCodexPath string
	fakeCodexErr  error
)

// buildFakeCodex compiles testdata/fakecodex once per test binary run and
// returns its path - a real subprocess, real os/exec, real exit codes,
// standing in for the codex CLI so this plugin's own subprocess handling
// (readRun, classifyRunError, the byte and event bounds) is exercised
// against something that actually behaves like a child process rather than
// a mock that only looks like one from inside Go.
func buildFakeCodex(t *testing.T) string {
	t.Helper()

	fakeCodexOnce.Do(func() {
		dir, err := os.MkdirTemp("", "fakecodex")
		if err != nil {
			fakeCodexErr = err
			return
		}
		out := filepath.Join(dir, "fakecodex")
		cmd := exec.Command("go", "build", "-o", out, "./testdata/fakecodex")
		if wd, err := os.Getwd(); err == nil {
			cmd.Dir = wd
		}
		if combined, err := cmd.CombinedOutput(); err != nil {
			fakeCodexErr = fmt.Errorf("building fakecodex: %v: %s", err, combined)
			return
		}
		fakeCodexPath = out
	})

	if fakeCodexErr != nil {
		t.Fatalf("buildFakeCodex: %v", fakeCodexErr)
	}
	return fakeCodexPath
}

// writeEventsFile writes newline-joined JSON event lines to a temp file
// under t.TempDir and returns its path, for FAKECODEX_EVENTS_FILE.
func writeEventsFile(t *testing.T, lines ...string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "events.jsonl")
	var body string
	for _, l := range lines {
		body += l + "\n"
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing events file: %v", err)
	}
	return path
}

// fakeCodexEnv is the set of environment variables this test needs set on
// FLOWSTATE_CODEX_BIN's target for one scenario, applied with t.Setenv so
// each test gets its own without cross-contamination.
type fakeCodexEnv struct {
	eventsFile string
	stderr     string
	exitCode   int
	sleepMS    int
}

func applyFakeCodexEnv(t *testing.T, binPath string, cfg fakeCodexEnv) {
	t.Helper()
	t.Setenv(codexBinaryEnv, binPath)
	if cfg.eventsFile != "" {
		t.Setenv("FAKECODEX_EVENTS_FILE", cfg.eventsFile)
	} else {
		t.Setenv("FAKECODEX_EVENTS_FILE", "")
	}
	t.Setenv("FAKECODEX_STDERR", cfg.stderr)
	t.Setenv("FAKECODEX_EXIT_CODE", strconv.Itoa(cfg.exitCode))
	if cfg.sleepMS > 0 {
		t.Setenv("FAKECODEX_SLEEP_MS", strconv.Itoa(cfg.sleepMS))
	} else {
		t.Setenv("FAKECODEX_SLEEP_MS", "")
	}
}
