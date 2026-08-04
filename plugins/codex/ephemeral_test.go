package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildEphemeralHomeCreatesAFreshIsolatedDirectory(t *testing.T) {
	dir, cleanup, err := buildEphemeralHome(defaultOperatorPolicy())
	if err != nil {
		t.Fatalf("buildEphemeralHome: unexpected error: %v", err)
	}
	defer cleanup()

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("buildEphemeralHome did not create a directory")
	}
	if info.Mode().Perm() != 0o700 {
		t.Errorf("ephemeral home mode = %o, want 0700", info.Mode().Perm())
	}

	// No operator config was given, so nothing should be written into it -
	// codex falls back to its own built-in defaults rather than anything
	// this plugin or an operator supplied.
	if _, err := os.Stat(filepath.Join(dir, "config.toml")); err == nil {
		t.Error("a config.toml was written with no operator RawConfig to write")
	}
}

func TestBuildEphemeralHomeCopiesTheOperatorsConfigThrough(t *testing.T) {
	policy := operatorPolicy{RawConfig: []byte("model_reasoning_effort = \"high\"\n")}

	dir, cleanup, err := buildEphemeralHome(policy)
	if err != nil {
		t.Fatalf("buildEphemeralHome: unexpected error: %v", err)
	}
	defer cleanup()

	data, err := os.ReadFile(filepath.Join(dir, "config.toml"))
	if err != nil {
		t.Fatalf("reading the ephemeral config.toml: %v", err)
	}
	if !strings.Contains(string(data), "model_reasoning_effort") {
		t.Errorf("config.toml = %q, want the operator's RawConfig copied through verbatim", data)
	}
}

func TestBuildEphemeralHomeCleanupRemovesTheDirectory(t *testing.T) {
	dir, cleanup, err := buildEphemeralHome(defaultOperatorPolicy())
	if err != nil {
		t.Fatalf("buildEphemeralHome: unexpected error: %v", err)
	}
	cleanup()

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("directory still exists after cleanup: err = %v", err)
	}
}

// TestChildEnvNeverIncludesTheHostProcesssOwnEnvironment is this design's
// own containment-shape test: it plants a variable in the *test process's*
// environment - standing in for anything a worker's own deployment
// environment might carry (a cloud credential, an internal hostname, a
// CODEX_HOME an operator set for some unrelated reason) - and proves
// childEnv's result does not carry it through. This is what
// distinguishes "an explicit allowlist" from "os.Environ() with some
// things added," which would still leak everything else.
func TestChildEnvNeverIncludesTheHostProcesssOwnEnvironment(t *testing.T) {
	t.Setenv("FLOWSTATE_CODEX_CONTAINMENT_CANARY", "must-not-appear-in-the-child")

	env := childEnv("sk-test-key", "/tmp/ephemeral-home")

	for _, kv := range env {
		if strings.Contains(kv, "FLOWSTATE_CODEX_CONTAINMENT_CANARY") || strings.Contains(kv, "must-not-appear-in-the-child") {
			t.Fatalf("childEnv leaked an unrelated host environment variable: %q", kv)
		}
	}
}

// TestChildEnvPlacesTheApiKeyOnlyWhereRequired proves the credential lands
// in exactly one place - CODEX_API_KEY - and nowhere else in the child's
// environment, so a future edit that echoes it into, say,
// CODEX_INTERNAL_ORIGINATOR_OVERRIDE for debugging would be caught here
// rather than shipped.
func TestChildEnvPlacesTheApiKeyOnlyWhereRequired(t *testing.T) {
	const key = "sk-containment-canary-do-not-duplicate"

	env := childEnv(key, "/tmp/ephemeral-home")

	found := 0
	for _, kv := range env {
		if strings.Contains(kv, key) {
			found++
			if !strings.HasPrefix(kv, "CODEX_API_KEY=") {
				t.Errorf("api key found outside CODEX_API_KEY: %q", kv)
			}
		}
	}
	if found != 1 {
		t.Errorf("api key appeared in %d environment entries, want exactly 1", found)
	}
}

func TestChildEnvOmitsTheApiKeyVariableWhenEmpty(t *testing.T) {
	env := childEnv("", "/tmp/ephemeral-home")
	for _, kv := range env {
		if strings.HasPrefix(kv, "CODEX_API_KEY=") {
			t.Errorf("CODEX_API_KEY was set with no api key given: %q", kv)
		}
	}
}
