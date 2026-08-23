package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"

	codexv1 "github.com/picatz/flowstate/plugins/codex/gen/codex/v1"
)

// TestWritableSandboxRequiresAWorkingContext is PR #191's first P1: a writable
// run with no working_context leaves both --cd and cmd.Dir unset, so the child
// inherits this plugin's own directory - which the host sets to the private
// plugin socket directory (pkg/flowstate/v1/plugin/launch.go), putting
// host-managed files inside a writable agent's reach. Refused, by name.
func TestWritableSandboxRequiresAWorkingContext(t *testing.T) {
	for _, mode := range []codexv1.SandboxMode{
		codexv1.SandboxMode_SANDBOX_MODE_WORKSPACE_WRITE,
		codexv1.SandboxMode_SANDBOX_MODE_DANGER_FULL_ACCESS,
	} {
		t.Run(mode.String(), func(t *testing.T) {
			// The operator ceiling is checked before this, so raise it:
			// what is under test is the missing working_context, not the
			// ceiling refusal that would otherwise mask it.
			policyPath := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(policyPath, []byte("sandbox_mode = \"danger-full-access\"\n"), 0o600); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			t.Setenv(policyEnv, policyPath)
			t.Setenv(workdirRootEnv, t.TempDir())

			_, err := codexExec(context.Background(), inputsFor(map[string]any{
				"prompt":       "do a thing",
				"sandbox_mode": mode.String(),
				"api_key":      "sk-test",
			}), nil)
			if err == nil {
				t.Fatal("a writable sandbox with no working_context was accepted")
			}
			if !strings.Contains(err.Error(), "working_context is required") {
				t.Errorf("error does not name the missing input: %v", err)
			}
		})
	}
}

// TestReadOnlyRunsStillNeedNoWorkingContext is the other direction: the
// refusal above must not make ordinary read-only runs unusable. A read-only
// run may still fail here for unrelated reasons (no codex binary configured
// in this test's environment); what it must never say is that a working
// context was required.
func TestReadOnlyRunsStillNeedNoWorkingContext(t *testing.T) {
	_, err := codexExec(context.Background(), inputsFor(map[string]any{
		"prompt":       "just read",
		"sandbox_mode": codexv1.SandboxMode_SANDBOX_MODE_READ_ONLY.String(),
		"api_key":      "sk-test",
	}), nil)
	if err != nil && strings.Contains(err.Error(), "working_context is required") {
		t.Fatalf("a read-only run was refused for want of a working_context: %v", err)
	}
}

// TestWorkingContextRefusesASymlinkOutOfTheRoot is #191's second P1: a
// directory lexically inside the root whose target is outside it passed the
// filepath.Rel check, and every later use followed the link.
func TestWorkingContextRefusesASymlinkOutOfTheRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation is not reliably available on this platform")
	}

	root := t.TempDir()
	outside := t.TempDir()

	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	t.Setenv(workdirRootEnv, root)

	if _, err := resolveWorkingContext("escape"); err == nil {
		t.Fatal("a symlink pointing out of the root was accepted")
	} else if !strings.Contains(err.Error(), "outside the configured root") {
		t.Errorf("error does not name the escape: %v", err)
	}
}

// TestWorkingContextAcceptsARealDirectoryInsideTheRoot pins the positive
// direction, so the refusal above cannot degenerate into refusing everything.
func TestWorkingContextAcceptsARealDirectoryInsideTheRoot(t *testing.T) {
	root := t.TempDir()
	inside := filepath.Join(root, "work")
	if err := os.Mkdir(inside, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	t.Setenv(workdirRootEnv, root)

	got, err := resolveWorkingContext("work")
	if err != nil {
		t.Fatalf("a real directory inside the root was refused: %v", err)
	}

	want, err := filepath.EvalSymlinks(inside)
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}
	if got != want {
		t.Errorf("resolveWorkingContext = %q, want the resolved path %q", got, want)
	}
}

// TestBoundEventsTreatsAnExhaustedBudgetAsNothingMore is #191's output-budget
// P2: a non-positive remainder - which is what max_output_bytes leaves once
// the final message and patch have filled it - used to disable the bound
// entirely and return every retained event.
func TestBoundEventsTreatsAnExhaustedBudgetAsNothingMore(t *testing.T) {
	lines := []eventLine{
		{kind: "a", summary: strings.Repeat("x", 100)},
		{kind: "b", summary: strings.Repeat("y", 100)},
	}

	for _, budget := range []int{0, -1, -4096} {
		got, truncated := boundEvents(lines, 10, budget, secrets.NewScrubber())
		if len(got) != 0 {
			t.Errorf("budget %d returned %d events, want none", budget, len(got))
		}
		if !truncated {
			t.Errorf("budget %d did not report truncation", budget)
		}
	}

	if got, truncated := boundEvents(lines, 10, 250, secrets.NewScrubber()); len(got) != 2 || truncated {
		t.Errorf("a sufficient budget returned %d events (truncated=%v), want 2 and false", len(got), truncated)
	}
}

// TestComputePatchRefusesAnUnprovableDelta is #191's third P1: `git diff HEAD`
// in a workspace that already had uncommitted edits reports those too, and this
// output feeds git.commit_push directly - so a dirty start would commit work
// this run never did. Pre-existing edits cannot be subtracted after the fact,
// so both "dirty at start" and "could not tell" produce no patch at all.
func TestComputePatchRefusesAnUnprovableDelta(t *testing.T) {
	files := []fileChange{{Path: "a.txt", ChangeType: "modified"}}

	for _, tc := range []struct {
		name     string
		baseline workspaceBaseline
	}{
		{"dirty at start", workspaceBaseline{observed: true, dirty: true}},
		{"could not be observed", workspaceBaseline{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch, got, truncated := computePatch(context.Background(), "", nil, t.TempDir(), true, tc.baseline, files)
			if patch != "" {
				t.Errorf("a workspace %s produced a patch: %q", tc.name, patch)
			}
			if len(got) != len(files) {
				t.Errorf("files_changed = %v, want it reported regardless", got)
			}
			if truncated {
				t.Error("truncated reported for an absent patch")
			}
		})
	}
}
