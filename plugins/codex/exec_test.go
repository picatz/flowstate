package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"

	codexv1 "github.com/picatz/flowstate/plugins/codex/gen/codex/v1"
)

func inputsFor(fields map[string]any) map[string]*flowstatev1.Value {
	out := make(map[string]*flowstatev1.Value, len(fields))
	for k, v := range fields {
		out[k] = flowstatev1.NewValue(v)
	}
	return out
}

// TestCodexExecReadsAFinalMessageAndUsage runs the full task function
// against a real subprocess (fakecodex) emitting a well-formed event
// stream, and checks that every field EncodeOutputs produced came from
// that stream rather than from nowhere.
func TestCodexExecReadsAFinalMessageAndUsage(t *testing.T) {
	bin := buildFakeCodex(t)
	events := writeEventsFile(t,
		`{"type":"thread.started","thread_id":"th_123"}`,
		`{"type":"turn.started"}`,
		`{"type":"item.completed","item":{"id":"1","type":"agent_message","text":"done: created a file"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":2,"output_tokens":5}}`,
	)
	applyFakeCodexEnv(t, bin, fakeCodexEnv{eventsFile: events})

	outputs, err := codexExec(context.Background(), inputsFor(map[string]any{
		"prompt": "say hello",
	}), nil)
	if err != nil {
		t.Fatalf("codexExec: unexpected error: %v", err)
	}

	got := outputs.GetNamedValues()
	if got["final_message"].GetLiteral().GetStringValue() != "done: created a file" {
		t.Errorf("final_message = %v, want %q", got["final_message"], "done: created a file")
	}
	if got["thread_id"].GetLiteral().GetStringValue() != "th_123" {
		t.Errorf("thread_id = %v, want %q", got["thread_id"], "th_123")
	}
	if got["input_tokens"].GetLiteral().GetInt64Value() != 10 {
		t.Errorf("input_tokens = %v, want 10", got["input_tokens"])
	}
	if got["output_tokens"].GetLiteral().GetInt64Value() != 5 {
		t.Errorf("output_tokens = %v, want 5", got["output_tokens"])
	}
	if got["truncated"].GetLiteral().GetBoolValue() {
		t.Errorf("truncated = true, want false for a run well within every bound")
	}
}

// TestCodexExecRefusesAnEmptyPrompt proves the required input is enforced
// before a subprocess is ever started - no FLOWSTATE_CODEX_BIN is even set
// for this test, and it must never be reached.
func TestCodexExecRefusesAnEmptyPrompt(t *testing.T) {
	_, err := codexExec(context.Background(), inputsFor(map[string]any{"prompt": ""}), nil)
	if err == nil {
		t.Fatal("codexExec with an empty prompt: got no error, want one")
	}
}

// TestCodexExecDefaultsToReadOnlySandbox proves sandbox_mode's fail-closed
// default: leaving it unset must resolve to READ_ONLY, not to whatever the
// codex CLI's own default happens to be. This is checked by observing what
// the plugin actually decided (sandboxCLIValue's return for the
// UNSPECIFIED case), independent of running a subprocess.
func TestCodexExecDefaultsToReadOnlySandbox(t *testing.T) {
	if defaultSandboxMode != codexv1.SandboxMode_SANDBOX_MODE_READ_ONLY {
		t.Fatalf("defaultSandboxMode = %v, want SANDBOX_MODE_READ_ONLY", defaultSandboxMode)
	}

	got, err := sandboxCLIValue(codexv1.SandboxMode_SANDBOX_MODE_UNSPECIFIED)
	if err == nil {
		t.Fatalf("sandboxCLIValue(UNSPECIFIED) = %q, <nil>; this task must resolve UNSPECIFIED to "+
			"defaultSandboxMode before calling sandboxCLIValue, not pass it through", got)
	}
}

// TestCodexExecRefusesAnUnrecognizedSandboxEnum proves a closed-set
// refusal rather than a guess for an enum number this build's schema does
// not define.
func TestCodexExecRefusesAnUnrecognizedSandboxEnum(t *testing.T) {
	if _, err := sandboxCLIValue(codexv1.SandboxMode(99)); err == nil {
		t.Fatal("sandboxCLIValue(99): got no error, want one")
	}
}

// TestCodexExecClassifiesATurnFailure proves a turn.failed event stops the
// run and is surfaced as a permanent failure, not silently ignored or
// retried.
func TestCodexExecClassifiesATurnFailure(t *testing.T) {
	bin := buildFakeCodex(t)
	events := writeEventsFile(t,
		`{"type":"thread.started","thread_id":"th_1"}`,
		`{"type":"turn.failed","error":{"message":"the model refused the request"}}`,
	)
	applyFakeCodexEnv(t, bin, fakeCodexEnv{eventsFile: events})

	_, err := codexExec(context.Background(), inputsFor(map[string]any{"prompt": "do something"}), nil)
	if err == nil {
		t.Fatal("codexExec with a turn.failed event: got no error, want one")
	}
	if !strings.Contains(err.Error(), "the model refused the request") {
		t.Errorf("error = %v, want it to name the turn failure", err)
	}
}

// TestCodexExecBoundsEventCount proves appendEvent's cap is enforced during
// collection, not only by EncodeOutputs afterward - a run emitting more
// events than max_events must report truncated rather than silently growing
// its own event slice without limit.
func TestCodexExecBoundsEventCount(t *testing.T) {
	bin := buildFakeCodex(t)

	var lines []string
	lines = append(lines, `{"type":"thread.started","thread_id":"th_1"}`)
	for i := 0; i < 20; i++ {
		lines = append(lines, `{"type":"item.completed","item":{"id":"r","type":"reasoning","text":"thinking"}}`)
	}
	lines = append(lines,
		`{"type":"item.completed","item":{"id":"1","type":"agent_message","text":"ok"}}`,
		`{"type":"turn.completed"}`,
	)
	events := writeEventsFile(t, lines...)
	applyFakeCodexEnv(t, bin, fakeCodexEnv{eventsFile: events})

	outputs, err := codexExec(context.Background(), inputsFor(map[string]any{
		"prompt":     "loop",
		"max_events": int64(5),
	}), nil)
	if err != nil {
		t.Fatalf("codexExec: unexpected error: %v", err)
	}

	got := outputs.GetNamedValues()
	eventsOut := got["events"].GetLiteral().GetListValue().GetValues()
	if len(eventsOut) > 5 {
		t.Errorf("got %d events, want at most 5 (max_events)", len(eventsOut))
	}
	if !got["truncated"].GetLiteral().GetBoolValue() {
		t.Error("truncated = false, want true: this run emitted more events than max_events allowed")
	}
}

// TestCodexExecRefusesMaxEventsOverCeiling proves a request over the
// ceiling is refused rather than silently clamped - the same reasoning
// plugins/vcs/validate.go's clampMaxCommits documents for its own ceiling.
func TestCodexExecRefusesMaxEventsOverCeiling(t *testing.T) {
	_, err := codexExec(context.Background(), inputsFor(map[string]any{
		"prompt":     "hi",
		"max_events": int64(maxMaxEvents + 1),
	}), nil)
	if err == nil {
		t.Fatal("codexExec with max_events over the ceiling: got no error, want one")
	}
}

// TestCodexExecFailsClosedWithNoBinaryConfigured proves that with
// FLOWSTATE_CODEX_BIN unset, codex.exec refuses rather than searching
// $PATH.
func TestCodexExecFailsClosedWithNoBinaryConfigured(t *testing.T) {
	t.Setenv(codexBinaryEnv, "")

	_, err := codexExec(context.Background(), inputsFor(map[string]any{"prompt": "hi"}), nil)
	if err == nil {
		t.Fatal("codexExec with no FLOWSTATE_CODEX_BIN: got no error, want one")
	}
	if !strings.Contains(err.Error(), codexBinaryEnv) {
		t.Errorf("error = %v, want it to name %s so an operator knows what to configure", err, codexBinaryEnv)
	}
}

// TestCodexExecRefusesSandboxModeOverTheOperatorCeiling proves the
// three-layer narrowing design end to end: with no FLOWSTATE_CODEX_BASE_CONFIG
// configured, the operator ceiling defaults to READ_ONLY (see policy.go),
// so a task asking for WORKSPACE_WRITE must be refused before a subprocess
// is ever started - no FLOWSTATE_CODEX_BIN is set for this test, and it
// must never be reached.
func TestCodexExecRefusesSandboxModeOverTheOperatorCeiling(t *testing.T) {
	t.Setenv(policyEnv, "")

	_, err := codexExec(context.Background(), inputsFor(map[string]any{
		"prompt":       "hi",
		"sandbox_mode": "SANDBOX_MODE_WORKSPACE_WRITE",
	}), nil)
	if err == nil {
		t.Fatal("codexExec requesting WORKSPACE_WRITE with no operator policy raising the ceiling: got no error, want one")
	}
	if !strings.Contains(err.Error(), policyEnv) {
		t.Errorf("error = %v, want it to name %s so an operator knows how to raise the ceiling", err, policyEnv)
	}
}

// TestCodexExecAllowsSandboxModeWithinAnOperatorRaisedCeiling is the other
// direction: an operator config that raises the ceiling must actually let a
// task use it, otherwise the ceiling check would only ever be able to
// refuse and this "layer 2" would be theater.
func TestCodexExecAllowsSandboxModeWithinAnOperatorRaisedCeiling(t *testing.T) {
	bin := buildFakeCodex(t)
	events := writeEventsFile(t,
		`{"type":"thread.started","thread_id":"th_1"}`,
		`{"type":"item.completed","item":{"id":"1","type":"agent_message","text":"ok"}}`,
		`{"type":"turn.completed"}`,
	)
	applyFakeCodexEnv(t, bin, fakeCodexEnv{eventsFile: events})

	policyPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(policyPath, []byte("sandbox_mode = \"workspace-write\"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	t.Setenv(policyEnv, policyPath)

	// A writable run must name where it may write (see
	// TestWritableSandboxRequiresAWorkingContext), so this ceiling test
	// supplies one rather than relying on the child inheriting a directory.
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "work"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	t.Setenv(workdirRootEnv, root)

	_, err := codexExec(context.Background(), inputsFor(map[string]any{
		"prompt":          "hi",
		"sandbox_mode":    "SANDBOX_MODE_WORKSPACE_WRITE",
		"working_context": "work",
	}), nil)
	if err != nil {
		t.Fatalf("codexExec requesting WORKSPACE_WRITE with an operator policy raising the ceiling: unexpected error: %v", err)
	}
}

// TestCodexExecRefusesNetworkAccessWithoutOperatorGrant proves
// allow_network is narrowed the same way sandbox_mode is: raising the
// sandbox ceiling to WORKSPACE_WRITE does not, on its own, grant network
// access - that is its own, separately-refused request.
func TestCodexExecRefusesNetworkAccessWithoutOperatorGrant(t *testing.T) {
	policyPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(policyPath, []byte("sandbox_mode = \"workspace-write\"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	t.Setenv(policyEnv, policyPath)

	_, err := codexExec(context.Background(), inputsFor(map[string]any{
		"prompt":        "hi",
		"sandbox_mode":  "SANDBOX_MODE_WORKSPACE_WRITE",
		"allow_network": true,
	}), nil)
	if err == nil {
		t.Fatal("codexExec requesting allow_network=true with no operator grant: got no error, want one")
	}
	if !strings.Contains(err.Error(), "allow_network") {
		t.Errorf("error = %v, want it to name allow_network", err)
	}
}

// TestCodexExecRefusesResetWorkingContextWithoutWorkingContext proves
// reset_working_context is refused as an authoring mistake, not silently
// ignored, when there is nothing named to reset - see CLAUDE.md,
// "Diagnostics are a feature."
func TestCodexExecRefusesResetWorkingContextWithoutWorkingContext(t *testing.T) {
	_, err := codexExec(context.Background(), inputsFor(map[string]any{
		"prompt":                "hi",
		"reset_working_context": true,
	}), nil)
	if err == nil {
		t.Fatal("codexExec requesting reset_working_context with no working_context: got no error, want one")
	}
	if !strings.Contains(err.Error(), "working_context") {
		t.Errorf("error = %v, want it to name working_context", err)
	}
}

// TestCodexExecResetWorkingContextFailsClosedWithoutGitConfigured proves
// this is not the same best-effort shape computePatch itself has: an
// author who asked for a reset and cannot get one must be told, not left to
// discover it later from a baseline that observeWorkspace reports dirty
// with no explanation.
func TestCodexExecResetWorkingContextFailsClosedWithoutGitConfigured(t *testing.T) {
	t.Setenv(gitBinaryEnv, "")

	// Not what this test is about: a codex binary must be configured for
	// codexExec to reach the reset logic at all, since that check runs
	// first (see resolveCodexBinary in exec.go).
	applyFakeCodexEnv(t, buildFakeCodex(t), fakeCodexEnv{})

	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "work"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	t.Setenv(workdirRootEnv, root)

	_, err := codexExec(context.Background(), inputsFor(map[string]any{
		"prompt":                "hi",
		"working_context":       "work",
		"reset_working_context": true,
	}), nil)
	if err == nil {
		t.Fatal("codexExec requesting reset_working_context with no git binary configured: got no error, want one")
	}
	if !strings.Contains(err.Error(), gitBinaryEnv) {
		t.Errorf("error = %v, want it to name %s", err, gitBinaryEnv)
	}
}

// TestCodexExecResetWorkingContextDiscardsLeftoverEdits is the end-to-end
// version of TestResetWorkingContextDiscardsTrackedAndUntrackedChanges
// (diff_test.go): proof that codexExec actually wires the reset in, ahead
// of the baseline read, rather than only that the helper function works in
// isolation. sandbox_mode is left at its READ_ONLY default deliberately -
// reset_working_context requires only working_context, not a write grant,
// since the reset itself is not the agent writing anything.
func TestCodexExecResetWorkingContextDiscardsLeftoverEdits(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	root := t.TempDir()
	repoDir := filepath.Join(root, "repo")
	if err := os.Mkdir(repoDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	initRepoWithCommit(t, gitBin, repoDir)
	t.Setenv(workdirRootEnv, root)

	// Stands in for a previous turn's own edits, still sitting in
	// working_context because nothing before this input existed to clean
	// them up - see #967.
	if err := os.WriteFile(filepath.Join(repoDir, "a.txt"), []byte("one\ntwo\n"), 0o600); err != nil {
		t.Fatalf("WriteFile a.txt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoDir, "untracked.txt"), []byte("new\n"), 0o600); err != nil {
		t.Fatalf("WriteFile untracked.txt: %v", err)
	}

	bin := buildFakeCodex(t)
	events := writeEventsFile(t,
		`{"type":"thread.started","thread_id":"th_1"}`,
		`{"type":"item.completed","item":{"id":"1","type":"agent_message","text":"ok"}}`,
		`{"type":"turn.completed"}`,
	)
	applyFakeCodexEnv(t, bin, fakeCodexEnv{eventsFile: events})

	_, err := codexExec(context.Background(), inputsFor(map[string]any{
		"prompt":                "hi",
		"working_context":       "repo",
		"reset_working_context": true,
	}), nil)
	if err != nil {
		t.Fatalf("codexExec: unexpected error: %v", err)
	}

	got, err := os.ReadFile(filepath.Join(repoDir, "a.txt"))
	if err != nil {
		t.Fatalf("ReadFile a.txt after codexExec: %v", err)
	}
	if string(got) != "one\n" {
		t.Errorf("a.txt after codexExec with reset_working_context = %q, want the committed content %q", got, "one\n")
	}
	if _, err := os.Stat(filepath.Join(repoDir, "untracked.txt")); !os.IsNotExist(err) {
		t.Errorf("untracked.txt after codexExec with reset_working_context: stat err = %v, want IsNotExist", err)
	}
}

// TestCodexExecResetWorkingContextEnablesARetriedPatch reproduces #967's
// exact mechanism end to end: a first turn's own edit left dirty in
// working_context makes a second, unreset turn's patch come back empty
// (computePatch fails closed on a dirty baseline) - the bug this input
// closes - and a third turn that resets first gets a patch again. fakecodex
// is told to write to working_context itself (FAKECODEX_WRITE_FILE), which
// is what a real WORKSPACE_WRITE turn's own edits would leave behind.
func TestCodexExecResetWorkingContextEnablesARetriedPatch(t *testing.T) {
	gitBin := realGitBinary(t)
	t.Setenv(gitBinaryEnv, gitBin)

	root := t.TempDir()
	repoDir := filepath.Join(root, "repo")
	if err := os.Mkdir(repoDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	initRepoWithCommit(t, gitBin, repoDir)
	t.Setenv(workdirRootEnv, root)

	policyPath := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(policyPath, []byte("sandbox_mode = \"workspace-write\"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	t.Setenv(policyEnv, policyPath)

	bin := buildFakeCodex(t)
	events := writeEventsFile(t,
		`{"type":"thread.started","thread_id":"th_1"}`,
		`{"type":"item.completed","item":{"id":"1","type":"file_change","status":"completed","changes":[{"path":"a.txt","kind":"update"}]}}`,
		`{"type":"turn.completed"}`,
	)

	runTurn := func(reset bool) string {
		t.Helper()
		applyFakeCodexEnv(t, bin, fakeCodexEnv{eventsFile: events})
		t.Setenv("FAKECODEX_WRITE_FILE", filepath.Join(repoDir, "a.txt"))
		t.Setenv("FAKECODEX_WRITE_CONTENT", "one\ntwo\n")

		outputs, err := codexExec(context.Background(), inputsFor(map[string]any{
			"prompt":                "fix it",
			"sandbox_mode":          "SANDBOX_MODE_WORKSPACE_WRITE",
			"working_context":       "repo",
			"reset_working_context": reset,
		}), nil)
		if err != nil {
			t.Fatalf("codexExec: unexpected error: %v", err)
		}
		return outputs.GetNamedValues()["patch"].GetLiteral().GetStringValue()
	}

	if patch := runTurn(false); !strings.Contains(patch, "+two") {
		t.Fatalf("first turn's patch = %q, want it to contain the agent's own edit", patch)
	}

	// Nothing reverted the first turn's edit - the same #967 mechanism a
	// git.commit_push step never touching working_context leaves behind -
	// so a second turn with no reset starts dirty and gets no patch at all.
	if patch := runTurn(false); patch != "" {
		t.Fatalf("second turn's patch with no reset = %q, want empty: the workspace should still be dirty from the first turn", patch)
	}

	// Reset first, and the same shape of turn produces a patch again.
	if patch := runTurn(true); !strings.Contains(patch, "+two") {
		t.Fatalf("third turn's patch with reset_working_context = %q, want it to contain the edit again", patch)
	}
}
