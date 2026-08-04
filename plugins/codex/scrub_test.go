package main

import (
	"context"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// TestCodexExecScrubsTheApiKeyFromATurnFailure is codex.exec's own
// containment-shape test end to end: it runs the real task function
// (codexExec) against a fake subprocess whose turn.failed message contains
// the exact api_key value this test gave it - standing in for a codex CLI
// that echoes a credential back in its own error text, which is exactly
// the class CLAUDE.md's "secrets never enter workflow history" section
// warns a resolved secret can leak through once it reaches code that never
// saw it as a secret.
//
// This is deliberately the highest-level test that can prove the bite: it
// goes through codexExec's actual scrubber construction and the actual
// classification call, not a hand-built scrubber standing in for them, so a
// regression in exec.go's own wiring - forgetting to register the key,
// scrubbing after classifying instead of before - fails here even if
// errors_test.go's more targeted classifyRunError test were somehow also
// broken in a way that happened to still pass.
func TestCodexExecScrubsTheApiKeyFromATurnFailure(t *testing.T) {
	bin := buildFakeCodex(t)
	events := writeEventsFile(t,
		`{"type":"thread.started","thread_id":"th_1"}`,
		`{"type":"turn.failed","error":{"message":"authentication failed for key `+containmentSecret+`"}}`,
	)
	applyFakeCodexEnv(t, bin, fakeCodexEnv{eventsFile: events})

	_, err := codexExec(context.Background(), inputsFor(map[string]any{
		"prompt":  "do something",
		"api_key": containmentSecret,
	}), nil)
	if err == nil {
		t.Fatal("codexExec: got no error, want one (the fake turn.failed event)")
	}
	if strings.Contains(err.Error(), containmentSecret) {
		t.Fatalf("codexExec's error leaked the api_key: %v", err)
	}
	if !strings.Contains(err.Error(), secrets.Redacted) {
		t.Errorf("codexExec's error = %v, want it to contain %q", err, secrets.Redacted)
	}
}

// TestCodexExecScrubsTheApiKeyFromOutputs proves the same containment
// property on the success path: a value that ends up inside an event
// summary or the final message - a run whose own transcript happens to
// quote the credential it was given, which a model can do - must not
// survive into this task's outputs, since a step's outputs are written to
// workflow history exactly as CLAUDE.md's own framing describes.
func TestCodexExecScrubsTheApiKeyFromOutputs(t *testing.T) {
	bin := buildFakeCodex(t)
	events := writeEventsFile(t,
		`{"type":"thread.started","thread_id":"th_1"}`,
		`{"type":"item.completed","item":{"id":"1","type":"agent_message","text":"the key you gave me was `+containmentSecret+`"}}`,
		`{"type":"turn.completed"}`,
	)
	applyFakeCodexEnv(t, bin, fakeCodexEnv{eventsFile: events})

	outputs, err := codexExec(context.Background(), inputsFor(map[string]any{
		"prompt":  "echo the key back",
		"api_key": containmentSecret,
	}), nil)
	if err != nil {
		t.Fatalf("codexExec: unexpected error: %v", err)
	}

	final := outputs.GetNamedValues()["final_message"].GetLiteral().GetStringValue()
	if strings.Contains(final, containmentSecret) {
		t.Fatalf("final_message leaked the api_key: %q", final)
	}
	if !strings.Contains(final, secrets.Redacted) {
		t.Errorf("final_message = %q, want it to contain %q where the key was", final, secrets.Redacted)
	}
}
