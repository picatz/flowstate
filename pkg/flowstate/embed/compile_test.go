package embed

import (
	"errors"
	"testing"
)

func TestCompile_ValidFlowfile(t *testing.T) {
	data := []byte(`
edition: v2026.3
name: hello
steps:
  - id: hello
    log:
      message: hello world
`)

	workflow, diags, err := Compile(data)
	if err != nil {
		t.Fatalf("Compile: unexpected error: %v", err)
	}
	if diags != nil {
		t.Fatalf("Compile: unexpected diagnostics: %v", diags)
	}
	if workflow == nil {
		t.Fatal("Compile: workflow is nil")
	}
	if workflow.GetName() != "hello" {
		t.Errorf("workflow name = %q, want %q", workflow.GetName(), "hello")
	}
}

func TestCompile_MalformedFlowfileIsADiagnostic(t *testing.T) {
	data := []byte(`
edition: v2026.3
name: hello
steps:
  - id: step1
    log:
      message: ${this is not valid CEL (((
`)

	workflow, diags, err := Compile(data)
	if err == nil {
		t.Fatal("Compile: expected an error compiling a step with a malformed expression")
	}
	if workflow != nil {
		t.Errorf("Compile: workflow should be nil on failure, got %v", workflow)
	}
	if len(diags) == 0 {
		t.Fatal("Compile: expected diagnostics naming the malformed expression")
	}
	if diags[0].Step != "step1" {
		t.Errorf("Compile: diagnostic step = %q, want %q", diags[0].Step, "step1")
	}

	// err wraps diags, so a caller checking only err still sees the same
	// failure errors.As would give them from diags directly.
	var wrapped Diagnostics
	if !errors.As(err, &wrapped) {
		t.Errorf("Compile: err does not wrap Diagnostics via errors.As")
	}
}

// TestCompile_DoesNotCheckTaskExistence pins down a deliberate property of
// the compile boundary [Compile]'s own doc names: it does not care whether a
// step's task is one this build actually registers. That question belongs to
// [flowfile.Validate] — exercised directly in
// TestTasksInstall_ValidationAndExecutionCoherence — and to the engine at
// run time, not to Compile.
func TestCompile_DoesNotCheckTaskExistence(t *testing.T) {
	data := []byte(`
edition: v2026.3
name: hello
steps:
  - id: step1
    a_task_nobody_registered:
      foo: bar
`)

	workflow, diags, err := Compile(data)
	if err != nil {
		t.Fatalf("Compile: unexpected error for an unregistered task name: %v", err)
	}
	if diags != nil {
		t.Fatalf("Compile: unexpected diagnostics: %v", diags)
	}
	if workflow == nil {
		t.Fatal("Compile: workflow is nil")
	}
}

func TestCompile_NotYAMLIsAnErrorWithNoDiagnostics(t *testing.T) {
	// A document that never becomes a workflow tree at all is not a
	// [Diagnostics] failure — see [Compile]'s doc.
	data := []byte("\x00\x01\x02 not yaml at all {{{")

	workflow, _, err := Compile(data)
	if err == nil {
		t.Fatal("Compile: expected an error for malformed input")
	}
	if workflow != nil {
		t.Errorf("Compile: workflow should be nil on failure, got %v", workflow)
	}
}

func TestCompile_CallStepIsRefused(t *testing.T) {
	// Compiling from bytes has no file identity to resolve a `call:` step's
	// target against, so it must be refused rather than silently ignored.
	data := []byte(`
edition: v2026.3
name: caller
steps:
  - id: step1
    call: ./other-workflow.yaml
`)

	_, _, err := Compile(data)
	if err == nil {
		t.Fatal("Compile: expected a `call:` step compiled from bytes to be refused")
	}
}
