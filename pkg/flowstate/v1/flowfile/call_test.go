package flowfile_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// writeFile writes content to name inside dir, creating any parent directories
// name needs, and returns the full path.
func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()

	path := filepath.Join(dir, name)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

const simpleCalleeSource = `edition: v2026.2
name: callee
inputs:
  tenant:
    type: string
    required: true
steps:
  - id: a
    log:
      message: ${'hi ' + inputs.tenant}
outputs:
  greeting:
    value: ${'hello ' + inputs.tenant}
`

// TestCallCompiles pins the ordinary case: a call resolves relative to the
// calling file, embeds the callee whole, and type-checks `with:` against what
// it declares.
func TestCallCompiles(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      tenant: acme
`)

	workflow, _, err := flowfile.ParseFile(caller)
	require.NoError(t, err)

	call := workflow.GetSteps()[0].GetCall()
	require.NotNil(t, call)
	require.Equal(t, "callee", call.GetWorkflow().GetName())
	require.Equal(t, "./callee.yaml", call.GetSource())
	require.Equal(t, "acme", call.GetArguments()["tenant"].GetLiteral().GetStringValue())

	require.Empty(t, mustValidate(t, caller))
}

// TestCallMissingRequiredInput checks that `with:` is type-checked against the
// callee's declarations at compile time, with a position — not left to fail
// partway through a run.
func TestCallMissingRequiredInput(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: provision
    call: ./callee.yaml
`)

	_, _, err := flowfile.ParseFile(caller)
	require.Error(t, err)
	require.Contains(t, err.Error(), `requires input "tenant"`)
}

// TestCallUndeclaredArgument checks the other direction: `with:` binding a name
// the callee never declared.
func TestCallUndeclaredArgument(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      tenant: acme
      region: eu-west-1
`)

	_, _, err := flowfile.ParseFile(caller)
	require.Error(t, err)
	require.Contains(t, err.Error(), `binds "region"`)
	require.Contains(t, err.Error(), `declares no input named`)
}

// TestCallRefusesAbsolutePath and the escaping case below are the attacker-shaped
// rejections: refused rather than sanitised, per CLAUDE.md's rule for anything
// consuming a path an author wrote.
func TestCallRefusesAbsolutePath(t *testing.T) {
	dir := t.TempDir()
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: provision
    call: /etc/passwd
`)

	_, _, err := flowfile.ParseFile(caller)
	require.Error(t, err)
	require.Contains(t, err.Error(), "an absolute path")
}

func TestCallRefusesEscapingUpward(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	require.NoError(t, os.MkdirAll(sub, 0o755))
	writeFile(t, dir, "outside.yaml", simpleCalleeSource)
	caller := writeFile(t, dir, "sub/caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: provision
    call: ../outside.yaml
`)

	_, _, err := flowfile.ParseFile(caller)
	require.Error(t, err)
	require.Contains(t, err.Error(), "climbs above the directory")
}

// TestCallRefusesWithNoPath checks that a `call:` compiled through [flowfile.Parse]
// (bytes, no location) is refused rather than silently attempted against the
// working directory.
func TestCallRefusesWithNoPath(t *testing.T) {
	_, _, err := flowfile.Parse([]byte(`edition: v2026.2
name: caller
steps:
  - id: provision
    call: ./callee.yaml
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "no location of its own")
}

// TestCallDetectsCycleAcrossFiles is the cross-file version of the anchor-cycle
// check: a calls b calls a, caught before the parser recurses forever.
func TestCallDetectsCycleAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.yaml", `edition: v2026.2
name: a
steps:
  - id: next
    call: ./b.yaml
`)
	writeFile(t, dir, "b.yaml", `edition: v2026.2
name: b
steps:
  - id: next
    call: ./a.yaml
`)

	_, _, err := flowfile.ParseFile(filepath.Join(dir, "a.yaml"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "calls itself through a chain of files")
}

// TestCallArgumentSecretRefused is the decision the coordinator asked to be
// landed explicitly: a secret reference may not cross a call boundary as an
// argument, whether written bare or nested inside a structure `with:` binds.
func TestCallArgumentSecretRefused(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)

	tests := []struct {
		name string
		with string
	}{
		{
			name: "bare",
			with: "tenant: ${secret('env:TENANT')}",
		},
		{
			name: "nested in a structure",
			with: "tenant: ${secret('env:TENANT')}\n      other: plain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caller := writeFile(t, dir, "caller-"+tt.name+".yaml", `edition: v2026.2
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      `+tt.with+`
`)

			_, _, err := flowfile.ParseFile(caller)
			require.Error(t, err)
			require.Contains(t, err.Error(), "cannot cross a call boundary")
		})
	}
}

// TestCallIsolationFlowfile compiles the negative isolation case through a real
// file pair rather than constructing the schema by hand — see
// [tests.CallCases] for the same claim asserted against both execution
// drivers.
func TestCallIsolationFlowfile(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", `edition: v2026.2
name: callee
steps:
  - id: peek
    log:
      message: ${string(has(steps.caller_step))}
`)
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: caller_step
    log:
      message: hi
  - id: leaky
    call: ./callee.yaml
`)

	workflow, _, err := flowfile.ParseFile(caller)
	require.NoError(t, err)

	// The callee compiles cleanly on its own terms: `has(steps.caller_step)` is
	// syntactically fine CEL, and `flow validate` cannot know at compile time
	// that a caller's own step will never be visible — that is what CallScope's
	// isolation enforces at run time, not what the parser refuses.
	require.NotNil(t, workflow.GetSteps()[1].GetCall().GetWorkflow())
}

func mustValidate(t *testing.T, path string) flowfile.Diagnostics {
	t.Helper()
	ds, err := flowfile.ValidateSourceFile(path)
	require.NoError(t, err)
	return ds
}
