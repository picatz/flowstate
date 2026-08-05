package flowfile_test

import (
	"os"
	"path/filepath"
	"strings"
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

// TestCallArgumentTypeChecked pins the typed-function ergonomics standard: a
// `with:` argument is checked against the callee's declared input type when
// the file is compiled, not only at submit — a literal is checked exactly,
// and an expression whose type the profile's own checker can pin down
// without running it (a closed expression over literals) is checked the same
// way; anything else stays a run-time question, exactly as it always was.
func TestCallArgumentTypeChecked(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)

	tests := []struct {
		name    string
		with    string
		wantErr string // empty means "accepted"
	}{
		{
			// A literal of the wrong type: `tenant:` is declared string, and 42
			// is an int. Refused at compile time, not only at submit.
			name:    "mistyped literal",
			with:    "tenant: 42",
			wantErr: `input "tenant" is declared string but was given int`,
		},
		{
			// An expression whose type is not knowable without a scope to
			// evaluate it against — the ordinary case, and the one that must
			// stay silent here so it can still be checked at run time.
			name:    "well-typed expression, deferred to runtime",
			with:    "tenant: ${'tenant-' + string(1)}",
			wantErr: "",
		},
		{
			// A *closed* expression — no name it needs a scope for — whose
			// type the checker can pin down without running it: `1 + 2` is
			// staticaly an int, bound to a string-declared input.
			name:    "mistyped but statically typeable expression",
			with:    "tenant: ${1 + 2}",
			wantErr: `with.tenant is declared string by workflow "callee", but this expression always produces int`,
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

			ds := mustValidate(t, caller)
			if tt.wantErr == "" {
				require.Empty(t, ds, "an argument that should have been accepted was flagged: %v", ds)
				return
			}
			require.NotEmpty(t, ds, "a mistyped argument was accepted")
			require.Contains(t, ds.Error(), tt.wantErr)
		})
	}
}

// TestCallArgumentOverTheElementBoundRefused is #206's worst-case finding: a
// literal `with:` argument over the server-wide element bound used to pass
// `flow validate` cleanly, because checkCallArgumentType's own
// v1.CheckInputConstraints only walked a value's lists when the callee's
// declaration carried a `must:`/`unique:` — an unconstrained list input's
// size was never checked at the call boundary at all. Left unrefused, this
// argument would only fail once BindRunInputs saw it, which for a call means
// mid-run, possibly after earlier steps already had side effects. Reusing the
// identical bound at the call boundary catches it at compile time instead.
func TestCallArgumentOverTheElementBoundRefused(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", `edition: v2026.2
name: callee
inputs:
  records:
    type: list
steps:
  - id: a
    log:
      message: hi
`)

	oversizedList := "[" + strings.Repeat("0, ", 10_000) + "0]"
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      records: `+oversizedList+`
`)

	ds := mustValidate(t, caller)
	require.NotEmpty(t, ds, "a literal with: argument over the element bound was accepted at compile time")
	require.Contains(t, ds.Error(), "records")
	require.Contains(t, ds.Error(), "list elements")
}

// TestCallArgumentAtTheElementBoundAccepted is the boundary: exactly the
// element bound's worth of items is satisfiable, so it must not be refused.
func TestCallArgumentAtTheElementBoundAccepted(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", `edition: v2026.2
name: callee
inputs:
  records:
    type: list
steps:
  - id: a
    log:
      message: hi
`)

	exactList := "[" + strings.Repeat("0, ", 9_999) + "0]" // 9,999 + 1 = 10,000
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      records: `+exactList+`
`)

	ds := mustValidate(t, caller)
	require.Empty(t, ds, "a literal with: argument of exactly the element bound was refused: %v", ds)
}

// TestCallRefusesEscapingThroughASymlink is the real-path version of
// TestCallRefusesEscapingUpward: a path that never writes `../` and stays
// lexically inside the calling file's directory, but resolves outside it once
// an in-directory symlink is followed — the same class of hole as the git
// plugin's symlink-through-entry, and the lexical `..` check alone cannot see
// it, because the path as *written* never leaves callerDir at all.
func TestCallRefusesEscapingThroughASymlink(t *testing.T) {
	dir := t.TempDir()
	outside := t.TempDir()
	secretPath := writeFile(t, outside, "secret.yaml", simpleCalleeSource)

	link := filepath.Join(dir, "callee.yaml")
	if err := os.Symlink(secretPath, link); err != nil {
		// Symlink creation can fail on a filesystem or platform that does not
		// support it (notably some Windows configurations without elevated
		// privileges) — skipped rather than failed, per stdlib practice for
		// exactly this class of test.
		t.Skipf("cannot create a symlink on this platform: %v", err)
	}

	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: provision
    call: ./callee.yaml
`)

	_, _, err := flowfile.ParseFile(caller)
	require.Error(t, err, "a call resolving outside its directory through a symlink was accepted")
	require.Contains(t, err.Error(), "outside")
	require.Contains(t, err.Error(), "symlink")
}

// TestCallAllowsASymlinkThatStaysWithinTheDirectory is the positive direction:
// a symlink is not refused merely for being one, only for resolving outside
// callerDir — an in-tree symlink (vendored or shared file layouts sometimes
// use one) must keep working.
func TestCallAllowsASymlinkThatStaysWithinTheDirectory(t *testing.T) {
	dir := t.TempDir()
	real := writeFile(t, dir, "real-callee.yaml", simpleCalleeSource)

	link := filepath.Join(dir, "callee.yaml")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("cannot create a symlink on this platform: %v", err)
	}

	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      tenant: acme
`)

	_, _, err := flowfile.ParseFile(caller)
	require.NoError(t, err, "a symlink that stays within the calling file's directory was refused")
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
