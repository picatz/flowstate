package flowfile_test

import (
	"fmt"
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

const simpleCalleeSource = `edition: v2026.3
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
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
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
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
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
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
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
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
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
	caller := writeFile(t, dir, "sub/caller.yaml", `edition: v2026.3
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
			caller := writeFile(t, dir, "caller-"+tt.name+".yaml", `edition: v2026.3
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

// TestCallEnumArgumentType is P2 of #621: a callee declaring an enum input is
// callable with a statically string-typed expression, because enum values
// travel as strings on the wire ([v1.StringShaped]) — the same shape
// [v1.CheckInputValue] already accepted for a literal. A non-member literal
// is still refused, and a statically mistyped expression (not a string at
// all) is still refused too, so the fix widens exactly the one comparison
// that was too narrow rather than the whole check.
func TestCallEnumArgumentType(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", `edition: v2026.3
name: callee
inputs:
  environment:
    type: enum
    values: [staging, production]
    required: true
steps:
  - id: a
    log:
      message: ${'env ' + inputs.environment}
`)

	tests := []struct {
		name    string
		with    string
		wantErr string // empty means "accepted"
	}{
		{
			name:    "member literal",
			with:    "environment: staging",
			wantErr: "",
		},
		{
			name:    "non-member literal",
			with:    "environment: canary",
			wantErr: `is "canary", which is not one of the values environment declares`,
		},
		{
			name:    "statically string-typed expression, a member",
			with:    "environment: ${\"staging\"}",
			wantErr: "",
		},
		{
			name:    "statically mistyped expression, not a string at all",
			with:    "environment: ${1 + 2}",
			wantErr: `with.environment is declared enum by workflow "callee", but this expression always produces int`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caller := writeFile(t, dir, "caller-"+tt.name+".yaml", `edition: v2026.3
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
	writeFile(t, dir, "callee.yaml", `edition: v2026.3
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
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
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
	writeFile(t, dir, "callee.yaml", `edition: v2026.3
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
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
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

	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
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

	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
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
	_, _, err := flowfile.Parse([]byte(`edition: v2026.3
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
	writeFile(t, dir, "a.yaml", `edition: v2026.3
name: a
steps:
  - id: next
    call: ./b.yaml
`)
	writeFile(t, dir, "b.yaml", `edition: v2026.3
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
			caller := writeFile(t, dir, "caller-"+tt.name+".yaml", `edition: v2026.3
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
// [conformance.CallCases] for the same claim asserted against both execution
// drivers.
func TestCallIsolationFlowfile(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", `edition: v2026.3
name: callee
steps:
  - id: peek
    log:
      message: ${string(has(steps.caller_step))}
`)
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
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

// buildCallDiamond writes a binary `call:` diamond of the given depth into dir
// and returns the root file's path.
//
// Files l0..l{depth-1} each `call:` the next file twice; leaf l{depth} holds
// leafSteps plain `log:` steps and calls nothing. Because a `call:` embeds the
// callee's compiled spec whole and nothing deduplicates a callee compiled more
// than once (see [v1.Call]'s doc), the leaf's steps appear 2^depth times in the
// fully-expanded tree — breadth multiplied by the fan-out of two at every
// level, the exact billion-laughs shape maxCallExpansionNodes bounds. The
// source files stay tiny; only the expansion is large, which is the point.
//
// depth is kept at or under [v1.MaxCallDepth] so the run reaches the expansion
// bound rather than tripping the depth bound first.
func buildCallDiamond(t *testing.T, dir string, depth, leafSteps int) string {
	t.Helper()

	var root string
	for i := range depth {
		var b strings.Builder
		fmt.Fprintf(&b, "edition: v2026.3\nname: l%d\nsteps:\n", i)
		// Two calls to the next file: the fan-out of two per level is what makes
		// the embedded copies multiply rather than add.
		fmt.Fprintf(&b, "  - id: c1\n    call: ./l%d.yaml\n", i+1)
		fmt.Fprintf(&b, "  - id: c2\n    call: ./l%d.yaml\n", i+1)
		path := writeFile(t, dir, fmt.Sprintf("l%d.yaml", i), b.String())
		if i == 0 {
			root = path
		}
	}

	var leaf strings.Builder
	fmt.Fprintf(&leaf, "edition: v2026.3\nname: l%d\nsteps:\n", depth)
	for s := range leafSteps {
		fmt.Fprintf(&leaf, "  - id: s%d\n    log:\n      message: hi\n", s)
	}
	writeFile(t, dir, fmt.Sprintf("l%d.yaml", depth), leaf.String())

	// The root — l0, the file a compile starts from — not the leaf written last.
	return root
}

// TestCallExpansionIsBounded is TestMergeExpansionIsBounded's sibling for
// `call:`, and the end-to-end proof for the same lesson: a diamond of calls
// multiplies breadth exactly as a repeated YAML alias does, so the compiler
// bounds the *total compiled node count* across the whole call tree
// (maxCallExpansionNodes = 100_000), not the depth or the per-file size.
//
// Every source file here is tiny; nothing in the tree is near any per-file
// limit. What is large is only the expansion — a depth-7 binary diamond embeds
// its leaf 2^7 = 128 times, so a leaf of ~850 log steps compiles to ~109k
// nodes, and the bound has to be the thing that stops it. The refusal is
// measured by its diagnostic, never by a wall clock: a bound test that reddens
// under load is worse than none, because "the box was busy" is the honest
// reading of a real regression too (the #246 lesson TestMergeExpansionIsBounded
// records).
//
// Asserted to both sides. The over case (~109k nodes) must be refused with the
// bound's own diagnostic; the under case, the identical shape one leaf-size step
// smaller (~90k nodes), must compile clean — which is what makes the refusal
// evidence the bound was *reached* rather than merely never crossed, and proves
// an ordinary large call tree is not rejected for being large.
func TestCallExpansionIsBounded(t *testing.T) {
	t.Parallel()

	// depth 7 (<= v1.MaxCallDepth) fans out to 2^7 = 128 embedded leaf copies.
	// 128 * 850 ~= 109k > 100k reaches the bound; 128 * 700 ~= 90k < 100k does
	// not. Both sides share the shape so only the leaf size decides the outcome.
	const depth = 7

	t.Run("over the bound is refused with the expansion diagnostic", func(t *testing.T) {
		t.Parallel()

		root := buildCallDiamond(t, t.TempDir(), depth, 850)

		_, _, err := flowfile.ParseFile(root)

		var ds flowfile.Diagnostics
		require.ErrorAs(t, err, &ds)
		// Reached, not merely survived — the bound's own sentence, so a tree
		// refused for some unrelated reason could not pass this by accident.
		require.Contains(t, ds.Error(), "meant to expand to",
			"the call-expansion bound is what should have stopped this")
		// Reported against a call step that overran the budget, not blamed on
		// the whole file: the diagnostic names the callee it was resolving.
		require.Contains(t, ds.Error(), ".yaml")
	})

	t.Run("under the bound compiles clean", func(t *testing.T) {
		t.Parallel()

		root := buildCallDiamond(t, t.TempDir(), depth, 700)

		workflow, _, err := flowfile.ParseFile(root)
		require.NoError(t, err,
			"a large-but-bounded call tree must still compile; the bound is a count, not a ban on calls")
		require.NotNil(t, workflow.GetSteps()[0].GetCall().GetWorkflow(),
			"the callee should have been embedded whole")
	})
}

func mustValidate(t *testing.T, path string) flowfile.Diagnostics {
	t.Helper()
	ds, err := flowfile.ValidateSourceFile(path)
	require.NoError(t, err)
	return ds
}
