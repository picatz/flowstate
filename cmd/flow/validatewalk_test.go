package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestValidateAcceptsADirectory is #394's central claim: `flow validate demo`
// walks demo exactly as `flow fix demo` and `flow test demo` already do,
// instead of refusing with a bare directory error. Both a workflow and a
// Flowfile test live in the directory, and both must be found and checked.
func TestValidateAcceptsADirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(cleanWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(
		"tests:\n  - name: it runs\n    workflow: ./workflow.yaml\n    expect:\n      ran: [s]\n"), 0o600))

	out, err := validateOutput(t, dir)
	require.NoError(t, err, "output: %s", out)

	require.Contains(t, out, "workflow.yaml")
	require.Contains(t, out, "workflow.test.yaml")
	require.Contains(t, out, "ok")
	require.NotContains(t, out, "is a directory")
}

// TestValidateOnADirectoryReportsATestFilesOwnSchemaProblem is the negative
// direction: a Flowfile test with a structural mistake is still found by the
// directory walk and reported as a diagnostic under its own schema, not
// silently accepted because it does not compile as a workflow.
func TestValidateOnADirectoryReportsATestFilesOwnSchemaProblem(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(cleanWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(
		"tests:\n  - name: broken\n    workflow: ./workflow.yaml\n    expect:\n      ran: \"s\"\n"), 0o600))

	out, err := validateOutput(t, dir)
	require.Error(t, err, "output: %s", out)
	require.Contains(t, out, "workflow.test.yaml")
}

// TestValidateOnAMissingFileMatchesRunLocalsWording is #394's second, smaller
// claim: a missing file is reported in the same shape `flow run local`
// already uses for the same failure, the path once as the position, then
// Go's own `open <path>: ...`, rather than the `error reading X: open X:
// ...` wrap that repeated the "reading" framing a second time for no fact the
// first mention had not already given.
func TestValidateOnAMissingFileMatchesRunLocalsWording(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	missing := filepath.Join(dir, "nope.yaml")

	err := runFlow(t, "validate", missing).Err
	require.Error(t, err)
	require.NotContains(t, err.Error(), "error reading")
	require.Equal(t, missing+": open "+missing+": no such file or directory", err.Error())
}

// TestValidateJSONOnADirectoryReportsBothFiles is [TestValidateAcceptsADirectory]
// through the machine surface, which walks a separate code path
// ([validateMachine]) that has to agree with the text one.
func TestValidateJSONOnADirectoryReportsBothFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(cleanWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(
		"tests:\n  - name: it runs\n    workflow: ./workflow.yaml\n    expect:\n      ran: [s]\n"), 0o600))

	out, err := validateOutput(t, dir, "-o", "json")
	require.NoError(t, err, "output: %s", out)

	var report struct {
		Files []struct {
			File        string `json:"file"`
			Diagnostics []any  `json:"diagnostics"`
		} `json:"files"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &report))
	require.Len(t, report.Files, 2)
}

// validateStdin runs `flow validate -` with body on stdin and returns stdout.
func validateStdin(t *testing.T, body string, extra ...string) (string, error) {
	t.Helper()

	res := runFlowStdin(t, body, append([]string{"validate", "-"}, extra...)...)

	return res.Stdout, res.Err
}

// TestValidateDashReadsAWorkflowFromStdin is #397's central claim: `flow
// validate -` reads the document from stdin instead of trying to open a file
// literally named "-", which is what it did before.
func TestValidateDashReadsAWorkflowFromStdin(t *testing.T) {
	t.Parallel()

	out, err := validateStdin(t, cleanWorkflow)
	require.NoError(t, err, "output: %s", out)
	require.Contains(t, out, "-: ")
	require.Contains(t, out, "ok")
}

// TestValidateDashReadsATestFileFromStdin proves stdin is checked under the
// right schema, not forced through the workflow validator regardless of what
// it actually holds: a Flowfile test piped in still validates as a test.
func TestValidateDashReadsATestFileFromStdin(t *testing.T) {
	t.Parallel()

	out, err := validateStdin(t, "tests:\n  - name: it runs\n    workflow: ./workflow.yaml\n    expect:\n      ran: [s]\n")
	require.NoError(t, err, "output: %s", out)
	require.Contains(t, out, "ok")
}

// TestValidateDashReportsAMalformedDocumentFromStdin is the negative
// direction: a broken document piped in is still reported as a diagnostic,
// not silently accepted just because it arrived over a pipe instead of a
// file.
func TestValidateDashReportsAMalformedDocumentFromStdin(t *testing.T) {
	t.Parallel()

	out, err := validateStdin(t, brokenWorkflow)
	require.Error(t, err, "output: %s", out)
	require.Contains(t, out, "-:")
}

// TestValidateDashCannotBeCombinedWithAnotherPath is #397's stated boundary:
// stdin is exactly one document, so mixing "-" with a real path in one
// invocation is refused with a message naming why, rather than silently
// picking one of the two or reading neither.
func TestValidateDashCannotBeCombinedWithAnotherPath(t *testing.T) {
	t.Parallel()

	path := writeWorkflow(t, "workflow.yaml", cleanWorkflow)

	out, err := validateStdin(t, cleanWorkflow, path)
	require.Error(t, err, "output: %s", out)
	require.Contains(t, err.Error(), "cannot be combined")
}

// TestValidateDashRefusesStdinPastTheByteLimit is the bound CLAUDE.md
// requires for anything reading untrusted input: stdin is refused rather
// than read without limit, matching the same document bound a file on disk
// gets.
func TestValidateDashRefusesStdinPastTheByteLimit(t *testing.T) {
	t.Parallel()

	oversized := strings.Repeat("a", maxValidateDocumentBytes+1)

	out, err := validateStdin(t, oversized)
	require.Error(t, err, "output: %s", out)
	require.Contains(t, err.Error(), "exceeds")
}

// TestValidateOnAnEmptyDirectoryIsRefused is the sibling refusal
// [collectFlowfiles] and [collectTestFiles] already give: a directory with no
// Flowfile and no Flowfile test in it must not exit 0 having silently
// checked nothing, which reads as CI having validated a path it never
// actually found anything in.
func TestValidateOnAnEmptyDirectoryIsRefused(t *testing.T) {
	t.Parallel()

	out, err := validateOutput(t, t.TempDir())
	require.Error(t, err, "output: %s", out)
	require.Contains(t, err.Error(), "no Flowfiles")
}

// TestValidateDirectoryWalkDoesNotReadAnOversizedFileWhole is #394's bound
// finding: a directory can hold a file of any size a caller (or an attacker)
// chose, and classifying it must not read the whole thing into memory first
// only to have flowfile's own size check answer false; the read itself has
// to be bounded, the same way stdin already is. The oversized file is
// skipped outright (never reported, ok or not) while a genuine, ordinary
// Flowfile beside it in the same directory is still found and validated,
// which is what proves the walk kept going rather than aborting.
func TestValidateDirectoryWalkDoesNotReadAnOversizedFileWhole(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(cleanWorkflow), 0o600))

	oversized := strings.Repeat("x", maxValidateDocumentBytes+1)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "huge.yaml"), []byte("name: huge\n"+oversized), 0o600))

	out, err := validateOutput(t, dir)
	require.NoError(t, err, "output: %s", out)
	require.Contains(t, out, "workflow.yaml")
	require.NotContains(t, out, "huge.yaml")
}

// TestValidateReportsMisspelledSignalNameInTestFile is #1443's validate-surface
// claim: `flow validate` compiles the referenced workflow and checks each
// scripted signal's name against its declared gates, so a typo is caught before
// `flow test` rather than passing green with a signal delivered into the void.
func TestValidateReportsMisspelledSignalNameInTestFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(`
edition: v2026.3
name: gated
steps:
  - id: gate
    wait_for_signal:
      name: approve
      timeout: 10s
outputs: {}
`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(`
tests:
  - name: misspelled signal
    workflow: ./workflow.yaml
    signals:
      - name: aprove
        at: 1s
        payload: {}
    expect:
      outputs: {}
`), 0o600))

	out, err := validateOutput(t, dir)
	require.Error(t, err, "output: %s", out)
	require.Contains(t, out, `"aprove"`)
	require.Contains(t, out, "approve")
}

// gatedWorkflow waits on one signal, so a scripted signal naming anything else
// is refused by the check `flow validate` runs over a test file (#1443).
const gatedWorkflow = `edition: v2026.3
name: gated
steps:
  - id: gate
    wait_for_signal:
      name: approve
      timeout: 10s
`

// TestValidateDoesNotPrintASecretSubstitutedIntoASignalName is the leak the
// signal-name check arrived with.
//
// A file's `vars:` are substituted into fixture positions at load, and
// `signals[].name` is one of them exactly as a case's `secrets:` value is — so
// one var can be both, which is an ordinary way to write a fixture that signs
// and then signals with the same material. When such a name matches no gate,
// `checkSignalNames` quotes it with %q, and this command wrote that sentence
// straight to a terminal and into its `-o json` report. `flow test` clears the
// identical sentence against the identical posture; `flow validate` did not,
// which is one value with one meaning rendered two ways (Codex).
//
// Both streams and both formats, because the diagnostic reaches stdout as text
// and travels again inside the machine report.
func TestValidateDoesNotPrintASecretSubstitutedIntoASignalName(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-9f4c2a7e"

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(gatedWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(
		"vars:\n"+
			"  token: "+secret+"\n"+
			"tests:\n"+
			"  - name: the gate is signalled\n"+
			"    workflow: ./workflow.yaml\n"+
			"    secrets:\n"+
			"      env:VENDOR_TOKEN: ${vars.token}\n"+
			"    signals:\n"+
			"      - name: ${vars.token}\n"+
			"        at: 1s\n"+
			"        payload: {}\n"+
			"    expect:\n"+
			"      ran: [gate]\n"), 0o600))

	for _, format := range []string{"text", "json"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()

			res := runFlow(t, "validate", "-o", format, dir)
			require.Error(t, res.Err, "the scripted signal names no gate, so this must be refused")

			require.NotContains(t, res.Output(), secret,
				"a value the case declares under `secrets:` reached the validate output")
			require.NotContains(t, res.Err.Error(), secret,
				"nor the error the command returns, which is what main prints")

			// A redaction, not a withholding: the author still has to be able
			// to see which case and which position the refusal is about.
			require.Contains(t, res.Output(), "the gate is signalled")
			require.Contains(t, res.Output(), "matches no gate")
		})
	}
}

// TestValidateRedactsASignalNameInATableRow is the same leak reached through a
// `cases:` row rather than a plain test.
//
// Rows are expanded at load, and a row that declares no `secrets:` inherits the
// entry's, so the posture a row's diagnostic is rendered under is the entry's
// too. Worth its own case because `validateTestFile` walks the *expanded*
// tests: if it ever walked the unexpanded ones, a row's signal name would be
// checked against a posture built from a test that is not the one it came from.
func TestValidateRedactsASignalNameInATableRow(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-row-4b21e"

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(gatedWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(
		"vars:\n"+
			"  token: "+secret+"\n"+
			"tests:\n"+
			"  - name: the gate is signalled\n"+
			"    workflow: ./workflow.yaml\n"+
			"    secrets:\n"+
			"      env:VENDOR_TOKEN: ${vars.token}\n"+
			"    cases:\n"+
			"      - name: by the wrong name\n"+
			"        signals:\n"+
			"          - name: ${vars.token}\n"+
			"            at: 1s\n"+
			"            payload: {}\n"+
			"        expect:\n"+
			"          ran: [gate]\n"), 0o600))

	res := runFlow(t, "validate", dir)
	require.Error(t, res.Err, "the scripted signal names no gate, so this must be refused")

	require.NotContains(t, res.Output(), secret,
		"a row inherited its entry's secrets, and the diagnostic printed one anyway")
	require.Contains(t, res.Output(), "matches no gate")
}

// TestValidateRedactsASignalNameDerivedFromASecret covers the other half of the
// posture: not a case's `secrets:` plaintext, but the material a `vars:` entry
// is withheld for because it was computed from one.
//
// `${'gate-' + vars.token}` is tainted by the secret it reads, so the loader
// records its whole value as withheld material — and a signal name substituted
// from it is that material, reaching the diagnostic under a different name.
// The two halves are separate lists inside the posture, so a redaction built
// from only one of them would pass the test above and leak here.
func TestValidateRedactsASignalNameDerivedFromASecret(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-derived-77c0"

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(gatedWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(
		"vars:\n"+
			"  token: "+secret+"\n"+
			"  gate: \"${'gate-' + vars.token}\"\n"+
			"tests:\n"+
			// The seed lives on a different case, deliberately. With the
			// `secrets:` entry on the signalling case, its own plaintext is in
			// the posture and the substring backstop clears `gate-<secret>`
			// without the withheld half ever being consulted — the test would
			// pass with that half deleted (flowstate-reviewer).
			"  - name: the case that holds the secret\n"+
			"    workflow: ./workflow.yaml\n"+
			"    secrets:\n"+
			"      env:VENDOR_TOKEN: ${vars.token}\n"+
			"    expect:\n"+
			"      ran: [gate]\n"+
			"  - name: the gate is signalled\n"+
			"    workflow: ./workflow.yaml\n"+
			"    signals:\n"+
			"      - name: ${vars.gate}\n"+
			"        at: 1s\n"+
			"        payload: {}\n"+
			"    expect:\n"+
			"      ran: [gate]\n"), 0o600))

	res := runFlow(t, "validate", dir)
	require.Error(t, res.Err)

	require.NotContains(t, res.Output(), secret,
		"the secret a withheld var was computed from reached the diagnostic")
	require.NotContains(t, res.Output(), "gate-"+secret,
		"the withheld var's own material reached the diagnostic")
	require.Contains(t, res.Output(), "matches no gate")
}

// TestValidateRedactsASignalNameEscapedByQuoting is the escaping half of the
// containment rule, which the value set cannot see on its own.
//
// `checkSignalNames` quotes the offending name with %q, and %q *transforms* a
// value holding a newline, a tab, a quote or a backslash before the redaction
// ever reads the sentence. The posture searches for the plaintext as written,
// which no longer occurs in it, so the escaped spelling printed — and an
// escaped secret is a secret (Codex).
func TestValidateRedactsASignalNameEscapedByQuoting(t *testing.T) {
	t.Parallel()

	// A tab, because YAML can carry one inside a double-quoted scalar and %q
	// renders it `\t`. The assertion is against the escaped spelling, since
	// the raw one is not what reaches the output.
	const secret = "sk-live\tescaped-9f31"

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(gatedWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(
		"vars:\n"+
			"  token: \"sk-live\\tescaped-9f31\"\n"+
			"tests:\n"+
			"  - name: the gate is signalled\n"+
			"    workflow: ./workflow.yaml\n"+
			"    secrets:\n"+
			"      env:VENDOR_TOKEN: ${vars.token}\n"+
			"    signals:\n"+
			"      - name: ${vars.token}\n"+
			"        at: 1s\n"+
			"        payload: {}\n"+
			"    expect:\n"+
			"      ran: [gate]\n"), 0o600))

	res := runFlow(t, "validate", dir)
	require.Error(t, res.Err)

	require.NotContains(t, res.Output(), secret,
		"the plaintext reached the output")
	require.NotContains(t, res.Output(), `sk-live\tescaped-9f31`,
		"the %q-escaped spelling of the secret reached the output")
	require.Contains(t, res.Output(), "matches no gate")
}

// TestValidateRedactsASignalNameFromASensitiveInput is the half of the posture
// only this caller can build.
//
// `runCase` widens its posture with the run's own sensitive values once the
// inputs are bound, so at run time a `sensitive:` input scripted as a signal
// name is already covered. `flow validate` never runs anything — but it does
// compile the workflow to check the names against it, so it holds both the
// declaration and the case's `inputs:`, which is everything needed to cover
// the same values without a run (Copilot).
func TestValidateRedactsASignalNameFromASensitiveInput(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-bound-3e77"

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(`edition: v2026.3
name: gated
inputs:
  token:
    type: string
    sensitive: true
    required: true
steps:
  - id: gate
    wait_for_signal:
      name: approve
      timeout: 10s
`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(
		"tests:\n"+
			"  - name: the gate is signalled\n"+
			"    workflow: ./workflow.yaml\n"+
			"    inputs:\n"+
			"      token: "+secret+"\n"+
			"    signals:\n"+
			"      - name: "+secret+"\n"+
			"        at: 1s\n"+
			"        payload: {}\n"+
			"    expect:\n"+
			"      ran: [gate]\n"), 0o600))

	res := runFlow(t, "validate", dir)
	require.Error(t, res.Err)

	require.NotContains(t, res.Output(), secret,
		"a value bound to a `sensitive:` input reached the signal-name diagnostic")
	require.Contains(t, res.Output(), "matches no gate")
}

// TestValidateRedactsASignalNameFromAnotherCasesLiteralSecretSeed is #2041's
// cross-case route: `token` is a literal var, named straight from the first
// case's `secrets:` with no `${...}` fence of its own, and the second case
// never declares a `secrets:` entry at all. It only reaches `token` through
// `${vars.token}` substitution, so nothing but the file-wide withheld set
// protects it — and that set used to exclude a literal seed on the assumption
// its plaintext reached only the case that named it, which substitution makes
// false: a fixture position may put `${vars.x}` in any case in the file.
func TestValidateRedactsASignalNameFromAnotherCasesLiteralSecretSeed(t *testing.T) {
	t.Parallel()

	const secret = "sk-live-crosstest-1234"

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(gatedWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(
		"vars:\n"+
			"  token: "+secret+"\n"+
			"tests:\n"+
			"  - name: the case that holds the secret\n"+
			"    workflow: ./workflow.yaml\n"+
			"    secrets:\n"+
			"      env:VENDOR_TOKEN: ${vars.token}\n"+
			"    expect:\n"+
			"      ran: [gate]\n"+
			"  - name: the case that never named the secret\n"+
			"    workflow: ./workflow.yaml\n"+
			"    signals:\n"+
			"      - name: ${vars.token}\n"+
			"        at: 1s\n"+
			"        payload: {}\n"+
			"    expect:\n"+
			"      ran: [gate]\n"), 0o600))

	res := runFlow(t, "validate", dir)
	require.Error(t, res.Err, "the scripted signal names no gate, so this must be refused")

	require.NotContains(t, res.Output(), secret,
		"a var seeded from one case's `secrets:` printed in full for another (#2041)")
	require.Contains(t, res.Output(), "matches no gate")
}

// TestValidateRedactsASignalNameFromAnEntrysSecretWhenARowDeclaresItsOwn is
// #2041's table route: a row's `secrets:` replaces its entry's `Secrets` map
// wholesale (deliberately — see table.go's own doc), which used to also drop
// the entry's plaintext from the posture that row's diagnostics render
// through, since nothing else carried it. `flowtest`'s unexported
// `entrySecretMaterial` is what protects it now, read once per entry and
// shared by every row for redaction only, independent of what `Secrets`
// itself binds.
//
// The entry's secret is a plain literal rather than a `${vars.x}` reference,
// so the var-taint closure never reaches it — proving this route
// independently of the cross-case one above, which that closure would
// otherwise cover regardless of how `secrets:` merges.
func TestValidateRedactsASignalNameFromAnEntrysSecretWhenARowDeclaresItsOwn(t *testing.T) {
	t.Parallel()

	const entrySecret = "sk-live-entrymat-8821"
	const rowSecret = "sk-live-rowown-4402"

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(gatedWorkflow), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(
		"tests:\n"+
			"  - name: entry\n"+
			"    workflow: ./workflow.yaml\n"+
			"    secrets:\n"+
			"      env:VENDOR_TOKEN: "+entrySecret+"\n"+
			"    cases:\n"+
			"      - name: row\n"+
			"        secrets:\n"+
			"          env:ROW_TOKEN: "+rowSecret+"\n"+
			"        signals:\n"+
			"          - name: "+entrySecret+"\n"+
			"            at: 1s\n"+
			"            payload: {}\n"+
			"        expect:\n"+
			"          ran: [gate]\n"), 0o600))

	res := runFlow(t, "validate", dir)
	require.Error(t, res.Err, "the scripted signal names no gate, so this must be refused")

	require.NotContains(t, res.Output(), entrySecret,
		"a row declaring its own `secrets:` lost the entry's from its posture (#2041)")
	require.Contains(t, res.Output(), "matches no gate")
}
