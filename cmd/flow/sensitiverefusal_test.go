package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// This file is #2044: three refusal surfaces the redaction set #2042 built
// for `refuseRunLocally` could not see, each traced to a different reason and
// fixed a different way. sensitiveRefusalWorkflow, defined in
// runlocal_refusal_test.go, is reused for the third case rather than copied,
// since the point of that fixture — a sensitive input and an ordinary one
// beside it, so a test can tell a redaction from a withholding — is exactly
// what these two surfaces need as well.

// overflowInputFileWorkflow declares a sensitive int input and an ordinary
// one beside it, the same shape sensitiveRefusalWorkflow uses for a flag's
// refusal, but sized for a file's: no `must:`, because the numeric-overflow
// refusal under test happens during conversion, before a bind — and before
// there is a *v1.Value for a `must:` to be checked against at all.
const overflowInputFileWorkflow = `edition: v2026.3
name: onboard-file
inputs:
  pin:
    type: int
    sensitive: true
  region:
    type: int
steps:
  - id: greet
    log:
      message: onboarding
`

// overflowNumber is a JSON number no int64 or float64 can carry — an exponent
// past what either can represent, so [normalizeJSON]'s own refusal quotes it
// verbatim rather than reporting a value it managed to convert.
const overflowNumber = "1e999"

// writeInputsFile writes a JSON object of arguments to a temp file and
// returns its path, for the two cases below that need --input-file rather
// than --input.
func writeInputsFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "inputs.json")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// writeWorkflowFile writes a Flowfile to a temp path, the way runLocal does
// for `flow run local`, for the two commands below that take a path directly
// rather than through that helper.
func writeWorkflowFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// TestInputFileNumericOverflowRedactsASensitiveField is #2044's first case:
// inputsFromJSON's numeric-overflow refusal quotes the raw JSON number
// verbatim, runInputs returns no submitted map on this path, and
// sensitiveInputWords only ever read --input flags — so a --input-file value
// too large for its declared type to carry reached both the JSON document
// and stderr in the clear for a `sensitive: true` input.
func TestInputFileNumericOverflowRedactsASensitiveField(t *testing.T) {
	t.Parallel()

	path := writeInputsFile(t, `{"pin": `+overflowNumber+`}`)

	stdout, stderr, err := runLocal(t, overflowInputFileWorkflow, "--output", "json", "--input-file", path)
	require.Error(t, err, "an out-of-range number is refused")

	assert.NotContains(t, stdout, overflowNumber,
		"the value reached the document a machine caller reads")
	assert.NotContains(t, stderr, overflowNumber,
		"the value reached the prose a person reads")
	assert.NotContains(t, err.Error(), overflowNumber,
		"the value reached the error the command returns, which is what main prints")

	// A redaction, not a withholding: the sentence still says which input it
	// is about.
	assert.Contains(t, stdout, "pin", "the refusal no longer says which input it is about")
}

// TestInputFileNumericOverflowLeavesAnOrdinaryFieldInTheClear is the
// direction the fix must not take with it: a field the workflow never
// declared sensitive keeps its value in the refusal, or the fix has widened
// into a withholding.
func TestInputFileNumericOverflowLeavesAnOrdinaryFieldInTheClear(t *testing.T) {
	t.Parallel()

	path := writeInputsFile(t, `{"region": `+overflowNumber+`}`)

	_, stderr, err := runLocal(t, overflowInputFileWorkflow, "--input-file", path)
	require.Error(t, err, "an out-of-range number is refused")

	assert.Contains(t, stderr, overflowNumber,
		"an ordinary field's overflow value was withheld, not only a sensitive one's")
}

// TestInputFileNumericOverflowNestedInAStructIsAlsoRedacted covers the case a
// flat collector would miss: the sensitive declaration is a struct, and the
// overflowing number is one of its fields rather than the field itself.
// normalizeJSON recurses through the whole value carrying the declared name
// at every depth, so the refusal names "creds" however deep the number that
// broke it was — and [numericOverflowError] carries that same name up from
// whatever depth [normalizeJSON] was at when it failed.
func TestInputFileNumericOverflowNestedInAStructIsAlsoRedacted(t *testing.T) {
	t.Parallel()

	nested := `edition: v2026.3
name: onboard-nested
inputs:
  creds:
    type: struct
    sensitive: true
steps:
  - id: greet
    log:
      message: onboarding
`
	path := writeInputsFile(t, `{"creds": {"limit": 5, "pin": `+overflowNumber+`}}`)

	stdout, stderr, err := runLocal(t, nested, "--output", "json", "--input-file", path)
	require.Error(t, err, "an out-of-range number nested in a struct is refused")

	assert.NotContains(t, stdout, overflowNumber)
	assert.NotContains(t, stderr, overflowNumber)
	assert.NotContains(t, err.Error(), overflowNumber)
	assert.Contains(t, stdout, "creds", "the refusal no longer says which input it is about")
}

// TestStructuredInputFlagNumericOverflowRedactsASensitiveField is #2044's
// second review finding: valueFromJSON and normalizeJSON are also what
// decodes a structured --input flag's own JSON (coerceInput, for a `list` or
// `struct` declaration), not only --input-file's document. Before the fix,
// [sensitiveInputWords] read only the flag's whole word — `creds={"pin":1e999}`
// — as a plaintext, which the substring backstop matches only where that
// exact text occurs; the overflowing number rendered inside the refusal
// sentence by itself, `holds 1e999`, is a different string and slipped
// through. Reading [numericOverflowError] off the refusal instead of the flag
// text redacts the number itself, for this source exactly as for the file's.
func TestStructuredInputFlagNumericOverflowRedactsASensitiveField(t *testing.T) {
	t.Parallel()

	structured := `edition: v2026.3
name: onboard-struct-flag
inputs:
  creds:
    type: struct
    sensitive: true
steps:
  - id: greet
    log:
      message: onboarding
`
	stdout, stderr, err := runLocal(t, structured, "--output", "json",
		"--input", `creds={"pin": `+overflowNumber+`}`)
	require.Error(t, err, "an out-of-range number in a structured flag's JSON is refused")

	assert.NotContains(t, stdout, overflowNumber)
	assert.NotContains(t, stderr, overflowNumber)
	assert.NotContains(t, err.Error(), overflowNumber)
	assert.Contains(t, stdout, "creds", "the refusal no longer says which input it is about")
}

// TestStructuredInputFlagNumericOverflowLeavesAnOrdinaryFieldInTheClear is
// the negative direction of the case above.
func TestStructuredInputFlagNumericOverflowLeavesAnOrdinaryFieldInTheClear(t *testing.T) {
	t.Parallel()

	structured := `edition: v2026.3
name: onboard-struct-flag-ordinary
inputs:
  meta:
    type: struct
steps:
  - id: greet
    log:
      message: onboarding
`
	_, stderr, err := runLocal(t, structured, "--input", `meta={"count": `+overflowNumber+`}`)
	require.Error(t, err, "an out-of-range number in a structured flag's JSON is refused")

	assert.Contains(t, stderr, overflowNumber,
		"an ordinary field's overflow value in a structured flag was withheld, not only a sensitive one's")
}

// sensitiveScheduleWorkflow is sensitiveRefusalWorkflow (runlocal_refusal_test.go)
// with a `triggers: schedule:` block, which `flow schedule create` refuses
// the command line for not having before it ever reaches the arguments —
// [sensitiveRefusalWorkflow] itself declares none, so a copy is needed here
// rather than a shared fixture the local-only test would also have to carry.
const sensitiveScheduleWorkflow = `edition: v2026.3
name: onboard-schedule
triggers:
  schedule:
    cron: "0 * * * *"
inputs:
  pin:
    type: int
    sensitive: true
    must: this > 9999
  region:
    type: string
    default: eu-west-1
steps:
  - id: greet
    log:
      message: ${"onboarding " + inputs.region}
`

// TestARemoteRunRefusalDoesNotPrintASensitiveArgument and
// TestAScheduleCreateRefusalDoesNotPrintASensitiveArgument are #2044's third
// case: `flow run` and `flow schedule create` call the identical
// runInputs/checkRunInputs pair `flow run local` does, and returned the bare
// error — no redaction at all, on either the coercion refusal or the binder's
// own. Neither writes a machine-readable document for this refusal (neither
// did before this fix, and adding one is a separate change this issue does
// not ask for), so only the two streams a person and a script both read are
// asserted here, the same two runlocal_refusal_test.go's sensitive tests
// check beside the document.
//
// Both refusals happen before any server address is resolved or dialed —
// runInputs and checkRunInputs are checked ahead of the client construction
// in both commands — so neither test passes --address and neither needs a
// server listening anywhere.
func TestARemoteRunRefusalDoesNotPrintASensitiveArgument(t *testing.T) {
	t.Parallel()

	for name, tt := range map[string]struct {
		args  []string
		value string
	}{
		"a submitted value that fails the declared must:": {
			args:  []string{"--input", "pin=4321"},
			value: "4321",
		},
		"a word the flag cannot coerce to the declared type": {
			args:  []string{"--input", "pin=hunter2"},
			value: "hunter2",
		},
		// #2073: a one-rune coercion word is short enough to survive
		// [v1.SensitiveValues.WithValues]'s two-rune substring floor, so the
		// fix has to keep this refusal from quoting the word at construction
		// rather than rely on that backstop to catch it afterward. Covered
		// here too since `flow run` reaches [inputCoercionError] through the
		// identical [runInputs] this table already exercises.
		"a one-rune word the flag cannot coerce to the declared type": {
			args:  []string{"--input", "pin=x"},
			value: "x",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path := writeWorkflowFile(t, sensitiveRefusalWorkflow)
			res := runFlow(t, append([]string{"run", path}, tt.args...)...)
			require.Error(t, res.Err, "the command line is refused")

			assert.NotContains(t, res.Stdout, tt.value)
			assert.NotContains(t, res.Stderr, tt.value,
				"the value reached the prose a person reads")
			assert.NotContains(t, res.Err.Error(), tt.value,
				"the value reached the error the command returns, which is what main prints")
			assert.Contains(t, res.Output(), "pin",
				"the refusal no longer says which input it is about")
		})
	}
}

func TestAScheduleCreateRefusalDoesNotPrintASensitiveArgument(t *testing.T) {
	t.Parallel()

	for name, tt := range map[string]struct {
		args  []string
		value string
	}{
		"a submitted value that fails the declared must:": {
			args:  []string{"--input", "pin=4321"},
			value: "4321",
		},
		"a word the flag cannot coerce to the declared type": {
			args:  []string{"--input", "pin=hunter2"},
			value: "hunter2",
		},
		// #2073, the same reason TestARemoteRunRefusalDoesNotPrintASensitiveArgument
		// carries it: a one-rune coercion word is below WithValues's substring
		// floor, so `flow schedule create` needs the same construction-site
		// fix, not a wider backstop.
		"a one-rune word the flag cannot coerce to the declared type": {
			args:  []string{"--input", "pin=x"},
			value: "x",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path := writeWorkflowFile(t, sensitiveScheduleWorkflow)
			res := runFlow(t, append([]string{"schedule", "create", path}, tt.args...)...)
			require.Error(t, res.Err, "the command line is refused")

			assert.NotContains(t, res.Stdout, tt.value)
			assert.NotContains(t, res.Stderr, tt.value,
				"the value reached the prose a person reads")
			assert.NotContains(t, res.Err.Error(), tt.value,
				"the value reached the error the command returns, which is what main prints")
			assert.Contains(t, res.Output(), "pin",
				"the refusal no longer says which input it is about")
		})
	}
}

// sensitiveWithOrdinaryMustWorkflow declares a sensitive int beside an
// ordinary one that has its own `must:`, so a test can fail the *ordinary*
// field's constraint and check that the value the binder's `got <value>`
// quotes for it — not just the field's name — survives redaction. A
// misspelled undeclared name (the shape these two tests used before) proves
// only that the sentence naming it was not withheld whole; the binder's
// refusal for an undeclared name never quotes a value at all, so it cannot
// stand in for the case redaction could still widen into a withholding.
const sensitiveWithOrdinaryMustWorkflow = `edition: v2026.3
name: onboard-ordinary-must
inputs:
  pin:
    type: int
    sensitive: true
    must: this > 9999
  region:
    type: string
    must: this in ["eu-west-1", "us-east-1"]
steps:
  - id: greet
    log:
      message: onboarding
`

// sensitiveScheduleWithOrdinaryMustWorkflow is sensitiveWithOrdinaryMustWorkflow
// with the `triggers: schedule:` block `flow schedule create` requires.
const sensitiveScheduleWithOrdinaryMustWorkflow = `edition: v2026.3
name: onboard-ordinary-must-schedule
triggers:
  schedule:
    cron: "0 * * * *"
inputs:
  pin:
    type: int
    sensitive: true
    must: this > 9999
  region:
    type: string
    must: this in ["eu-west-1", "us-east-1"]
steps:
  - id: greet
    log:
      message: onboarding
`

// ordinaryMustValue is the value sensitiveWithOrdinaryMustWorkflow's own
// `region` must: refuses, quoted in the binder's "got <value>" the same way
// a sensitive field's would be — the value under test, not the field name.
const ordinaryMustValue = "oceania"

// TestARemoteRunRefusalStillPrintsAnOrdinaryArgument and
// TestAScheduleCreateRefusalStillPrintsAnOrdinaryArgument are the direction
// the fix must not take with it, the same property
// TestARefusedCommandLineStillPrintsAnOrdinaryArgument pins for `flow run
// local`: a workflow that declares one sensitive input must still quote an
// ordinary one's own refused *value*, not only report that it was refused.
func TestARemoteRunRefusalStillPrintsAnOrdinaryArgument(t *testing.T) {
	t.Parallel()

	path := writeWorkflowFile(t, sensitiveWithOrdinaryMustWorkflow)
	res := runFlow(t, "run", path, "--input", "pin=10000", "--input", "region="+ordinaryMustValue)
	require.Error(t, res.Err)

	assert.Contains(t, res.Output(), ordinaryMustValue,
		"an ordinary field's own must: refusal withheld the value it quotes, not only a sensitive one's")
}

func TestAScheduleCreateRefusalStillPrintsAnOrdinaryArgument(t *testing.T) {
	t.Parallel()

	path := writeWorkflowFile(t, sensitiveScheduleWithOrdinaryMustWorkflow)
	res := runFlow(t, "schedule", "create", path, "--input", "pin=10000", "--input", "region="+ordinaryMustValue)
	require.Error(t, res.Err)

	assert.Contains(t, res.Output(), ordinaryMustValue,
		"an ordinary field's own must: refusal withheld the value it quotes, not only a sensitive one's")
}

// TestInputCoercionErrorNeverQuotesASensitiveWord is #2073, unit-tested
// directly against [inputCoercionError] rather than through a full run.
//
// [v1.SensitiveValues.WithValues]'s substring backstop — the only thing
// [sensitiveInputWords] can offer this refusal's word to — floors matching
// at [minSensitiveSubstringRunes] (two runes) by documented design, and this
// is the one refusal whose word never becomes a [*v1.Value], so no set built
// afterward can redact it at any length: a one-rune word used to survive in
// the clear where a longer one did not. The fix has to be made here, at
// construction, against the declaration this function already holds — the
// same standing [runArgumentFlags] and [redactedIfSensitive] already give a
// declaration elsewhere in this binary.
//
// Two spellings of the same length, for the reason the integration tests
// above carry both: a plain rune and one that needs `%q`/JSON escaping to
// render at all, so the fix is proven independent of whether the word is one
// this binary's own quoting would have had to escape.
func TestInputCoercionErrorNeverQuotesASensitiveWord(t *testing.T) {
	t.Parallel()

	sensitive := &v1.InputDeclaration{Name: "pin", Type: v1.InputDeclaration_TYPE_INT, Sensitive: true}
	ordinary := &v1.InputDeclaration{Name: "region", Type: v1.InputDeclaration_TYPE_INT}

	words := map[string]string{
		"a plain one-rune word":                   "x",
		"a one-rune word needing %q-style escape": `\`,
	}

	for name, word := range words {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := inputCoercionError("pin", word, sensitive, "a whole number, e.g. 3", false)
			require.Error(t, err)
			assert.NotContains(t, err.Error(), word,
				"a sensitive input's raw word reached the refusal's own text")
			assert.Contains(t, err.Error(), v1.SensitiveMarker,
				"the word's place in the sentence was not marked as redacted")
			assert.Contains(t, err.Error(), "pin",
				"the refusal no longer says which input it is about")
		})
	}

	// The direction the fix must not take with it: an ordinary declaration's
	// word still prints, at the same lengths that a sensitive one no longer
	// does, or the fix has widened into a withholding nobody asked for.
	for name, word := range words {
		t.Run(name+" (an ordinary declaration)", func(t *testing.T) {
			t.Parallel()

			err := inputCoercionError("region", word, ordinary, "a whole number, e.g. 3", false)
			require.Error(t, err)
			assert.Contains(t, err.Error(), word,
				"an ordinary input's own word was withheld too")
		})
	}
}

// TestInputCoercionErrorHonorsRevealSensitive is the direction the fix must
// not take with it either: [inputCoercionError] produces no [*v1.Value], so
// it never reaches [refusedRunSensitiveValues]'s own reveal check the way
// every other refusal on this path does — the escape hatch has to be asked
// for here directly, or `--reveal-sensitive` stops working for exactly the
// refusal this issue is about.
func TestInputCoercionErrorHonorsRevealSensitive(t *testing.T) {
	t.Parallel()

	sensitive := &v1.InputDeclaration{Name: "pin", Type: v1.InputDeclaration_TYPE_INT, Sensitive: true}

	err := inputCoercionError("pin", "x", sensitive, "a whole number, e.g. 3", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "x",
		"--reveal-sensitive did not reveal the word an operator asked to see")
	assert.NotContains(t, err.Error(), v1.SensitiveMarker,
		"a revealed word was marked as redacted anyway")
}
