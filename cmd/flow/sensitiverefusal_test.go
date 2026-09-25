package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
// broke it was — and sensitiveInputFileWords has to walk exactly as deep to
// catch it.
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

// TestARemoteRunRefusalStillPrintsAnOrdinaryArgument and
// TestAScheduleCreateRefusalStillPrintsAnOrdinaryArgument are the direction
// the fix must not take with it, the same property
// TestARefusedCommandLineStillPrintsAnOrdinaryArgument pins for `flow run
// local`: a workflow that declares one sensitive input must still report an
// ordinary one's value in the clear.
func TestARemoteRunRefusalStillPrintsAnOrdinaryArgument(t *testing.T) {
	t.Parallel()

	path := writeWorkflowFile(t, sensitiveRefusalWorkflow)
	res := runFlow(t, "run", path, "--input", "pin=10000", "--input", "reigon=eu-west-1")
	require.Error(t, res.Err)

	assert.Contains(t, res.Output(), "reigon",
		"the misspelled name the refusal is about was withheld too")
}

func TestAScheduleCreateRefusalStillPrintsAnOrdinaryArgument(t *testing.T) {
	t.Parallel()

	path := writeWorkflowFile(t, sensitiveScheduleWorkflow)
	res := runFlow(t, "schedule", "create", path, "--input", "pin=10000", "--input", "reigon=eu-west-1")
	require.Error(t, res.Err)

	assert.Contains(t, res.Output(), "reigon",
		"the misspelled name the refusal is about was withheld too")
}
