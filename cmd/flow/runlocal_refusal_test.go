package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// refusalWorkflow declares one of each thing a command line can get wrong: a
// required input, a typed one, and a constrained one.
const refusalWorkflow = `edition: v2026.3
name: onboard
inputs:
  tenant:
    type: string
    required: true
  shards:
    type: int
    must: this > 0
steps:
  - id: greet
    log:
      message: ${"onboarding " + inputs.tenant}
`

// TestARefusedCommandLineIsStillADocument is #1552's first acceptance criterion.
//
// `--output json` means the answer is a document, and a submit refusal was the
// one outcome of `flow run local` that broke that promise: a run that started
// and failed wrote a GetResponse with STATUS_FAILED and a `kind`, while a
// command line the workflow's own `inputs:` refused wrote prose on stderr and
// an empty stdout — for the failure a caller is most likely to hit first.
//
// All four refusal shapes the issue names, because they travel four different
// paths: coercion refuses before the binder is reached, the binder's own three
// come from three different functions, and a test covering one would leave the
// others exactly as they were.
func TestARefusedCommandLineIsStillADocument(t *testing.T) {
	t.Parallel()

	for name, args := range map[string][]string{
		"a required input nobody gave":          {},
		"a value the flag cannot coerce":        {"--input", "tenant=acme", "--input", "shards=many"},
		"a name the workflow does not declare":  {"--input", "tenant=acme", "--input", "bogus=1"},
		"a value that fails the declared must:": {"--input", "tenant=acme", "--input", "shards=0"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			stdout, _, err := runLocal(t, refusalWorkflow, append([]string{"--output", "json"}, args...)...)

			// The exit code is unchanged: a refusal is still a refusal, and a
			// document about it is not a success.
			require.Error(t, err, "a refused command line reported success")

			var document map[string]any
			require.NoError(t, json.Unmarshal([]byte(stdout), &document),
				"stdout is not a single JSON document:\n%s", stdout)

			assert.Equal(t, "STATUS_FAILED", document["status"])

			failure, reported := document["error"].(map[string]any)
			require.True(t, reported, "the document carries no error:\n%s", stdout)

			// The whole point: a caller's own bad argument, not a defect in
			// Flowstate. "Internal" is asserted against by name because that is
			// what this reported before, and what a future unclassified refusal
			// would silently go back to.
			assert.Equal(t, "InvalidInput", failure["kind"],
				"a refusal about the caller's argument is classified %v", failure["kind"])
			assert.NotEqual(t, "Internal", failure["kind"])

			// The sentence a person reads is in the document too, so a program
			// need not also scrape stderr to say what went wrong.
			assert.NotEmpty(t, failure["message"])
		})
	}
}

// TestARefusedCommandLineNamesTheInputItIsAbout keeps the fact a caller acts on
// addressable: which input was wrong.
//
// Asserted on the message rather than on a field of its own, which is what the
// issue asked for and what this deliberately does not do: `RunResponse.Error`
// has `message` and `kind` and nothing to hold a name, and adding one is a
// schema change that belongs with #1439's structured step address rather than
// beside it. The binder already writes the name into every one of these
// sentences, so this pins that it stays there.
func TestARefusedCommandLineNamesTheInputItIsAbout(t *testing.T) {
	t.Parallel()

	stdout, _, err := runLocal(t, refusalWorkflow, "--output", "json",
		"--input", "tenant=acme", "--input", "shards=0")
	require.Error(t, err)

	var document struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &document))
	assert.Contains(t, document.Error.Message, `"shards"`,
		"the refusal does not say which input it is about")
}

// TestATextFormattedRefusalWritesNoDocument is the direction the change must
// not take with it.
//
// The text shape writes nothing on a failed run on purpose — an empty stdout is
// a meaningful value there, because the answer is the outputs and a refused
// command line has none. `{}` would claim it produced none *successfully*.
func TestATextFormattedRefusalWritesNoDocument(t *testing.T) {
	t.Parallel()

	stdout, stderr, err := runLocal(t, refusalWorkflow)
	require.Error(t, err)
	assert.Empty(t, strings.TrimSpace(stdout),
		"the default format wrote a document to stdout for a refusal")
	assert.Contains(t, stderr, "tenant",
		"the person reading stderr was not told which input is missing")
}

// sensitiveRefusalWorkflow declares one `sensitive:` input that a command line
// can get wrong in two ways — a word that is not the declared type, and a value
// that is but fails the constraint — and one ordinary input beside it, so a
// test can tell a redaction from a withholding.
//
// No `default:` on pin, deliberately. A default that fails its own `must:` is
// refused when the *file* is validated, long before a command line is read, so
// a fixture with one never reaches the refusals under test here — it fails at
// [loadWorkflow] with the compiler's own diagnostic about the file. That
// diagnostic quotes the default in the clear and is meant to: it is the
// author's own text, in the author's own file, with no run in sight, which is
// the case pkg/flowstate/v1/constraints.go's inputValueRendering names when it
// says a submitted value prints whole.
const sensitiveRefusalWorkflow = `edition: v2026.3
name: onboard
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

// TestARefusedCommandLineDoesNotPrintASensitiveArgument is the disclosure the
// document in TestARefusedCommandLineIsStillADocument arrived with.
//
// A run that starts and fails has always had its failure sentence cleared of
// the run's own `sensitive:` values, on stdout and on stderr both — that is the
// fourth surface `sensitive:` names in workflow.proto, and #974 is the loop that
// put a value into a sentence nothing was looking at. A run *refused before it
// starts* went through neither: `refuseRunLocally` copied the refusal straight
// into `RunResponse.Error.Message` and wrote it to the machine channel a caller
// stores or forwards (Codex).
//
// Two shapes, because they compose the value in two different places, and the
// second is the one a set of *values* cannot reach: the binder's `got <value>`
// over a submitted argument, and the coercion's, which quotes a word that never
// became a value at all.
func TestARefusedCommandLineDoesNotPrintASensitiveArgument(t *testing.T) {
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

			stdout, stderr, err := runLocal(t, sensitiveRefusalWorkflow,
				append([]string{"--output", "json"}, tt.args...)...)
			require.Error(t, err, "the command line is refused")

			require.NotContains(t, stdout, tt.value,
				"the value reached the document a machine caller reads")
			require.NotContains(t, stderr, tt.value,
				"the value reached the prose a person reads")
			require.NotContains(t, err.Error(), tt.value,
				"the value reached the error the command returns, which is what main prints")

			// A redaction and not a withholding, which is what the schema
			// promises for this surface: the sentence still says which input
			// and what is wrong with it, because nothing else answers that.
			var document struct {
				Status string `json:"status"`
				Error  struct {
					Message string `json:"message"`
					Kind    string `json:"kind"`
				} `json:"error"`
			}
			require.NoError(t, json.Unmarshal([]byte(stdout), &document),
				"stdout is not a single JSON document:\n%s", stdout)
			assert.Equal(t, "STATUS_FAILED", document.Status)
			assert.Contains(t, document.Error.Message, "pin",
				"the refusal no longer says which input it is about")

			// And the classification survives the redaction. It is read off
			// the original chain on purpose: [redactFailureError] does not
			// unwrap, so a kind computed after it would report Internal — a
			// defect in Flowstate — for the caller's own argument (#1552).
			assert.Equal(t, "InvalidInput", document.Error.Kind)
		})
	}
}

// TestARefusedCommandLineStillPrintsAnOrdinaryArgument is the direction the
// change must not take with it.
//
// The set is the run's `sensitive:` values, not everything: a workflow that
// declares one sensitive input must still report an ordinary one's value in the
// clear, or the redaction has become the withholding the schema refuses. The
// undeclared name is the sharpest case — it is the value most likely to be a
// typo the author needs to see.
func TestARefusedCommandLineStillPrintsAnOrdinaryArgument(t *testing.T) {
	t.Parallel()

	stdout, _, err := runLocal(t, sensitiveRefusalWorkflow, "--output", "json",
		"--input", "pin=10000", "--input", "reigon=eu-west-1")
	require.Error(t, err)

	assert.Contains(t, stdout, "reigon",
		"the misspelled name the refusal is about was withheld too")
}

// TestARevealedRefusalPrintsTheSensitiveArgument pins the one deliberate escape
// hatch on this path, so it stays one decision rather than becoming a flag that
// works on the surfaces somebody remembered.
func TestARevealedRefusalPrintsTheSensitiveArgument(t *testing.T) {
	t.Parallel()

	stdout, _, err := runLocal(t, sensitiveRefusalWorkflow, "--output", "json",
		"--reveal-sensitive", "--input", "pin=4321")
	require.Error(t, err)

	assert.Contains(t, stdout, "4321")
}
