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
