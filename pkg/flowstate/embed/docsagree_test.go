package embed

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The four places #1674 found the facade's docs and its behavior disagreeing,
// each pinned so the docs are checked by the tree they describe.

// TestRunLocal_UnknownTaskNamesWhatToRegister pins the first failure an
// embedder meets: a misspelled task is refused before the first step runs,
// with the words every other surface uses and the remedy in the sentence,
// rather than "required task capabilities are unavailable" on its own.
func TestRunLocal_UnknownTaskNamesWhatToRegister(t *testing.T) {
	workflow, diags, err := Compile([]byte(`
edition: v2026.3
name: typo
steps:
  - id: step1
    nosuchtask:
      foo: bar
`))
	require.NoError(t, err, "diags=%v", diags)

	_, runErr := RunLocal(context.Background(), workflow, RunOptions{})
	require.Error(t, runErr)
	require.Contains(t, runErr.Error(), `unknown task "nosuchtask"`)
	require.Contains(t, runErr.Error(), "Tasks.Register", "the sentence says what an embedder does about it")
}

// TestCompile_DoesNotCheckReferences pins what [Compile]'s doc now says: a
// step reading another that does not exist compiles cleanly, because the
// checks across steps are [flowfile.Validate]'s, and Validate on the same
// workflow is where the ghost reference is named.
func TestCompile_DoesNotCheckReferences(t *testing.T) {
	workflow, diags, err := Compile([]byte(`
edition: v2026.3
name: ghost
steps:
  - id: step1
    log:
      message: ${steps.nope.x}
`))
	require.NoError(t, err, "Compile: %v diags=%v", err, diags)
	require.Nil(t, diags, "Compile does not run the reference checks; its doc says so and names Validate")

	validated := flowfile.Validate(workflow)
	require.NotEmpty(t, validated, "Validate is where the ghost reference is caught")
	var messages []string
	for _, d := range validated {
		messages = append(messages, d.Message)
	}
	require.Contains(t, strings.Join(messages, "\n"), `references unknown step "nope"`)
}

// TestRunLocal_OutputsAreNotRedacted pins the fail-closed table's last row:
// `sensitive:` bounds what Flowstate renders, not what a run returns, so a
// sensitive input echoed by a step comes back in the clear and an embedder
// that prints it applies the declared set itself.
func TestRunLocal_OutputsAreNotRedacted(t *testing.T) {
	workflow, diags, err := Compile([]byte(`
edition: v2026.3
name: sensitive
inputs:
  token:
    type: string
    sensitive: true
steps:
  - id: echo
    value: ${inputs.token}
`))
	require.NoError(t, err, "diags=%v", diags)

	outputs, runErr := RunLocal(context.Background(), workflow, RunOptions{
		Inputs: map[string]any{"token": "hunter2"},
	})
	require.NoError(t, runErr)

	got, ok := StepOutputString(outputs, "echo", "value")
	require.True(t, ok, "outputs: %v", outputs)
	require.Equal(t, "hunter2", got, "a run returns its history as recorded; redaction is the renderer's")

	set := v1.SensitiveInputValues(map[string]*v1.Value{"token": v1.NewLiteral("hunter2")}, map[string]bool{"token": true})
	require.NotContains(t, set.RedactSubstrings(got), "hunter2", "the declared set is what an embedder applies before printing")
}
