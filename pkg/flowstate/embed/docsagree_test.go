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
	require.Contains(t, runErr.Error(), `task "nosuchtask": unknown task:`,
		"TaskError names the task once and the cause leads with the words every surface uses")
	require.Equal(t, 1, strings.Count(runErr.Error(), `"nosuchtask"`), "the task is named once, not by the wrapper and the cause both")
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

// TestRunLocal_OutputsAreNotRedacted pins what the fail-closed section says:
// `sensitive:` bounds what Flowstate renders, not what a run returns, so a
// sensitive input echoed by a step comes back in the clear; and a value a
// step derived from it is not in the declared set, which is why an embedder
// withholds a step's values rather than redacting by value, as the CLI does.
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
  - id: derived
    value: ${inputs.token.upperAscii()}
`))
	require.NoError(t, err, "diags=%v", diags)

	outputs, runErr := RunLocal(context.Background(), workflow, RunOptions{
		Inputs: map[string]any{"token": "hunter2"},
	})
	require.NoError(t, runErr)

	echoed, ok := StepOutputString(outputs, "echo", "value")
	require.True(t, ok, "outputs: %v", outputs)
	require.Equal(t, "hunter2", echoed, "a run returns its history as recorded; redaction is the renderer's")
	derived, ok := StepOutputString(outputs, "derived", "value")
	require.True(t, ok, "outputs: %v", outputs)
	require.Equal(t, "HUNTER2", derived)

	set := v1.SensitiveInputValues(map[string]*v1.Value{"token": v1.NewLiteral("hunter2")}, map[string]bool{"token": true})
	require.True(t, set.IsSensitive(echoed), "the declared value is recognised")
	require.False(t, set.IsSensitive(derived),
		"a value derived from the secret is not, which is why withholding the step's values is the fail-closed line")
}
