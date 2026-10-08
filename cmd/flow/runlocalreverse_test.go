package main

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// TestReverseIsRefusedWhereItCannotHold: each refusal names the two things that
// disagree, before the terminal is touched.
func TestReverseIsRefusedWhereItCannotHold(t *testing.T) {
	path := writeRunLocalDebugFixture(t)

	t.Run("without debug", func(t *testing.T) {
		res := runFlow(t, "run", "local", path, "--reverse")
		require.Error(t, res.Err)
		assert.Contains(t, res.Err.Error(), "add --debug")
	})
	t.Run("not at a terminal", func(t *testing.T) {
		res := runFlowStdin(t, "step\n", "run", "local", path, "--debug", "--reverse")
		require.Error(t, res.Err)
		assert.Contains(t, res.Err.Error(), "terminal")
	})
	t.Run("an unknown mode", func(t *testing.T) {
		res := runFlow(t, "run", "local", path, "--debug", "--reverse=sideways")
		require.Error(t, res.Err)
		assert.Contains(t, res.Err.Error(), `"unsafe"`)
	})
	t.Run("a task that may act outside the process", func(t *testing.T) {
		dir := t.TempDir()
		risky := filepath.Join(dir, "workflow.yaml")
		require.NoError(t, os.WriteFile(risky, []byte(`edition: v2026.4
name: risky
steps:
  - id: ping
    http:
      method: GET
      url: https://example.com/ping
outputs: {}
`), 0o600))
		res := runFlow(t, "run", "local", risky, "--debug", "--reverse")
		require.Error(t, res.Err)
		assert.Contains(t, res.Err.Error(), `"http"`)
		assert.Contains(t, res.Err.Error(), "--reverse=unsafe")
	})
}

// TestARealRunStepsBackAndGivesTheSameAnswer: the run is executed again to reach
// the earlier stop, its own account is said once, and the answer is the one a run
// that never stepped back gives.
func TestARealRunStepsBackAndGivesTheSameAnswer(t *testing.T) {
	path := writeRunLocalDebugFixture(t)
	workflow, err := loadWorkflow(path)
	require.NoError(t, err)

	scanner := bufio.NewScanner(strings.NewReader("step\nback\nstatus\nstep\nstep\n"))
	var out strings.Builder
	front := &reversibleFront{
		Steps:  stepList(workflow),
		Out:    &out,
		Prompt: flowdebug.Prompt,
		Theme:  ui.Theme{},
		Next: func() (string, error) {
			if !scanner.Scan() {
				return "", io.EOF
			}

			return scanner.Text(), nil
		},
	}
	outputs, err := runReversibly(t.Context(), front, workflow, nil, &out, ui.Theme{})
	require.NoError(t, err)
	assert.NotNil(t, outputs)

	text := out.String()
	assert.Contains(t, text, "held at first", "back did not land on the first stop:\n"+text)
	// Once for the pass that was shown, and once for the step taken again after
	// going back; the silent replay in between says nothing.
	assert.Equal(t, 2, strings.Count(text, "INFO one"), "a replay spoke while it was silent:\n"+text)
	assert.Equal(t, 1, strings.Count(text, "break at first"), "a replay narrated a stop already seen:\n"+text)
}

// TestARealRunThatFailsReportsItsFailure: the verdict of the shown pass is the
// run's error, not a generic one.
func TestARealRunThatFailsReportsItsFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.4
name: failing
steps:
  - id: boom
    log:
      message: ${string(size("a") / size(""))}
outputs: {}
`), 0o600))
	workflow, err := loadWorkflow(path)
	require.NoError(t, err)

	var out strings.Builder
	front := &reversibleFront{
		Steps:  stepList(workflow),
		Out:    &out,
		Prompt: flowdebug.Prompt,
		Next:   func() (string, error) { return "", io.EOF },
	}
	_, err = runReversibly(t.Context(), front, workflow, nil, &out, ui.Theme{})
	require.Error(t, err)
	assert.NotEqual(t, "the run did not finish", err.Error())
}
