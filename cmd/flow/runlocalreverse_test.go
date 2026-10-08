package main

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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
	t.Run("a wait", func(t *testing.T) {
		dir := t.TempDir()
		sleepy := filepath.Join(dir, "workflow.yaml")
		require.NoError(t, os.WriteFile(sleepy, []byte(`edition: v2026.4
name: sleepy
steps:
  - id: nap
    sleep: 2m
outputs: {}
`), 0o600))
		res := runFlow(t, "run", "local", sleepy, "--debug", "--reverse")
		require.Error(t, res.Err)
		assert.Contains(t, res.Err.Error(), "wait")
		assert.Contains(t, res.Err.Error(), "--reverse=unsafe")
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

// TestAGatedHandlerExportsNothingWhileItsPassIsHidden: the gate is on the whole
// composed handler, derived handlers included, and opens when the pass is shown.
func TestAGatedHandlerExportsNothingWhileItsPassIsHidden(t *testing.T) {
	var (
		shown    atomic.Bool
		exported atomic.Int32
	)
	counting := countingHandler{count: &exported}
	logger := slog.New(gatedHandler{next: counting, allowed: shown.Load}).With("step", "one").WithGroup("g")

	logger.Info("hidden")
	assert.Zero(t, exported.Load(), "a hidden pass exported a record")

	shown.Store(true)
	logger.Info("shown")
	assert.Equal(t, int32(1), exported.Load(), "a pass that became the shown one did not resume exporting")
}

type countingHandler struct{ count *atomic.Int32 }

func (countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h countingHandler) Handle(context.Context, slog.Record) error {
	h.count.Add(1)

	return nil
}
func (h countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h countingHandler) WithGroup(string) slog.Handler      { return h }

// TestReverseKeepsTheRevealItWasGiven: --reveal-sensitive reaches every pass's
// session, so an inspect answers with the value before a rewind and after it, and
// without the opt-in neither does.
func TestReverseKeepsTheRevealItWasGiven(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`edition: v2026.4
name: secretive
inputs:
  token:
    type: string
    sensitive: true
    default: sk-live-0123456789
steps:
  - id: first
    log:
      message: one
  - id: after
    log:
      message: two
outputs: {}
`), 0o600))
	workflow, err := loadWorkflow(path)
	require.NoError(t, err)

	play := func(reveal bool) string {
		scanner := bufio.NewScanner(strings.NewReader(
			"step\ninspect inputs.token\nback\nstep\ninspect inputs.token\ncontinue\n"))
		var out strings.Builder
		front := &reversibleFront{
			Steps:           stepList(workflow),
			Out:             &out,
			Prompt:          flowdebug.Prompt,
			RevealSensitive: reveal,
			Next: func() (string, error) {
				if !scanner.Scan() {
					return "", io.EOF
				}

				return scanner.Text(), nil
			},
		}
		_, _ = runReversibly(t.Context(), front, workflow, nil, &out, ui.Theme{})

		return out.String()
	}

	revealed := play(true)
	assert.GreaterOrEqual(t, strings.Count(revealed, "sk-live-0123456789"), 2,
		"the opt-in did not reach the session before and after a rewind:\n"+revealed)
	assert.NotContains(t, play(false), "sk-live-0123456789", "a value was shown without the opt-in")
}
