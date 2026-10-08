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

	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// playReversible runs the fixture's selected case through the reversible
// front, reading the given lines, and returns what the front wrote and the
// case's result. The input is a script, not a terminal: the loop is the same
// one a terminal feeds, and the console is only where a line comes from.
func playReversible(t *testing.T, input string, record *attachRecording) (string, flowtest.RunResult) {
	t.Helper()

	dir := writeDebugFixture(t)
	scanner := bufio.NewScanner(strings.NewReader(input))
	var out strings.Builder
	front := &reversibleFront{
		Path:   filepath.Join(dir, "workflow.test.yaml"),
		Run:    flowtest.RunOptions{Select: func(name string) bool { return name == "the debugged case" }},
		Steps:  workflowStepList(filepath.Join(dir, "workflow.yaml")),
		Out:    &out,
		Prompt: flowdebug.Prompt,
		Record: record,
		Next: func() (string, error) {
			if !scanner.Scan() {
				return "", io.EOF
			}

			return scanner.Text(), nil
		},
	}

	result, err := front.run(t.Context())
	require.NoError(t, err)

	return out.String(), result
}

// TestTheReversibleFrontSaysEachStopOnce: the shown run narrates a forward
// movement itself, so the answer to `step` does not say it again.
func TestTheReversibleFrontSaysEachStopOnce(t *testing.T) {
	out, result := playReversible(t, "step\n", nil)

	assert.Equal(t, 1, strings.Count(out, "break at second"), out)
	assert.NotContains(t, out, "held at second", "a stop was said twice")
	assert.Contains(t, out, "first completed")
	require.NotNil(t, result.Report)
	assert.Empty(t, result.Report.GetRefused())
}

// TestTheReversibleFrontStepsBackToThePreviousStop is the capability: `back`
// lands on the stop before, which the prompt of a live session cannot do.
func TestTheReversibleFrontStepsBackToThePreviousStop(t *testing.T) {
	out, result := playReversible(t, "step\nback\nstatus\nstep\nstep\n", nil)

	assert.Contains(t, out, "held at first", "back did not land on the first stop")
	assert.NotContains(t, out, "refused", out)
	// The run it replays is not narrated again until the person moves it.
	assert.Equal(t, 1, strings.Count(out, "break at first"), "a replay narrated a stop the person had already seen:\n"+out)
	require.NotNil(t, result.Report)
	assert.Empty(t, result.Report.GetRefused())
}

// TestTheReversibleFrontRefusesBackAtTheFirstStop: there is nowhere to go, and
// it says so rather than doing nothing.
func TestTheReversibleFrontRefusesBackAtTheFirstStop(t *testing.T) {
	out, _ := playReversible(t, "back\n", nil)

	assert.Contains(t, out, "refused: this is the first stop")
}

// TestTheReversibleFrontQuitFailsTheCase: the verdict is the one the prompt has
// always given for a case the person ended.
func TestTheReversibleFrontQuitFailsTheCase(t *testing.T) {
	out, result := playReversible(t, "step\nquit\n", nil)

	require.NotNil(t, result.Report)
	assert.True(t, testReportFailed(result.Report), "a quit case passed: %s", out)
}

// TestTheReversibleFrontReleasesTheRunAtTheEndOfInput: with nothing left to
// read, every stop is resumed, as a session with no console does.
func TestTheReversibleFrontReleasesTheRunAtTheEndOfInput(t *testing.T) {
	out, result := playReversible(t, "step\n", nil)

	assert.Contains(t, out, "second completed")
	require.NotNil(t, result.Report)
	assert.False(t, testReportFailed(result.Report))
}

// TestTheReversibleFrontRecordsWhatTheRunAccepted: a refused rewind is not part
// of the session it would reproduce, and an accepted one ends the recording,
// because a script replays forward only.
func TestTheReversibleFrontRecordsWhatTheRunAccepted(t *testing.T) {
	var recording attachRecording
	playReversible(t, "step\nback\nback\nfrobnicate\nstep\n", &recording)

	assert.Equal(t, []string{"step"}, recording.lines,
		"a script cannot step back, so what follows the first rewind would replay from another stop")
	assert.True(t, recording.truncated, "the recording does not say it stopped early")
}

// TestASeededRunStepsBackToTheSameFault: the seed's schedule is part of what a
// rewind re-executes, so the stop at the step a fault fails is the same stop
// the second time, and the run never reaches the step after it.
func TestASeededRunStepsBackToTheSameFault(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.yaml"), []byte(`edition: v2026.4
name: bare
steps:
  - id: warm
    http:
      method: GET
      url: https://example.com/warm
  - id: fetch
    retry: {attempts: 1}
    http:
      method: GET
      url: https://example.com/ping
  - id: done
    http:
      method: GET
      url: https://example.com/done
outputs: {}
`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "workflow.test.yaml"), []byte(`edition: v2026.4
tests:
  - name: survives a flaky fetch
    workflow: ./workflow.yaml
    stubs:
      - task: http
        returns: {status_code: 200, body: ''}
    faults:
      - step: fetch
        rate: 1
        fails: {kind: Upstream, message: connection reset}
    invariants:
      - that: run.failed == false
        because: a single failed attempt must be absorbed
    expect: {failed: false}
`), 0o600))

	seed := uint64(1)
	scanner := bufio.NewScanner(strings.NewReader("step\nback\nstep\nstep\n"))
	var out strings.Builder
	front := &reversibleFront{
		Path: filepath.Join(dir, "workflow.test.yaml"),
		Run: flowtest.RunOptions{
			Budget: dst.Budget{Pinned: &seed},
			Select: func(name string) bool { return name == "survives a flaky fetch" },
		},
		Steps: workflowStepList(filepath.Join(dir, "workflow.yaml")),
		Out:   &out,
		Next: func() (string, error) {
			if !scanner.Scan() {
				return "", io.EOF
			}

			return scanner.Text(), nil
		},
	}
	_, err := front.run(t.Context())
	require.NoError(t, err)

	text := out.String()
	assert.Contains(t, text, "held at warm", "the rewind did not land on the stop before the fault:\n"+text)
	assert.Contains(t, text, "fetch FAILED", "the seeded fault did not fire after the rewind:\n"+text)
	assert.NotContains(t, text, "break at done", "the fault failed the run at fetch")
}

// TestTheReversibleFrontHoldsAFailedCaseForQuestions: a case that fails stops
// once more after its verdict, as it does at the session's own prompt, and the
// person can still step back from there.
func TestTheReversibleFrontHoldsAFailedCaseForQuestions(t *testing.T) {
	dir := writeDebugFixture(t)
	tests := filepath.Join(dir, "workflow.test.yaml")
	body, err := os.ReadFile(tests)
	require.NoError(t, err)
	// The first case claims `second` was skipped, which the run contradicts.
	failing := strings.Replace(string(body), "ran: [first, second]", "skipped: [second]", 1)
	require.NoError(t, os.WriteFile(tests, []byte(failing), 0o600))

	scanner := bufio.NewScanner(strings.NewReader("step\nstep\nstatus\nback\nstatus\n"))
	var out strings.Builder
	front := &reversibleFront{
		Path:   tests,
		Run:    flowtest.RunOptions{Select: func(name string) bool { return name == "the debugged case" }},
		Steps:  workflowStepList(filepath.Join(dir, "workflow.yaml")),
		Out:    &out,
		Prompt: flowdebug.Prompt,
		Next: func() (string, error) {
			if !scanner.Scan() {
				return "", io.EOF
			}

			return scanner.Text(), nil
		},
	}
	result, err := front.run(t.Context())
	require.NoError(t, err)

	text := out.String()
	assert.True(t, testReportFailed(result.Report), "the contradicted claim passed")
	assert.Contains(t, text, "held at second", "back from the autopsy did not return to the last stop:\n"+text)
}

// TestTheReversibleFrontDoesNotPromptForACaseThatEndedBeforeAnyStop: with no
// stop (here its workflow is missing) the case is over before the first prompt,
// so none is asked.
func TestTheReversibleFrontDoesNotPromptForACaseThatEndedBeforeAnyStop(t *testing.T) {
	dir := writeDebugFixture(t)
	require.NoError(t, os.Remove(filepath.Join(dir, "workflow.yaml")))

	var out strings.Builder
	front := &reversibleFront{
		Path:   filepath.Join(dir, "workflow.test.yaml"),
		Run:    flowtest.RunOptions{Select: func(name string) bool { return name == "the debugged case" }},
		Out:    &out,
		Prompt: flowdebug.Prompt,
		Next: func() (string, error) {
			t.Error("a finished case was prompted for a line")

			return "", io.EOF
		},
	}
	result, err := front.run(t.Context())
	require.NoError(t, err)
	assert.True(t, testReportFailed(result.Report), "a missing workflow cannot pass")
	assert.NotContains(t, out.String(), flowdebug.Prompt)
}

// TestTheReversibleFrontRecordsAnAcceptedQuit: a session abandoned by the person
// replays as abandoned, not released at the end of its input.
func TestTheReversibleFrontRecordsAnAcceptedQuit(t *testing.T) {
	var recording attachRecording
	playReversible(t, "step\nq\n", &recording)

	assert.Equal(t, []string{"step", "quit"}, recording.lines)
}

// TestTheReversibleFrontSaysWhyAForwardVerbWasRefused: a forward verb narrates
// itself only when it moves, so a refusal is the answer's to say.
func TestTheReversibleFrontSaysWhyAForwardVerbWasRefused(t *testing.T) {
	out, _ := playReversible(t, "until nosuch\nstep\n", nil)

	assert.Contains(t, out, "nosuch", "the refusal of `until nosuch` was swallowed:\n"+out)
}

// TestTheReversibleFrontDoesNotRecordARefusedRewind: `back` at the first stop is
// refused, so it neither enters the recording nor ends it.
func TestTheReversibleFrontDoesNotRecordARefusedRewind(t *testing.T) {
	var recording attachRecording
	playReversible(t, "back\nfrobnicate\nstep\n", &recording)

	assert.Equal(t, []string{"step"}, recording.lines)
	assert.False(t, recording.truncated)
}

// TestTheReversibleFrontDoesNotReleaseTheRunOnAFailedRead: input that failed is
// not input that ended. The case is ended and fails, rather than every
// remaining stop resuming unattended.
func TestTheReversibleFrontDoesNotReleaseTheRunOnAFailedRead(t *testing.T) {
	dir := writeDebugFixture(t)
	var out strings.Builder
	reads := 0
	front := &reversibleFront{
		Path:  filepath.Join(dir, "workflow.test.yaml"),
		Run:   flowtest.RunOptions{Select: func(name string) bool { return name == "the debugged case" }},
		Steps: workflowStepList(filepath.Join(dir, "workflow.yaml")),
		Out:   &out,
		Next: func() (string, error) {
			reads++
			if reads == 1 {
				return "step", nil
			}

			return "", bufio.ErrTooLong
		},
	}
	result, err := front.run(t.Context())
	require.NoError(t, err)

	require.NotNil(t, result.Report)
	assert.True(t, testReportFailed(result.Report), "a failed read released the run to a pass:\n"+out.String())
	assert.Contains(t, out.String(), "input failed")
	assert.NotContains(t, out.String(), "second completed", "the run resumed past a failed read")
}
