package flowtest_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// The case transcript (#929 slice 2): every fact the account renders, proven
// through a real run — virtual timestamps, stub attribution, skips, waits,
// scripted deliveries with their sender, the switch arm taken — and the
// redaction that keeps it printable.

func transcriptText(lines []flowtest.TranscriptLine) string {
	texts := make([]string, 0, len(lines))
	for _, line := range lines {
		texts = append(texts, line.Text)
	}
	return strings.Join(texts, "\n")
}

// TestTranscriptAccountsForTheRun is the flagship: one case exercising each
// kind of fact, and the rendered account naming all of them with the virtual
// times they happened at.
func TestTranscriptAccountsForTheRun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: release
inputs:
  risk:
    type: string
    required: true
steps:
  - id: build
    log:
      message: building
  - id: prod_gate
    if: ${false}
    log:
      message: never
  - id: approval
    wait_for_signal:
      name: ship-approved
      timeout: 1h
      outputs:
        approved: ${!timed_out}
  - id: route
    switch:
      value: ${inputs.risk}
      cases:
        - case: high
          steps: []
      default:
        steps: []
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the whole account
    workflow: ./workflow.yaml
    inputs:
      risk: high
    stubs:
      - step: build
        returns: {}
    signals:
      - name: ship-approved
        at: 5m
        payload:
          approved: true
        sender:
          subject: approver@corp
          issuer: https://idp.corp
    expect:
      ran: [build, approval, route]
      skipped: [prod_gate]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	require.Len(t, result.Report.GetCases(), 1)
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	require.Len(t, result.Transcripts, 1, "one account per case, parallel to the cases")
	text := transcriptText(result.Transcripts[0])

	assert.Contains(t, text, `stub 1 (step "build")`, "the answering stub is named in the numbering every stub diagnostic uses")
	assert.Contains(t, text, "skipped by its if:")
	assert.Contains(t, text, "waiting: ship-approved (timeout 1h)")
	assert.Contains(t, text, `signal ship-approved {approved: true}`)
	assert.Contains(t, text, "sender: approver@corp")
	assert.Contains(t, text, "-> approved: true", "the wait's own shaped outputs are the account of how it resolved")
	assert.Contains(t, text, `took case "high"`, "the switch line reads as the decision, not as two opaque outputs")
	assert.Contains(t, text, "t=0s", "the run starts at the virtual epoch")
	assert.Contains(t, text, "t=5m", "the delivery and what it unblocked happen at the scripted moment")
}

// TestTranscriptRedactsSensitiveValues: a value that originates in a
// `sensitive: true` input never renders in the account, wherever a step
// carried it — the same one redaction set the stub diagnostics use.
func TestTranscriptRedactsSensitiveValues(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: secretive
inputs:
  token:
    type: string
    required: true
    sensitive: true
steps:
  - id: use
    log:
      message: ${inputs.token}
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the token travels into a step's outputs
    workflow: ./workflow.yaml
    inputs:
      token: hunter2-super-secret
    stubs:
      - task: log
        returns:
          said: ${inputs.message}
    expect:
      ran: [use]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	require.Len(t, result.Transcripts, 1)
	text := transcriptText(result.Transcripts[0])
	assert.NotContains(t, text, "hunter2-super-secret",
		"a sensitive input's value must never render in the account")
	assert.Contains(t, text, "[redacted]")
}

// TestTranscriptOfAFailingRunEndsOnTheFailure: the account a failing case
// arrives with shows the steps that ran and then the step it died on, in the
// danger tone — the whole reason the transcript exists.
func TestTranscriptOfAFailingRunEndsOnTheFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: fragile
steps:
  - id: first
    log:
      message: fine
  - id: second
    log:
      message: doomed
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the second step fails
    workflow: ./workflow.yaml
    stubs:
      - task: log
        times: 1
        returns: {}
      - task: log
        fails:
          message: upstream said no
    expect:
      failed: true
      error_contains: upstream said no
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	lines := result.Transcripts[0]
	text := transcriptText(lines)
	assert.Contains(t, text, "FAILED:")
	assert.Contains(t, text, "upstream said no")

	var failing *flowtest.TranscriptLine
	for i := range lines {
		if strings.Contains(lines[i].Text, "FAILED:") {
			failing = &lines[i]
		}
	}
	require.NotNil(t, failing)
	assert.Equal(t, flowtest.ToneDanger, failing.Tone, "what failed the run renders in the danger tone")
}
