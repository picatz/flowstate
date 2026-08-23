package flowtest_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
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

	// Causal order, deterministically (Codex, #1052): the delivery is
	// recorded under the recorder's lock around the send itself, so the wait
	// it wakes can never record its completion first — the account always
	// reads delivery, then what it unblocked.
	delivery := strings.Index(text, "signal ship-approved")
	unblocked := strings.Index(text, "-> approved: true")
	require.GreaterOrEqual(t, delivery, 0)
	require.GreaterOrEqual(t, unblocked, 0)
	assert.Less(t, delivery, unblocked,
		"the delivery must appear before the completion it caused")
}

// TestTranscriptRedactsTestDeclaredSecrets pins the P1 on #1052's second
// round: a case's own `secrets:` plaintext reaches stub expressions
// ([resolveSecretInputs] resolves it precisely so `where:` and `returns:` can
// read it), so a stub echoing `${inputs.bearer}` puts the material into a
// step's outputs — and the transcript's redaction set used to hold only
// `sensitive:` workflow inputs. A resolved secret never prints, whatever path
// it took.
func TestTranscriptRedactsTestDeclaredSecrets(t *testing.T) {
	t.Parallel()

	const material = "leak-me-not-0451"

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: bearer-request
steps:
  - id: call
    http:
      url: https://api.example.com/status
      bearer: ${secret('env:TOKEN')}
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the stub echoes the resolved secret
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: `+material+`
    stubs:
      - task: http
        returns:
          status_code: 200
          seen: ${inputs.bearer}
    expect:
      ran: [call]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	text := transcriptText(result.Transcripts[0])
	require.NotContains(t, text, material,
		"a resolved secret must never render in the account, whatever path it took")
	assert.Contains(t, text, "[redacted]")
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

// TestTranscriptSuppressesAWaitTheRunNeverParkedOn pins the observer
// contract's "the moment it parks" (Codex, #1052): a delivery buffered before
// the gate is reached is consumed without parking, and the account must not
// say `waiting:` about a gate the run walked straight through — the same rule
// the local wait announcement itself follows. The sleep is what makes the
// ordering deterministic: the scripted goroutine holds a clock registration
// until its delivery is done, so the sleep cannot lapse before the signal is
// buffered.
func TestTranscriptSuppressesAWaitTheRunNeverParkedOn(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: early
steps:
  - id: nap
    sleep: 1m
  - id: gate
    wait_for_signal:
      name: ship
      timeout: 1h
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the approval arrives before the gate
    workflow: ./workflow.yaml
    signals:
      - name: ship
        payload:
          approved: true
    expect:
      ran: [nap, gate]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	text := transcriptText(result.Transcripts[0])
	assert.Contains(t, text, "sleeping 1m")
	assert.Contains(t, text, "signal ship")
	assert.NotContains(t, text, "waiting: ship",
		"a gate answered from the buffer never parked, so the account must not say it did")
}

// TestTranscriptRecordsARefusedDeliveryAsRefused (Codex, #1052): a scripted
// sender a declared signal policy denies is never queued, and the account
// must say refused — an account showing it as delivered would be a false
// transcript in exactly the runs that need debugging.
func TestTranscriptRecordsARefusedDeliveryAsRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: policed
signals:
  approve:
    allow:
      - subject: https://issuer.example.com#approver@example.com
steps:
  - id: approval
    wait_for_signal:
      name: approve
      timeout: 1h
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the wrong sender is refused and the gate lapses
    workflow: ./workflow.yaml
    signals:
      - name: approve
        at: 5m
        payload:
          approved: true
        sender:
          subject: nobody@example.com
          issuer: https://issuer.example.com
    expect:
      ran: [approval]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	text := transcriptText(result.Transcripts[0])
	assert.Contains(t, text, "signal approve refused:")
	assert.NotContains(t, text, "signal approve {",
		"a refused delivery must not render as a delivered one")
	assert.Contains(t, text, "waiting: approve (timeout 1h)",
		"the gate really parked and lapsed; that part of the account stands")
}

// TestTranscriptRedactsAScriptedSendersSubject pins the P1 on #1052: a case
// may spell its `sender.subject` from the same value a sensitive input
// carries, and the sender annotation was the one rendered string that
// bypassed the redaction set every other value passes through.
func TestTranscriptRedactsAScriptedSendersSubject(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: approver-secret
inputs:
  approver:
    type: string
    required: true
    sensitive: true
steps:
  - id: gate
    wait_for_signal:
      name: approve
      timeout: 1h
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the sender is the sensitive value
    workflow: ./workflow.yaml
    inputs:
      approver: approver@corp.example
    signals:
      - name: approve
        at: 5m
        payload:
          approved: true
        sender:
          subject: approver@corp.example
          issuer: https://idp.corp
    expect:
      ran: [gate]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	text := transcriptText(result.Transcripts[0])
	assert.NotContains(t, text, "approver@corp.example",
		"the sender annotation must pass the same redaction every other value does")
	assert.Contains(t, text, "sender: [redacted]")
}

// TestTranscriptRedactsAPayloadKey pins round three's P1 on #1052: a payload
// or `returns:` key is authored text a sensitive value can be spelled into,
// and per-value redaction alone printed it. The joined fragment now passes
// the substring backstop, keys included.
func TestTranscriptRedactsAPayloadKey(t *testing.T) {
	t.Parallel()

	const material = "hunter2-super-secret"

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: keyed
inputs:
  token:
    type: string
    required: true
    sensitive: true
steps:
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the sensitive value is a payload key
    workflow: ./workflow.yaml
    inputs:
      token: `+material+`
    signals:
      - name: go
        at: 5m
        payload:
          `+material+`: true
    expect:
      ran: [gate]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	text := transcriptText(result.Transcripts[0])
	require.NotContains(t, text, material,
		"a sensitive value spelled as a key must redact exactly as one spelled as a value")
	assert.Contains(t, text, "[redacted]")
}

// TestTranscriptDoesNotMistakeAnOutputNamedCaseForASwitch pins round five's
// first half on #1052: "took case ..." is said only where the compiled
// workflow declares a `switch:`, never inferred from an output's name — an
// ordinary task may call an output `case`, and the old inference rendered it
// as a decision and suppressed its other outputs.
func TestTranscriptDoesNotMistakeAnOutputNamedCaseForASwitch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: task-named-case
steps:
  - id: lookup
    log:
      message: looking
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: an ordinary output happens to be called case
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns:
          case: high
          other: kept
    expect:
      ran: [lookup]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	text := transcriptText(result.Transcripts[0])
	assert.NotContains(t, text, "took case")
	assert.Contains(t, text, `case: "high", other: "kept"`,
		"an ordinary step's outputs render whole, whatever their names")
}

// TestTranscriptDoesNotInventADefaultArm: a no-default switch that matched
// nothing ran nothing, and the account must not claim a default body that
// does not exist took it.
func TestTranscriptDoesNotInventADefaultArm(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: no-default
inputs:
  kind:
    type: string
    required: true
steps:
  - id: route
    switch:
      value: ${inputs.kind}
      cases:
        - case: a
          steps: []
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: nothing matches and there is no default
    workflow: ./workflow.yaml
    inputs:
      kind: z
    expect:
      ran: [route]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	text := transcriptText(result.Transcripts[0])
	assert.NotContains(t, text, "took default")
	assert.Contains(t, text, "matched no case (and there is no default:)")
}

// TestTranscriptKeepsTheArmOnAFailedSwitchBody pins round five's second
// half: a failed switch body's record deliberately preserves the matched
// case, and the account shows it beside the failure — the decision is most
// worth reading exactly when the branch it chose is what failed.
func TestTranscriptKeepsTheArmOnAFailedSwitchBody(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: fragile-route
inputs:
  kind:
    type: string
    required: true
steps:
  - id: route
    switch:
      value: ${inputs.kind}
      cases:
        - case: high
          steps:
            - id: ship
              log:
                message: shipping
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the taken body fails
    workflow: ./workflow.yaml
    inputs:
      kind: high
    stubs:
      - task: log
        fails:
          message: refused upstream
    expect:
      failed: true
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	text := transcriptText(result.Transcripts[0])
	assert.Contains(t, text, `(took case "high")`,
		"the failed switch line must carry the arm its record preserves")
}

// TestTranscriptNamesACalleeSwitchsArm pins round six's call finding: a
// `call:` embeds its compiled callee, whose steps — switches included —
// report through the same run's observer under their own ids, so the switch
// index descends into the callee exactly as the run will.
func TestTranscriptNamesACalleeSwitchsArm(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "callee.yaml"), `
edition: v2026.3
name: callee
inputs:
  kind:
    type: string
    required: true
steps:
  - id: route
    switch:
      value: ${inputs.kind}
      cases:
        - case: high
          steps: []
outputs: {}
`)
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: caller
steps:
  - id: delegate
    call: ./callee.yaml
    with:
      kind: high
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the callee's switch decides
    workflow: ./workflow.yaml
    expect:
      ran: [delegate]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	text := transcriptText(result.Transcripts[0])
	assert.Contains(t, text, `took case "high"`,
		"a callee switch's decision renders exactly as a caller's does")
}

// TestTranscriptTreatsAMixedKindIdCollisionAsAmbiguous pins round six's
// other switch finding: two isolated bodies may legally reuse one id, one
// for a switch and one for an ordinary task — and a walk that indexed only
// the switch rendered the task's `case`-named output as that switch's arm.
// Ambiguous means plain outputs, for both of them.
func TestTranscriptTreatsAMixedKindIdCollisionAsAmbiguous(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: colliding
steps:
  - id: loop_a
    for_each:
      items: ${["x"]}
      as: item
      steps:
        - id: route
          switch:
            value: ${item}
            cases:
              - case: x
                steps: []
  - id: loop_b
    for_each:
      items: ${["y"]}
      as: item
      steps:
        - id: route
          log:
            message: plain
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: one id, two kinds
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns:
          case: not-an-arm
    expect:
      ran: [loop_a, loop_b]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	text := transcriptText(result.Transcripts[0])
	assert.NotContains(t, text, "took case",
		"an id declared as both a switch and a task renders plainly for both — the account never guesses")
	assert.Contains(t, text, `case: "not-an-arm"`)
}

// TestTranscriptRedactsASensitiveStructsKeys pins round four's P1 on #1052,
// fixed in the one shared walk ([sensitiveNativeValues]) so the stub
// diagnostics gain it too: a `sensitive: true` struct may carry its material
// in the map *keys* — account ids — and a walk that only enqueued what they
// map to let a stub echo a key in the clear.
func TestTranscriptRedactsASensitiveStructsKeys(t *testing.T) {
	t.Parallel()

	const material = "leak-key-9931"

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: keyed-secret
inputs:
  creds:
    type: struct
    required: true
    sensitive: true
steps:
  - id: use
    log:
      message: using
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: a stub echoes a sensitive struct's key
    workflow: ./workflow.yaml
    inputs:
      creds:
        `+material+`: some-value
    stubs:
      - task: log
        returns:
          echo: `+material+`
    expect:
      ran: [use]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	text := transcriptText(result.Transcripts[0])
	require.NotContains(t, text, material,
		"a sensitive struct's keys are part of the declared value and must redact like its values")
	assert.Contains(t, text, "[redacted]")
}

// TestTranscriptClearsAStaleStubAttribution pins round three's other finding:
// a retried step whose first attempt a times: stub answered, and whose final
// attempt nothing did, must not render that stub's identity beside a failure
// the unanswered attempt produced — the failure's own text names what did not
// match.
func TestTranscriptClearsAStaleStubAttribution(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: retried
steps:
  - id: flaky
    retry:
      attempts: 2
      interval: 1s
    log:
      message: trying
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the retry outlives the stub
    workflow: ./workflow.yaml
    stubs:
      - task: log
        times: 1
        fails:
          message: transient
    expect:
      failed: true
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	var failing string
	for _, line := range result.Transcripts[0] {
		if strings.Contains(line.Text, "FAILED:") {
			failing = line.Text
		}
	}
	require.NotEmpty(t, failing, "the step's failure must be in the account")
	// The failure text itself rightly lists the verdicts ("stub 1 requires:
	// ... drained"); what must not appear is the renderer's *attribution*
	// suffix claiming that stub answered the outcome.
	assert.NotContains(t, failing, `stub 1 (task "log")`,
		"the failing attempt was answered by nothing; attributing it to the drained stub would be a false claim")
	assert.Contains(t, failing, "drained", "the diagnostic's own account of the drained stub stands")
}

// TestTranscriptSurvivesSeededExploration guards the direction of the
// only-record-the-baseline optimization (Codex, #1052): seeded runs record no
// account, and the written-order baseline — the one run whose account is
// kept — still must.
func TestTranscriptSurvivesSeededExploration(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: greet
steps:
  - id: hello
    log:
      message: hi
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: explored and accounted
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{Budget: dst.Budget{Schedules: 2, Seed0: 1}})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	require.Len(t, result.Transcripts, 1)
	require.NotEmpty(t, result.Transcripts[0],
		"the written-order baseline's account is the kept one, and exploration must not cost it")
	assert.Contains(t, transcriptText(result.Transcripts[0]), "hello")
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
