package flowtest_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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
edition: v2026.4
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
edition: v2026.4
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
edition: v2026.4
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
edition: v2026.4
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
edition: v2026.4
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
edition: v2026.4
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
edition: v2026.4
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
edition: v2026.4
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
edition: v2026.4
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
edition: v2026.4
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
edition: v2026.4
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
edition: v2026.4
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
edition: v2026.4
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
edition: v2026.4
name: keyed-secret
inputs:
  creds:
    type: map(string, dyn)
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
edition: v2026.4
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
edition: v2026.4
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

// TestTranscriptSurvivesACallerInstalledScheduler pins round sixteen's
// finding: a caller may install a scheduler on the context — the
// AdversarialOrder rehearsal pattern — with a zero budget, and that single
// run's account is the kept one, so it records; suppression is only ever
// about exploratory invocations whose accounts are discarded.
func TestTranscriptSurvivesACallerInstalledScheduler(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
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
  - name: adversarially scheduled, still accounted
    workflow: ./workflow.yaml
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [hello]
`)

	ctx := v1.NewContextWithScheduler(t.Context(), v1.AdversarialOrder)
	result := flowtest.RunPath(ctx, path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())

	require.Len(t, result.Transcripts, 1)
	require.NotEmpty(t, result.Transcripts[0],
		"the only run is the kept run, whatever scheduler the caller installed")
}

// TestTranscriptOfAFailingRunEndsOnTheFailure: the account a failing case
// arrives with shows the steps that ran and then the step it died on, in the
// danger tone — the whole reason the transcript exists.
func TestTranscriptOfAFailingRunEndsOnTheFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
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

// TestAnOutputMismatchRedactsAndBoundsWhatItPrints.
//
// `expect.outputs` and `expect.check` are two halves of one `expect:` block,
// and only one of them went through the redaction seam. `assertChecks` was
// handed the sensitive set (run.go:882); `assertExpectation`, on the line
// above it, was not — so a mismatched output rendered through a bare `%v`
// straight onto stdout, in the clear and unbounded, on an ordinary `flow test`
// run with no flag involved.
//
// Both halves of that are asserted here, because they fail independently: the
// value must not appear, and the message must not be the size of the value.
// An output may legitimately be [v1.MaxTaskOutputBytes] — near two megabytes —
// and one mismatched comparison used to print all of it as a single terminal
// line.
func TestAnOutputMismatchRedactsAndBoundsWhatItPrints(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: secretive-output
inputs:
  token:
    type: string
    required: true
    sensitive: true
steps:
  - id: use
    log:
      message: ${inputs.token}
outputs:
  echoed:
    value: ${steps.use.said}
    description: what the step said back
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the expected output is wrong, so the actual one is printed
    workflow: ./workflow.yaml
    inputs:
      token: hunter2-super-secret
    stubs:
      - task: log
        returns:
          said: ${inputs.message}
    expect:
      outputs:
        echoed: not-what-the-run-produced
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]

	require.False(t, c.GetPassed(), "the case has to fail, or this test is about nothing")
	require.NotEmpty(t, c.GetFailures())

	message := c.GetFailures()[0].GetMessage()

	assert.NotContains(t, message, "hunter2-super-secret",
		"a sensitive input's value reached expect.outputs and must not print in the clear")
	assert.Contains(t, message, "[redacted]",
		"and the reader is told something was withheld rather than shown a gap")

	// Bounded on the same 48-rune cap the sibling `expect.check` witnesses
	// take, so one large output cannot be most of a terminal line.
	assert.Less(t, len([]rune(message)), 200,
		"a mismatch message is a diagnostic, not a copy of the value it is about")
}

// TestTranscriptWithholdsACalleesSensitiveInput is #2211: a value only a
// called workflow declares `sensitive: true` is withheld from the case's
// transcript and its report, as the root's own declarations are, although the
// case's posture is built from the root's alone. Rendering only: a claim over
// the failure's real text still holds.
func TestTranscriptWithholdsACalleesSensitiveInput(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-callee-only-secret"
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "child.yaml"), `
edition: v2026.4
name: child
inputs:
  api_key:
    type: string
    required: true
    sensitive: true
steps:
  - id: boom
    value: ${{"a":1}[inputs.api_key]}
`)
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: parent
steps:
  - id: nested
    call: ./child.yaml
    with:
      api_key: ${"`+secret+`"}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the failure is reported
    workflow: ./workflow.yaml
    expect:
      failed: false
  - name: the failure's real text is what claims read
    workflow: ./workflow.yaml
    expect:
      failed: true
      error_contains: "no such key: `+secret+`"
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	cases := result.Report.GetCases()
	require.Len(t, cases, 2)

	reported := cases[0]
	require.False(t, reported.GetPassed(), "the case expecting success passed, so its report proves nothing")
	var messages []string
	for _, failure := range reported.GetFailures() {
		messages = append(messages, failure.GetMessage())
	}
	joined := strings.Join(messages, "\n") + "\n" + reported.GetError()
	assert.Contains(t, joined, "no such key", "the failure is not quoted, so this proves nothing")
	assert.NotContains(t, joined, secret, "the report showed the callee's sensitive input")

	require.Len(t, result.Transcripts, 2)
	text := transcriptText(result.Transcripts[0])
	assert.Contains(t, text, "FAILED", "the transcript has no failure, so this proves nothing")
	assert.NotContains(t, text, secret, "the transcript showed the callee's sensitive input")

	assert.True(t, cases[1].GetPassed(), "a claim over the real failure text failed: %v / %v", cases[1].GetError(), cases[1].GetFailures())
}

// TestASeededRunWithholdsACalleesSensitiveInputAsTheRecordedOneDoes: a
// seeded schedule's run discards its account, but renders its failures as the
// recorded written-order run does. Otherwise the two differ by the withheld
// value alone, which is reported as a divergence the schedule never made, and
// carries that value (Codex, #2215).
func TestASeededRunWithholdsACalleesSensitiveInputAsTheRecordedOneDoes(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-callee-only-secret"
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "child.yaml"), `
edition: v2026.4
name: child
inputs:
  api_key:
    type: string
    required: true
    sensitive: true
steps:
  - id: boom
    value: ${{"a":1}[inputs.api_key]}
`)
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: parent
steps:
  - id: nested
    call: ./child.yaml
    with:
      api_key: ${"`+secret+`"}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the failure is reported
    workflow: ./workflow.yaml
    expect:
      failed: false
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{Budget: dst.Budget{Schedules: 3, Seed0: 1}})
	require.NotNil(t, result.Schedules, "nothing was explored, so this proves nothing")
	require.Equal(t, 3, result.Schedules.Schedules)
	if divergence := result.Schedules.Divergence; divergence != nil {
		t.Fatalf("a schedule-insensitive case diverged under seed %d:\nwritten order:\n%s\nseeded:\n%s",
			divergence.Seed, divergence.WrittenOrder, divergence.Seeded)
	}
	encoded, err := protojson.Marshal(result.Report)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), secret, "the report showed the callee's sensitive input")
}

// TestTranscriptWithholdsACalleesSensitiveInputReadBackByTheCaller: the
// transcript is rendered after the run, from everything its steps withheld, so
// a callee's sensitive input handed back as an output and read by a later step
// of the caller is withheld there too (#2211). A claim still reads the value.
func TestTranscriptWithholdsACalleesSensitiveInputReadBackByTheCaller(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-callee-only-secret"
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "child.yaml"), `
edition: v2026.4
name: child
inputs:
  api_key:
    type: string
    required: true
    sensitive: true
steps:
  - id: use
    value: ${1}
outputs:
  key:
    value: ${inputs.api_key}
`)
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: parent
steps:
  - id: nested
    call: ./child.yaml
    with:
      api_key: ${"`+secret+`"}
  - id: echo
    value: ${steps.nested.key}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the caller reads what the callee handed back
    workflow: ./workflow.yaml
    expect:
      ran: [nested, echo]
      check:
        - that: steps.echo.value == "`+secret+`"
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.True(t, c.GetPassed(), "a claim over the real value failed: %v / %v", c.GetError(), c.GetFailures())

	require.Len(t, result.Transcripts, 1)
	text := transcriptText(result.Transcripts[0])
	assert.Contains(t, text, "echo", "the caller's step is not in the transcript, so this proves nothing")
	assert.NotContains(t, text, secret, "the transcript showed a callee's sensitive input read back by the caller")
}

// TestAReportUnderARootsLargeSensitiveInputStaysRedacted: a callee's position
// holds the root's sensitive values, and so does the failure it carries out.
// Counted twice, 600 of them passed the bound, the gathered set withheld
// everything, and the expectation's message printed the run's error as it
// was (exact-head review, #2215). Held once, they stay under it.
func TestAReportUnderARootsLargeSensitiveInputStaysRedacted(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "child.yaml"), `
edition: v2026.4
name: child
inputs:
  k:
    type: string
    required: true
steps:
  - id: boom
    value: ${{"a":1}[inputs.k]}
`)
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: parent
inputs:
  items:
    type: list(dyn)
    required: true
    sensitive: true
steps:
  - id: nested
    call: ./child.yaml
    with:
      k: ${inputs.items[0]}
`)
	items := make([]string, 0, 600)
	for i := range 600 {
		items = append(items, fmt.Sprintf("      - rootsecret%04d", i))
	}
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the failure is reported
    workflow: ./workflow.yaml
    inputs:
      items:
`+strings.Join(items, "\n")+`
    expect:
      failed: false
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	cases := result.Report.GetCases()
	require.Len(t, cases, 1)
	require.NotEmpty(t, cases[0].GetFailures(), "the case reported no failure, so this proves nothing")
	message := cases[0].GetFailures()[0].GetMessage()
	assert.Contains(t, message, "no such key: [redacted]", "the failure was not redacted value by value")
	assert.NotContains(t, message, "rootsecret0000", "the report showed the root's sensitive input")
}

// TestAReportWithholdsWhatACalleesUnenumerableSetQuotes: a callee whose own
// sensitive input is too large to enumerate makes the gathered set withhold
// everything. The run did not run under that posture, so nothing at the stub
// boundary shaped its error, and the expectation's message withholds it whole
// rather than printing it (exact-head review, #2215).
func TestAReportWithholdsWhatACalleesUnenumerableSetQuotes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "child.yaml"), `
edition: v2026.4
name: child
inputs:
  items:
    type: list(dyn)
    required: true
    sensitive: true
steps:
  - id: boom
    value: ${{"a":1}[inputs.items[0]]}
`)
	items := make([]string, 0, 1100)
	for i := range 1100 {
		items = append(items, fmt.Sprintf("%q", fmt.Sprintf("calleesecret%04d", i)))
	}
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: parent
steps:
  - id: nested
    call: ./child.yaml
    with:
      items: ${[`+strings.Join(items, ", ")+`]}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the failure is reported
    workflow: ./workflow.yaml
    expect:
      failed: false
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	cases := result.Report.GetCases()
	require.Len(t, cases, 1)
	require.NotEmpty(t, cases[0].GetFailures(), "the case reported no failure, so this proves nothing: %s", cases[0].GetError())
	message := cases[0].GetFailures()[0].GetMessage()
	assert.Contains(t, message, "[withheld]", "the failure was not withheld whole")
	assert.NotContains(t, message, "calleesecret0000", "the report showed the callee's sensitive input")
}

// TestAReportUnderAnUnenumerablePostureWithholdsAnUnshapedError: a case whose
// own posture withholds everything prints a run's error as it is only when the
// stub boundary shaped it. An evaluation error quoting the value it could not
// find was shaped by nothing, and is withheld whole (exact-head review, #2215).
func TestAReportUnderAnUnenumerablePostureWithholdsAnUnshapedError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: parent
inputs:
  items:
    type: list(dyn)
    required: true
    sensitive: true
steps:
  - id: boom
    value: ${{"a":1}[inputs.items[0]]}
`)
	items := make([]string, 0, 1100)
	for i := range 1100 {
		items = append(items, fmt.Sprintf("      - rootsecret%04d", i))
	}
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the failure is reported
    workflow: ./workflow.yaml
    inputs:
      items:
`+strings.Join(items, "\n")+`
    expect:
      failed: false
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	cases := result.Report.GetCases()
	require.Len(t, cases, 1)
	require.NotEmpty(t, cases[0].GetFailures(), "the case reported no failure, so this proves nothing: %s", cases[0].GetError())
	message := cases[0].GetFailures()[0].GetMessage()
	assert.Contains(t, message, "[withheld]", "the failure was not withheld whole")
	assert.NotContains(t, message, "rootsecret0000", "the report showed the root's sensitive input")
}

// TestAReportPrintsOnlyTheStubsOwnDiagnosticRaw: the raw rendering a
// withhold-everything posture keeps for a stub's diagnostic covers that
// diagnostic alone. A compensation's failure the run appends after it was
// shaped by nothing, and it withholds the whole error rather than riding out
// beside the diagnostic (exact-head review, #2215).
func TestAReportPrintsOnlyTheStubsOwnDiagnosticRaw(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: parent
inputs:
  items:
    type: list(dyn)
    required: true
    sensitive: true
steps:
  - id: network
    log:
      message: hi
    undo:
      log:
        message: ${"undo " + inputs.items[0]}
  - id: volume
    http:
      url: https://example.invalid/volume
`)
	items := make([]string, 0, 1100)
	for i := range 1100 {
		items = append(items, fmt.Sprintf("      - rootsecret%04d", i))
	}
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the failure and its compensation are reported
    workflow: ./workflow.yaml
    inputs:
      items:
`+strings.Join(items, "\n")+`
    stubs:
      - task: log
        where: inputs.message == "hi"
        returns: {}
      - task: log
        returns:
          said: ${{"a":1}[inputs.message]}
    expect:
      failed: false
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	cases := result.Report.GetCases()
	require.Len(t, cases, 1)
	require.NotEmpty(t, cases[0].GetFailures(), "the case reported no failure, so this proves nothing: %s", cases[0].GetError())
	message := cases[0].GetFailures()[0].GetMessage()
	assert.NotContains(t, message, "rootsecret0000", "the compensation's failure rode out beside the stub's diagnostic")
	assert.Contains(t, message, "[withheld]", "the error was not withheld whole")
}

// TestAnUnstubbedTasksDiagnosticIsReadableUnderAnUnenumerablePosture: an
// unstubbed task's diagnostic names only the task, and the stub boundary
// built it, so a posture that withholds everything still prints it rather
// than an unactionable `[withheld]`.
func TestAnUnstubbedTasksDiagnosticIsReadableUnderAnUnenumerablePosture(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: parent
inputs:
  items:
    type: list(dyn)
    required: true
    sensitive: true
steps:
  - id: volume
    http:
      url: https://example.invalid/volume
`)
	items := make([]string, 0, 1100)
	for i := range 1100 {
		items = append(items, fmt.Sprintf("      - rootsecret%04d", i))
	}
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the unstubbed task is named
    workflow: ./workflow.yaml
    inputs:
      items:
`+strings.Join(items, "\n")+`
    expect:
      failed: false
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	cases := result.Report.GetCases()
	require.Len(t, cases, 1)
	require.NotEmpty(t, cases[0].GetFailures(), "the case reported no failure, so this proves nothing: %s", cases[0].GetError())
	assert.Contains(t, cases[0].GetFailures()[0].GetMessage(), `task "http" was invoked, but this case declares no stub for it`)
}

// TestAnUnmetErrorContainsWithholdsItsOwnExpectation: a case can expect the
// very value it withholds, and when that expectation is unmet its diagnostic
// quotes it. It is rendered as the run's error is (Codex, #2215); the
// comparison still reads it as written.
func TestAnUnmetErrorContainsWithholdsItsOwnExpectation(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-callee-only-secret"
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "child.yaml"), `
edition: v2026.4
name: child
inputs:
  api_key:
    type: string
    required: true
    sensitive: true
steps:
  - id: boom
    value: ${{"a":1}[inputs.api_key]}
`)
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: parent
steps:
  - id: nested
    call: ./child.yaml
    with:
      api_key: ${"`+secret+`"}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: expects a failure the run does not report
    workflow: ./workflow.yaml
    expect:
      failed: true
      error_contains: "denied: `+secret+`"
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	cases := result.Report.GetCases()
	require.Len(t, cases, 1)
	var message string
	for _, failure := range cases[0].GetFailures() {
		if failure.GetField() == "expect.error_contains" {
			message = failure.GetMessage()
		}
	}
	require.NotEmpty(t, message, "no error_contains diagnostic, so this proves nothing: %v", cases[0].GetFailures())
	assert.Contains(t, message, "denied: ", "the expectation was not quoted, so this proves nothing")
	assert.NotContains(t, message, secret, "the unmet expectation showed the callee's sensitive input")
}

// TestAReportIsNotFooledByADiagnosticRepeatedAtTheEnd: a compensation that
// reaches the same unstubbed task as the failure ends the run's error with the
// same diagnostic, so its end alone cannot tell the diagnostic from what was
// appended; another compensation's failure between them quotes a value. The
// error is judged layer by layer, and withheld whole (exact-head review,
// #2215).
func TestAReportIsNotFooledByADiagnosticRepeatedAtTheEnd(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: parent
inputs:
  items:
    type: list(dyn)
    required: true
    sensitive: true
steps:
  - id: first
    log:
      message: hi
    undo:
      http:
        url: https://example.invalid/first
  - id: second
    log:
      message: hi
    undo:
      log:
        message: ${"undo " + inputs.items[0]}
  - id: volume
    http:
      url: https://example.invalid/volume
`)
	items := make([]string, 0, 1100)
	for i := range 1100 {
		items = append(items, fmt.Sprintf("      - rootsecret%04d", i))
	}
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the failure and its compensations are reported
    workflow: ./workflow.yaml
    inputs:
      items:
`+strings.Join(items, "\n")+`
    stubs:
      - task: log
        where: inputs.message == "hi"
        returns: {}
      - task: log
        returns:
          said: ${{"a":1}[inputs.message]}
    expect:
      failed: false
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	cases := result.Report.GetCases()
	require.Len(t, cases, 1)
	require.NotEmpty(t, cases[0].GetFailures(), "the case reported no failure, so this proves nothing: %s", cases[0].GetError())
	message := cases[0].GetFailures()[0].GetMessage()
	assert.NotContains(t, message, "rootsecret0000", "a compensation's failure rode out between two copies of the diagnostic")
	assert.Contains(t, message, "[withheld]", "the error was not withheld whole")
}

// TestAReportKeepsADiagnosticShapedInsideAnUnenumerableCallee: an unmatched
// stub inside a callee whose sensitive input cannot be enumerated builds its
// diagnostic under that callee's position, which withholds every input it
// quotes. The report prints it as it is, as it does for the same diagnostic
// under a root that cannot be enumerated, rather than withholding the task's
// name and the remedy with it (Codex, #2215). Its `where:` is withheld, so a
// root secret written into one does not print.
func TestAReportKeepsADiagnosticShapedInsideAnUnenumerableCallee(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "child.yaml"), `
edition: v2026.4
name: child
inputs:
  items:
    type: list(dyn)
    required: true
    sensitive: true
steps:
  - id: probe
    http:
      url: ${"https://example.invalid/" + inputs.items[0]}
`)
	items := make([]string, 0, 1100)
	for i := range 1100 {
		items = append(items, fmt.Sprintf("%q", fmt.Sprintf("calleesecret%04d", i)))
	}
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: parent
inputs:
  token:
    type: string
    required: true
    sensitive: true
steps:
  - id: nested
    call: ./child.yaml
    with:
      items: ${[`+strings.Join(items, ", ")+`]}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the unmatched stub is reported
    workflow: ./workflow.yaml
    inputs:
      token: rootsecretvalue
    stubs:
      - task: http
        where: inputs.url == 'https://nope.invalid/rootsecretvalue'
    expect:
      failed: false
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	cases := result.Report.GetCases()
	require.Len(t, cases, 1)
	require.NotEmpty(t, cases[0].GetFailures(), "the case reported no failure, so this proves nothing: %s", cases[0].GetError())
	message := cases[0].GetFailures()[0].GetMessage()
	assert.Contains(t, message, "could not be enumerated", "the stub's own diagnostic was withheld with the values it withholds")
	assert.Contains(t, message, "[redacted: url]")
	assert.NotContains(t, message, "calleesecret0000", "the report showed the callee's sensitive input")
	// The root's value, written into a `where:` the diagnostic keeps as
	// written, is still withheld by the posture that could enumerate it
	// (exact-head review, #2215).
	assert.Contains(t, message, "[withheld: where:]", "the stub's where: was not withheld")
	assert.NotContains(t, message, "rootsecretvalue", "the report showed a root's sensitive value written into a where:")
}

// TestAReportWithholdsASensitiveRootInputTheBindRefused: a root input
// declared `sensitive:` that its own `must:` refuses is quoted by the
// refusal, and no step ever holds it for the run's gatherer to hear. The
// case's report withholds it by what the case submitted, as `cmd/flow`
// withholds the same failure (Codex, #2215).
func TestAReportWithholdsASensitiveRootInputTheBindRefused(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-refused-root-secret"
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: refused
inputs:
  token:
    type: string
    required: true
    sensitive: true
    must: this == "expected"
steps:
  - id: use
    value: ${inputs.token}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the refusal is reported
    workflow: ./workflow.yaml
    inputs:
      token: `+secret+`
    expect:
      failed: false
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	cases := result.Report.GetCases()
	require.Len(t, cases, 1)
	var shown []string
	for _, failure := range cases[0].GetFailures() {
		shown = append(shown, failure.GetMessage())
	}
	joined := strings.Join(shown, "\n") + "\n" + cases[0].GetError()
	require.Contains(t, joined, "must satisfy", "the refusal is not reported, so this proves nothing")
	assert.NotContains(t, joined, secret, "the report showed the refused sensitive input")
	assert.Contains(t, joined, "[redacted]")
}

// TestAReportWithholdsADiagnosticRaisedBeforeEverythingWasWithheld: an
// unmatched stub at the root quotes a value a callee that could not enumerate
// its set handed back. Before #2213 the root's position did not know what the
// callee handed back, so its diagnostic was not shaped and the report
// withheld it whole (Codex, #2215). The root's position now withholds
// everything the callee handed back, so the diagnostic is shaped where it is
// raised, every input withheld, and printed; the value never appears.
func TestAReportWithholdsADiagnosticRaisedBeforeEverythingWasWithheld(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "child.yaml"), `
edition: v2026.4
name: child
inputs:
  items:
    type: list(dyn)
    required: true
    sensitive: true
steps:
  - id: use
    value: ${1}
outputs:
  first:
    value: ${inputs.items[0]}
`)
	items := make([]string, 0, 1100)
	for i := range 1100 {
		items = append(items, fmt.Sprintf("%q", fmt.Sprintf("calleesecret%04d", i)))
	}
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: parent
steps:
  - id: nested
    call: ./child.yaml
    with:
      items: ${[`+strings.Join(items, ", ")+`]}
  - id: probe
    http:
      url: ${"https://example.invalid/" + steps.nested.first}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the unmatched stub is reported
    workflow: ./workflow.yaml
    stubs:
      - task: http
        where: inputs.url == 'https://nope.invalid/'
    expect:
      failed: false
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	cases := result.Report.GetCases()
	require.Len(t, cases, 1)
	require.NotEmpty(t, cases[0].GetFailures(), "the case reported no failure, so this proves nothing: %s", cases[0].GetError())
	message := cases[0].GetFailures()[0].GetMessage()
	assert.Contains(t, message, "could not be enumerated", "the root's position did not withhold what the callee handed back")
	assert.Contains(t, message, "[redacted: url]")
	assert.NotContains(t, message, "calleesecret0000", "the report showed the callee's sensitive input")
}

// TestAReportUnderAnUnenumerableRootWithholdsWhatItCanStillEnumerate: one
// root input too large to enumerate makes the case's posture withhold
// everything, and a stub diagnostic shaped under it is printed as it is. Its
// `where:` is withheld, so another root input's value, or the case's own
// `secrets:` plaintext, written into it does not print (Codex, #2215).
func TestAReportUnderAnUnenumerableRootWithholdsWhatItCanStillEnumerate(t *testing.T) {
	t.Parallel()

	var bulk strings.Builder
	for i := range 1100 {
		fmt.Fprintf(&bulk, "\n        - element-%d", i)
	}
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: bulk
inputs:
  bulk:
    type: list(dyn)
    sensitive: true
    required: true
  token:
    type: string
    sensitive: true
    required: true
steps:
  - id: call
    http:
      url: https://example.invalid/probe
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the unmatched stub is reported
    workflow: ./workflow.yaml
    inputs:
      token: rootsecretvalue
      bulk:`+bulk.String()+`
    secrets:
      env:TOKEN: casesecretplain
    stubs:
      - task: http
        where: inputs.url == 'https://nope.invalid/rootsecretvalue/casesecretplain'
    expect:
      failed: false
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	cases := result.Report.GetCases()
	require.Len(t, cases, 1)
	require.NotEmpty(t, cases[0].GetFailures(), "the case reported no failure, so this proves nothing: %s", cases[0].GetError())
	message := cases[0].GetFailures()[0].GetMessage()
	assert.Contains(t, message, "could not be enumerated", "the stub's diagnostic was not printed, so this proves nothing")
	assert.Contains(t, message, "[withheld: where:]", "the stub's where: was not withheld")
	assert.NotContains(t, message, "rootsecretvalue", "another root input's value printed")
	assert.NotContains(t, message, "casesecretplain", "the case's secret printed")
	assert.NotContains(t, message, "element-7", "the unenumerable input printed")
}

// TestAReportWithholdsInputsThatPassTheBoundOnlyTogether: two root inputs
// declared sensitive, each enumerable on its own, together past the bound.
// The case's posture withholds everything, a stub diagnostic shaped under it
// is printed as it is, and its `where:`, holding a value of each, is withheld
// (Codex, #2215).
func TestAReportWithholdsInputsThatPassTheBoundOnlyTogether(t *testing.T) {
	t.Parallel()

	var alpha, beta strings.Builder
	for i := range 600 {
		fmt.Fprintf(&alpha, "\n        - alpha%04d", i)
		fmt.Fprintf(&beta, "\n        - beta%04d", i)
	}
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: two-lists
inputs:
  alpha:
    type: list(dyn)
    sensitive: true
    required: true
  beta:
    type: list(dyn)
    sensitive: true
    required: true
steps:
  - id: call
    http:
      url: https://example.invalid/probe
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the unmatched stub is reported
    workflow: ./workflow.yaml
    inputs:
      alpha:`+alpha.String()+`
      beta:`+beta.String()+`
    stubs:
      - task: http
        where: inputs.url == 'https://nope.invalid/alpha0005/beta0007'
    expect:
      failed: false
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	cases := result.Report.GetCases()
	require.Len(t, cases, 1)
	require.NotEmpty(t, cases[0].GetFailures(), "the case reported no failure, so this proves nothing: %s", cases[0].GetError())
	message := cases[0].GetFailures()[0].GetMessage()
	assert.Contains(t, message, "could not be enumerated", "the stub's diagnostic was not printed, so this proves nothing")
	assert.Contains(t, message, "[withheld: where:]", "the stub's where: was not withheld")
	assert.NotContains(t, message, "alpha0005", "the first input's value printed")
	assert.NotContains(t, message, "beta0007", "the second input's value printed")
}

// TestAReportWithholdsOverlappingValuesWhole: under an opaque root, a case's
// `secrets:` plaintext found inside a root input's value written into a
// `where:` leaves no fragment of either in the printed diagnostic, since the
// `where:` is withheld whole (exact-head review, #2215).
func TestAReportWithholdsOverlappingValuesWhole(t *testing.T) {
	t.Parallel()

	var bulk strings.Builder
	for i := range 1100 {
		fmt.Fprintf(&bulk, "\n        - element-%d", i)
	}
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: overlap
inputs:
  bulk:
    type: list(dyn)
    sensitive: true
    required: true
  token:
    type: string
    sensitive: true
    required: true
steps:
  - id: call
    http:
      url: https://example.invalid/probe
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the unmatched stub is reported
    workflow: ./workflow.yaml
    inputs:
      token: hunter2passwordtail
      bulk:`+bulk.String()+`
    secrets:
      env:TOKEN: password
    stubs:
      - task: http
        where: inputs.url == 'https://nope.invalid/hunter2passwordtail'
    expect:
      failed: false
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	cases := result.Report.GetCases()
	require.Len(t, cases, 1)
	require.NotEmpty(t, cases[0].GetFailures(), "the case reported no failure, so this proves nothing: %s", cases[0].GetError())
	message := cases[0].GetFailures()[0].GetMessage()
	assert.Contains(t, message, "[withheld: where:]", "the stub's where: was not withheld")
	assert.NotContains(t, message, "hunter2", "a fragment of the root input's value printed")
	assert.NotContains(t, message, "tail'", "a fragment of the root input's value printed")
}

// TestTranscriptWithholdsACalleesSensitiveOutput: an output a called workflow
// declares `sensitive:`, computed from nothing it declares sensitive, is
// withheld where the call hands it back, and wherever the caller reads it
// after that (#2213).
func TestTranscriptWithholdsACalleesSensitiveOutput(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-callee-output-secret"
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "child.yaml"), `
edition: v2026.4
name: child
inputs:
  seed:
    type: string
    required: true
steps:
  - id: use
    value: ${1}
outputs:
  token:
    value: ${inputs.seed}
    sensitive: true
`)
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: parent
steps:
  - id: nested
    call: ./child.yaml
    with:
      seed: ${"`+secret+`"}
  - id: copied
    value: ${"Bearer " + steps.nested.token}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
tests:
  - name: the case fails so its transcript prints
    workflow: ./workflow.yaml
    expect:
      failed: true
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	require.Len(t, result.Transcripts, 1)
	text := transcriptText(result.Transcripts[0])
	require.Contains(t, text, "copied", "the caller's later step is not in the transcript, so this proves nothing")
	assert.NotContains(t, text, secret, "the transcript showed the callee's sensitive output")
	assert.Contains(t, text, "[redacted]")
}

// TestACaseErrorWithholdsASensitiveSubject is #2100 on `flow test`: a gate
// whose `subject:` reads a sensitive input, bound to a value that is not
// `<issuer>#<subject>`, is refused before the run, and the case's error
// quotes what it resolved to. It is rendered through the run's own set, as
// the transcript is.
func TestACaseErrorWithholdsASensitiveSubject(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.4
name: sensitive-subject
inputs:
  approver:
    type: string
    required: true
    sensitive: true
signals:
  approve:
    distinct_from_starter: true
    allow:
      - subject: ${inputs.approver}
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
  - name: a bare subject is refused
    workflow: ./workflow.yaml
    inputs:
      approver: approver-"lead"@corp.example
    expect:
      ran: [gate]
`)

	result := flowtest.RunPath(t.Context(), path, flowtest.RunOptions{})
	c := result.Report.GetCases()[0]
	require.Contains(t, c.GetError(), "<issuer>#<subject>", "the case was not refused at its signal policy, so this proves nothing")
	assert.NotContains(t, c.GetError(), "lead", "the case's error quotes the sensitive input")
}
