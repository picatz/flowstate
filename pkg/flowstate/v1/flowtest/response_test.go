package flowtest_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// `expect.response:` asserts the document a waiting receiver would answer a
// replayed delivery with. It is built by the function the receiver calls, from
// the run the case rehearsed, so what is pinned here is the shape a caller
// reads: completed with outputs, failed with a sentence, running when the run
// is parked. What a case cannot rehearse is the wait itself.

// respondWorkflow declares a bounded wait, two outputs (one sensitive), a step
// that fails on demand and a gate nothing answers unless the case scripts it.
const respondWorkflow = `
edition: v2026.4
name: respond
inputs:
  order_id:
    type: string
    required: true
  mode:
    type: string
    default: ok
triggers:
  - webhook: checkout
    verify:
      stripe: ${secret('env:CHECKOUT_SECRET')}
    idempotency_key: ${event.body.id}
    with:
      order_id: ${event.body.order.id}
      mode: ${event.body.mode}
    respond_within: 5s
steps:
  - id: charge
    log:
      message: ${'order ' + inputs.order_id}
  - id: hold
    if: ${inputs.mode == "hold"}
    wait_for_signal:
      name: released
outputs:
  order:
    value: ${inputs.order_id}
  token:
    value: ${'tok-' + inputs.order_id}
    sensitive: true
`

func respondDelivery(mode string) string {
	return `{
  "headers": {"Stripe-Signature": "t=1,v1=abc"},
  "body": {"id": "evt_9", "mode": "` + mode + `", "order": {"id": "ord_9"}}
}`
}

// runRespondCase runs one case against respondWorkflow and returns its report
// entry.
func runRespondCase(t *testing.T, mode, caseBody string) *v1.TestCase {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", respondWorkflow)
	writeFile(t, dir+"/delivery.json", respondDelivery(mode))
	writeFile(t, dir+"/x.test.yaml", `
defaults:
  stubs:
    - task: log
      returns: {}
tests:
  - name: a replayed checkout
    workflow: ./workflow.yaml
    trigger:
      webhook: checkout
      payload: ./delivery.json
`+caseBody)

	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused(), "the file was refused: %s", report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	return report.GetCases()[0]
}

func TestACompletedRunRehearsesTheCompletedDocument(t *testing.T) {
	t.Parallel()

	// The sensitive output is compared as the document carries it: withheld,
	// under the run document's own marker.
	got := runRespondCase(t, "ok", fmt.Sprintf(`
    expect:
      response:
        status: completed
        outputs:
          order: ord_9
          token: %q
`, v1.SensitiveRedactedMarker("token")))
	assert.True(t, got.GetPassed(), "failures: %v", failureText(got.GetFailures()))

	// The claim the other way: naming the real value is a claim about what the
	// caller would be told, and it is not what the caller is told.
	leaked := runRespondCase(t, "ok", `
    expect:
      response:
        status: completed
        outputs:
          order: ord_9
          token: tok-ord_9
`)
	require.False(t, leaked.GetPassed(), "a case asserted the sensitive value was in the response and passed")
}

func TestACompletedResponseHoldsOnlyTheDeclaredOutputsInBothDirections(t *testing.T) {
	t.Parallel()

	missing := runRespondCase(t, "ok", `
    expect:
      response:
        status: completed
        outputs:
          order: ord_9
`)
	require.False(t, missing.GetPassed(), "an output the claim does not name was not reported")
	assert.Contains(t, failureText(missing.GetFailures()), `unexpected output "token"`)

	extra := runRespondCase(t, "ok", `
    expect:
      response:
        status: completed
        outputs:
          order: ord_9
          nope: 1
          token: x
`)
	require.False(t, extra.GetPassed())
	assert.Contains(t, failureText(extra.GetFailures()), `expected output "nope"`)

	wrong := runRespondCase(t, "ok", `
    expect:
      response:
        status: completed
        outputs:
          order: ord_other
          token: x
`)
	require.False(t, wrong.GetPassed())
	assert.Contains(t, failureText(wrong.GetFailures()), `output "order": expected`)
}

func TestAFailingRunRehearsesTheFailedDocument(t *testing.T) {
	t.Parallel()

	failing := `
    stubs:
      - task: log
        fails:
          kind: Unavailable
          message: the ledger is down
`
	got := runRespondCase(t, "ok", failing+`
    expect:
      failed: true
      response:
        status: failed
`)
	assert.True(t, got.GetPassed(), "failures: %v", failureText(got.GetFailures()))

	// And a case claiming completed for that same run is told what it answers.
	wrong := runRespondCase(t, "ok", failing+`
    expect:
      failed: true
      response:
        status: completed
`)
	require.False(t, wrong.GetPassed())
	assert.Contains(t, failureText(wrong.GetFailures()), `answers "failed"`)
}

func TestAParkedRunRehearsesTheRunningDocument(t *testing.T) {
	t.Parallel()

	got := runRespondCase(t, "hold", `
    expect:
      ran: [charge]
      response:
        status: running
`)
	assert.True(t, got.GetPassed(), "failures: %v", failureText(got.GetFailures()))

	// Not completed, and not failed: the parked run is neither, and a case that
	// says either is told so.
	for _, claimed := range []string{"completed", "failed"} {
		wrong := runRespondCase(t, "hold", `
    expect:
      response:
        status: `+claimed+`
`)
		require.False(t, wrong.GetPassed(), "a parked run was claimed %s and passed", claimed)
		assert.Contains(t, failureText(wrong.GetFailures()), `answers "running"`)
	}
}

// TestAScriptedSignalLetsAGateCompleteSoTheRunAnswersCompleted: the park is the
// absence of an answer, not a property of the gate.
func TestAScriptedSignalLetsAGateCompleteSoTheRunAnswersCompleted(t *testing.T) {
	t.Parallel()

	got := runRespondCase(t, "hold", fmt.Sprintf(`
    signals:
      - name: released
    expect:
      response:
        status: completed
        outputs:
          order: ord_9
          token: %q
`, v1.SensitiveRedactedMarker("token")))
	assert.True(t, got.GetPassed(), "a gate the case answers was still reported parked: %v", failureText(got.GetFailures()))
}

func TestAResponseClaimNeedsAWebhookThatDeclaresRespondWithin(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", strings.Replace(respondWorkflow, "    respond_within: 5s\n", "", 1))
	writeFile(t, dir+"/delivery.json", respondDelivery("ok"))
	writeFile(t, dir+"/x.test.yaml", `
defaults:
  stubs:
    - task: log
      returns: {}
tests:
  - name: a claim with nothing to assert
    workflow: ./workflow.yaml
    trigger:
      webhook: checkout
      payload: ./delivery.json
    expect:
      response:
        status: completed
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Len(t, report.GetCases(), 1)
	require.False(t, report.GetCases()[0].GetPassed(), "a claim about a document that is never sent passed")
	assert.Contains(t, failureText(report.GetCases()[0].GetFailures()), "declares no `respond_within:`")
}

// TestAMalformedResponseClaimIsRefusedWhenTheFileLoads: nothing evaluates a
// claim that cannot mean anything, and a test that asserts nothing passes.
func TestAMalformedResponseClaimIsRefusedWhenTheFileLoads(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct{ body, want string }{
		"no status": {
			`    trigger: {webhook: checkout, payload: ./delivery.json}
    expect:
      response: {outputs: {order: x}}
`, "names no `status:`"},
		"an unknown status": {
			`    trigger: {webhook: checkout, payload: ./delivery.json}
    expect:
      response: {status: done}
`, "is not a status a receiver answers with"},
		"outputs on a failed answer": {
			`    trigger: {webhook: checkout, payload: ./delivery.json}
    expect:
      response: {status: failed, outputs: {order: x}}
`, "only a completed run's answer carries them"},
		"no trigger at all": {
			`    inputs: {order_id: ord_9}
    expect:
      response: {status: completed}
`, "replays no delivery"},
		"a stated context": {
			`    trigger: {kind: schedule, name: nightly}
    expect:
      response: {status: completed}
`, "replays none"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			writeFile(t, dir+"/workflow.yaml", respondWorkflow)
			writeFile(t, dir+"/delivery.json", respondDelivery("ok"))
			writeFile(t, dir+"/x.test.yaml", "tests:\n  - name: a malformed claim\n    workflow: ./workflow.yaml\n"+test.body)

			report := flowtest.RunFile(dir + "/x.test.yaml")
			require.Contains(t, report.GetRefused(), test.want)
		})
	}
}
