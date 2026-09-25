package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// A `trigger:` fixture replays a delivery that *starts* a run. A webhook
// carrying `signal:` starts nothing, so the two forms are not interchangeable —
// and the failure of letting them look interchangeable is the one this harness
// exists to prevent: a case that passes while rehearsing a different production
// path than the one it names.

// bridgeWorkflow declares a webhook that answers a gate rather than starting a
// run, which is the shape `trigger:` cannot replay.
const bridgeWorkflow = `
edition: v2026.3
name: bridged
signals:
  stage-approved:
    allow:
      - subject: flowstate://webhook#bridged/slack
triggers:
  - webhook: slack
    verify:
      hmac_sha256: ${secret('env:SLACK_SIGNING_SECRET')}
    idempotency_key: ${event.body.trigger_id}
    signal:
      name: stage-approved
      correlate: ${event.body.order}
      with:
        approved: ${event.body.action == "approve"}
steps:
  - id: gate
    wait_for_signal:
      name: stage-approved
      timeout: 1h
      outputs:
        approved: ${payload.?approved.orValue(false)}
`

// bridgeDelivery is a click, as a stored delivery would carry it.
const bridgeDelivery = `{
  "headers": {"X-Flowstate-Signature": "abc"},
  "body": {"trigger_id": "evt_1", "order": "order-4471", "action": "approve"}
}`

// TestATriggerFixtureIsRefusedForABridgeAndNamesTheAlternative is the review
// finding: this form silently rehearsed a run *start* for a webhook that
// answers a gate, binding through `BindWebhookTriggerInputs`, never evaluating
// `correlate:` and never delivering the payload to the gate.
//
// Refused rather than approximated. The refusal has to carry the alternative,
// because an author reaching for `trigger:` here is not making a typo — they
// are asking for the thing the other form does.
func TestATriggerFixtureIsRefusedForABridgeAndNamesTheAlternative(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", bridgeWorkflow)
	writeFile(t, dir+"/delivery.json", bridgeDelivery)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: a click replayed as a run start
    workflow: ./workflow.yaml
    trigger:
      webhook: slack
      payload: ./delivery.json
    expect:
      ran: [gate]
`)

	report := flowtest.RunFile(dir + "/x.test.yaml")

	// A fixture the harness cannot replay is a case *error* rather than a
	// failed assertion — the case never ran, so there is nothing to have failed
	// — which is where a mistake in the file itself belongs.
	require.Len(t, report.GetCases(), 1)
	require.False(t, report.GetCases()[0].GetPassed(), "a bridge replayed as a run start was accepted")

	reported := report.GetCases()[0].GetError()

	require.Containsf(t, reported, "declares `signal:`",
		"the case error does not say why the fixture cannot be replayed; it was %q", reported)
	assert.Contains(t, reported, "`signals:` entry naming",
		"the refusal has to name the form that does work")
	assert.Contains(t, reported, "delivery_id:",
		"the refusal has to name the key that expresses a redelivery")
	assert.Contains(t, reported, "stage-approved",
		"the refusal has to name the signal the author should script")
}

// TestAScriptedAnswerRehearsesTheBridgeTheTriggerFormRefuses is the other half
// of that refusal: the form it names actually works, on the same workflow and
// the same payload the delivery carries.
//
// The two together are what makes the refusal a signpost rather than a wall —
// and the redelivery case is the one an author most needs, because it is the
// behaviour that has no analogue on the start path.
func TestAScriptedAnswerRehearsesTheBridgeTheTriggerFormRefuses(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", bridgeWorkflow)
	writeFile(t, dir+"/x.test.yaml", `
defaults:
  sender:
    issuer: flowstate://webhook
    subject: bridged/slack
tests:
  - name: the click the trigger form could not replay answers the gate
    workflow: ./workflow.yaml
    signals:
      - name: stage-approved
        delivery_id: evt_1
        payload:
          approved: true
    expect:
      ran: [gate]

  - name: the same click delivered twice still answers one gate
    workflow: ./workflow.yaml
    signals:
      - name: stage-approved
        delivery_id: evt_1
        payload:
          approved: true
      - name: stage-approved
        delivery_id: evt_1
        payload:
          approved: true
    expect:
      ran: [gate]
`)

	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 2)
	for _, c := range report.GetCases() {
		assert.Truef(t, c.GetPassed(), "%s: %v", c.GetName(), c.GetFailures())
	}
}
