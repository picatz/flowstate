package flowtest_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// Replaying a stored delivery is what makes a trigger's argument mapping
// testable at all, so what these pin is mostly the *negative* direction: a case
// asserting a refusal that does not happen must fail, a mapping that is wrong
// must fail, and a case naming a webhook the workflow does not declare must be
// refused rather than quietly passing.

// triggerWorkflow is the file every case below replays against.
const triggerWorkflow = `
edition: v2026.3
name: delivery
inputs:
  order_id:
    type: string
    required: true
  amount:
    type: int
    required: true
triggers:
  - webhook: stripe
    verify:
      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}
    idempotency_key: ${event.body.id}
    with:
      order_id: ${event.body.order.id}
      amount: ${event.body.order.total}
steps:
  - id: record
    log:
      message: ${'order ' + inputs.order_id}
`

// storedDelivery is one arrival: headers and body, the way a delivery arrives.
const storedDelivery = `{
  "headers": {"Stripe-Signature": "t=1,v1=abc"},
  "body": {"id": "evt_9", "order": {"id": "ord_9", "total": 4200}}
}`

// writeDeliveryFixture lays down the workflow and the stored delivery a case
// replays, and returns the directory holding them.
func writeDeliveryFixture(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", triggerWorkflow)
	writeFile(t, dir+"/delivery.json", storedDelivery)

	return dir
}

// TestAStoredDeliveryProducesTheMappedInputs is the happy path, and the reason
// the feature is worth having offline: the mapping is evaluated for real, so a
// `with:` reaching a field the payload does not carry fails here rather than in
// production.
func TestAStoredDeliveryProducesTheMappedInputs(t *testing.T) {
	t.Parallel()

	dir := writeDeliveryFixture(t)
	writeFile(t, dir+"/x.test.yaml", `
defaults:
  stubs:
    - task: log
      returns: {}
tests:
  - name: a delivery starts a run with the mapped inputs
    workflow: ./workflow.yaml
    trigger:
      webhook: stripe
      payload: ./delivery.json
    expect:
      inputs:
        order_id: ord_9
        amount: 4200
      idempotency_key: evt_9
      ran: [record]
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "failures: %v", report.GetCases()[0].GetFailures())
}

// TestAFilesBaseInputsDoNotOverrideADelivery: a `defaults.inputs:` block is
// inherited by an ordinary case and not by a trigger case, whose inputs are the
// thing under test. Inheriting them would override the mapping silently, and
// would refuse the case for something its author never wrote.
func TestAFilesBaseInputsDoNotOverrideADelivery(t *testing.T) {
	t.Parallel()

	dir := writeDeliveryFixture(t)
	writeFile(t, dir+"/x.test.yaml", `
defaults:
  inputs:
    order_id: ord_default
    amount: 1
  stubs:
    - task: log
      returns: {}
tests:
  - name: the delivery decides the inputs
    workflow: ./workflow.yaml
    trigger:
      webhook: stripe
      payload: ./delivery.json
    expect:
      inputs:
        order_id: ord_9
        amount: 4200
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "failures: %v", report.GetCases()[0].GetFailures())
}

// TestAnUnverifiableDeliveryIsRefused is the negative direction #491 asks for,
// and the shape it has to take: a refused delivery does not produce a failed run,
// it produces no run.
func TestAnUnverifiableDeliveryIsRefused(t *testing.T) {
	t.Parallel()

	dir := writeDeliveryFixture(t)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: an unverifiable delivery is refused
    workflow: ./workflow.yaml
    trigger:
      webhook: stripe
      payload: ./delivery.json
      signature: invalid
    expect:
      refused: true
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	assert.True(t, report.GetCases()[0].GetPassed(), "failures: %v", report.GetCases()[0].GetFailures())
}

// TestARefusalThatDoesNotHappenFailsTheCase is the assertion that keeps the one
// above honest. A harness where `refused: true` passed whatever happened would be
// a green test that should be red, which is the one failure mode a test framework
// may not have.
func TestARefusalThatDoesNotHappenFailsTheCase(t *testing.T) {
	t.Parallel()

	dir := writeDeliveryFixture(t)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: a verified delivery is claimed to be refused
    workflow: ./workflow.yaml
    trigger:
      webhook: stripe
      payload: ./delivery.json
    expect:
      refused: true
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Len(t, report.GetCases(), 1)
	assert.False(t, report.GetCases()[0].GetPassed())
	assert.Contains(t, failureText(report.GetCases()[0].GetFailures()), "it was accepted and mapped to inputs")
}

// TestAMappingThatDisagreesFailsTheCase, in both directions: an input the case
// expects and did not get, and one it got and does not name — the second being
// the drift a `with:` block exists to make visible when an input is added and a
// call site is not updated.
func TestAMappingThatDisagreesFailsTheCase(t *testing.T) {
	t.Parallel()

	dir := writeDeliveryFixture(t)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: the mapping is asserted wrongly
    workflow: ./workflow.yaml
    trigger:
      webhook: stripe
      payload: ./delivery.json
    expect:
      inputs:
        order_id: ord_wrong
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Len(t, report.GetCases(), 1)
	assert.False(t, report.GetCases()[0].GetPassed())

	text := failureText(report.GetCases()[0].GetFailures())
	assert.Contains(t, text, `input "order_id": expected string "ord_wrong", got string "ord_9"`)
	assert.Contains(t, text, `unexpected input "amount"`)
}

// TestAnIdempotencyKeyThatDisagreesFailsTheCase: the key decides whether a
// redelivery starts a second run, and an expression reaching the wrong field is
// wrong in the direction nothing else notices.
func TestAnIdempotencyKeyThatDisagreesFailsTheCase(t *testing.T) {
	t.Parallel()

	dir := writeDeliveryFixture(t)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: the key is asserted wrongly
    workflow: ./workflow.yaml
    trigger:
      webhook: stripe
      payload: ./delivery.json
    expect:
      idempotency_key: evt_other
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Len(t, report.GetCases(), 1)
	assert.False(t, report.GetCases()[0].GetPassed())
	assert.Contains(t, failureText(report.GetCases()[0].GetFailures()), "expected the delivery to be named")
}

// TestARetrySignedAfreshReplaysAsTheSameEvent is the rehearsal's side of
// [conformance.WebhookRedeliveryCases]: every attempt, signed afresh against
// the harness's own epoch, verifies under the bound key and is named by the
// value the sender repeats. The served receiver asserts that the same name is
// the same run; what this pins is that `flow test` does not name a retry
// differently from production, which is the one way a rehearsal could pass a
// file whose deployment starts a run per retry.
func TestARetrySignedAfreshReplaysAsTheSameEvent(t *testing.T) {
	t.Parallel()

	// The harness's clock epoch, which is what its computed verification
	// measures a signed timestamp against — public in docs/DSL.md rather than
	// exported, so it is spelled here rather than read.
	epoch := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	key := secrets.NewSecret(secrets.NewRef("env", "STRIPE_WEBHOOK_SECRET"), "whsec_conformance")

	for _, test := range conformance.WebhookRedeliveryCases() {
		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: `+test.Workflow.GetName()+`
inputs:
  order_id:
    type: string
    required: true
triggers:
  - webhook: `+test.Trigger().GetName()+`
    verify:
      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}
    idempotency_key: ${event.body.id}
    with:
      order_id: ${event.body.data.object.metadata.order_id}
steps:
  - id: record
    value: ${"order " + inputs.order_id}
`)

			var cases strings.Builder
			for attempt := range test.SignedAt {
				headers, err := json.Marshal(test.Headers(key, epoch, attempt))
				require.NoError(t, err)

				// The body is the case's exact bytes, because the signature
				// is over them and a re-rendering would be a different body.
				fixture := fmt.Sprintf(`{"headers": %s, "body": %s}`, headers, test.Body)
				writeFile(t, fmt.Sprintf("%s/attempt-%d.json", dir, attempt), fixture)

				fmt.Fprintf(&cases, `
  - name: attempt %d is named the same event
    workflow: ./workflow.yaml
    secrets:
      "env:STRIPE_WEBHOOK_SECRET": whsec_conformance
    trigger:
      webhook: %s
      payload: ./attempt-%d.json
    expect:
      idempotency_key: %s
      ran: [record]
`, attempt, test.Trigger().GetName(), attempt, test.ExpectedKey)
			}
			writeFile(t, dir+"/x.test.yaml", "tests:"+cases.String())

			report := flowtest.RunFile(dir + "/x.test.yaml")
			require.Empty(t, report.GetRefused())
			require.Len(t, report.GetCases(), len(test.SignedAt))
			for _, c := range report.GetCases() {
				assert.True(t, c.GetPassed(), "%s: %v — %s", c.GetName(), c.GetFailures(), test.Why)
				assert.Empty(t, c.GetError())
			}
		})
	}
}

// TestAnUnknownWebhookIsRefused: a case addressing a source the workflow does not
// declare is a mistake in the case, reported as one — naming what the workflow
// does declare, rather than reporting a mapping that produced nothing.
func TestAnUnknownWebhookIsRefused(t *testing.T) {
	t.Parallel()

	dir := writeDeliveryFixture(t)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: a source the workflow does not declare
    workflow: ./workflow.yaml
    expect: {failed: false}
    trigger:
      webhook: shopify
      payload: ./delivery.json
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Len(t, report.GetCases(), 1)
	assert.Contains(t, report.GetCases()[0].GetError(), `trigger "shopify"`)
	assert.Contains(t, report.GetCases()[0].GetError(), "declares stripe")
}

// TestATriggerCaseIsCheckedWhenTheFileLoads covers the mistakes knowable from the
// test file alone, which are refused with the file rather than a virtual day
// later — the timing every other check in this loader keeps.
func TestATriggerCaseIsCheckedWhenTheFileLoads(t *testing.T) {
	t.Parallel()

	for name, testFile := range map[string]string{
		"a third answer about the signature": `
tests:
  - name: x
    workflow: ./workflow.yaml
    trigger: {webhook: stripe, payload: ./delivery.json, signature: unchecked}
`,
		"inputs stated beside a delivery": `
tests:
  - name: x
    workflow: ./workflow.yaml
    trigger: {webhook: stripe, payload: ./delivery.json}
    inputs: {order_id: ord_1}
`,
		"a refusal expected with nothing to refuse": `
tests:
  - name: x
    workflow: ./workflow.yaml
    inputs: {order_id: ord_1, amount: 1}
    expect: {refused: true}
`,
		"no payload to replay": `
tests:
  - name: x
    workflow: ./workflow.yaml
    trigger: {webhook: stripe}
`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			dir := writeDeliveryFixture(t)
			writeFile(t, dir+"/x.test.yaml", testFile)

			report := flowtest.RunFile(dir + "/x.test.yaml")
			assert.NotEmpty(t, report.GetRefused(), "the file should have been refused when it loaded")
			assert.Empty(t, report.GetCases())
		})
	}
}

// TestADeliveryTooLargeToReadIsRefused: a delivery is attacker-chosen input and
// gets a byte bound before it is read into memory, like every other reader here.
// The fixture is the only reader of one today, and it inherits the bound the live
// receiver will apply to a request body.
func TestADeliveryTooLargeToReadIsRefused(t *testing.T) {
	t.Parallel()

	dir := writeDeliveryFixture(t)
	writeFile(t, dir+"/huge.json", `{"headers": {}, "body": {"pad": "`+
		strings.Repeat("a", 1<<20)+`"}}`)
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: an oversized delivery
    workflow: ./workflow.yaml
    expect: {failed: false}
    trigger:
      webhook: stripe
      payload: ./huge.json
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Len(t, report.GetCases(), 1)
	assert.Contains(t, report.GetCases()[0].GetError(), "more than the")
}

// failureText joins a case's failures so an assertion can look for one sentence
// among them.
func failureText(failures []*v1.Diagnostic) string {
	messages := make([]string, 0, len(failures))
	for _, failure := range failures {
		messages = append(messages, failure.GetMessage())
	}

	return strings.Join(messages, "\n")
}
