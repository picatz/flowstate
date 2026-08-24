package flowtest_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// Computed webhook verification (#935): when a case's `secrets:` binds every
// key the trigger's `verify:` names, the outcome comes from
// [v1.VerifyWebhookDelivery] — the arithmetic the served receiver runs — over
// the fixture's exact `body` bytes, not from the `signature:` declaration.
// These pin both directions of the arithmetic, the refusal of the two
// spellings side by side, the refusal of a partial binding, and the epoch
// pinning that keeps a timestamped scheme's fixture deterministic forever.

const fixtureKey = "whsec_fixture_key"

// verifiedBody is the exact byte spelling the fixture stores and the
// signature covers. Compact deliberately: the signed bytes are the fixture's
// own spelling of `body`, so the test signs this constant and embeds it
// verbatim.
const verifiedBody = `{"order":{"id":"ord_9","total":4200}}`

func hmacHex(key, payload string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// writeVerifyWorkflow lays down a workflow whose trigger verifies with the
// generic HMAC scheme, in a directory of its own.
func writeVerifyWorkflow(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: verified
inputs:
  order_id:
    type: string
    required: true
triggers:
  - webhook: orders
    verify:
      hmac_sha256: ${secret('env:HOOK_KEY')}
    idempotency_key: ${event.headers["x-request-id"]}
    with:
      order_id: ${event.body.order.id}
steps:
  - id: record
    log:
      message: ${'order ' + inputs.order_id}
`)

	return dir
}

// writeVerifyFixture is that workflow beside a delivery whose signature is
// computed by this test over the embedded body — genuine unless a caller
// tampers with it.
func writeVerifyFixture(t *testing.T, body, signature string) string {
	t.Helper()

	dir := writeVerifyWorkflow(t)
	writeFile(t, dir+"/delivery.json", fmt.Sprintf(`{
  "headers": {"X-Flowstate-Signature": %q, "X-Request-Id": "r-1"},
  "body": %s
}`, signature, body))

	return dir
}

// TestABoundKeyComputesTheVerification is the acceptance in both directions:
// the same case shape passes when the fixture's signature is right and is
// refused when it is wrong, through the production function — no `signature:`
// declared anywhere.
func TestABoundKeyComputesTheVerification(t *testing.T) {
	t.Parallel()

	t.Run("a genuine signature starts the run", func(t *testing.T) {
		t.Parallel()

		dir := writeVerifyFixture(t, verifiedBody, hmacHex(fixtureKey, verifiedBody))
		writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: verifies against the bound key and starts a run
    workflow: ./workflow.yaml
    secrets:
      "env:HOOK_KEY": whsec_fixture_key
    trigger:
      webhook: orders
      payload: ./delivery.json
    stubs:
      - task: log
        returns: {}
    expect:
      inputs:
        order_id: ord_9
      ran: [record]
`)
		report := flowtest.RunFile(dir + "/x.test.yaml")
		require.Empty(t, report.GetRefused())
		require.Len(t, report.GetCases(), 1)
		c := report.GetCases()[0]
		assert.True(t, c.GetPassed(), "error: %v / failures: %v", c.GetError(), c.GetFailures())
	})

	t.Run("a tampered body is refused, and refused is a passing verdict", func(t *testing.T) {
		t.Parallel()

		// The signature is over the untampered body; the fixture carries a
		// different one — the capture-and-edit shape the arithmetic exists to
		// refuse.
		dir := writeVerifyFixture(t, `{"order":{"id":"ord_FORGED","total":4200}}`,
			hmacHex(fixtureKey, verifiedBody))
		writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: a forged delivery starts nothing
    workflow: ./workflow.yaml
    secrets:
      "env:HOOK_KEY": whsec_fixture_key
    trigger:
      webhook: orders
      payload: ./delivery.json
    expect:
      refused: true
`)
		report := flowtest.RunFile(dir + "/x.test.yaml")
		require.Empty(t, report.GetRefused())
		c := report.GetCases()[0]
		assert.True(t, c.GetPassed(), "error: %v / failures: %v", c.GetError(), c.GetFailures())
	})
}

// TestAWrongSignatureFailsWithTheArithmeticsReason: a case expecting
// acceptance over a fixture that does not verify fails with the verifier's
// own reason and the re-signing cost named — the diagnostic an author edits a
// stored payload into.
func TestAWrongSignatureFailsWithTheArithmeticsReason(t *testing.T) {
	t.Parallel()

	dir := writeVerifyFixture(t, verifiedBody, hmacHex("the-wrong-key", verifiedBody))
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: expects a run the signature cannot start
    workflow: ./workflow.yaml
    secrets:
      "env:HOOK_KEY": whsec_fixture_key
    trigger:
      webhook: orders
      payload: ./delivery.json
    expect:
      inputs:
        order_id: ord_9
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	require.NotEmpty(t, c.GetFailures())
	message := c.GetFailures()[0].GetMessage()
	assert.Contains(t, message, "did not verify against the case's bound keys")
	assert.Contains(t, message, "re-signed", "the fixture-maintenance cost is the diagnostic's to name")
}

// TestSignatureBesideBoundKeysIsRefused: a declaration that could contradict
// the arithmetic is the two-spellings bug as a test fixture, refused naming
// both.
func TestSignatureBesideBoundKeysIsRefused(t *testing.T) {
	t.Parallel()

	dir := writeVerifyFixture(t, verifiedBody, hmacHex(fixtureKey, verifiedBody))
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: declares what the arithmetic computes
    workflow: ./workflow.yaml
    secrets:
      "env:HOOK_KEY": whsec_fixture_key
    trigger:
      webhook: orders
      payload: ./delivery.json
      signature: invalid
    expect:
      refused: true
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	assert.Contains(t, c.GetError(), "computed")
	assert.Contains(t, c.GetError(), "signature: invalid")
	assert.Contains(t, c.GetError(), "Drop `signature:`")
}

// TestAPartialKeyBindingIsRefused: some keys bound and some not is not a
// rehearsal of anything — refused naming what is missing rather than quietly
// falling back to the declared outcome.
func TestAPartialKeyBindingIsRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: doubly-verified
inputs:
  order_id:
    type: string
    required: true
triggers:
  - webhook: orders
    verify:
      hmac_sha256: ${secret('env:HOOK_KEY')}
      stripe: ${secret('env:STRIPE_KEY')}
    idempotency_key: ${event.headers["x-request-id"]}
    with:
      order_id: ${event.body.order.id}
steps:
  - id: record
    log:
      message: ok
`)
	writeFile(t, dir+"/delivery.json", fmt.Sprintf(`{
  "headers": {"X-Request-Id": "r-1"},
  "body": %s
}`, verifiedBody))
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: binds one key of two
    workflow: ./workflow.yaml
    secrets:
      "env:HOOK_KEY": whsec_fixture_key
    trigger:
      webhook: orders
      payload: ./delivery.json
    expect:
      refused: true
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	assert.Contains(t, c.GetError(), "binds 1 of the 2 keys")
	assert.Contains(t, c.GetError(), "env:STRIPE_KEY")
}

// TestAStripeFixturePinsItsTimestampToTheEpoch: replay is offline, so `now`
// is the virtual clock's epoch — a Stripe fixture whose `t=` is the epoch
// verifies today and verifies identically in ten years, which is the whole
// determinism a stored fixture is for. This is the decision #935 left to
// review, pinned: fixture-pinned timestamps, the receiver's own tolerance
// bounds unchanged.
func TestAStripeFixturePinsItsTimestampToTheEpoch(t *testing.T) {
	t.Parallel()

	// 2020-01-01T00:00:00Z, the flowtest epoch, as Stripe's `t=` spells it.
	const epochSeconds = "1577836800"
	signature := fmt.Sprintf("t=%s,v1=%s", epochSeconds,
		hmacHex(fixtureKey, epochSeconds+"."+verifiedBody))

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", `
edition: v2026.3
name: stripe-verified
inputs:
  order_id:
    type: string
    required: true
triggers:
  - webhook: stripe
    verify:
      stripe: ${secret('env:STRIPE_KEY')}
    idempotency_key: ${event.headers["stripe-signature"]}
    with:
      order_id: ${event.body.order.id}
steps:
  - id: record
    log:
      message: ok
`)
	writeFile(t, dir+"/delivery.json", fmt.Sprintf(`{
  "headers": {"Stripe-Signature": %q},
  "body": %s
}`, signature, verifiedBody))
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: a stripe delivery signed at the epoch verifies
    workflow: ./workflow.yaml
    secrets:
      "env:STRIPE_KEY": whsec_fixture_key
    trigger:
      webhook: stripe
      payload: ./delivery.json
    stubs:
      - task: log
        returns: {}
    expect:
      inputs:
        order_id: ord_9
      ran: [record]
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	assert.True(t, c.GetPassed(), "error: %v / failures: %v", c.GetError(), c.GetFailures())
}

// TestUnboundKeysKeepTheDeclaredRehearsal: binding a secret the trigger's
// `verify:` does not name changes nothing — the keyless rehearsal, declared
// outcome included, stays exactly as it was.
func TestUnboundKeysKeepTheDeclaredRehearsal(t *testing.T) {
	t.Parallel()

	dir := writeVerifyFixture(t, verifiedBody, "not-checked-at-all")
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: an unrelated secret does not opt into arithmetic
    workflow: ./workflow.yaml
    secrets:
      "env:SOMETHING_ELSE": irrelevant
    trigger:
      webhook: orders
      payload: ./delivery.json
      signature: invalid
    expect:
      refused: true
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	assert.True(t, c.GetPassed(), "error: %v / failures: %v", c.GetError(), c.GetFailures())
}

// TestARawBodyCarriesACapturedBodysExactBytes (Codex, #1109): a sender signs
// the HTTP body's exact bytes, whitespace included, and an embedded JSON
// value can never carry the whitespace around one — the decoder owns it. So
// a captured "  {...}\n" body rides in `raw_body` verbatim, the computed
// verification signs over exactly those bytes, and the genuine capture
// verifies offline while its mappings still read the decoded payload.
func TestARawBodyCarriesACapturedBodysExactBytes(t *testing.T) {
	t.Parallel()

	captured := "  " + verifiedBody + "\n"
	dir := writeVerifyWorkflow(t)
	writeFile(t, dir+"/delivery.json", fmt.Sprintf(`{
  "headers": {"X-Flowstate-Signature": %q, "X-Request-Id": "r-1"},
  "raw_body": %q
}`, hmacHex(fixtureKey, captured), captured))
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: a captured body verifies over its exact bytes
    workflow: ./workflow.yaml
    secrets:
      "env:HOOK_KEY": whsec_fixture_key
    trigger:
      webhook: orders
      payload: ./delivery.json
    stubs:
      - task: log
        returns: {}
    expect:
      inputs:
        order_id: ord_9
      ran: [record]
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	assert.True(t, c.GetPassed(), "error: %v / failures: %v", c.GetError(), c.GetFailures())
}

// TestBothBodySpellingsAreRefused: two spellings of one body is the
// two-sources-of-truth bug as a fixture, and a signature can only be over one
// byte sequence — refused naming both.
func TestBothBodySpellingsAreRefused(t *testing.T) {
	t.Parallel()

	dir := writeVerifyWorkflow(t)
	writeFile(t, dir+"/delivery.json", fmt.Sprintf(`{
  "headers": {"X-Request-Id": "r-1"},
  "body": %s,
  "raw_body": %q
}`, verifiedBody, verifiedBody))
	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: never gets to a verdict
    workflow: ./workflow.yaml
    secrets:
      "env:HOOK_KEY": whsec_fixture_key
    trigger:
      webhook: orders
      payload: ./delivery.json
    expect:
      refused: true
`)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.False(t, c.GetPassed())
	assert.Contains(t, c.GetError(), "both `body` and `raw_body`")
}
