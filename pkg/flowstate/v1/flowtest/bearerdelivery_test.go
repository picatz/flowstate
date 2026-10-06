package flowtest_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// A `jwt` scheme names a trust policy entry, which a rehearsal does not have, so
// what a case can say about it is whether the sender's token verified. These pin
// that the declaration reaches the receiver's own verifier in both directions,
// and that a bound signing key beside it is still computed.

func writeBearerFixture(t *testing.T, verify string) string {
	t.Helper()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", fmt.Sprintf(`
edition: v2026.4
name: bearer
inputs:
  order_id:
    type: string
    required: true
triggers:
  - webhook: orders
    verify:
%s
    idempotency_key: ${event.headers["x-request-id"]}
    with:
      order_id: ${event.body.order.id}
steps:
  - id: record
    log:
      message: ${'order ' + inputs.order_id}
`, verify))
	writeFile(t, dir+"/delivery.json", fmt.Sprintf(`{
  "headers": {"X-Flowstate-Signature": %q, "X-Request-Id": "r-1"},
  "body": %s
}`, hmacHex(fixtureKey, verifiedBody), verifiedBody))

	return dir
}

func runBearerCase(t *testing.T, dir, stanza string) *v1.TestCase {
	t.Helper()

	writeFile(t, dir+"/x.test.yaml", `
tests:
  - name: case
    workflow: ./workflow.yaml
`+stanza)
	report := flowtest.RunFile(dir + "/x.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	return report.GetCases()[0]
}

func TestABearerSchemeIsRehearsedThroughTheReceiversVerifier(t *testing.T) {
	t.Parallel()

	const trigger = `
    trigger:
      webhook: orders
      payload: ./delivery.json
`

	t.Run("a token that verified starts the run", func(t *testing.T) {
		t.Parallel()

		dir := writeBearerFixture(t, `      jwt: github-actions`)
		c := runBearerCase(t, dir, trigger+`
    stubs:
      - task: log
        returns: {}
    expect:
      inputs:
        order_id: ord_9
      ran: [record]
`)
		assert.True(t, c.GetPassed(), "error: %v / failures: %v", c.GetError(), c.GetFailures())
	})

	t.Run("a token declared invalid is refused", func(t *testing.T) {
		t.Parallel()

		dir := writeBearerFixture(t, `      jwt: github-actions`)
		c := runBearerCase(t, dir, `
    trigger:
      webhook: orders
      payload: ./delivery.json
      signature: invalid
    expect:
      refused: true
`)
		assert.True(t, c.GetPassed(), "error: %v / failures: %v", c.GetError(), c.GetFailures())
	})

	t.Run("a bound signing key is still computed beside the token", func(t *testing.T) {
		t.Parallel()

		dir := writeBearerFixture(t, "      jwt: github-actions\n      hmac_sha256: ${secret('env:HOOK_KEY')}")

		good := runBearerCase(t, dir, `
    secrets:
      "env:HOOK_KEY": whsec_fixture_key
`+trigger+`
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [record]
`)
		assert.True(t, good.GetPassed(), "error: %v / failures: %v", good.GetError(), good.GetFailures())

		// The token is declared fine and the key is wrong: the signature leg
		// refuses on its own arithmetic.
		bad := runBearerCase(t, dir, `
    secrets:
      "env:HOOK_KEY": not-the-key
`+trigger+`
    expect:
      refused: true
`)
		assert.True(t, bad.GetPassed(), "error: %v / failures: %v", bad.GetError(), bad.GetFailures())
	})
}
