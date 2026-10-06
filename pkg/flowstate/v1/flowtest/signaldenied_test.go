package flowtest_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// deniedSignalCase runs one case over the fund-transfer workflow's signal
// policy with a scripted sender and a claim about whether it was denied.
func deniedSignalCase(t *testing.T, role, claim string) (passed bool, failures string, refused string) {
	t.Helper()

	dir := t.TempDir()
	workflow, err := os.ReadFile("../../../../examples/enterprise-fund-transfer/workflow.yaml")
	require.NoError(t, err)
	writeFile(t, dir+"/workflow.yaml", string(workflow))
	report := flowtest.RunFile(writeInline(t, dir, `
tests:
  - name: the case
    workflow: ./workflow.yaml
    inputs:
      from_account_id: acct-treasury-001
      to_account_id: acct-supplier-042
      amount_cents: 5000000
      idempotency_key: transfer-denied-0001
    stubs:
      - task: log
        returns: {}
      - task: http
        returns: {status_code: 200, body: {reference: ref-1}}
    signals:
      - name: transfer-approved
        payload: {approved: true}
        sender:
          subject: someone@example.com
          issuer: https://issuer.example.com
          claims: {role: `+role+`}
    expect:
`+claim))
	if report.GetRefused() != "" {
		return false, "", report.GetRefused()
	}
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]

	return c.GetPassed(), failureTexts(c), c.GetError()
}

// TestDeniedSignalsJudgesThePolicysOwnRefusal: a sender the policy refuses
// satisfies the claim, and the same claim fails the moment a qualifying sender
// is admitted, which is the negative direction a claim read from the
// transcript text could not prove.
func TestDeniedSignalsJudgesThePolicysOwnRefusal(t *testing.T) {
	t.Parallel()

	passed, failures, refused := deniedSignalCase(t, "engineer", `
      failed: false
      denied_signals: [transfer-approved]
`)
	require.Empty(t, refused)
	assert.True(t, passed, "%s", failures)

	passed, failures, refused = deniedSignalCase(t, "treasury-approver", `
      failed: false
      denied_signals: [transfer-approved]
`)
	require.Empty(t, refused)
	assert.False(t, passed)
	assert.Contains(t, failures, `expect.denied_signals[0]`)
	assert.Contains(t, failures, "it was delivered, so its policy admitted the sender")
}

// TestDeniedSignalsRefusesNamesNothingCouldDeny: a claim about a signal the
// workflow has no policy for, or the case never sends, is refused before the
// run rather than passing or failing for the wrong reason.
func TestDeniedSignalsRefusesNamesNothingCouldDeny(t *testing.T) {
	t.Parallel()

	_, _, refused := deniedSignalCase(t, "engineer", `
      failed: false
      denied_signals: [transfer-aproved]
`)
	assert.Contains(t, refused, `names signal "transfer-aproved"`)
	assert.Contains(t, refused, `did you mean "transfer-approved"`)
}
