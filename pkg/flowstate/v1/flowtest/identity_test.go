package flowtest_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// The identity seam of #344's slice 3: a case says who its run starts as
// (`starter:`) and who each scripted signal stands in for (`sender:`), and the
// same `v1.SignalPolicyCheck` production runs decides the delivery - including
// `distinct_from_starter:`, which is unreachable without a starter to be
// distinct from.
//
// Two directions are asserted for every guard here, because the admit
// direction alone is a functionality test wearing a security test's clothes
// (CLAUDE.md, "test that A cannot reach B"): the approver a policy names is
// admitted, AND the starter approving their own request is refused, AND an
// approver the policy does not name is refused.

// policedGateWorkflow is a gate whose `signals:` policy names an approver by
// qualified subject and claim, refuses the run's own starter, and reports what
// the delivery attested about itself.
//
// `rehearsal` is the output the sender-marker cases turn on: `sender.local`
// reads true for every local delivery, scripted or not, which is what keeps a
// scripted sender distinguishable from an attested production one.
const policedGateWorkflow = `
edition: v2026.2
name: policed-gate

signals:
  approve:
    allow:
      - subject: https://issuer.example.com#approver@example.com
        namespace: payments
        claims:
          team: release-managers
    distinct_from_starter: true

steps:
  - id: approval
    wait_for_signal:
      name: approve
      timeout: 1h

  - id: acted
    if: ${!steps.approval.timed_out}
    log:
      message: approved

outputs:
  decision:
    value: '${steps.approval.timed_out ? "refused" : "admitted"}'
  approver:
    value: ${steps.approval.sender.identity.subject}
  rehearsal:
    value: ${steps.approval.sender.local}
`

// qualifyingSignal is the scripted delivery the policy above admits: the
// subject, issuer, namespace and claim its one rule requires, all four.
const qualifyingSignal = `
    signals:
      - name: approve
        payload:
          approved: true
        sender:
          subject: approver@example.com
          issuer: https://issuer.example.com
          namespace: payments
          claims:
            team: release-managers
`

// TestScriptedSenderDeliversAsARehearsal is the inconsistency #365 flagged,
// closed: a scripted `sender:` used to deliver with `local` false, so a gate's
// own `sender.local` output rendered a scripted sender exactly like a sender a
// server had authenticated. [v1.SignalWaiter]'s contract says the opposite -
// every local delivery, "`flow run local --signal` or a `flow test` script", is
// marked local - and this asserts the observable half of it, through the
// output an author actually reads.
//
// The identity still arrives intact alongside the marker, which is the pair
// that matters: `local` true with an identity is a rehearsal, and the policy
// admitted it on the strength of that identity.
func TestScriptedSenderDeliversAsARehearsal(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", policedGateWorkflow)
	writeFile(t, dir+"/rehearsal.test.yaml", `
tests:
  - name: a scripted sender is admitted, and says it is a rehearsal
    workflow: ./workflow.yaml
`+qualifyingSignal+`
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [approval, acted]
      outputs:
        decision: admitted
        approver: approver@example.com
        rehearsal: true
`)

	report := flowtest.RunFile(dir + "/rehearsal.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(), "failures: %v", report.GetCases()[0].GetFailures())
}

// TestScriptedSenderNeverClaimsToBeAttested is the negative direction of the
// case above, and the one that would have caught the old behavior: a case
// asserting the pre-#344 rendering - `sender.local` false, the shape only a
// server's own attestation may produce - must now fail.
//
// Asserted as a failing case rather than by reading the value directly,
// because the value an author reads is the whole point: nothing about this
// would matter if the marker were true internally and rendered false in the
// gate's own outputs.
func TestScriptedSenderNeverClaimsToBeAttested(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", policedGateWorkflow)
	writeFile(t, dir+"/attested.test.yaml", `
tests:
  - name: a scripted sender renders as an attested production one
    workflow: ./workflow.yaml
`+qualifyingSignal+`
    stubs:
      - task: log
        returns: {}
    expect:
      outputs:
        decision: admitted
        approver: approver@example.com
        rehearsal: false
`)

	report := flowtest.RunFile(dir + "/attested.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)

	c := report.GetCases()[0]
	require.False(t, c.GetPassed(),
		"a scripted sender rendering with sender.local false is the pre-#344 shape a rehearsal must never claim")
	require.Len(t, c.GetFailures(), 1)
	require.Equal(t, "expect.outputs", c.GetFailures()[0].GetField())
	require.Contains(t, c.GetFailures()[0].GetMessage(), "rehearsal")
}

// TestStarterRefusesTheApproverWhoStartedTheRun is what `starter:` exists for:
// `distinct_from_starter:` compares the qualified sender against the run's own
// starter, and until a case could name a starter the refusal was unreachable -
// every case ran as nobody, and nobody is distinct from everybody.
//
// Both directions, in one file: the same qualifying approver is admitted under
// a starter who is somebody else, and refused under a starter who is them.
// Nothing else differs between the two cases, so the starter is what decided
// it.
func TestStarterRefusesTheApproverWhoStartedTheRun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", policedGateWorkflow)
	writeFile(t, dir+"/starter.test.yaml", `
tests:
  - name: the approver who started the run cannot approve it
    workflow: ./workflow.yaml
    starter:
      subject: approver@example.com
      issuer: https://issuer.example.com
`+qualifyingSignal+`
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [approval]
      skipped: [acted]
      outputs:
        decision: refused
        approver: ""
        rehearsal: true

  - name: a distinct approver is admitted for the very same run
    workflow: ./workflow.yaml
    starter:
      subject: requester@example.com
      issuer: https://issuer.example.com
`+qualifyingSignal+`
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [approval, acted]
      outputs:
        decision: admitted
        approver: approver@example.com
        rehearsal: true
`)

	report := flowtest.RunFile(dir + "/starter.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 2)
	for _, c := range report.GetCases() {
		require.True(t, c.GetPassed(), "%s: failures: %v", c.GetName(), c.GetFailures())
	}
}

// TestStarterIsTheSameSubjectAcrossIssuers checks the half of a qualified
// comparison a bare subject would get wrong: a starter who shares the sender's
// local part but was attested by a different issuer is a different person, so
// the delivery is admitted.
//
// The refusal above and this admission are the pair that shows
// `distinct_from_starter:` compares [v1.QualifiedSubject] on each side rather
// than the subject alone - which is the same multi-IdP reasoning that makes
// `subject:` and `issuer:` travel together in the grammar.
func TestStarterIsTheSameSubjectAcrossIssuers(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", policedGateWorkflow)
	writeFile(t, dir+"/issuers.test.yaml", `
tests:
  - name: the same local part from another issuer is another person
    workflow: ./workflow.yaml
    starter:
      subject: approver@example.com
      issuer: https://other-issuer.example.com
`+qualifyingSignal+`
    stubs:
      - task: log
        returns: {}
    expect:
      outputs:
        decision: admitted
        approver: approver@example.com
        rehearsal: true
`)

	report := flowtest.RunFile(dir + "/issuers.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(), "failures: %v", report.GetCases()[0].GetFailures())
}

// TestStarterDefaultsToNobody pins what a case that names no `starter:` runs
// as, which is every case written before the field existed: nobody, recorded
// rather than unknown, so a `distinct_from_starter:` policy admits a
// qualifying approver instead of refusing every case outright.
func TestStarterDefaultsToNobody(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", policedGateWorkflow)
	writeFile(t, dir+"/nobody.test.yaml", `
tests:
  - name: a case naming no starter still exercises its gate
    workflow: ./workflow.yaml
`+qualifyingSignal+`
    stubs:
      - task: log
        returns: {}
    expect:
      outputs:
        decision: admitted
        approver: approver@example.com
        rehearsal: true
`)

	report := flowtest.RunFile(dir + "/nobody.test.yaml")
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	require.True(t, report.GetCases()[0].GetPassed(), "failures: %v", report.GetCases()[0].GetFailures())
}

// TestSenderMustSatisfyEveryConstraint is the other negative direction: the
// starter is irrelevant to a sender the policy's own rule never matched.
// Each case drops exactly one of the four things the rule requires, so a
// passing case names which constraint it proved.
func TestSenderMustSatisfyEveryConstraint(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		sender string
	}{
		{
			name: "another subject",
			sender: `
          subject: intruder@example.com
          issuer: https://issuer.example.com
          namespace: payments
          claims:
            team: release-managers`,
		},
		{
			name: "another issuer",
			sender: `
          subject: approver@example.com
          issuer: https://other-issuer.example.com
          namespace: payments
          claims:
            team: release-managers`,
		},
		{
			name: "another namespace",
			sender: `
          subject: approver@example.com
          issuer: https://issuer.example.com
          namespace: marketing
          claims:
            team: release-managers`,
		},
		{
			name: "a claim the rule does not find",
			sender: `
          subject: approver@example.com
          issuer: https://issuer.example.com
          namespace: payments
          claims:
            team: interns`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			writeFile(t, dir+"/workflow.yaml", policedGateWorkflow)
			writeFile(t, dir+"/refused.test.yaml", `
tests:
  - name: `+tc.name+` never reaches the gate
    workflow: ./workflow.yaml
    signals:
      - name: approve
        payload:
          approved: true
        sender:`+tc.sender+`
    stubs:
      - task: log
        returns: {}
    expect:
      ran: [approval]
      skipped: [acted]
      outputs:
        decision: refused
        approver: ""
        rehearsal: true
`)

			report := flowtest.RunFile(dir + "/refused.test.yaml")
			require.Empty(t, report.GetRefused())
			require.Len(t, report.GetCases(), 1)
			require.True(t, report.GetCases()[0].GetPassed(),
				"failures: %v", report.GetCases()[0].GetFailures())
		})
	}
}

// TestLoadRefusesAMalformedIdentity checks the diagnostics an author gets for
// an identity no policy could read the way they meant it - reported when the
// file loads, naming the case, the position within it, what is wrong and what
// to do, rather than at a gate a virtual day later where it would look like
// nobody answered.
//
// Both ends of the seam, because both are matched by the same comparison: a
// `starter:` and a signal's `sender:` are one type for one reason.
func TestLoadRefusesAMalformedIdentity(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		file    string
		wants   []string
		unwants []string
	}{
		{
			name: "a starter subject with no issuer",
			file: `
tests:
  - name: half a starter
    workflow: ./workflow.yaml
    starter:
      subject: requester@example.com
`,
			wants: []string{`test "half a starter" starter:`, "subject is only unique within its issuer", "<issuer>#<subject>"},
		},
		{
			name: "a starter issuer with no subject",
			file: `
tests:
  - name: the other half of a starter
    workflow: ./workflow.yaml
    starter:
      issuer: https://issuer.example.com
`,
			wants: []string{`test "the other half of a starter" starter:`, "give both"},
		},
		{
			name: "a sender subject with no issuer",
			file: `
tests:
  - name: half a sender
    workflow: ./workflow.yaml
    signals:
      - name: approve
        sender:
          subject: approver@example.com
`,
			wants:   []string{`test "half a sender" signal 1 ("approve") sender:`, "never a bare subject"},
			unwants: []string{"starter:"},
		},
		{
			name: "a claim with no value",
			file: `
tests:
  - name: an empty claim value
    workflow: ./workflow.yaml
    signals:
      - name: approve
        sender:
          claims:
            team: ""
`,
			wants: []string{`test "an empty claim value" signal 1 ("approve") sender:`, "empty value", "name: value"},
		},
		{
			name: "a claim with no name",
			file: `
tests:
  - name: an empty claim name
    workflow: ./workflow.yaml
    starter:
      claims:
        "": release-managers
`,
			wants: []string{`test "an empty claim name" starter:`, "empty name"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := dir + "/malformed.test.yaml"
			writeFile(t, path, tc.file)

			_, err := flowtest.Load(path)
			require.Error(t, err)
			for _, want := range tc.wants {
				require.Contains(t, err.Error(), want)
			}
			for _, unwant := range tc.unwants {
				require.NotContains(t, err.Error(), unwant)
			}
		})
	}
}

// TestRunFileRefusesAMalformedIdentity checks that the load-time refusal above
// is what `flow test` actually reports for a file on disk: refused outright,
// with no case pretending to have run. A case that ran and timed out would be
// the failure mode this check exists to prevent - a mistake in the file
// rendered as a gate nobody answered.
func TestRunFileRefusesAMalformedIdentity(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir+"/workflow.yaml", policedGateWorkflow)
	writeFile(t, dir+"/malformed.test.yaml", `
tests:
  - name: a starter with half an identity
    workflow: ./workflow.yaml
    starter:
      subject: requester@example.com
    expect:
      outputs:
        decision: refused
`)

	report := flowtest.RunFile(dir + "/malformed.test.yaml")
	require.Contains(t, report.GetRefused(), `test "a starter with half an identity" starter:`)
	require.Empty(t, report.GetCases())
}
