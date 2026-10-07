package policycheck_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/policycheck"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const perAction = `
edition: v2026.4
name: per-action
signals:
  decide:
    allow: ${payload.decision == "reject" || (payload.decision == "approve" && sender.identity.claims.team == "release-approvers")}
steps:
  - id: a
    wait_for_signal: {name: decide, timeout: 1h}
`

// A rehearsal with no delivery cannot decide a gate whose predicate reads
// `payload`: it says the answer depends on it, and never admits. Handed a
// payload, the same gate is decided by the engine's own function.
func TestAGateReadingPayloadDependsOnItWhenNoneIsGiven(t *testing.T) {
	t.Parallel()

	wf := compile(t, perAction)
	gate := policycheck.Gate{Stanza: policycheck.StanzaSignals, Name: "decide"}
	approver := id("lead@example.com", "team=release-approvers")
	intern := id("intern@example.com", "team=interns")

	unbound := decide(t, wf, gate, policycheck.Subject{Sender: approver})
	require.False(t, unbound.Admitted, "an undecidable gate was reported as admitted")
	require.True(t, unbound.DependsOnPayload)
	require.Contains(t, unbound.Reason, "depends on payload")

	delivery := func(decision string) map[string]*v1.Value {
		return map[string]*v1.Value{"decision": v1.NewLiteral(decision)}
	}

	for name, tc := range map[string]struct {
		sender   *policycheck.Subject
		admitted bool
	}{
		"an approver approving":      {&policycheck.Subject{Sender: approver, Payload: delivery("approve")}, true},
		"a non-approver approving":   {&policycheck.Subject{Sender: intern, Payload: delivery("approve")}, false},
		"a non-approver rejecting":   {&policycheck.Subject{Sender: intern, Payload: delivery("reject")}, true},
		"an approver with no answer": {&policycheck.Subject{Sender: approver, Payload: map[string]*v1.Value{}}, false},
	} {
		got := decide(t, wf, gate, *tc.sender)
		require.Equal(t, tc.admitted, got.Admitted, name)
		require.False(t, got.DependsOnPayload, name)
	}
}
