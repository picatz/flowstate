package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The two parts of the lease mechanics an end-to-end run cannot reach, tested
// where a fixture can drive them.
//
// Both are here for the same reason and it is worth stating once: the durable
// half of #928 stage 2 is exercised through the SDK's test environment, where a
// delayed callback delivers its signal *after* the timer's workflow task has run
// to completion. So the one wake in which a lease lapses and a pause ask is
// already buffered — a real Temporal task carrying TimerFired and
// WorkflowExecutionSignaled together — never occurs there. Every mutation of the
// branch that handles it survived every end-to-end test written for it, which is
// how this file came to exist.

// TestWhatAPauseAskDoes is the decision table, all eight combinations, including
// the two only production reaches.
//
// The rows are asserted individually rather than by a loop over a table that
// mirrors the implementation: a table written beside the code it checks agrees
// with it by construction, which is the shape #1141 found could not fail.
func TestWhatAPauseAskDoes(t *testing.T) {
	t.Parallel()

	// Nothing holds the run, and the run is running: an ask starts a session.
	assert.Equal(t, pauseGrants, dispositionOfPause(false, false, false),
		"a pause ask against an unheld run at a boundary starts a session")
	assert.Equal(t, pauseGrants, dispositionOfPause(false, false, true),
		"and whether the asker held some earlier lease does not change that — a lapsed session is over")

	// Something holds the run.
	assert.Equal(t, pauseExtends, dispositionOfPause(false, true, true),
		"the holder asking again is a renewal")
	assert.Equal(t, pauseExtends, dispositionOfPause(true, true, true),
		"including while the run is parked, which is where a renewal normally arrives")
	assert.Equal(t, pauseRefused, dispositionOfPause(false, true, false),
		"somebody else asking for a held run is refused rather than queued")
	assert.Equal(t, pauseRefused, dispositionOfPause(true, true, false),
		"and refused while parked too, which is where that normally arrives")

	// The two only a real Temporal task reaches: the run is inside a hold and
	// nothing holds it, because the lease lapsed in this very wake.
	assert.Equal(t, pausePutBy, dispositionOfPause(true, false, false),
		"a second caller's ask landing as a hold ends takes the next boundary, so the run makes progress")
	assert.Equal(t, pausePutBy, dispositionOfPause(true, false, true),
		"and so does the last holder's own ask, because their session is over and this starts a new one")

	// The property the table exists for, stated where a reader meets it: being
	// parked never turns an ask into a *stronger* answer than it would have had.
	// A `parked` that granted would be the queue this refuses to be.
	for _, held := range []bool{true, false} {
		for _, holder := range []bool{true, false} {
			if dispositionOfPause(false, held, holder) == pauseGrants {
				assert.Equal(t, pausePutBy, dispositionOfPause(true, held, holder),
					"an ask that would start a session at a boundary must be put by inside a hold")
			}
		}
	}
}

// TestTheExpiryTimerNamesItsHolder is the attribution claim at the one surface
// that carries it.
//
// A hold ends two ways and an operator has to be able to tell them apart: a
// release is a `flowstate_debug_resume` delivery in history naming its sender,
// and an expiry is this timer firing. Without a holder on it, "the run resumed
// on its own" is an event with nobody attached to it — so the summary is the
// whole of what makes the vanished-debugger case attributable.
//
// Tested here rather than through a run because the SDK's test environment
// gives no handle on a timer's summary, and the engine's own logger is not a
// contract. What is a contract is what this function returns.
func TestTheExpiryTimerNamesItsHolder(t *testing.T) {
	t.Parallel()

	summary := debugLeaseSummary(&v1.DebugSession{
		SessionId: "run-1/debug/0",
		AttachedBy: &v1.WorkloadIdentity{
			Issuer: "https://issuer.example.com", Subject: "sre-1@example.com",
		},
	})

	assert.Contains(t, summary, "run-1/debug/0", "the summary does not say which lease expired")
	assert.Contains(t, summary, "https://issuer.example.com#sre-1@example.com",
		"the summary does not say who was holding the run, so an expiry has nobody attached to it")

	// Issuer-qualified rather than the bare subject, for [v1.DebugLeaseHolder]'s
	// reason: two identity providers can each mint an `sre-1`, and a record that
	// named only the subject would be a record of the wrong person a third of
	// the time it mattered.
	assert.NotEqual(t, "sre-1@example.com", summary,
		"the holder is written issuer-qualified, as everywhere else a subject is compared")
}

// TestTheSummaryBoundsWhatACallerPutInIt: a subject is attested but not
// grammar-constrained the way a step id is, so the one caller-influenced value
// this function renders is bounded before it reaches a Temporal-rendered
// surface.
//
// The unbounded direction is checked first, because a bound that truncated
// everything would satisfy the assertion below and describe a summary that
// never names anybody.
func TestTheSummaryBoundsWhatACallerPutInIt(t *testing.T) {
	t.Parallel()

	ordinary := "https://issuer.example.com#sre-1@example.com"
	require.Equal(t, ordinary, boundSummaryText(ordinary),
		"an ordinary qualified subject is rendered whole")

	atTheBound := strings.Repeat("s", maxSummaryTextBytes)
	require.Equal(t, atTheBound, boundSummaryText(atTheBound),
		"a value exactly at the bound is not cut, so the bound is a ceiling rather than a target")

	overlong := strings.Repeat("s", maxSummaryTextBytes*4)
	bounded := boundSummaryText(overlong)
	assert.Less(t, len(bounded), len(overlong), "an over-long value reached the summary intact")
	assert.True(t, strings.HasPrefix(bounded, strings.Repeat("s", maxSummaryTextBytes)),
		"the value was cut somewhere other than at the bound")
	assert.True(t, strings.HasSuffix(bounded, "…"),
		"a cut value does not say it was cut, so a reader cannot tell a truncation from a subject")

	// And through the function that uses it, so the bound is on the path rather
	// than only on the helper.
	assert.NotContains(t, debugLeaseSummary(&v1.DebugSession{
		AttachedBy: &v1.WorkloadIdentity{Issuer: "https://i", Subject: overlong},
	}), overlong, "the summary rendered an unbounded subject a caller's issuer minted")
}

// TestAnAskPutByGoesOnTheRunsOwnCarry is the other half: where a put-by ask
// waits.
//
// The carry rather than a field of [debugControl], because the carry is the one
// place a delivery survives a Continue-As-New — `drainSignals` starts from it and
// [v1.CheckRunStateSize] weighs it. An ask held anywhere else vanishes at a seam,
// which is a `flow signal` that reported success and did nothing.
func TestAnAskPutByGoesOnTheRunsOwnCarry(t *testing.T) {
	t.Parallel()

	sender := &v1.SignalSender{Identity: &v1.WorkloadIdentity{
		Issuer: "https://issuer.example.com", Subject: "sre-2@example.com",
	}}
	payload := &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
		v1.DebugLeaseInput: v1.NewLiteral("45s"),
	}}

	exec := &executor{signals: &signalCarry{}}
	require.Empty(t, exec.signals.pending, "the carry starts empty, so anything on it below was put there here")

	exec.deferDebugAsk(v1.DebugPauseSignal, &v1.SignalDelivery{Payload: payload, Sender: sender})

	require.Len(t, exec.signals.pending, 1, "the put-by ask was dropped rather than kept")

	kept := exec.signals.pending[0]
	assert.Equal(t, v1.DebugPauseSignal, kept.GetName(),
		"it waits on the channel it arrived on, so the next boundary reads it as a pause ask")
	assert.Equal(t, "sre-2@example.com", kept.GetSender().GetIdentity().GetSubject(),
		"with the sender the server attested, because that is who the lease will name")
	assert.Equal(t, 45*time.Second, v1.DebugLeaseRequested(kept.GetPayload()),
		"and the duration it asked for, which is the only thing read back out of the payload")

	// It appends rather than replaces: an approval already waiting on the carry
	// is not lost by a debugger asking to pause the run.
	exec.signals.pending = append(exec.signals.pending, &v1.PendingSignal{Name: "deploy-approved"})
	exec.deferDebugAsk(v1.DebugPauseSignal, &v1.SignalDelivery{Payload: payload, Sender: sender})

	names := make([]string, 0, len(exec.signals.pending))
	for _, pending := range exec.signals.pending {
		names = append(names, pending.GetName())
	}
	assert.Equal(t,
		[]string{v1.DebugPauseSignal, "deploy-approved", v1.DebugPauseSignal}, names,
		"a put-by ask joins the back of the carry, in arrival order, disturbing nothing already on it")
}
