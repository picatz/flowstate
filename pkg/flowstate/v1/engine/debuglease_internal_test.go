package engine

import (
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
