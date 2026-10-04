package flowstatev1_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// lateDeliveryClock is a clock whose deadline never fires on its own and whose
// notion of "now" the test moves, so a delivery can land *after* a quorum's
// fixed deadline in the clock's terms while the wait is still processing the
// one before it.
//
// The first After, which a bounded wait makes the instant it arms its deadline
// and so only once it has parked, starts onParked. Until release is closed, any
// read of Now waits: the wait is held at the point it processes the first
// delivery, which is what makes the second one provably queued behind it.
type lateDeliveryClock struct {
	base    time.Time
	offset  atomic.Int64
	holding atomic.Bool
	release chan struct{}

	once     sync.Once
	onParked func()
}

func (c *lateDeliveryClock) Now() time.Time {
	if c.holding.Load() {
		<-c.release
	}

	return c.base.Add(time.Duration(c.offset.Load()))
}

func (c *lateDeliveryClock) After(time.Duration) <-chan time.Time {
	c.once.Do(func() { go c.onParked() })

	return make(chan time.Time)
}

// TestAQuorumNeverCountsADeliveryQueuedPastItsDeadline is the local half of the
// single timer a durable quorum wait has. alice's approval is what wakes the
// parked wait; by the time it is processed the clock is past the deadline the
// wait fixed, and bob's approval is already queued behind it. The wait must
// report `timed_out` with one approval, as the durable driver does, and not
// take bob's approval as though the deadline had not passed.
//
// The pre-buffered case is the other direction, and is the shared table's own:
// a gate answered from what was already queued never reads a deadline at all.
func TestAQuorumNeverCountsADeliveryQueuedPastItsDeadline(t *testing.T) {
	t.Parallel()

	signals := v1.NewLocalSignals()
	approve := func(subject string) error {
		return signals.DeliverFrom("release-approved",
			&v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)}},
			&v1.SignalSender{Identity: &v1.WorkloadIdentity{Subject: subject, Issuer: "https://idp.example"}})
	}

	clock := &lateDeliveryClock{base: time.Unix(0, 0), release: make(chan struct{})}
	delivered := make(chan error, 1)
	clock.onParked = func() {
		// Past the deadline the wait fixed, then alice, then bob behind her, and
		// only then is the wait let go of.
		clock.offset.Store(int64(2 * time.Minute))
		clock.holding.Store(true)
		defer close(clock.release)

		if err := approve("alice"); err != nil {
			delivered <- err

			return
		}
		delivered <- approve("bob")
	}

	ctx := v1.NewContextWithSignalWaiter(v1.NewContextWithClock(t.Context(), clock), signals)

	outputs, err := v1.Run(ctx, quorumGate(time.Minute))
	require.NoError(t, err)
	require.NoError(t, <-delivered)

	gate := outputs.GetStepValues()["gate"].GetNamedValues()
	assert.Equal(t, v1.QuorumTimedOut, gate["decision"].GetLiteral().GetStringValue(),
		"a delivery queued after the deadline completed the quorum")
	assert.Equal(t, []string{"alice"}, stringsOf(gate["approvers"]),
		"the approval counted before the deadline was lost, or the one after it was kept")
}
