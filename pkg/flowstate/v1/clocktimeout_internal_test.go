package flowstatev1

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAVirtualBoundReportsNoErrorBeforeDoneCloses holds the [context.Context]
// contract on the virtual bound: Done closes on its own goroutine after the
// embedded context ends, and Err must stay nil until then.
func TestAVirtualBoundReportsNoErrorBeforeDoneCloses(t *testing.T) {
	t.Parallel()

	for range 300 {
		// The test is a participant until it cancels, so the clock cannot
		// see every participant parked and jump to the deadline first.
		clock := NewVirtualClock(time.Unix(0, 0))
		clock.Enter()
		parent := NewContextWithClock(t.Context(), clock)
		ctx, cancel := withClockTimeout(parent, time.Hour, nil)

		require.NoError(t, ctx.Err())
		cancel()
		clock.Leave()

		// Poll until Done closes; every non-nil Err seen on the way must
		// already have Done closed.
		for {
			err := ctx.Err()
			select {
			case <-ctx.Done():
				require.Error(t, ctx.Err())
			default:
				require.NoError(t, err, "Err was set while Done was still open")
				continue
			}

			break
		}
	}
}
