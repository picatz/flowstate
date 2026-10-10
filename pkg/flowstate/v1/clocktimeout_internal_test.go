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
		parent := NewContextWithClock(t.Context(), NewVirtualClock(time.Unix(0, 0)))
		ctx, cancel := withClockTimeout(parent, time.Hour, nil)

		require.NoError(t, ctx.Err())
		cancel()

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
