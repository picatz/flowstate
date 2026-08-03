package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestValidateWaitRefusesATimeoutThatDoesNothing covers the shape of wait a
// Flowfile cannot write but the Run RPC can carry.
//
// `timeout:` bounds a `wait_for_signal:`, where the thing being waited for may
// never arrive. On a `sleep:` it was already refused, and on a `wait_until:` it was
// silently accepted and then ignored by both drivers: `timed_out` stayed false for
// as long as the wait ran, so a caller who set a timeout and branched on the output
// was branching on something that could not happen. `parse_wait.go` sets the field
// only under `wait_for_signal:`, which is why no Flowfile and no example could see
// this — a hand-built spec is the reachable path, and it is not a trusted one.
//
// Both directions, because a refusal that also refuses the legitimate use is the
// more expensive bug: the signal case must still be accepted.
func TestValidateWaitRefusesATimeoutThatDoesNothing(t *testing.T) {
	t.Parallel()

	timeout := durationpb.New(time.Minute)

	for _, test := range []struct {
		name    string
		wait    *v1.Wait
		wantErr string
	}{
		{
			name: "wait_until with a timeout",
			wait: &v1.Wait{
				Kind:    &v1.Wait_Until{Until: v1.NewLiteral("2000-01-01T00:00:00Z")},
				Timeout: timeout,
			},
			wantErr: "the moment is already how long it waits",
		},
		{
			name: "sleep with a timeout",
			wait: &v1.Wait{
				Kind:    &v1.Wait_Duration{Duration: durationpb.New(time.Hour)},
				Timeout: timeout,
			},
			wantErr: "the duration is already how long it waits",
		},
		{
			name: "wait_for_signal with a timeout",
			wait: &v1.Wait{
				Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "approval"}},
				Timeout: timeout,
			},
		},
		{
			name: "wait_until without a timeout",
			wait: &v1.Wait{Kind: &v1.Wait_Until{Until: v1.NewLiteral("2000-01-01T00:00:00Z")}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := v1.ValidateWait(test.wait)
			if test.wantErr == "" {
				require.NoError(t, err, "a wait that a timeout does bound was refused")
				return
			}

			require.Error(t, err, "a timeout that does nothing was accepted, so an author is told nothing")
			require.Contains(t, err.Error(), test.wantErr,
				"the diagnostic does not say why the timeout does nothing")
		})
	}
}
