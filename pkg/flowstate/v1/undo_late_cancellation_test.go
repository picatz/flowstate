package flowstatev1_test

import (
	"context"
	"log/slog"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
	"github.com/stretchr/testify/require"
)

// cancelOnMessage stops a run from inside the chosen `log:` task. The task does
// not consult its context, so it still reports success after the cancellation —
// the local equivalent of an activity winning WaitForCancellation's race.
type cancelOnMessage struct {
	message string
	cancel  context.CancelFunc
}

func (h cancelOnMessage) Enabled(context.Context, slog.Level) bool { return true }

func (h cancelOnMessage) Handle(_ context.Context, record slog.Record) error {
	if record.Message == h.message {
		h.cancel()
	}
	return nil
}

func (h cancelOnMessage) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h cancelOnMessage) WithGroup(string) slog.Handler      { return h }

// TestRunWorkflowUndoOnLateCancellation is the local caller of the shared cases.
// The durable caller holds a real Temporal activity in the corresponding window.
func TestRunWorkflowUndoOnLateCancellation(t *testing.T) {
	const message = "the stop lands here"

	for index, outline := range conformance.UndoLateCancellationCases(undoPlaceholderBase, "log", message) {
		t.Run(outline.Name, func(t *testing.T) {
			base, recorded := conformance.NewUndoServer(t)
			test := conformance.UndoLateCancellationCases(base, "log", message)[index]

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctx = v1.ContextWithLogger(ctx, slog.New(cancelOnMessage{message: message, cancel: cancel}))

			transcript, err := v1.Run(ctx, test.Workflow)
			require.Error(t, err,
				"a run stopped while its last step succeeded reported success, with its compensations never run")
			require.ErrorIs(t, err, context.Canceled,
				"a stopped run stopped reading as cancelled once it compensated: %v", err)

			if test.Summary == "" {
				require.NotContains(t, err.Error(), "compensation ran",
					"a run with nothing registered reported compensating anyway")
			} else {
				require.Contains(t, err.Error(), test.Summary,
					"the cancellation does not carry the account of what was compensated")
			}
			require.NotContains(t, err.Error(), "said",
				"declared outputs were evaluated after the stop")
			require.Nil(t, transcript.GetRunOutputs(),
				"a partial transcript is documented to carry no run outputs")
			require.Equal(t, test.Recorded, recorded(),
				"the effects that happened, and their order, are not what stopping this run should have produced")
		})
	}
}
