package flowstatev1_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestVendorBidsCutoffJudgesAcceptanceTime proves the half of examples/vendor-bids
// that `flow test` cannot: a rehearsal leaves `sender.accepted_at` unset and marks
// the sender local, so every scripted case takes the local arm. Here the senders
// are attested the way the server's Signal door attests them, with a timestamp,
// against a run start this test pins. A sender's accepted_at reaches the workflow
// as RFC 3339 text, whole seconds, so the late bid sits two seconds past the
// cutoff: a bid accepted inside the cutoff's last second is judged on time.
func TestVendorBidsCutoffJudgesAcceptanceTime(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "..", "examples", "vendor-bids", "workflow.yaml")
	wf, _, err := flowfile.ParseFile(path)
	require.NoError(t, err)

	// The deadline in the example's own inputs.json.
	deadline := time.Second
	start := time.Now()

	tests := []struct {
		name  string
		local bool
		at    *time.Time
		award bool
	}{
		{"accepted before the cutoff is on time", false, ptr(start.Add(deadline / 2)), true},
		{"accepted after the cutoff is late", false, ptr(start.Add(deadline + 2*time.Second)), false},
		{"attested with no acceptance time is late, not on time", false, nil, false},
		{"a local rehearsal sender is never judged late", true, nil, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inputs, err := conformance.BindExampleInputs(t, wf, path)
			require.NoError(t, err)
			inputs["min_bids"] = v1.NewLiteral(int64(1))

			sender := &v1.SignalSender{
				Identity: &v1.WorkloadIdentity{Principal: &v1.Principal{Subject: "acme@example.com", Issuer: "https://issuer.example.com"}},
				Local:    tc.local,
			}
			if tc.at != nil {
				sender.AcceptedAt = timestamppb.New(*tc.at)
			}

			waiter := v1.NewLocalSignals()
			require.NoError(t, waiter.DeliverFrom("bid", &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"price_cents": v1.NewLiteral(int64(90000)),
				"lead_days":   v1.NewLiteral(int64(5)),
			}}, sender))

			ctx := v1.NewContextWithRunStart(t.Context(), start)
			ctx = v1.NewContextWithSignalWaiter(ctx, waiter)

			outputs, err := v1.RunWithInputs(ctx, wf, inputs)
			require.NoError(t, err)

			quorum := outputs.GetStepValues()["quorum"].GetNamedValues()["value"].GetLiteral().GetBoolValue()
			require.Equal(t, tc.award, quorum)
		})
	}
}
