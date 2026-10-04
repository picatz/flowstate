package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// delivery builds one attested delivery with the given `approved`, or an empty
// payload when approved is nil.
func delivery(issuer, subject string, approved *bool) *v1.SignalDelivery {
	d := &v1.SignalDelivery{Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{}}}
	if approved != nil {
		d.Payload.NamedValues["approved"] = v1.NewLiteral(*approved)
	}
	if subject != "" || issuer != "" {
		d.Sender = &v1.SignalSender{Identity: &v1.WorkloadIdentity{Issuer: issuer, Subject: subject}}
	}

	return d
}

func ptr[T any](v T) *T { return &v }

// TestQuorumTallyDecides pins the one fold both drivers share, directly and
// without either driver, so a rule changed here fails here rather than only in
// the shared table's longer cases.
func TestQuorumTallyDecides(t *testing.T) {
	t.Parallel()

	const idp = "https://idp.example"

	for _, test := range []struct {
		name       string
		quorum     *v1.SignalQuorum
		deliveries []*v1.SignalDelivery
		decision   string
		approvals  uint32
	}{
		{
			name:       "enough distinct approvals approve",
			quorum:     &v1.SignalQuorum{Approve: 2},
			deliveries: []*v1.SignalDelivery{delivery(idp, "a", ptr(true)), delivery(idp, "b", ptr(true))},
			decision:   v1.QuorumApproved,
			approvals:  2,
		},
		{
			name:       "one sender approving twice counts once",
			quorum:     &v1.SignalQuorum{Approve: 2},
			deliveries: []*v1.SignalDelivery{delivery(idp, "a", ptr(true)), delivery(idp, "a", ptr(true))},
			decision:   "",
			approvals:  1,
		},
		{
			name:       "the same subject from another issuer is another sender",
			quorum:     &v1.SignalQuorum{Approve: 2},
			deliveries: []*v1.SignalDelivery{delivery(idp, "a", ptr(true)), delivery("https://other.example", "a", ptr(true))},
			decision:   v1.QuorumApproved,
			approvals:  2,
		},
		{
			name:       "distinct false counts repeats",
			quorum:     &v1.SignalQuorum{Approve: 2, Distinct: ptr(false)},
			deliveries: []*v1.SignalDelivery{delivery(idp, "a", ptr(true)), delivery(idp, "a", ptr(true))},
			decision:   v1.QuorumApproved,
			approvals:  2,
		},
		{
			name:       "an unattested approval is ignored under distinct",
			quorum:     &v1.SignalQuorum{Approve: 1},
			deliveries: []*v1.SignalDelivery{delivery("", "", ptr(true))},
			decision:   "",
			approvals:  0,
		},
		{
			name:       "a rejection vetoes by default",
			quorum:     &v1.SignalQuorum{Approve: 2},
			deliveries: []*v1.SignalDelivery{delivery(idp, "a", ptr(true)), delivery(idp, "b", ptr(false))},
			decision:   v1.QuorumVetoed,
			approvals:  1,
		},
		{
			name:       "a payload that says nothing is neither approval nor veto",
			quorum:     &v1.SignalQuorum{Approve: 1},
			deliveries: []*v1.SignalDelivery{delivery(idp, "a", nil)},
			decision:   "",
			approvals:  0,
		},
		{
			name:       "nothing is taken once decided",
			quorum:     &v1.SignalQuorum{Approve: 1},
			deliveries: []*v1.SignalDelivery{delivery(idp, "a", ptr(true)), delivery(idp, "b", ptr(false))},
			decision:   v1.QuorumApproved,
			approvals:  1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			tally := v1.NewQuorumTally(test.quorum)
			for _, d := range test.deliveries {
				require.NoError(t, tally.Take(t.Context(), d, v1.NewScope("", &v1.Workflow_StepOutputs{}), time.Unix(0, 0)))
			}

			assert.Equal(t, test.decision, tally.Decision())
			assert.Equal(t, test.approvals, tally.Approvals())

			wait := &v1.PendingWait{}
			tally.Report(wait)
			assert.Equal(t, test.approvals, wait.GetApprovals())
			assert.Equal(t, test.quorum.GetApprove(), wait.GetApprovalsNeeded())

			want := test.decision
			if want == "" {
				want = v1.QuorumTimedOut
			}
			assert.Equal(t, want, tally.Outputs().GetNamedValues()["decision"].GetLiteral().GetStringValue())
		})
	}
}

// TestQuorumTallyRefusesAnUnboundedStream is invariant 5 for the loop a quorum
// is: deliveries that decide nothing cannot keep the wait reading forever.
func TestQuorumTallyRefusesAnUnboundedStream(t *testing.T) {
	t.Parallel()

	tally := v1.NewQuorumTally(&v1.SignalQuorum{Approve: 2})
	repeat := delivery("https://idp.example", "a", ptr(true))
	scope := v1.NewScope("", &v1.Workflow_StepOutputs{})

	var err error
	for range v1.MaxQuorumDeliveries + 1 {
		if err = tally.Take(t.Context(), repeat, scope, time.Unix(0, 0)); err != nil {
			break
		}
	}

	require.Error(t, err, "a sender repeating itself held the wait open past the bound")
	assert.Empty(t, tally.Decision(), "reaching the bound must fail the step, not invent a decision")
}

// TestValidateWaitChecksAQuorum is the backstop for a specification built in Go,
// which the schema's own rules do not reach.
func TestValidateWaitChecksAQuorum(t *testing.T) {
	t.Parallel()

	wait := func(quorum *v1.SignalQuorum, maxBatch int32) *v1.Wait {
		return &v1.Wait{
			Kind:    &v1.Wait_SignalBatch{SignalBatch: &v1.SignalBatch{Name: "go", Quorum: quorum, MaxBatch: maxBatch}},
			Timeout: durationpb.New(time.Minute),
		}
	}

	for _, test := range []struct {
		name    string
		wait    *v1.Wait
		wantErr string
	}{
		{"a valid quorum", wait(&v1.SignalQuorum{Approve: 2}, 0), ""},
		{"approve of zero", wait(&v1.SignalQuorum{}, 0), "at least 1"},
		{"approve beyond the batch bound", wait(&v1.SignalQuorum{Approve: 5}, 3), "could never be met"},
		{"an empty exclude expression", wait(&v1.SignalQuorum{Approve: 1, Exclude: []*v1.Value{{}}}, 0), "exclude[0] has no expression"},
		{"an empty veto expression", wait(&v1.SignalQuorum{Approve: 1, Veto: &v1.Value{}}, 0), "veto has no expression"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := v1.ValidateWait(test.wait)
			if test.wantErr == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}
