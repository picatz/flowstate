package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// quorumSource writes a gate over a policy, with the quorum's own lines and the
// policy's subjects as parameters.
func quorumSource(quorum string, subjects ...string) string {
	var b strings.Builder
	b.WriteString("edition: v2026.4\nname: release-gate\nsteps:\n  - id: gate\n    wait_for_signals:\n      name: release-approved\n      timeout: 1h\n      quorum:\n")
	b.WriteString(quorum)
	if len(subjects) > 0 {
		quoted := make([]string, len(subjects))
		for i, subject := range subjects {
			quoted[i] = `"https://idp.example#` + subject + `"`
		}
		b.WriteString("signals:\n  release-approved:\n    allow: ${sender.identity.principal in [" + strings.Join(quoted, ", ") + "]}\n")
	}

	return b.String()
}

// TestAQuorumParsesAndRoundTrips pins what `quorum:` compiles to and that
// marshalling it back loses nothing.
func TestAQuorumParsesAndRoundTrips(t *testing.T) {
	t.Parallel()

	source := quorumSource("        approve: 2\n        distinct: false\n        exclude:\n          - ${run.identity.subject}\n        veto: ${payload.approved == false}\n")

	workflow, positions, err := flowfile.Parse([]byte(source))
	require.NoError(t, err)

	quorum := workflow.GetSteps()[0].GetWait().GetSignalBatch().GetQuorum()
	require.NotNil(t, quorum)
	assert.EqualValues(t, 2, quorum.GetApprove())
	require.NotNil(t, quorum.Distinct, "an explicit distinct: false was dropped, and false is not the default")
	assert.False(t, quorum.GetDistinct())
	assert.Len(t, quorum.GetExclude(), 1)
	assert.NotNil(t, quorum.GetVeto())

	_, ok := positions.At("steps[0].wait_for_signals.quorum")
	assert.True(t, ok, "no recorded position for the quorum block")

	marshalled, err := flowfile.Marshal(workflow)
	require.NoError(t, err)

	again, err := flowfile.Unmarshal(marshalled)
	require.NoError(t, err)
	assert.True(t, proto.Equal(workflow, again), "marshalling a quorum and parsing it back changed it:\n%s", marshalled)
}

// TestAQuorumWithoutDistinctDefaultsToDistinct is the other direction of the
// round trip: an absent `distinct:` stays absent rather than becoming false.
func TestAQuorumWithoutDistinctDefaultsToDistinct(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(quorumSource("        approve: 2\n")))
	require.NoError(t, err)

	assert.Nil(t, workflow.GetSteps()[0].GetWait().GetSignalBatch().GetQuorum().Distinct)
}

// TestAQuorumNeedsAnApproveItCanMeet refuses the quorum nothing could ever
// complete, and an approve beyond what one wait will take.
func TestAQuorumNeedsAnApproveItCanMeet(t *testing.T) {
	t.Parallel()

	for _, quorum := range []string{
		"        approve: 0\n",
		"        veto: ${payload.approved == false}\n",
		"        approve: 129\n",
	} {
		_, err := flowfile.Unmarshal([]byte(quorumSource(quorum)))
		require.Error(t, err, "a quorum with %q was accepted", strings.TrimSpace(quorum))
	}
}

// TestAnUnsatisfiableQuorumIsRefusedByValidate is the validator's rule against
// a closed policy: asking for more distinct approvers than the policy names can
// never be met, and is better said before the run than after a timeout.
func TestAnUnsatisfiableQuorumIsRefusedByValidate(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		source   string
		wantDiag bool
	}{
		{"approve beyond a closed policy", quorumSource("        approve: 3\n", "alice", "bob"), true},
		{"approve equal to a closed policy", quorumSource("        approve: 2\n", "alice", "bob"), false},
		{"a repeated subject counts once", quorumSource("        approve: 2\n", "alice", "alice"), true},
		{"distinct false needs only one sender", quorumSource("        approve: 3\n        distinct: false\n", "alice"), false},
		{"no policy admits any authenticated sender", quorumSource("        approve: 3\n"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			diagnostics, err := flowfile.ValidateSource([]byte(test.source))
			require.NoError(t, err)

			var found bool
			for _, d := range diagnostics {
				if d.Field == "wait_for_signals.quorum.approve" {
					found = true
					assert.Contains(t, d.Message, "can never be met")
				}
			}
			assert.Equal(t, test.wantDiag, found, "diagnostics: %v", diagnostics)
		})
	}
}

// TestAQuorumPolicyThatAdmitsByClaimIsNotCounted keeps the validator from
// refusing a quorum it cannot know is unsatisfiable: a claims predicate admits any
// number of subjects.
func TestAQuorumPolicyThatAdmitsByClaimIsNotCounted(t *testing.T) {
	t.Parallel()

	source := quorumSource("        approve: 5\n") +
		"signals:\n  release-approved:\n    allow: ${sender.identity.claims.team == \"release-managers\"}\n"

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)

	for _, d := range diagnostics {
		assert.NotEqual(t, "wait_for_signals.quorum.approve", d.Field, d.Message)
	}
}
