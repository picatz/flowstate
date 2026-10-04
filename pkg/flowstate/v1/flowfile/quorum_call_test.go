package flowfile_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// quorumCallee is a policy-free workflow whose gate wants two distinct approvals.
func quorumCallee(policy map[string]*v1.SignalPolicy) *v1.Workflow {
	return &v1.Workflow{
		Name:    "approve-twice",
		Signals: policy,
		Steps: []*v1.Node{{
			Id: "gate",
			Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_SignalBatch{SignalBatch: &v1.SignalBatch{
					Name:   "release-approved",
					Quorum: &v1.SignalQuorum{Approve: 2},
				}},
				Timeout: durationpb.New(time.Hour),
			}},
		}},
	}
}

func quorumCaller(rootPolicy map[string]*v1.SignalPolicy, callee *v1.Workflow) *v1.Workflow {
	return &v1.Workflow{
		Name:    "caller",
		Signals: rootPolicy,
		Steps: []*v1.Node{{
			Id:   "delegate",
			Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}},
		}},
	}
}

func allowOnly(subjects ...string) map[string]*v1.SignalPolicy {
	policy := &v1.SignalPolicy{}
	for _, subject := range subjects {
		policy.Allow = append(policy.Allow, &v1.SignalPolicyRule{Subject: "https://idp.example#" + subject})
	}

	return map[string]*v1.SignalPolicy{"release-approved": policy}
}

// hasQuorumDiagnostic reports whether diagnostics carry the unsatisfiable-quorum
// finding at step.
func hasQuorumDiagnostic(ds flowfile.Diagnostics, step string) (flowfile.Diagnostic, bool) {
	for _, d := range ds {
		if d.Field == "wait_for_signals.quorum.approve" && d.Step == step {
			return d, true
		}
	}

	return flowfile.Diagnostic{}, false
}

// TestAQuorumInACalleeIsCheckedAgainstTheRootPolicy pins which policy admits a
// signal to a called workflow's gate: the root's. The server records the
// top-level workflow's `signals:`, so a callee's own are never consulted, and a
// root that admits only alice makes a callee's two-approver gate unreachable
// however open the callee's own policy looks.
func TestAQuorumInACalleeIsCheckedAgainstTheRootPolicy(t *testing.T) {
	t.Parallel()

	t.Run("a root policy too small for the callee's quorum is refused at the call", func(t *testing.T) {
		t.Parallel()

		ds := flowfile.Validate(quorumCaller(allowOnly("alice"), quorumCallee(nil)))

		d, ok := hasQuorumDiagnostic(ds, "delegate")
		require.True(t, ok, "the unsatisfiable callee quorum went unreported: %v", ds)
		assert.Contains(t, d.Message, `workflow "approve-twice"`)
		assert.Contains(t, d.Message, "can never be met")
	})

	t.Run("a callee's own roomy policy does not rescue a small root policy", func(t *testing.T) {
		t.Parallel()

		ds := flowfile.Validate(quorumCaller(allowOnly("alice"), quorumCallee(allowOnly("alice", "bob", "carol"))))

		_, ok := hasQuorumDiagnostic(ds, "delegate")
		assert.True(t, ok, "a policy the server never consults admitted the quorum: %v", ds)
	})

	t.Run("a callee's own small policy does not condemn a roomy root policy", func(t *testing.T) {
		t.Parallel()

		ds := flowfile.Validate(quorumCaller(allowOnly("alice", "bob"), quorumCallee(allowOnly("alice"))))

		_, ok := hasQuorumDiagnostic(ds, "delegate")
		assert.False(t, ok, "a policy the server never consults refused the quorum: %v", ds)
	})

	t.Run("a root with no policy admits any sender", func(t *testing.T) {
		t.Parallel()

		ds := flowfile.Validate(quorumCaller(nil, quorumCallee(nil)))

		_, ok := hasQuorumDiagnostic(ds, "delegate")
		assert.False(t, ok, "an open gate was called unsatisfiable: %v", ds)
	})
}
