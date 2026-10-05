package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestIdentityPrincipalLocal runs the shared identity cases against the local
// driver: `run.identity` (through the rehearsal identity) and a wait's
// `sender.identity` (through a rehearsal delivery) must read the same
// `principal` and `claims` as the durable driver does in
// engine.TestIdentityPrincipalDurable.
func TestIdentityPrincipalLocal(t *testing.T) {
	t.Parallel()

	for _, c := range conformance.IdentityCases() {
		t.Run("run/"+c.Name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			if c.Identity != nil {
				ctx = v1.NewContextWithRehearsalIdentity(ctx, c.Identity)
			}

			outputs, err := v1.Run(ctx, conformance.RunIdentityPrincipalWorkflow())
			require.NoError(t, err)
			conformance.AssertRunIdentityPrincipal(t, outputs, c)
		})

		t.Run("sender/"+c.Name, func(t *testing.T) {
			t.Parallel()

			signals := v1.NewLocalSignals()
			ctx := v1.NewContextWithSignalWaiter(t.Context(), signals)

			type result struct {
				outputs *v1.Workflow_StepOutputs
				err     error
			}
			done := make(chan result, 1)
			go func() {
				outputs, err := v1.Run(ctx, conformance.SenderIdentityWorkflow())
				done <- result{outputs, err}
			}()

			require.Eventually(t, func() bool {
				return signals.DeliverFrom(conformance.SenderIdentitySignal,
					&v1.Node_Outputs{NamedValues: map[string]*v1.Value{"approved": v1.NewLiteral(true)}},
					v1.RehearsalSignalSender(c.Identity)) == nil
			}, 5*time.Second, 10*time.Millisecond)

			select {
			case got := <-done:
				require.NoError(t, got.err)
				conformance.AssertSenderIdentity(t, got.outputs, c)
			case <-time.After(15 * time.Second):
				t.Fatal("the local run never finished after the delivery")
			}
		})
	}
}
