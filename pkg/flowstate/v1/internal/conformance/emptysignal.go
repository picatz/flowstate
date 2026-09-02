package conformance

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// EmptySignalPayloadCase is one empty signal delivery and what both drivers
// must let its waiting step observe.
type EmptySignalPayloadCase struct {
	Name            string
	Workflow        *v1.Workflow
	SignalName      string
	Payload         *v1.Node_Outputs
	ExpectedOutputs *v1.Workflow_StepOutputs
}

// EmptySignalPayloadCases proves that an empty [v1.Node_Outputs] is a real
// payload rather than an invalid message requiring ingress substitution. Both
// drivers send the same non-nil empty message and evaluate the same shaped
// outputs: the gate was answered, and the sender supplied zero values.
func EmptySignalPayloadCases() []EmptySignalPayloadCase {
	return []EmptySignalPayloadCase{{
		Name:       "an empty signal payload answers the gate as an empty map",
		SignalName: "approved",
		Payload:    &v1.Node_Outputs{NamedValues: map[string]*v1.Value{}},
		Workflow: &v1.Workflow{
			Name: "empty-signal-payload",
			Steps: []*v1.Node{{
				Id: "approval",
				Kind: &v1.Node_Wait{Wait: &v1.Wait{
					Timeout: durationpb.New(time.Second),
					Kind: &v1.Wait_Signal{Signal: &v1.Signal{
						Name: "approved",
						Outputs: map[string]*v1.Value{
							"timed_out":    v1.NewExpr("timed_out"),
							"payload_size": v1.NewExpr("size(payload)"),
						},
					}},
				}},
			}},
		},
		ExpectedOutputs: &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
			"approval": {NamedValues: map[string]*v1.Value{
				"timed_out":    v1.NewLiteral(false),
				"payload_size": v1.NewLiteral(int64(0)),
			}},
		}},
	}}
}

// AssertEmptySignalPayloadCases runs the shared cases through one driver's own
// delivery mechanism.
func AssertEmptySignalPayloadCases(t *testing.T, run func(*v1.Workflow, string, *v1.Node_Outputs) (*v1.Workflow_StepOutputs, error)) {
	t.Helper()

	for _, c := range EmptySignalPayloadCases() {
		t.Run(c.Name, func(t *testing.T) {
			outputs, err := run(c.Workflow, c.SignalName, c.Payload)
			require.NoError(t, err)
			require.Truef(t, proto.Equal(c.ExpectedOutputs, outputs), "%s\n%s",
				c.Name, cmp.Diff(c.ExpectedOutputs, outputs, protocmp.Transform()))
		})
	}
}
