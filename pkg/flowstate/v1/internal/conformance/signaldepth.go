package conformance

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// SignalPayloadDepthCase is one signal payload and whether a door admits it.
//
// The bound is [v1.MaxStructureDepth], the same one a run input is refused
// over at submit; these cases pin that a signal payload is held to it at
// every door — the server's Signal and SignalWithStart, and the local
// driver's [v1.LocalSignals] behind `flow run local` and `flow test` — in the
// same words. Before #1770 the doors bounded bytes alone, and 64 KiB is room
// for a few thousand levels.
type SignalPayloadDepthCase struct {
	Name    string
	Payload *v1.Node_Outputs

	// Refusal is a fragment the door's error must carry, or empty for a
	// payload every door must admit. The fragment is the depth sentence
	// itself — which field, how deep, over which bound — so a door refusing
	// for some other reason (size, policy, a queue bound) does not pass as
	// this one.
	Refusal string
}

// SignalPayloadDepthCases returns the boundary in both directions and both
// shapes a payload field can arrive in: at the bound admitted, one past it
// refused, whether the nesting is a CEL literal (what `flow signal --data`
// and every JSON caller builds) or a [v1.Value_Structure] (what a hand-built
// request may carry).
func SignalPayloadDepthCases() []SignalPayloadDepthCase {
	return []SignalPayloadDepthCase{
		{
			Name: "a literal at the depth bound is admitted",
			Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"approved": v1.NewLiteral(true),
				"doc":      NestedMapLiteral(v1.MaxStructureDepth),
			}},
		},
		{
			Name: "a literal one level past the depth bound is refused",
			Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"approved": v1.NewLiteral(true),
				"doc":      NestedMapLiteral(v1.MaxStructureDepth + 1),
			}},
			Refusal: SignalPayloadDepthRefusal("doc", v1.MaxStructureDepth+1),
		},
		{
			Name: "a structure at the depth bound is admitted",
			Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"doc": nestedStructure(v1.MaxStructureDepth),
			}},
		},
		{
			Name: "a structure one level past the depth bound is refused",
			Payload: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"doc": nestedStructure(v1.MaxStructureDepth + 1),
			}},
			Refusal: SignalPayloadDepthRefusal("doc", v1.MaxStructureDepth+1),
		},
	}
}

// SignalPayloadDepthRefusal is the sentence every door refuses a too-deep
// payload field with — the submit door's own input refusal, naming the field
// and the depth, adapted to say what it is bounding. One spelling here so a
// door that drifted to its own wording fails these cases rather than a
// reader's eye.
func SignalPayloadDepthRefusal(field string, depth int) string {
	return fmt.Sprintf("signal payload field %q nests %d levels deep, over the %d levels this server "+
		"can walk cheaply while evaluating an expression over it", field, depth, v1.MaxStructureDepth)
}

// nestedStructure is [NestedMapLiteral]'s [v1.Value_Structure] twin: a
// mapping nested depth levels around one string leaf.
func nestedStructure(depth int) *v1.Value {
	value := v1.NewLiteral("leaf")
	for range depth {
		value = v1.NewStructureMap(map[string]*v1.Value{"k": value})
	}

	return value
}

// AssertSignalPayloadDepthCases runs the shared cases through one door.
// deliver is the door: it returns the door's refusal, or nil for a payload
// it admitted.
func AssertSignalPayloadDepthCases(t *testing.T, deliver func(*v1.Node_Outputs) error) {
	t.Helper()

	for _, c := range SignalPayloadDepthCases() {
		t.Run(c.Name, func(t *testing.T) {
			err := deliver(c.Payload)
			if c.Refusal == "" {
				require.NoError(t, err, "a payload at the bound must be deliverable, or the bound is a lie by one")
				return
			}

			require.Error(t, err, "a payload past the bound was admitted")
			require.Contains(t, err.Error(), c.Refusal, "refused, but not for depth, or not in the shared words")
		})
	}
}
