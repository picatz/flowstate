package conformance

import (
	"testing"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// PayloadField returns one field of the payload a wait produced, by name, and
// fails the test when the wait produced no payload mapping or the mapping has
// no such field — naming what it does hold, so a wrong name is a one-line fix.
//
// Here rather than in internal/testkit because it reads the engine's own
// types, and the engine's dependencies test themselves with that package
// (#1709): the local driver, the durable driver and the server each carried a
// copy of this, which is the both-drivers shape this package exists for.
func PayloadField(t testing.TB, outputs *v1.Node_Outputs, name string) *expr.Value {
	t.Helper()

	payload := outputs.GetNamedValues()[v1.PayloadOutput].GetLiteral().GetMapValue()
	require.NotNil(t, payload, "the wait produced no payload mapping")

	for _, entry := range payload.GetEntries() {
		if entry.GetKey().GetStringValue() == name {
			return entry.GetValue()
		}
	}

	t.Fatalf("the payload has no %q; it holds %d entries", name, len(payload.GetEntries()))
	return nil
}
