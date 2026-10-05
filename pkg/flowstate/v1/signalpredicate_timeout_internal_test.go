package flowstatev1

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The deadline, not the caller's context and not the cost bound, is what
// denies here: the context is live and the predicate is within its cost, so
// only the timeout wired into the evaluation can refuse it.
func TestSignalPolicyPredicateDeniesWhenItsOwnDeadlineExpires(t *testing.T) {
	t.Parallel()

	values := make([]any, 60)
	for i := range values {
		values[i] = i
	}
	inputs := map[string]*Value{"items": NewLiteralList(values...)}
	src := `inputs.items.all(a, inputs.items.all(b, a >= 0)) && sender.identity.claims["team"] == "release"`
	approver := &WorkloadIdentity{Claims: map[string]string{"team": "release"}}

	require.NoError(t, signalPolicyExprAllows(context.Background(), src, approver, nil, false, inputs),
		"the control: the same predicate admits under the normal timeout")

	require.Error(t, signalPolicyExprAllowsWithin(context.Background(), time.Nanosecond, src, approver, nil, false, inputs),
		"an evaluation past its own deadline admitted")
}
