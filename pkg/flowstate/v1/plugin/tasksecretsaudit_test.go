package plugin

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/audit"
)

// A plugin task's secret inputs are resolved through the same seam the built-in
// http task uses ([flowstatev1.ResolveSecret]), so they inherit its audit
// record — and, until this, its misclassification of a failed one (Codex,
// picatz/flowstate#1394).

// deadSink is a required audit sink that cannot write, which is what a
// collector outage looks like from inside a worker.
type deadSink struct{ err error }

func (s deadSink) Emit(context.Context, *flowstatev1.AuditRecord) error { return s.err }

// TestAPluginSecretInputIsRetryableWhenTheAuditSinkIsDown: under
// --audit-required the resolution is refused before the store is consulted, so
// nothing was read and the step can be attempted again. Classified permanent,
// this made an operator's collector outage into a permanent failure of every
// plugin task with a secret input.
//
// Asserted on [resolvePluginSecretInputs] directly for the reason
// tasksecrets_tenancy_test.go states at length: both drivers reach this one
// function through [Plugin.taskFunc], and there is no second copy of it for the
// durable path to disagree with.
//
// Mutation-proved: dropping [flowstatev1.AuditRecorderUnavailable] from the
// classification returns PolicyDenied here.
func TestAPluginSecretInputIsRetryableWhenTheAuditSinkIsDown(t *testing.T) {
	store := tenantEnvSecrets(t, "team-a-secret", "team-b-secret")

	recorder, err := audit.NewRecorder(
		audit.WithoutStderr(),
		audit.WithEmitter(deadSink{err: errors.New("the collector is down")}),
		audit.Required(),
	)
	require.NoError(t, err)

	ctx := flowstatev1.ContextWithTaskRuntime(t.Context(), tenantRuntime(t, store, "team-a"))
	ctx = flowstatev1.NewContextWithEnforcementAuditor(ctx, recorder)

	_, _, err = resolvePluginSecretInputs(ctx, "example.task",
		[]string{"message"}, []string{"message"}, tokenRef)
	require.Error(t, err)

	kind := flowstatev1.ClassifyError(err)
	require.Equal(t, flowstatev1.ErrorKindUpstream, kind,
		"a sink outage refused a read that never happened; that is worth another attempt, not a permanent denial")
	require.True(t, kind.Retryable())
	require.True(t, flowstatev1.AuditRecorderUnavailable(err),
		"the recorder's own failure stays recognizable through the task error that carries it")
}
