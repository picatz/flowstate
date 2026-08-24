package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A trigger's expressions were checked for scope and never for types.
//
// #491 gave the language two new expression positions — a webhook's `with:`
// arguments and its `idempotency_key` — and taught validateWebhookTriggers what
// *names* they may read. Nothing taught the type-check traversal about them, so
// `${nosuchfunc(event.body)}` and `${1 + true}` validated clean and then failed,
// deterministically and identically every time, when a delivery was bound. That is
// knowable from the document alone, which is celcheck.go's own definition of what
// belongs to a validator.
//
// A trigger fires with nobody present, so it is the position it costs most to be
// silent about.

// TestABadOperatorInATriggerArgumentIsAPositionedDiagnostic is the mutation proof:
// drop the trigger walk from checkExpressionTypes and this reports nothing.
func TestABadOperatorInATriggerArgumentIsAPositionedDiagnostic(t *testing.T) {
	t.Parallel()

	source := strings.Replace(webhookSource,
		"      amount: ${event.body.data.object.amount}",
		"      amount: ${1 + true}", 1)

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.Len(t, diagnostics, 1)

	assert.Equal(t, v1.DiagnosticCodeTypeMismatch, diagnostics[0].Code)
	assert.Contains(t, diagnostics[0].Message, "no matching overload")
	assert.Positive(t, diagnostics[0].Line, "a diagnostic names its position")
	assert.Positive(t, diagnostics[0].Column)
}

// TestAnUnknownFunctionInATriggerArgumentIsReported: the other half of what the
// checker can see, and the one with advice attached.
func TestAnUnknownFunctionInATriggerArgumentIsReported(t *testing.T) {
	t.Parallel()

	source := strings.Replace(webhookSource,
		"      amount: ${event.body.data.object.amount}",
		"      amount: ${nosuchfunc(event.body)}", 1)

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.Len(t, diagnostics, 1)

	assert.Contains(t, diagnostics[0].Message, `no function called "nosuchfunc"`)
	assert.Positive(t, diagnostics[0].Line)
}

// TestABadOperatorInAnIdempotencyKeyIsReported covers the trigger's other
// expression position, which is required rather than optional and is therefore the
// one every webhook has.
func TestABadOperatorInAnIdempotencyKeyIsReported(t *testing.T) {
	t.Parallel()

	// References event so [v1.CheckWebhookIdempotencyKey]'s own,
	// independent check — introduced by #689, which requires a delivery-
	// derived idempotency key — stays quiet and this isolates the single
	// diagnostic under test: the ill-typed `1 + true` on its own would
	// *also* trip that check, since it depends on nothing that varies per
	// delivery, and this test would then see two diagnostics for one
	// mistake instead of the one it is written to pin.
	source := strings.Replace(webhookSource,
		`    idempotency_key: ${event.headers["stripe-signature"]}`,
		`    idempotency_key: ${event != null && 1 + true == 2}`, 1)

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.Len(t, diagnostics, 1)

	assert.Equal(t, v1.DiagnosticCodeTypeMismatch, diagnostics[0].Code)
	assert.Positive(t, diagnostics[0].Line)
	assert.Positive(t, diagnostics[0].Column)
}

// TestTriggerTypeCheckingStaysQuietOnTheShippedSpelling is what keeps the check
// from being worse than the gap: a false diagnostic is worse than a missing one,
// and every real trigger expression is made of references to `event`, which the
// walk declares as `dyn` exactly as it does for a step.
func TestTriggerTypeCheckingStaysQuietOnTheShippedSpelling(t *testing.T) {
	t.Parallel()

	diagnostics, err := flowfile.ValidateSource([]byte(webhookSource))
	require.NoError(t, err)
	assert.Empty(t, diagnostics)
}
