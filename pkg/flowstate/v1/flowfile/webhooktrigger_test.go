package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/google/go-cmp/cmp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The spelling a webhook trigger ships under, written once so a change to it
// fails a test rather than quietly becoming a second grammar.
const webhookSource = `edition: v2026.3
name: order-webhook
inputs:
  order_id: { type: string, required: true }
  amount: { type: int, required: true }
triggers:
  - webhook: stripe
    verify:
      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}
    idempotency_key: ${event.headers["stripe-signature"]}
    with:
      order_id: ${event.body.data.object.metadata.order_id}
      amount: ${event.body.data.object.amount}
steps:
  - id: record
    log:
      message: ${'order ' + inputs.order_id}
`

// TestParsingAWebhookTrigger pins what the call-site spelling compiles to, and
// that every key a mistake can be written under has a position.
func TestParsingAWebhookTrigger(t *testing.T) {
	t.Parallel()

	workflow, positions, err := flowfile.Parse([]byte(webhookSource))
	require.NoError(t, err)

	webhooks := workflow.GetTriggers().GetWebhooks()
	require.Len(t, webhooks, 1)
	webhook := webhooks[0]

	assert.Equal(t, "stripe", webhook.GetName())
	assert.Equal(t, "env", webhook.GetVerify()["stripe"].GetSecretRef().GetScheme())
	assert.Equal(t, "STRIPE_WEBHOOK_SECRET", webhook.GetVerify()["stripe"].GetSecretRef().GetName())
	assert.NotNil(t, webhook.GetIdempotencyKey().GetExpr(), "the key is an expression over `event`")
	assert.Len(t, webhook.GetArguments(), 2)

	for _, path := range []string{
		"triggers", "triggers[0]", "triggers[0].webhook", "triggers[0].verify.stripe",
		"triggers[0].idempotency_key", "triggers[0].with", "triggers[0].with.order_id",
		"triggers[0].with.amount",
	} {
		_, ok := positions.At(path)
		assert.True(t, ok, "no recorded position for %q", path)
	}
}

// TestAWebhookTriggerIsAccepted is the base case every diagnostic below is a
// deviation from: the shipped spelling validates clean, and declaring a webhook
// does not stop the file being an ordinary file both drivers run.
func TestAWebhookTriggerIsAccepted(t *testing.T) {
	t.Parallel()

	diagnostics, err := flowfile.ValidateSource([]byte(webhookSource))
	require.NoError(t, err)
	require.Empty(t, diagnostics, "the settled spelling is not a validation problem")

	workflow, err := flowfile.Unmarshal([]byte(webhookSource))
	require.NoError(t, err)
	require.Len(t, workflow.GetSteps(), 1, "the steps are what a driver runs, and they are untouched")
}

// TestAWebhookMustSupplyEveryRequiredInput is the diagnostic this whole design
// was chosen for, in the words #491 specifies.
//
// It is the payoff of spelling a trigger as a call site: `inputs:` is a
// signature, so the check is static. A per-trigger mapping block would state the
// same fact twice with nothing forcing the two to agree, and this diagnostic
// could not exist.
func TestAWebhookMustSupplyEveryRequiredInput(t *testing.T) {
	t.Parallel()

	source := strings.Replace(webhookSource,
		"      amount: ${event.body.data.object.amount}\n", "", 1)

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.Len(t, diagnostics, 1)

	assert.Contains(t, diagnostics[0].Message, `webhook "stripe" does not supply required input "amount"`)
	assert.Contains(t, diagnostics[0].Message, "`with:`", "a diagnostic says what to do instead")
	assert.Positive(t, diagnostics[0].Line, "a diagnostic names its position")
	assert.Positive(t, diagnostics[0].Column)
}

// TestAWebhookMayNotBindAnUndeclaredInput is the other direction, which is the
// half a functionality test would miss: an argument nothing declares is almost
// always a rename in one place and not the other.
func TestAWebhookMayNotBindAnUndeclaredInput(t *testing.T) {
	t.Parallel()

	source := strings.Replace(webhookSource,
		"      amount: ${event.body.data.object.amount}",
		"      amount_cents: ${event.body.data.object.amount}", 1)

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.Len(t, diagnostics, 2, "the extra argument and the required input it was meant to be")

	assert.Contains(t, diagnostics[0].Message,
		`webhook "stripe" binds "amount_cents", which this workflow declares no input named`)
	assert.Contains(t, diagnostics[0].Message, "order_id, amount", "the inputs it does take are named")
	assert.Positive(t, diagnostics[0].Line)
}

// TestTwoWebhooksMayNotShareAName: a duplicate is refused rather than resolved,
// because a test case naming one of them would silently reach whichever came
// first.
func TestTwoWebhooksMayNotShareAName(t *testing.T) {
	t.Parallel()

	// The second entry belongs to the `triggers:` block, which sits above
	// `steps:`, so it is spliced in above that key rather than appended.
	source := strings.Replace(webhookSource, "steps:\n", `  - webhook: stripe
    verify:
      hmac_sha256: ${secret('env:OTHER')}
    idempotency_key: ${event.headers["x-id"]}
    with:
      order_id: ${event.body.id}
      amount: ${event.body.total}
steps:
`, 1)

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics)

	assert.Contains(t, diagnostics[0].Message, `webhook "stripe" is already declared by entry 1`)
	assert.Positive(t, diagnostics[0].Line)
}

// TestAWebhookIsRefusedWithoutVerification is the fail-closed posture, declared
// in the file: there is no spelling that means "accept anything", so a webhook
// with no scheme is refused rather than treated as permissive.
func TestAWebhookIsRefusedWithoutVerification(t *testing.T) {
	t.Parallel()

	source := strings.Replace(webhookSource,
		"    verify:\n      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}\n", "", 1)

	// Reported when the file compiles rather than after it, which is where the
	// entry's own position is — and early enough that nothing downstream ever
	// holds a webhook that could not refuse a delivery.
	_, err := flowfile.ValidateSource([]byte(source))
	require.Error(t, err)

	assert.Contains(t, err.Error(), `webhook "stripe" declares no `+"`verify:`")
	assert.Contains(t, err.Error(), "refused rather than accepted")
	assert.Regexp(t, `^\d+:\d+:`, err.Error(), "a diagnostic names its position")
}

// TestAnUnknownVerificationSchemeIsRefused: a scheme nothing implements is a
// webhook that can never accept a delivery, which is better said with a line and
// a column than discovered by an integration that silently never runs.
func TestAnUnknownVerificationSchemeIsRefused(t *testing.T) {
	t.Parallel()

	source := strings.Replace(webhookSource,
		"      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}",
		"      rot13: ${secret('env:STRIPE_WEBHOOK_SECRET')}", 1)

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics)

	assert.Contains(t, diagnostics[0].Message, `webhook "stripe" verifies with "rot13"`)
	assert.Contains(t, diagnostics[0].Message, strings.Join(v1.WebhookVerificationSchemes(), ", "))
	assert.Positive(t, diagnostics[0].Line)
}

// TestASigningKeyMayNotBeWrittenInTheFile: the material behind a scheme is a
// reference, resolved where the delivery is checked, never a value committed
// beside the workflow.
func TestASigningKeyMayNotBeWrittenInTheFile(t *testing.T) {
	t.Parallel()

	source := strings.Replace(webhookSource,
		"      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}",
		"      stripe: whsec_012345", 1)

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics)

	assert.Contains(t, diagnostics[0].Message, "using a value written in the file")
	assert.Contains(t, diagnostics[0].Message, "${secret(")
	assert.Positive(t, diagnostics[0].Line)
}

// TestAWebhookIsRefusedWithoutAnIdempotencyKey is the bound the transport forces:
// delivery is at-least-once, so a trigger with no dedupe key turns every retried
// delivery into a second run.
func TestAWebhookIsRefusedWithoutAnIdempotencyKey(t *testing.T) {
	t.Parallel()

	source := strings.Replace(webhookSource,
		"    idempotency_key: ${event.headers[\"stripe-signature\"]}\n", "", 1)

	_, err := flowfile.ValidateSource([]byte(source))
	require.Error(t, err)

	assert.Contains(t, err.Error(), `webhook "stripe" declares no `+"`idempotency_key:`")
	assert.Contains(t, err.Error(), "at-least-once")
	assert.Regexp(t, `^\d+:\d+:`, err.Error(), "a diagnostic names its position")
}

// TestAConstantIdempotencyKeyIsRefused: a key that does not depend on the
// delivery names every delivery alike, which is the failure the required field
// exists to prevent, arrived at the long way.
func TestAConstantIdempotencyKeyIsRefused(t *testing.T) {
	t.Parallel()

	source := strings.Replace(webhookSource,
		`    idempotency_key: ${event.headers["stripe-signature"]}`,
		"    idempotency_key: 7", 1)

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics)

	assert.Contains(t, diagnostics[0].Message, "does not depend on the delivery")
	assert.Positive(t, diagnostics[0].Line)
}

// TestAnIdempotencyKeyThatOnlyMentionsTheDeliveryIsAccepted is the author-facing
// half of the residual recorded in #733: a key holding a real, unshadowed `event`
// in a branch nothing takes satisfies the check that asks whether the delivery is
// *named*, and every delivery is still named `"all-events"`.
//
// Asserted rather than merely known, because the alternative — refusing it on the
// evidence of a few synthetic deliveries — was built and backed out: the same
// evidence refuses `${event.body.type == "invoice.paid" ? event.body.id :
// "ignored"}`, which is a working file, and `flow validate` shares this check with
// the path that binds a live delivery.
func TestAnIdempotencyKeyThatOnlyMentionsTheDeliveryIsAccepted(t *testing.T) {
	t.Parallel()

	for _, key := range []string{
		`${true ? "all-events" : event.body.id}`,
		`${event.body.type == "invoice.paid" ? event.body.id : "ignored"}`,
	} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()

			source := strings.Replace(webhookSource,
				`    idempotency_key: ${event.headers["stripe-signature"]}`,
				"    idempotency_key: '"+key+"'", 1)

			diagnostics, err := flowfile.ValidateSource([]byte(source))
			require.NoError(t, err)
			assert.Empty(t, diagnostics)
		})
	}
}

// TestATriggerReadsOnlyTheEvent: a trigger is evaluated before the run exists, so
// every other name is reported — with the scope named, because an author reaching
// for `${inputs.x}` here has a coherent model that is one step out of order.
func TestATriggerReadsOnlyTheEvent(t *testing.T) {
	t.Parallel()

	for _, reference := range []string{"inputs.order_id", "steps.record.value", "vars.region"} {
		t.Run(reference, func(t *testing.T) {
			t.Parallel()

			source := strings.Replace(webhookSource,
				"${event.body.data.object.metadata.order_id}", "${"+reference+"}", 1)

			diagnostics, err := flowfile.ValidateSource([]byte(source))
			require.NoError(t, err)
			require.NotEmpty(t, diagnostics)

			assert.Contains(t, diagnostics[0].Message, `webhook "stripe" reads`)
			assert.Contains(t, diagnostics[0].Message, "only name in scope is `event`")
			assert.Positive(t, diagnostics[0].Line)
		})
	}
}

// TestTheEventIsNotInScopeInAStep is the reverse rule, and it has to be a
// positioned diagnostic rather than a silent nil: `event` exists, so "unknown
// name" would send an author looking for a step they never wrote.
func TestTheEventIsNotInScopeInAStep(t *testing.T) {
	t.Parallel()

	source := strings.Replace(webhookSource,
		"      message: ${'order ' + inputs.order_id}",
		"      message: ${'order ' + event.body.id}", 1)

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	require.NotEmpty(t, diagnostics)

	assert.Contains(t, diagnostics[0].Message, "`event` is the delivery a trigger was started by")
	assert.Contains(t, diagnostics[0].Message, "`inputs.<name>`", "it says what to do instead")
	assert.Positive(t, diagnostics[0].Line)
}

// TestASecretMayNotBeBoundToAnInput: `with:` is resolved into `inputs:`, which
// the workflow evaluates and writes to durable history, so a reference cannot go
// there — while `verify:` is exactly where one belongs.
func TestASecretMayNotBeBoundToAnInput(t *testing.T) {
	t.Parallel()

	source := strings.Replace(webhookSource,
		"      order_id: ${event.body.data.object.metadata.order_id}",
		"      order_id: ${secret('env:ORDER')}", 1)

	_, err := flowfile.Unmarshal([]byte(source))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "a secret reference cannot be bound to an input")
}

// TestMarshalIsTheInverseForAWebhook is the guard against a formatter that
// silently deletes what an author wrote — the worst thing this package can do.
//
// Compared by compiling the round trip rather than by eyeballing the text: the
// claim is that no meaning is lost, and bytes are a proxy for it.
func TestMarshalIsTheInverseForAWebhook(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(webhookSource))
	require.NoError(t, err)

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)

	// Every key survives the trip, named individually so a writer that dropped one
	// fails saying which.
	for _, key := range []string{"webhook: stripe", "verify:", "idempotency_key:", "with:", "order_id:", "amount:"} {
		assert.Contains(t, string(written), key)
	}

	again, err := flowfile.Unmarshal(written)
	require.NoError(t, err)
	assert.Empty(t, cmp.Diff(workflow, again, protocmp.Transform()))
}

// TestAScheduleKeepsItsMappingSpelling: the spelling every scheduled file in the
// corpus uses still means what it meant, and a formatter does not migrate it to
// the list form for no reason its author can see.
func TestAScheduleKeepsItsMappingSpelling(t *testing.T) {
	t.Parallel()

	workflow, err := flowfile.Unmarshal([]byte(triggeredSource))
	require.NoError(t, err)

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)

	assert.Contains(t, string(written), "triggers:\n  schedule:")
	assert.NotContains(t, string(written), "- schedule:")
}

// TestAScheduleAndAWebhookCoexist: several sources starting one workload is the
// ordinary case, which is the whole reason the list spelling exists.
func TestAScheduleAndAWebhookCoexist(t *testing.T) {
	t.Parallel()

	source := strings.Replace(webhookSource, "steps:\n", `  - schedule:
      cron: "0 2 * * *"
steps:
`, 1)

	workflow, err := flowfile.Unmarshal([]byte(source))
	require.NoError(t, err)
	require.Len(t, workflow.GetTriggers().GetWebhooks(), 1)
	assert.Equal(t, []string{"0 2 * * *"}, workflow.GetTriggers().GetSchedule().GetCron())

	diagnostics, err := flowfile.ValidateSource([]byte(source))
	require.NoError(t, err)
	assert.Empty(t, diagnostics)

	written, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	again, err := flowfile.Unmarshal(written)
	require.NoError(t, err)
	assert.Empty(t, cmp.Diff(workflow, again, protocmp.Transform()))
}

// TestATriggerEntryMustNameItsKind: each entry says what it is first, and an
// entry that does not is reported rather than dropped.
func TestATriggerEntryMustNameItsKind(t *testing.T) {
	t.Parallel()

	source := strings.Replace(webhookSource, "  - webhook: stripe", "  - hook: stripe", 1)

	_, err := flowfile.Unmarshal([]byte(source))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "names no kind of trigger")
}

// TestFixLeavesTheEventBindingAlone is the rewriter rule CLAUDE.md demands of any
// new bound name, tested the way that section says to test it: by comparing bytes.
//
// A file may legitimately contain a step called `event`, and `flow fix` knowing
// less about scope than the language does is how this package has corrupted a
// valid file twice. A rewriter that rooted the name here would produce a file that
// still validates and computes something else.
func TestFixLeavesTheEventBindingAlone(t *testing.T) {
	t.Parallel()

	// A pre-root file, so `flow fix` has real work to do in the same document: the
	// step reference in the log message is rewritten, and the trigger's `event` is
	// not.
	source := `edition: v2026.2
name: order-webhook
inputs:
  order_id: { type: string }
triggers:
  - webhook: stripe
    verify:
      stripe: ${secret('env:STRIPE_WEBHOOK_SECRET')}
    idempotency_key: ${event.headers["stripe-signature"]}
    with:
      order_id: ${event.body.id}
steps:
  - id: event
    log:
      message: recording
  - id: after
    log:
      message: ${'done ' + inputs.order_id}
`

	result, err := flowfile.Fix([]byte(source))
	require.NoError(t, err)

	assert.Contains(t, string(result.Source), "${event.headers[\"stripe-signature\"]}",
		"the delivery is bound in a trigger, whatever a step is called")
	assert.Contains(t, string(result.Source), "order_id: ${event.body.id}")
	assert.NotContains(t, string(result.Source), "steps.event",
		"rooting the trigger's binding would make the file compute something else")

	// And the result still compiles, which is the other half of the rule: a name
	// left bare where it should have been rooted fails the same way round.
	_, err = flowfile.Unmarshal(result.Source)
	require.NoError(t, err)
}
