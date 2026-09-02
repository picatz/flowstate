package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// bridgeSource is a compiling bridge, with with: substituted in where a case
// wants the contradiction.
func bridgeSource(with string) string {
	return `edition: v2026.3
name: deploy-gate
signals:
  stage-approved:
    allow:
      - subject: flowstate://webhook#deploy-gate/slack-approval
triggers:
  - webhook: slack-approval
    verify:
      hmac_sha256: ${secret('env:SLACK_SIGNING_SECRET')}
    idempotency_key: ${event.body.trigger_id}
` + with + `    signal:
      name: stage-approved
      correlate: ${event.body.order}
steps:
  - id: gate
    wait_for_signal:
      name: stage-approved
      timeout: 1h
`
}

// TestAWrittenWithBesideSignalIsRefusedHoweverEmpty is the review finding that
// the contradiction was judged by what `with:` compiled to rather than by
// whether it was written.
//
// `with: {}` is a file saying both things outright. The compiler drops an empty
// mapping — [compiler.triggerArguments] returns nil — so the specification
// carried only one of the two constructs and the check that read the
// specification found nothing to refuse, on a file whose text refuses itself.
//
// The span matters as much as the refusal: the key an author has to delete is
// `with:`, so that is the one underlined.
func TestAWrittenWithBesideSignalIsRefusedHoweverEmpty(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		with    string
		refused bool
	}{
		{
			name: "no with: at all is an ordinary bridge",
		},
		{
			name:    "an empty with: is still both constructs",
			with:    "    with: {}\n",
			refused: true,
		},
		{
			name:    "a populated with: is the obvious case",
			with:    "    with:\n      order_id: ${event.body.order}\n",
			refused: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Refused where the two keys are, which is the compiler rather than
			// the validator: the entry itself is the contradiction, and the
			// span the author needs is the one the parser is holding.
			_, err := flowfile.Unmarshal([]byte(bridgeSource(test.with)))

			if !test.refused {
				require.NoErrorf(t, err, "an ordinary bridge was refused")

				return
			}

			require.Error(t, err, "the contradiction was accepted")
			require.Contains(t, err.Error(), "declares both `with:` and `signal:`")
			require.Containsf(t, err.Error(), "triggers[0].with",
				"the refusal underlines the key an author has to delete; got %q", err.Error())
		})
	}
}

// TestABridgeAddressedFromUnsignedHeadersIsRefusedInTheFile is the positioned
// half of the header-addressing rule: the diagnostic lands on the expression
// that derives the address, because that is the line the fix edits.
func TestABridgeAddressedFromUnsignedHeadersIsRefusedInTheFile(t *testing.T) {
	t.Parallel()

	src := strings.Replace(bridgeSource(""),
		"correlate: ${event.body.order}", `correlate: ${event.headers["x-order"]}`, 1)

	ds := validateSource(t, src)

	var found *flowfile.Diagnostic
	for i, d := range ds {
		if strings.Contains(d.Message, "cannot prove reads only signed bytes") {
			found = &ds[i]

			break
		}
	}

	require.NotNilf(t, found, "a bridge addressed from unsigned headers was accepted; got %v", ds)
	require.Equal(t, "triggers[0].signal.correlate", found.Field)
	require.Containsf(t, found.Message, "hmac_sha256",
		"the diagnostic has to name the scheme whose signature does not cover this")
}

// TestTheAddressingRefusalUnderlinesTheExpressionItIsAbout pins the routing the
// diagnostic does, which used to be a substring match on the refusal's prose:
// correct until somebody reworded the sentence, at which point the squiggle
// moves to the wrong line and nothing fails.
//
// Both positions, because a rule with one arm under test is a rule whose other
// arm is decided by whatever the string happens to contain.
func TestTheAddressingRefusalUnderlinesTheExpressionItIsAbout(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		from  string
		to    string
		field string
	}{
		{
			name:  "a header-derived correlation underlines correlate:",
			from:  "correlate: ${event.body.order}",
			to:    `correlate: ${event.headers["x-order"]}`,
			field: "triggers[0].signal.correlate",
		},
		{
			name:  "a header-derived delivery id underlines idempotency_key:",
			from:  "idempotency_key: ${event.body.trigger_id}",
			to:    `idempotency_key: ${event.headers["x-request-id"]}`,
			field: "triggers[0].idempotency_key",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ds := validateSource(t, strings.Replace(bridgeSource(""), test.from, test.to, 1))

			var found *flowfile.Diagnostic
			for i, d := range ds {
				if strings.Contains(d.Message, "cannot prove reads only signed bytes") {
					found = &ds[i]

					break
				}
			}

			require.NotNilf(t, found, "the addressing rule did not fire; diagnostics were %v", ds)
			require.Equalf(t, test.field, found.Field,
				"the diagnostic underlines the expression an author has to edit")
		})
	}
}
