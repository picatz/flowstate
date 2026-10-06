package flowtest

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestAResponseMismatchNeverPrintsAWithheldExpectation: a case that expects a
// sensitive output's real value instead of the marker the receiver answers with
// fails, and its diagnostic is redacted like every other output diagnostic. When
// the run's sensitive values are too many to enumerate the report is withheld
// whole, and the final redaction pass is skipped for that case, so the
// diagnostic itself has to be the safe one.
func TestAResponseMismatchNeverPrintsAWithheldExpectation(t *testing.T) {
	t.Parallel()

	const secret = "tok-real-secret-value"

	document := v1.WebhookResponseDocument{
		Status:  v1.WebhookRunCompleted,
		Outputs: json.RawMessage(`{"token":"` + v1.SensitiveMarker + `"}`),
	}

	failures := compareResponseOutputs(map[string]any{"token": secret}, document, v1.WithheldSensitiveValues())
	require.Len(t, failures, 1, "the expectation of a real value matched the withheld marker")
	assert.NotContains(t, failures[0].GetMessage(), secret, "a withheld expectation was printed in the report")
}
