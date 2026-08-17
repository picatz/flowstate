package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	enums "go.temporal.io/api/enums/v1"
)

// TestTimeoutKindText covers #788's server.go half: [failureError]'s fallback
// branch — reached when a run ends with no application error in its chain,
// which is what an uncompensated workflow-level timeout looks like — used to
// return Temporal's own sentence, "activity StartToClose timeout (type:
// StartToClose)", verbatim. Every kind [timeoutKindText] is asked about must
// come back readable and without that vocabulary; an ordinary step timeout is
// already translated before failureError ever sees it
// (engine.durableStepTimeoutMessage), so this is the one place left where a
// bare Temporal sentence could still reach a caller.
func TestTimeoutKindText(t *testing.T) {
	cases := []enums.TimeoutType{
		enums.TIMEOUT_TYPE_START_TO_CLOSE,
		enums.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		enums.TIMEOUT_TYPE_SCHEDULE_TO_START,
		enums.TIMEOUT_TYPE_HEARTBEAT,
		enums.TIMEOUT_TYPE_UNSPECIFIED,
	}

	seen := map[string]bool{}
	for _, kind := range cases {
		text := timeoutKindText(kind)
		require.NotEmpty(t, text, "every timeout type must produce some text, kind=%s", kind)
		require.NotContains(t, text, "TIMEOUT_TYPE",
			"the text must not repeat Temporal's own enum spelling, kind=%s", kind)
		require.NotContains(t, text, "StartToClose",
			"the text must not repeat Temporal's own vocabulary, kind=%s", kind)

		seen[text] = true
	}
	require.Len(t, seen, len(cases), "every timeout kind must read as a distinct sentence")
}

// TestTimeoutKindTextDescribesWorkflowNotActivityScope is the regression for a
// Codex finding on #788: this function's call site (failureError's fallback)
// is reached only for a *workflow*-level timeout — an ordinary step timeout is
// translated before it gets here — so its wording must not borrow
// activity-attempt language ("a single attempt exceeded its time budget") that
// would misdescribe a timeout covering the whole run, including every
// Continue-As-New segment, as if it were one activity's retry budget.
func TestTimeoutKindTextDescribesWorkflowNotActivityScope(t *testing.T) {
	for _, kind := range []enums.TimeoutType{
		enums.TIMEOUT_TYPE_START_TO_CLOSE,
		enums.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
	} {
		text := timeoutKindText(kind)
		require.NotContains(t, text, "attempt",
			"kind=%s reads as activity-attempt scope, not workflow/run scope: %q", kind, text)
	}
}
