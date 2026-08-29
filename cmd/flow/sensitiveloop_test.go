package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// #974. A `for_each` over an input declared `sensitive:` binds each element
// into the body's scope, and a step that fails there composes a sentence
// around whatever it was given — an http task names the URL it dialed. That
// sentence is not the transcript, so none of the redaction this binary
// already applies to a run's values touched it: the same invocation printed
//
//	{"steps":{"enrich":{"results":"[redacted: step transcript withheld: …]"}}}
//
// on stdout while stderr, two lines up, read
//
//	step "enrich": iteration 0: step "lookup": task "http":
//	Get "http://127.0.0.1:1/alice@corp.example": …
//
// with the bound item — an element of the sensitive list — in the clear, and
// `-o json` carried the identical text in the failure document's own
// `error.message`. `flow test` was already clearing that exact sentence
// against that exact set (flowtest's check.go); `flow run local` and `flow
// run` were not, which is one value with one meaning rendered two ways.
//
// The item is caught without anything at the loop declaring it: it is a
// descendant of the list `sensitive:` names, and [v1.SensitiveValues] holds
// every descendant. See TestALoopItemIsSensitiveBecauseTheListItCameFromIs in
// pkg/flowstate/v1 for that half.
const sensitiveLoopWorkflow = `edition: v2026.3
name: sensitive-loop
inputs:
  customers:
    type: list
    required: true
    sensitive: true
    description: who to enrich
steps:
  - id: enrich
    for_each:
      items: ${inputs.customers}
      as: customer
      steps:
        - id: lookup
          http:
            method: GET
            url: http://127.0.0.1:1/${customer}
`

// sensitiveLoopItem is the element of the sensitive list the loop binds, and
// so the string neither stream may carry. Shaped like a customer's email —
// the schema's own example of a value that is private without being secret.
const sensitiveLoopItem = "alice@corp.example"

// TestALoopItemFromASensitiveInputStaysOutOfTheFailureText is the reproduction:
// the run fails inside the loop body and the item must not reach either the
// prose on stderr or the failure document on stdout.
//
// Mutation-proven: dropping either the redactFailureError or the
// redactFailureText call in runlocal.go puts `alice@corp.example` back into
// one of the two streams, and this fails on that stream.
func TestALoopItemFromASensitiveInputStaysOutOfTheFailureText(t *testing.T) {
	// Not t.Parallel(): the loopback denial reads the process-wide egress
	// policy, the same reason [TestLoopbackDenialUnderTheDefaultPolicyNamesItsOwnRemedy]
	// stays serial.
	stdout, stderr, err := runLocal(t, sensitiveLoopWorkflow,
		"--input", `customers=["`+sensitiveLoopItem+`"]`, "-o", "json")
	require.Error(t, err, "the loop body dials a port nothing listens on")

	require.NotContains(t, stderr, sensitiveLoopItem,
		"the bound item of a sensitive input must not print in the failure prose")
	require.NotContains(t, err.Error(), sensitiveLoopItem,
		"nor in the error the command returns, which is what main prints")
	require.NotContains(t, stdout, sensitiveLoopItem,
		"nor in the failure document a machine caller reads")

	// The reason still has to be readable: this is a redaction, not a
	// withholding. What replaces the item is the marker, and everything
	// around it — which step, which task, what went wrong — survives, because
	// CLAUDE.md's "diagnostics are a feature" is the standard and #975's own
	// argument against withholding a failure message outright still stands.
	require.Contains(t, stderr, v1.SensitiveMarker)
	require.Contains(t, stderr, `step "enrich"`)

	var document map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &document))
	failure, ok := document["error"].(map[string]any)
	require.True(t, ok, "a failed run's document carries the reason: %s", stdout)
	message, ok := failure["message"].(string)
	require.True(t, ok)
	require.NotContains(t, message, sensitiveLoopItem)
	require.Contains(t, message, v1.SensitiveMarker)
	// Everything around the item survives — which step, which iteration,
	// which task — because this is a redaction and not a withholding. The
	// document is the place to assert it: the prose on stderr is wrapped to
	// the terminal's width, so a sentence there can break across lines.
	//
	// What the dial itself says is deliberately not asserted. The default
	// egress policy refuses loopback and another test in this package may
	// have registered a permissive one into the process-wide
	// [v1.DefaultRegistry] by the time this runs, so the tail of the sentence
	// is either a policy denial or a connection refusal — a pre-existing gap
	// in this package's test isolation ([TestLoopbackDenialUnderTheDefaultPolicyNamesItsOwnRemedy]
	// names it), and not something this test is about either way.
	require.Contains(t, message, `step "enrich"`)
	require.Contains(t, message, "iteration 0")
	require.Contains(t, message, `task "http"`)
	require.NotEmpty(t, failure["kind"],
		"the classification is not a value the workload chose, and stays")
}

// --reveal-sensitive is the one deliberate escape hatch, and it has to reach
// this surface too: a rule the flag does not cover is a rule an author cannot
// get out from under, and this binary's other redactions all answer to it.
func TestRevealSensitiveShowsTheLoopItemInTheFailureText(t *testing.T) {
	// Not t.Parallel(): see above.
	_, stderr, err := runLocal(t, sensitiveLoopWorkflow,
		"--input", `customers=["`+sensitiveLoopItem+`"]`, "--reveal-sensitive")
	require.Error(t, err)

	require.Contains(t, stderr, sensitiveLoopItem)
	require.Contains(t, err.Error(), sensitiveLoopItem)
}

// A workflow that declares nothing sensitive is untouched: the failure text
// is the whole sentence, with no marker anywhere in it. Without this, a
// redaction that fired unconditionally — replacing every value it was handed
// — would pass the test above and destroy every other run's diagnostics.
func TestAFailureTextIsUntouchedWhenNothingIsDeclaredSensitive(t *testing.T) {
	// Not t.Parallel(): see above.
	ordinary := `edition: v2026.3
name: ordinary-loop
inputs:
  customers:
    type: list
    required: true
steps:
  - id: enrich
    for_each:
      items: ${inputs.customers}
      as: customer
      steps:
        - id: lookup
          http:
            method: GET
            url: http://127.0.0.1:1/${customer}
`

	_, stderr, err := runLocal(t, ordinary, "--input", `customers=["`+sensitiveLoopItem+`"]`)
	require.Error(t, err)

	require.Contains(t, stderr, sensitiveLoopItem,
		"a run declaring nothing sensitive loses nothing from its failure text")
	require.NotContains(t, stderr, v1.SensitiveMarker)
}

// The durable driver's counterpart, at the seam the two share. Both drivers
// compose the same failure sentence (the local one directly, the durable one
// out of Temporal's answer), and both render it through this binary — so a
// value withheld from one and printed by the other would make `flow run local`
// a rehearsal that lies about production, which is exactly what CLAUDE.md's
// "both execution drivers must agree" forbids.
//
// The verb, not the driver, is what decides whether there is anything to
// redact against: `flow run` holds the arguments it just submitted and so
// carries the set into its follow loop, the same way `flow run local` holds
// the ones it just bound. `flow watch <id>` and `flow get <id>` hold neither
// and are unchanged — the case below it.
func TestTheDurableFollowRedactsTheSameFailureText(t *testing.T) {
	// Not t.Parallel(): [serveFake] points the client at itself with
	// [testing.T.Setenv].

	failure := `step "enrich": iteration 0: step "lookup": task "http": ` +
		`GET http://enrich.invalid/` + sensitiveLoopItem + ` returned status 404`

	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			Status: v1.RunResponse_STATUS_FAILED,
			Kind: &v1.GetResponse_Error{Error: &v1.RunResponse_Error{
				Message: failure,
				Kind:    "TaskError",
			}},
			PendingActivities: []*v1.PendingActivity{
				{Attempt: 1, LastFailure: failure},
			},
		},
	}
	address := serveFake(t, fake)

	spec := &v1.Workflow{
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "customers", Type: v1.InputDeclaration_TYPE_LIST, Sensitive: true},
		},
	}
	sensitive := runSensitiveValues(spec,
		map[string]*v1.Value{"customers": v1.NewLiteralList(sensitiveLoopItem)}, false)

	got, err := clientPoller{
		workflowID: "flowstate-workflow-3f7c",
		server:     serverFlags{address: address},
		spec:       spec,
		sensitive:  sensitive,
	}.Poll(t.Context())
	require.NoError(t, err)

	require.NotContains(t, got.GetError().GetMessage(), sensitiveLoopItem)
	require.Contains(t, got.GetError().GetMessage(), v1.SensitiveMarker)
	require.Contains(t, got.GetError().GetMessage(), `step "enrich"`,
		"the reason survives; only the value leaves it")
	require.Equal(t, "TaskError", got.GetError().GetKind())

	// The other workload-chosen text on this message, which is the same class
	// of value reached by another route: a pending activity's last failure.
	require.NotContains(t, got.GetPendingActivities()[0].GetLastFailure(), sensitiveLoopItem)

	// And the poller itself may be printed — a follow loop logs its own state
	// — without printing the arguments it carries.
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		rendered := fmt.Sprintf(verb, clientPoller{spec: spec, sensitive: sensitive})
		require.NotContainsf(t, rendered, sensitiveLoopItem,
			"a poller rendered with %s leaked the run's own arguments: %s", verb, rendered)
	}
}

// `flow watch <id>` is a later invocation holding neither the file nor the
// arguments, so it has no set to build and its failure text is unchanged.
// That is not an oversight: withholding it would silence the only field that
// answers "why did this fail" on every failed run anybody looks up by id, and
// [redactGetResponse]'s own comment argues at length why that trade is worse
// than the disclosure. The gap is stated, in the schema and in the PR, rather
// than closed by a rule that cannot see the value it would be redacting.
func TestAPollerWithNoArgumentsLeavesTheFailureTextAlone(t *testing.T) {
	// Not t.Parallel(): see above.

	failure := "task \"http\": GET http://enrich.invalid/" + sensitiveLoopItem + " returned status 404"

	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			Status: v1.RunResponse_STATUS_FAILED,
			Kind:   &v1.GetResponse_Error{Error: &v1.RunResponse_Error{Message: failure}},
		},
	}
	address := serveFake(t, fake)

	got, err := clientPoller{
		workflowID: "flowstate-workflow-3f7c",
		server:     serverFlags{address: address},
	}.Poll(t.Context())
	require.NoError(t, err)

	require.Equal(t, failure, got.GetError().GetMessage())
}

// [runSensitiveValues] runs before the engine's own bind, so a refusal there
// means the arguments could not be enumerated — and an unenumerable set has to
// withhold rather than allow, which is the direction a "return the empty set
// on error" would have gone (CLAUDE.md, "fail closed": a component that allows
// when it cannot decide will eventually allow everything).
func TestASensitiveRunWhoseArgumentsCannotBeBoundWithholdsItsFailureText(t *testing.T) {
	t.Parallel()

	// A `sensitive: true` declaration whose type refuses the value sent for
	// it: the bind fails, so nothing about that value can be enumerated.
	unbindable := &v1.Workflow{
		DeclaredInputs: []*v1.InputDeclaration{
			{Name: "customers", Type: v1.InputDeclaration_TYPE_INT, Sensitive: true},
		},
	}

	sensitive := runSensitiveValues(unbindable,
		map[string]*v1.Value{"customers": v1.NewLiteralList(sensitiveLoopItem)}, false)
	require.True(t, sensitive.WithholdAll())

	failure := errors.New("task \"http\": GET http://enrich.invalid/" + sensitiveLoopItem)
	redacted := redactFailureError(failure, sensitive)
	require.NotContains(t, redacted.Error(), sensitiveLoopItem)
	require.Contains(t, redacted.Error(), "could not be enumerated")

	// And the chain stops here: a scrubbed error that still unwraps to the
	// original hands the raw text to anything that walks it (CLAUDE.md,
	// "unwrapping into persisted failures"). Every containment shape, on the
	// value and inside a struct and a slice of them, because a wrapper's own
	// %+v is how the original would come back out.
	require.NotErrorIs(t, redacted, failure)
	require.Nil(t, errors.Unwrap(redacted))

	type holder struct{ Err error }
	for _, subject := range []any{redacted, holder{Err: redacted}, []error{redacted}, []holder{{Err: redacted}}} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
			require.NotContainsf(t, fmt.Sprintf(verb, subject), sensitiveLoopItem,
				"a redacted failure rendered with %s must not reach the original text", verb)
		}
	}
}
