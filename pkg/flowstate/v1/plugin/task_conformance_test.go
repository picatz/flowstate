package plugin

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// The error pipeline, end to end, once per SDK constructor.
//
// Issue #184's gap 1: a plugin author chooses one of the SDK's failure
// constructors and should get "my errors read correctly everywhere" for free —
// the classification the constructor names survives the wire to the host and
// renders the same way at every surface an operator or an agent reads. Nothing
// pinned that whole chain as one thing. The SDK's half is covered in
// sdk_test.go (each constructor produces the right connect code and
// ExecuteResponse verdict), and the host's half is covered piecewise in
// classify_test.go (a code maps to a kind) and taskerror_scrub_test.go
// (classification survives scrubbing). But the *join* — constructor → wire →
// host classification → rendered text and JSON, all agreeing — was written down
// nowhere, so a plugin author had no single test proving their errors do not
// garble.
//
// # Why a real subprocess rather than a marshalled envelope
//
// The garbling this guards against is structure flattened into a string too
// early and then wrapped again — a code prefix doubled, a message prefixed
// twice. That failure lives in the *serialization*: connect writes the failure
// to an HTTP error body and the host's client rebuilds it, and whether the code
// prefix ends up in the rebuilt message or beside it is decided there, not in
// any in-process call. The only code that turns an SDK constructor into the wire
// form is [sdk] `asConnectError`, reachable only through the SDK's own serving
// path, so the faithful test starts from `sdk.Unavailable(...)` inside a real
// plugin and reads the failure back across a real socket. A hand-built
// connect.Error marshalled and unmarshalled in one process — the lighter option
// the task notes — would skip exactly the hop where a doubled prefix appears, so
// it is not used here: the same reason the reachability tests launch a real
// plugin rather than calling the executor in-process.
//
// The fixture plugin is [runErrorsPlugin], a real SDK plugin (this test binary
// wearing the `errors` name, served through [sdk.Run]); each of its tasks returns
// one constructor's error and nothing else.

// errorConformanceCase is one constructor and everything the host must make of it.
//
// The constructor and the expectations are one value so the fixture that serves
// the error and the test that reads it back cannot disagree about which task
// means what — the "one value, not written twice" rule applied to a table that
// is consumed from two sides (the subprocess builds tasks from `task`/`make`,
// the test asserts against the `want*` fields).
type errorConformanceCase struct {
	// task is the bare task name the fixture serves it under.
	task string

	// message is the sentinel the constructor carries, distinctive so a surface
	// can be searched for it and its occurrences counted — a message that must
	// appear exactly once anywhere it appears at all.
	message string

	// make builds the SDK error, run inside the fixture subprocess.
	make func(msg string) error

	// wantKind, wantRetryable, wantRetryAfter are what the host must classify the
	// wire form into, read back on this side.
	wantKind       flowstatev1.ErrorKind
	wantRetryable  bool
	wantRetryAfter time.Duration
}

// errorConformanceCases is one row per constructor [sdk] exposes.
//
// The kinds are the host's deliberate mapping, not a guess: several SDK
// constructors collapse onto one host [flowstatev1.ErrorKind] because the
// engine's kind set names causes the host can act on rather than every
// distinction a plugin can draw. `Conflict` and `Failed` both land on
// InvalidInput — the one permanent kind that describes "the task failed" without
// naming a cause the host cannot know (see taskError's own reasoning) — so the
// vocabulary a plugin dispatches on at the SDK level (via [sdk.IsConflict]) is
// richer than what reaches a Flowfile's kind. That is a property to pin here,
// not a bug: at each surface the *same* kind is reported, which is what "reads
// correctly everywhere" means. Distinguishing Conflict from Failed downstream is
// #184's later work, not something this test can assert into existence.
var errorConformanceCases = []errorConformanceCase{
	{
		task:     "not_found",
		message:  "conformance-sentinel-not-found",
		make:     func(m string) error { return sdk.NotFound("%s", m) },
		wantKind: flowstatev1.ErrorKindUnknownTask,
	},
	{
		task:     "permission_denied",
		message:  "conformance-sentinel-permission-denied",
		make:     func(m string) error { return sdk.PermissionDenied("%s", m) },
		wantKind: flowstatev1.ErrorKindPolicyDenied,
	},
	{
		task:     "invalid_input",
		message:  "conformance-sentinel-invalid-input",
		make:     func(m string) error { return sdk.InvalidInput("%s", m) },
		wantKind: flowstatev1.ErrorKindInvalidInput,
	},
	{
		task:     "conflict",
		message:  "conformance-sentinel-conflict",
		make:     func(m string) error { return sdk.Conflict("%s", m) },
		wantKind: flowstatev1.ErrorKindInvalidInput,
	},
	{
		task:     "failed",
		message:  "conformance-sentinel-failed",
		make:     func(m string) error { return sdk.Failed("%s", m) },
		wantKind: flowstatev1.ErrorKindInvalidInput,
	},
	{
		task:     "outcome_unknown",
		message:  "conformance-sentinel-outcome-unknown",
		make:     func(m string) error { return sdk.OutcomeUnknown("%s", m) },
		wantKind: flowstatev1.ErrorKindUpstreamUnknown,
	},
	{
		task:          "unavailable",
		message:       "conformance-sentinel-unavailable",
		make:          func(m string) error { return sdk.Unavailable("%s", m) },
		wantKind:      flowstatev1.ErrorKindUpstream,
		wantRetryable: true,
	},
	{
		task:           "unavailable_after",
		message:        "conformance-sentinel-unavailable-after",
		make:           func(m string) error { return sdk.UnavailableAfter(30*time.Second, "%s", m) },
		wantKind:       flowstatev1.ErrorKindUpstream,
		wantRetryable:  true,
		wantRetryAfter: 30 * time.Second,
	},
}

// runErrorsPlugin serves the conformance fixture: a real SDK plugin whose every
// task returns exactly one constructor's error.
//
// It runs in the subprocess [TestMain] hands to a host that launched this binary
// under the `errors` name, and returns the process exit code. Because it goes
// through [sdk.Run], the errors it returns cross the wire the way any real
// plugin's do — which is the whole point of not hand-rolling the handler.
func runErrorsPlugin() int {
	tasks := make([]sdk.Task, 0, len(errorConformanceCases))
	for _, c := range errorConformanceCases {
		c := c
		tasks = append(tasks, sdk.Task{
			Name:    c.task,
			Summary: "returns one classified error, for round-trip conformance",
			// Reuses a message the host already has, so the fixture ships no
			// descriptor and the task shape is beside the point — every Fn fails
			// before it would read an input.
			Input:  &flowstatev1.Task_Log_Inputs{},
			Output: &flowstatev1.Task_Log_Outputs{},
			Fn: func(context.Context, map[string]*flowstatev1.Value, *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
				return nil, c.make(c.message)
			},
		})
	}

	err := sdk.Run(context.Background(), sdk.Plugin{
		Name:        "errors",
		Version:     "0.0.1",
		Description: "a fixture plugin that returns each classified SDK error kind",
		Tasks:       tasks,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "errors fixture: %v\n", err)
		return 1
	}

	return 0
}

// TestPluginErrorPipelineRoundTrip is issue #184 gap 1: for every constructor the
// SDK exposes, the classification survives constructor → wire → host, and the
// text and JSON surfaces agree on it with the message rendered exactly once.
func TestPluginErrorPipelineRoundTrip(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "errors")))

	defs := make(map[string]flowstatev1.TaskDef, len(errorConformanceCases))
	for _, def := range host.TaskDefs() {
		defs[def.Name] = def
	}

	require.Len(t, defs, len(errorConformanceCases),
		"the fixture served a different number of tasks than the table describes")

	for _, tc := range errorConformanceCases {
		t.Run(tc.task, func(t *testing.T) {
			t.Parallel()

			def, ok := defs["errors."+tc.task]
			require.True(t, ok, "the fixture did not serve task %q", tc.task)

			_, err := def.Fn(t.Context(), nil, nil)
			require.Error(t, err, "the task was supposed to fail")

			// The classification, read back on the host side after a real wire
			// crossing. This is the single fact every surface below must agree on.
			var taskErr *flowstatev1.TaskError
			require.ErrorAs(t, err, &taskErr)
			assert.Equal(t, tc.wantKind, taskErr.Kind, "the kind did not survive the wire")
			assert.Equal(t, tc.wantRetryable, taskErr.Retryable(),
				"the retry verdict did not survive the wire")
			assert.Equal(t, tc.wantRetryAfter, taskErr.RetryAfter,
				"the retry-after did not survive the wire")

			// Text surface 1: the wrapped error. It names the step's task and the
			// plugin, and carries the plugin's message once — a second copy would be
			// the double-prefixing the issue counts as garbling too.
			text := taskErr.Error()
			assert.Contains(t, text, "errors."+tc.task, "the error does not name the task")
			assert.Contains(t, text, `plugin "errors"`, "the error does not name the plugin")
			assert.Equalf(t, 1, strings.Count(text, tc.message),
				"the message is not rendered exactly once in %q", text)

			// Text surface 2: `${steps.<id>.error}` — the one an author's `if:`
			// compares and the one that spells the kind. This is where "text agrees
			// with JSON" is checked: the kind it prints is the JSON kind.
			stepText := flowstatev1.StepErrorText(err)
			assert.Contains(t, stepText, tc.wantKind.String(),
				"the tolerated-step text does not carry the classification")
			assert.Equalf(t, 1, strings.Count(stepText, tc.message),
				"the message is not rendered exactly once in the step-error text %q", stepText)

			// JSON / machine surface: the shape `flow get` and the MCP tools return
			// for a failed run. Kind is a string mirroring the ErrorKind; Message is
			// the rendered error. Built the way the local driver builds it
			// (runlocal.go), so this is that surface and not a parallel one.
			failure := &flowstatev1.RunResponse_Error{
				Message: err.Error(),
				Kind:    flowstatev1.ClassifyError(err).String(),
			}
			assert.Equal(t, tc.wantKind.String(), failure.GetKind(),
				"the JSON kind disagrees with the classification")
			assert.Equal(t, text, failure.GetMessage(),
				"the JSON message is not the rendered error text")

			// And rendered as JSON for real, since that is what a consumer parses.
			raw, marshalErr := protojson.Marshal(failure)
			require.NoError(t, marshalErr)
			jsonText := string(raw)
			assert.Contains(t, jsonText, tc.wantKind.String(),
				"the marshalled JSON does not carry the kind")
			assert.Equalf(t, 1, strings.Count(jsonText, tc.message),
				"the message is not present exactly once in the JSON %q", jsonText)

			// The kinds the three surfaces reported are literally the same one — the
			// property that makes "reads correctly everywhere" true rather than three
			// renderers that happen to be right today.
			assert.Truef(t,
				strings.Contains(stepText, tc.wantKind.String()) &&
					failure.GetKind() == tc.wantKind.String() &&
					taskErr.Kind == tc.wantKind,
				"the text and JSON surfaces disagree about the kind")

			// Containment shapes: the message survives fmt's verbs without being
			// doubled. The secrets half of this matrix — that a resolved secret in a
			// plugin error is scrubbed under every verb, on a struct, and on a slice
			// — is TestTaskErrorClassifiesBeforeScrubbing's assertNoLeak in this same
			// package; here the point is only that no verb duplicates the message.
			for _, verb := range []string{"%v", "%+v", "%s"} {
				rendered := fmtSprint(verb, err)
				assert.Equalf(t, 1, strings.Count(rendered, tc.message),
					"the message is duplicated under %s: %q", verb, rendered)
			}
		})
	}
}
