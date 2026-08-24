package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The values these tests look for. Distinct strings for the two maps, because
// the failure that matters is one of them being covered and the other not: a fix
// that redacted `vars` and left `loop_state` alone would pass a test that only
// looked for one string anywhere in the output.
const (
	carriedVarSecret  = "shh-carried-var-do-not-print-me"
	carriedLoopSecret = "shh-carried-loop-do-not-print-me"
)

// runningEntityResponse is the shape #975 is about, and every part of it is
// load-bearing.
//
// STATUS_RUNNING with neither arm of the outputs oneof populated is not a corner
// case: it is what an *entity* — a run shaped as `loop:` + `wait_for_signal:`
// that is never meant to finish — looks like for its whole life, which is why
// [v1.EntityState] exists at all. It is also exactly the response
// redactGetResponse's field-presence guard used to return untouched, so the
// carried state rendered in the clear on a run a person could still be watching.
func runningEntityResponse() *v1.GetResponse {
	return &v1.GetResponse{
		WorkflowId: "flowstate-workflow-3f7c",
		RunId:      "0198f1e2-0000-7000-8000-000000000000",
		Status:     v1.RunResponse_STATUS_RUNNING,
		EntityState: &v1.EntityState{
			Vars:      map[string]*v1.Value{"token": v1.NewLiteral(carriedVarSecret)},
			LoopState: map[string]*v1.Value{"poll": v1.NewLiteral(carriedLoopSecret)},
		},
	}
}

// TestGetWithholdsTheCarriedStateOfARunningRun is #975, exercised through the
// verb rather than through the redactor: `flow get <id>` builds the request,
// calls a stand-in deployment, and renders the answer, and it is the rendering
// at the end of that path a person or a script actually reads.
//
// Both formats, because they are two different readers reached by two different
// branches of runGet: the document path returns before the text path runs, so a
// fix checked in one says nothing about the other.
func TestGetWithholdsTheCarriedStateOfARunningRun(t *testing.T) {
	t.Run("json", func(t *testing.T) {
		serveFake(t, &fakeWorkflowService{getResponse: runningEntityResponse()})
		cmd, out, _ := getCommand(t)
		require.NoError(t, cmd.Flags().Set("output", "json"))

		require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

		require.NotContains(t, out.String(), carriedVarSecret,
			"a running run's top-level vars must not render in the clear — this is #975")
		require.NotContains(t, out.String(), carriedLoopSecret,
			"a loop's carried state:, the same binding the transcript withholds, must not either")

		// The shape survives: a reader still learns which vars exist and which
		// loops are carrying state, the way the step transcript keeps its step
		// ids. Read out of the document rather than matched as a substring, so
		// this asserts the *field* is still there rather than that the word
		// appears somewhere.
		var document struct {
			EntityState struct {
				Vars      map[string]string `json:"vars"`
				LoopState map[string]string `json:"loopState"`
			} `json:"entityState"`
		}
		require.NoError(t, json.Unmarshal([]byte(out.String()), &document))
		require.Contains(t, document.EntityState.Vars, "token")
		require.Contains(t, document.EntityState.LoopState, "poll")
		require.Contains(t, document.EntityState.Vars["token"], entityStateMarkerUnverified,
			"the marker has to say why, and `flow get` holds no specification to check against")
	})

	// The text path is the weaker of the two on purpose, and it is worth saying
	// what it does and does not prove. `flow get`'s text renderer prints no part
	// of [v1.EntityState] today, so this passes whether or not the redaction
	// exists — the json subtest above is what pins the fix. What this pins is
	// the *other* direction: a renderer added later that starts printing the
	// carried state on a terminal has to keep passing this, and it will, because
	// it is rendering an already-redacted message.
	t.Run("text", func(t *testing.T) {
		serveFake(t, &fakeWorkflowService{getResponse: runningEntityResponse()})
		cmd, out, errOut := getCommand(t)

		require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

		require.NotContains(t, out.String(), carriedVarSecret)
		require.NotContains(t, out.String(), carriedLoopSecret)
		require.NotContains(t, errOut.String(), carriedVarSecret)
		require.NotContains(t, errOut.String(), carriedLoopSecret)
	})

	t.Run("--reveal-sensitive still reveals", func(t *testing.T) {
		serveFake(t, &fakeWorkflowService{getResponse: runningEntityResponse()})
		cmd, out, _ := getCommand(t)
		require.NoError(t, cmd.Flags().Set("output", "json"))
		require.NoError(t, cmd.Flags().Set(revealSensitiveFlagName, "true"))

		require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

		require.Contains(t, out.String(), carriedVarSecret,
			"the one deliberate escape hatch has to defeat this path like every other")
	})
}

// TestCarriedStateIsWithheldForASensitiveInputAlone is the half a
// declared-outputs-only decision could not see.
//
// `vars:` is very often `${inputs.<name>}`, so a file that marks one *input*
// sensitive and declares no sensitive outputs at all still puts that value into
// [v1.EntityState.Vars]. Deciding on [sensitiveOutputNames] would answer "this
// file declares nothing sensitive" for exactly that file and pass the value
// through — see [decideCarriedValues], which is why the decision reads both.
//
// Through [clientPoller.Poll] rather than the redactor, because a poller holding
// a specification is the only real path that reaches this case: `flow get` never
// holds one, and it is `flow run`'s own follow that does.
func TestCarriedStateIsWithheldForASensitiveInputAlone(t *testing.T) {
	address := serveFake(t, &fakeWorkflowService{getResponse: runningEntityResponse()})

	spec := &v1.Workflow{
		DeclaredInputs: []*v1.InputDeclaration{{Name: "token", Sensitive: true}},
	}

	got, err := clientPoller{
		workflowID: "flowstate-workflow-3f7c",
		server:     serverFlags{address: address},
		spec:       spec,
	}.Poll(t.Context())
	require.NoError(t, err)

	require.Equal(t, redactedMarker(entityStateMarkerDeclared),
		got.GetEntityState().GetVars()["token"].GetLiteral().GetStringValue(),
		"a sensitive input reaches vars: and must be withheld there")
	require.Equal(t, redactedMarker(entityStateMarkerDeclared),
		got.GetEntityState().GetLoopState()["poll"].GetLiteral().GetStringValue())

	// The marker names the reason that is true here: a specification *is* in
	// hand and it declares sensitive data, which is a different sentence from
	// the one `flow get` gets, and sending a reader to a file that does not
	// exist is the mistake the two markers exist to avoid.
	require.NotContains(t, got.GetEntityState().GetVars()["token"].GetLiteral().GetStringValue(),
		entityStateMarkerUnverified)
}

// TestCarriedStateSurvivesASpecificationThatDeclaresNothingSensitive is the
// non-regression direction, and the one a fail-closed fix breaks by being too
// enthusiastic: a real specification that marks nothing sensitive must leave the
// carried state alone, or `flow run`'s own follow goes dark on every run.
func TestCarriedStateSurvivesASpecificationThatDeclaresNothingSensitive(t *testing.T) {
	address := serveFake(t, &fakeWorkflowService{getResponse: runningEntityResponse()})

	spec := &v1.Workflow{
		DeclaredInputs:  []*v1.InputDeclaration{{Name: "token"}},
		DeclaredOutputs: []*v1.OutputDeclaration{{Name: "url"}},
	}

	got, err := clientPoller{
		workflowID: "flowstate-workflow-3f7c",
		server:     serverFlags{address: address},
		spec:       spec,
	}.Poll(t.Context())
	require.NoError(t, err)

	require.Equal(t, carriedVarSecret,
		got.GetEntityState().GetVars()["token"].GetLiteral().GetStringValue())
	require.Equal(t, carriedLoopSecret,
		got.GetEntityState().GetLoopState()["poll"].GetLiteral().GetStringValue())
}

// TestRedactedCarriedStateContainsNoValueInAnyFormattingShape is CLAUDE.md's
// containment matrix applied to the message that carries this data.
//
// The reason it is not enough to read the field back: `fmt` reaches a value
// through reflection, so a redaction that replaced the *rendered* string while
// leaving the original anywhere in the message would still print it under `%#v`
// — and `%#v` on a struct holding the message, or on a slice of them, is what a
// `%+v` in somebody's debug logging actually produces. The shapes are checked on
// the [*v1.EntityState] itself, on the [*v1.GetResponse] holding it, on a struct
// holding that, and on a slice of those.
func TestRedactedCarriedStateContainsNoValueInAnyFormattingShape(t *testing.T) {
	redacted := redactGetResponse(runningEntityResponse(), nil, false)

	type holder struct {
		Response *v1.GetResponse
		State    *v1.EntityState
	}

	held := holder{Response: redacted, State: redacted.GetEntityState()}
	slice := []holder{held, held}

	subjects := []any{redacted, redacted.GetEntityState(), held, slice, &slice}

	for _, subject := range subjects {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
			rendered := fmt.Sprintf(verb, subject)
			require.NotContains(t, rendered, carriedVarSecret,
				"%T rendered with %s leaked the carried var", subject, verb)
			require.NotContains(t, rendered, carriedLoopSecret,
				"%T rendered with %s leaked the carried loop state", subject, verb)
		}
	}

	// The control: this matrix would pass just as well against an empty
	// message, so it has to be shown that the subject really did hold the
	// values and really is being rendered.
	require.Contains(t, fmt.Sprintf("%v", redacted), entityStateMarkerUnverified)
	require.Contains(t, fmt.Sprintf("%v", runningEntityResponse()), carriedVarSecret,
		"the unredacted fixture must leak, or this test proves nothing about the fix")
}

// TestTheGetToolWithholdsCarriedStateOverTheWire is the MCP half.
//
// cmd/flow/internal/mcp/result.go touches [v1.EntityState] — it drops it as a
// size-reduction rung — and that is not redaction: rung 0 is the untouched
// answer, so an entity whose state fits under the ceiling walks the ladder
// unchanged. What makes the tool safe is that dispatch hands the response to
// Deps.Redact *before* the ladder sees it (cmd/flow/internal/mcp/mcp.go, in the
// generic RPC handler), so fixing redactGetResponse covers this surface with no
// change to the ladder. That is a claim about wiring, and this is the test of it:
// an agent's context is an untrusted-consumer surface exactly like a terminal.
func TestTheGetToolWithholdsCarriedStateOverTheWire(t *testing.T) {
	// Not parallel: connectRemoteMCP stands up a fake deployment through
	// t.Setenv, which the testing package forbids alongside t.Parallel.
	posture := defaultLocalRunPosture()
	session := connectRemoteMCP(t, posture, &fakeWorkflowService{getResponse: runningEntityResponse()})

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{
		Name:      flowmcp.ToolName("Get"),
		Arguments: map[string]any{"workflowId": "flowstate-workflow-3f7c"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.Content)

	var whole strings.Builder
	for _, content := range result.Content {
		text, ok := content.(*mcp.TextContent)
		require.True(t, ok, "flowstate_get answered with a %T content block", content)
		whole.WriteString(text.Text)
	}

	require.NotContains(t, whole.String(), carriedVarSecret)
	require.NotContains(t, whole.String(), carriedLoopSecret)
	require.Contains(t, whole.String(), entityStateMarkerUnverified,
		"the state is withheld rather than dropped, so the answer still says the run carries some")
}

// TestWithholdingCarriedStateCannotInflateABoundedAnswer is Codex's finding on PR #1067.
//
// [v1.EntityState] is bounded on purpose — `entityStateMaxBytes`, 256 KiB, in
// pkg/flowstate/v1/engine/progress.go — because a query answer is its own
// resource and how many keys a run carries is the workload's choice. The marker
// is around eighty bytes and a `vars:` entry can be two, so replacing every
// value with a sentence is a multiplier the peer controls the input to: a
// message that passed the server's bound arrives at a reader several times over
// it. What is asserted is the rule this function can check by itself, which
// preserves the server's bound without naming it: the withheld answer is no
// larger than the arrived one or than [redactedEntityStateAllowance], whichever
// is larger.
//
// The fixture is a thousand two-byte vars, which is a legal shape the server's
// bound allows several times over and which this function used to inflate by
// roughly eight.
func TestWithholdingCarriedStateCannotInflateABoundedAnswer(t *testing.T) {
	t.Parallel()

	many := &v1.EntityState{Vars: make(map[string]*v1.Value, 1000)}
	for i := range 1000 {
		many.Vars[fmt.Sprintf("v%d", i)] = v1.NewLiteral("ab")
	}

	arrived := proto.Size(many)
	response := &v1.GetResponse{
		Status:      v1.RunResponse_STATUS_RUNNING,
		EntityState: many,
	}

	state := redactGetResponse(response, nil, false).GetEntityState()

	require.LessOrEqual(t, proto.Size(state), max(arrived, redactedEntityStateAllowance),
		"redaction must never hand a reader more bytes than the server's bounded answer held")
	require.True(t, state.GetTruncated(),
		"the schema's own spelling for 'the keys are not all here' is the fallback, not silence")
	require.Empty(t, state.GetVars(),
		"a truncated answer omits vars entirely rather than reporting a partial map")

	// The ordinary case still gets the marker: the fallback is for the shape a
	// workload can weaponize, not for every running run.
	few := redactGetResponse(runningEntityResponse(), nil, false).GetEntityState()
	require.False(t, few.GetTruncated())
	require.Contains(t, few.GetVars(), "token")
}
