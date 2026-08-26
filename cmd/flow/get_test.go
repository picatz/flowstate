package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Get implements [flowstatev1connect.WorkflowServiceHandler].
//
// Defined on the same fake the signal tests use, so one stand-in server covers
// every verb the CLI has.
func (f *fakeWorkflowService) Get(_ context.Context, req *connect.Request[v1.GetRequest]) (*connect.Response[v1.GetResponse], error) {
	f.gotGet = req.Msg
	if f.onGet != nil {
		f.onGet()
	}
	if f.getErr != nil {
		return nil, f.getErr
	}
	return connect.NewResponse(f.getResponse), nil
}

// getCommand builds the command runGet expects, with the flags it declares.
//
// Declared here rather than reset, because they are no longer package variables: a
// flag lives in the FlagSet of the command that declared it, so a fresh command is a
// fresh set of flags and there is nothing to leak into the next test.
func getCommand(t *testing.T) (*cobra.Command, *strings.Builder, *strings.Builder) {
	t.Helper()

	var out, errOut strings.Builder
	cmd := &cobra.Command{}
	cmd.Flags().String("run-id", "", "")
	addOutputFlag(cmd)
	addRawOutputFlag(cmd)
	addServerFlags(cmd)
	addRevealSensitiveFlag(cmd)
	cmd.SetContext(t.Context())
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)

	return cmd, &out, &errOut
}

// TestGetSeparatesOutputsFromStatus is the property that makes `flow get x | jq`
// work: a workload's data goes to stdout and nothing else does.
func TestGetSeparatesOutputsFromStatus(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_COMPLETED,
			Kind: &v1.GetResponse_Outputs{
				Outputs: &v1.Workflow_StepOutputs{
					StepValues: map[string]*v1.Node_Outputs{
						"greet": {NamedValues: map[string]*v1.Value{"result": v1.NewLiteral("hello")}},
					},
				},
			},
		},
	}
	serveFake(t, fake)
	cmd, out, errOut := getCommand(t)

	require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

	require.Equal(t, "flowstate-workflow-3f7c", fake.gotGet.GetWorkflowId())

	// Everything on stdout has to parse as the outputs, or a pipe into jq breaks.
	require.Contains(t, out.String(), `"greet"`)
	require.NotContains(t, out.String(), "COMPLETED",
		"the status was written to stdout, which corrupts anything piping the outputs")

	require.Contains(t, errOut.String(), "COMPLETED")
	require.Contains(t, errOut.String(), "flowstate-workflow-3f7c")
}

// TestGetReportsWhatTheRunAnswered is the other half of a finished run: the
// transcript says what each step produced, and `run_outputs` says what the workflow
// promised to report.
//
// Both directions matter here. The values reach a person on stderr, named, without
// their having to read the document — and they do not reach stdout, where a second
// copy would break the one property this command is built around.
func TestGetReportsWhatTheRunAnswered(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_COMPLETED,
			Kind: &v1.GetResponse_Outputs{
				Outputs: &v1.Workflow_StepOutputs{
					StepValues: map[string]*v1.Node_Outputs{
						"deploy": {NamedValues: map[string]*v1.Value{"status_code": v1.NewLiteral(200)}},
					},
				},
			},
			RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
				"url":          v1.NewLiteral("https://example.com/build/12"),
				"hosts_placed": v1.NewLiteral(3),
			}},
		},
	}
	serveFake(t, fake)
	cmd, out, errOut := getCommand(t)

	// `flow get` asks about a run by id alone and never holds the workflow
	// specification that declared these outputs, so it fails closed by
	// default (see TestGetRedactsRunOutputsByDefault) — --reveal-sensitive is
	// what this test's own subject (that a value is written as itself) needs.
	require.NoError(t, cmd.Flags().Set(revealSensitiveFlagName, "true"))

	require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

	require.Contains(t, errOut.String(), "outputs", "the run's declared outputs were not named")
	require.Contains(t, errOut.String(), "url")
	require.Contains(t, errOut.String(), "https://example.com/build/12",
		"a string output was not written as itself, so it cannot be copied off the terminal")
	require.Contains(t, errOut.String(), "hosts_placed")

	require.NotContains(t, out.String(), "https://example.com/build/12",
		"the human section was written to stdout, where the answer document lives")
	require.Contains(t, out.String(), `"deploy"`,
		"the transcript stopped being written when the answer started being")
}

// TestGetRedactsRunOutputsByDefault is the fail-closed case CLAUDE.md's "fail
// closed" section requires: `flow get` never holds the workflow specification
// that declared these outputs, so it cannot determine which of them, if any, are
// `sensitive: true` — and the safe answer to "cannot determine" is to redact, not
// to reveal. This holds even for a value nothing ever declared sensitive, which is
// the whole point: without a specification in hand there is no "non-sensitive" to
// tell it apart from.
//
// The actual secret string must be absent from the rendered bytes, on both
// streams — not merely a marker present, which is a different and weaker
// assertion that a value printed twice could still satisfy.
func TestGetRedactsRunOutputsByDefault(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_COMPLETED,
			Kind: &v1.GetResponse_Outputs{
				Outputs: &v1.Workflow_StepOutputs{
					RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
						"url": v1.NewLiteral("https://example.com/build/12"),
					}},
				},
			},
			RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
				"url": v1.NewLiteral("https://example.com/build/12"),
			}},
		},
	}
	serveFake(t, fake)
	cmd, out, errOut := getCommand(t)

	require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

	require.NotContains(t, errOut.String(), "https://example.com/build/12",
		"the real value must not appear on stderr when no specification could vouch for it")
	require.NotContains(t, out.String(), "https://example.com/build/12",
		"the real value must not appear on stdout either — machine output is not exempt")
	require.Contains(t, errOut.String(), "[redacted: url]",
		"the honest marker the schema promises should say which output was withheld")
}

// TestGetRedactsStepTranscriptForStepComputedSensitiveOutput is the Codex finding
// on PR #212, exercised through the actual CLI verb rather than the helper
// directly: `flow get` never holds a workflow specification, so a value fed to a
// declared output through a step — `outputs.token.value: ${steps.fetch.token}`
// with `sensitive: true` — used to render in the clear in the step transcript
// even though the same value was withheld at the name it surfaced under in
// `run_outputs`. Both streams and the machine format are checked, because a
// person reading stderr, a person reading stdout, and a program reading JSON are
// three different readers this bug reached.
func TestGetRedactsStepTranscriptForStepComputedSensitiveOutput(t *testing.T) {
	newFake := func() *fakeWorkflowService {
		return &fakeWorkflowService{
			getResponse: &v1.GetResponse{
				WorkflowId: "flowstate-workflow-3f7c",
				RunId:      "0198f1e2-0000-7000-8000-000000000000",
				Status:     v1.RunResponse_STATUS_COMPLETED,
				Kind: &v1.GetResponse_Outputs{
					Outputs: &v1.Workflow_StepOutputs{
						RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
							"token": v1.NewLiteral("sk-live-0123456789abcdef"),
						}},
						StepValues: map[string]*v1.Node_Outputs{
							"fetch": {NamedValues: map[string]*v1.Value{
								"token": v1.NewLiteral("sk-live-0123456789abcdef"),
							}},
						},
					},
				},
				RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
					"token": v1.NewLiteral("sk-live-0123456789abcdef"),
				}},
			},
		}
	}

	t.Run("text", func(t *testing.T) {
		fake := newFake()
		serveFake(t, fake)
		cmd, out, errOut := getCommand(t)

		require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

		require.NotContains(t, errOut.String(), "sk-live-0123456789abcdef",
			"the real value must not appear on stderr's outputs summary")
		require.NotContains(t, out.String(), "sk-live-0123456789abcdef",
			"the real value must not appear in the step transcript on stdout — this is the Codex gap")
		require.Contains(t, out.String(), `"fetch"`,
			"the step still ran and the transcript should say so, only with the value withheld")
	})

	t.Run("json", func(t *testing.T) {
		fake := newFake()
		serveFake(t, fake)
		cmd, out, _ := getCommand(t)
		require.NoError(t, cmd.Flags().Set("output", "json"))

		require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

		require.NotContains(t, out.String(), "sk-live-0123456789abcdef",
			"a machine reader must not recover the value from the step transcript either")
	})
}

// TestGetRevealSensitiveShowsValues checks the one deliberate escape hatch:
// --reveal-sensitive shows the real value, and a stderr note records that it was
// used.
func TestGetRevealSensitiveShowsValues(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_COMPLETED,
			Kind: &v1.GetResponse_Outputs{
				Outputs: &v1.Workflow_StepOutputs{
					RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
						"url": v1.NewLiteral("https://example.com/build/12"),
					}},
				},
			},
			RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
				"url": v1.NewLiteral("https://example.com/build/12"),
			}},
		},
	}
	serveFake(t, fake)
	cmd, _, errOut := getCommand(t)
	require.NoError(t, cmd.Flags().Set(revealSensitiveFlagName, "true"))

	require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

	require.Contains(t, errOut.String(), "https://example.com/build/12",
		"--reveal-sensitive must show the real value")
	require.Contains(t, errOut.String(), "--reveal-sensitive",
		"revealing a declared-sensitive value should leave a note on stderr recording it was asked for")
}

// TestGetJSONOutputRedactsToo is the requirement that machine-readable output is
// redacted by default exactly as the human form is: JSON is what gets piped into
// logs and CI artifacts, which is precisely where a value should not land because
// somebody assumed only the human form needed protecting.
func TestGetJSONOutputRedactsToo(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_COMPLETED,
			Kind: &v1.GetResponse_Outputs{
				Outputs: &v1.Workflow_StepOutputs{
					RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
						"url": v1.NewLiteral("https://example.com/build/12"),
					}},
				},
			},
			RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
				"url": v1.NewLiteral("https://example.com/build/12"),
			}},
		},
	}
	serveFake(t, fake)
	cmd, out, _ := getCommand(t)
	require.NoError(t, cmd.Flags().Set("output", "json"))

	require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

	require.NotContains(t, out.String(), "https://example.com/build/12",
		"a machine reader must not recover the value a human reader is denied")
	require.Contains(t, out.String(), "[redacted: url]")
}

// TestGetRawWritesTheDocumentWithoutDashOJSON is the regression for a Codex
// follow-on on #666: `flow get x --raw` with the default text format used to
// ask format.Machine() alone, which is false without `-o json`, so the whole
// GetResponse document --raw promises never got written — the last of the
// run-answering verbs whose --raw only worked when paired with -o json.
func TestGetRawWritesTheDocumentWithoutDashOJSON(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_COMPLETED,
		},
	}
	serveFake(t, fake)
	cmd, out, _ := getCommand(t)
	require.NoError(t, cmd.Flags().Set("raw", "true"))

	require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

	var document map[string]any
	require.NoError(t, json.Unmarshal([]byte(out.String()), &document),
		"--raw without -o json wrote nothing a program could parse:\n%s", out.String())
	require.Equal(t, "STATUS_COMPLETED", document["status"])
}

// TestGetNonSensitiveInputUnaffected is the non-regression direction on a surface
// that *does* have a specification to consult: with none of the workflow's
// outputs declared sensitive, redaction must not appear at all. `flow get` itself
// has no specification (see TestGetRedactsRunOutputsByDefault for that surface's
// own fail-closed default); this exercises the shared helper the way a caller
// that does hold one uses it.
func TestGetNonSensitiveInputUnaffected(t *testing.T) {
	workflow := &v1.Workflow{
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "url"},
		},
	}
	response := &v1.GetResponse{
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
			"url": v1.NewLiteral("https://example.com/build/12"),
		}},
	}

	redacted := redactGetResponse(response, workflow, false)

	require.Equal(t, "https://example.com/build/12",
		redacted.GetRunOutputs().GetValues()["url"].GetLiteral().GetStringValue(),
		"a value nothing declared sensitive must render unchanged")
}

// TestGetOnARunningRunProducesNoOutputs checks the honest answer to "what did it
// produce" while it is still producing it.
func TestGetOnARunningRunProducesNoOutputs(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_RUNNING,
		},
	}
	serveFake(t, fake)
	cmd, out, errOut := getCommand(t)

	require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))

	require.Empty(t, out.String(), "a run still going wrote outputs it does not have")
	require.Contains(t, errOut.String(), "RUNNING")
}

// TestGetReportsAFailedRunAsAFailure checks that `flow get id && ...` behaves the
// way a shell reader expects.
func TestGetReportsAFailedRunAsAFailure(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_FAILED,
			Kind: &v1.GetResponse_Error{
				Error: &v1.RunResponse_Error{Message: "step \"deploy\" failed"},
			},
		},
	}
	serveFake(t, fake)
	cmd, _, _ := getCommand(t)

	err := runGet(cmd, []string{"flowstate-workflow-3f7c"})
	require.Error(t, err, "a failed run was reported as a success")
	require.ErrorContains(t, err, "failed")
}

// TestGetRefusesARunIDThatIsNotAUUIDBeforeSending checks the schema's rule runs
// before the round trip.
//
// GetRequest constrains run_id to a UUID, unlike SignalRequest, so this is the
// one place a run id can be wrong in a way worth catching early.
func TestGetRefusesARunIDThatIsNotAUUIDBeforeSending(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)
	cmd, _, _ := getCommand(t)

	require.NoError(t, cmd.Flags().Set("run-id", "the-latest-one"))

	err := runGet(cmd, []string{"flowstate-workflow-3f7c"})
	require.Error(t, err, "a run id the schema forbids was accepted")
	require.Nil(t, fake.gotGet, "an invalid run id was sent anyway")
}

// TestGetWithoutARunIDLeavesItAbsent checks that unset means "whichever attempt is
// current" rather than an empty string the schema would refuse for not being a
// UUID.
func TestGetWithoutARunIDLeavesItAbsent(t *testing.T) {
	fake := &fakeWorkflowService{
		getResponse: &v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			Status:     v1.RunResponse_STATUS_RUNNING,
		},
	}
	serveFake(t, fake)
	cmd, _, _ := getCommand(t)

	require.NoError(t, runGet(cmd, []string{"flowstate-workflow-3f7c"}))
	require.Nil(t, fake.gotGet.RunId, "an empty run id was sent instead of none at all")
}

// TestGetOnAnUnaddressableRunNamesEveryCause checks that reading gets the same
// three-cause explanation signalling does, named for what was attempted.
func TestGetOnAnUnaddressableRunNamesEveryCause(t *testing.T) {
	fake := &fakeWorkflowService{
		getErr: connect.NewError(connect.CodeNotFound, errors.New(`no such run`)),
	}
	serveFake(t, fake)
	cmd, _, _ := getCommand(t)

	err := runGet(cmd, []string{"flowstate-workflow-3f7c"})
	require.ErrorContains(t, err, "check the id")
	require.ErrorContains(t, err, "tenant")
	require.ErrorContains(t, err, "retention")
}

// TestAPendingActivityLineSaysWhatItIsDoing is the render half of heartbeat phases.
//
// The phase travels a long way to be seen — a task reports it, a ticker heartbeats
// it, Temporal stores it, the server projects it out of a Describe response — and
// every one of those steps can be right while the last one is missing, in which
// case the feature is present, tested, and invisible. That is the failure the house
// rule about reachability describes, and this is the assertion that closes it for
// the two surfaces a person actually reads.
//
// Both `flow get` and `flow watch` render through this function, which is why there
// is one test rather than two: the two surfaces cannot drift because there is only
// one renderer.
func TestAPendingActivityLineSaysWhatItIsDoing(t *testing.T) {
	t.Parallel()

	now := time.Now()

	lines := pendingActivityLines([]*v1.PendingActivity{
		{Attempt: 1, Phase: "reading the response"},
	}, now)

	require.Equal(t, []string{"retrying, attempt 1, reading the response"}, lines)

	// A phase is an aside about the attempt running now; a failure and a countdown
	// describe attempts that are over. So the phase goes last, and the order is
	// asserted rather than the presence of each part — a line that reported what a
	// finished attempt is doing would read as a contradiction.
	lines = pendingActivityLines([]*v1.PendingActivity{{
		Attempt:                  3,
		LastFailure:              "connection refused",
		NextAttemptScheduledTime: timestamppb.New(now.Add(4 * time.Second)),
		Phase:                    "requesting",
	}}, now)

	require.Equal(t,
		[]string{"retrying, attempt 3: connection refused (next attempt in 4s), requesting"},
		lines)
}

// TestAPendingActivityWithNoPhaseSaysNothingAboutIt is the negative direction, and
// the one that matters most for honesty.
//
// A phase is absent in three situations that are not the same: the attempt has not
// reported yet, it is waiting to be retried and so nothing is running to have a
// phase, or the worker predates the field. None of them is "the step is doing
// nothing", so a renderer that printed a word for the empty phase would be
// inventing a fact about a workload — which is the failure this repository treats
// as worse than saying nothing at all.
func TestAPendingActivityWithNoPhaseSaysNothingAboutIt(t *testing.T) {
	t.Parallel()

	lines := pendingActivityLines([]*v1.PendingActivity{
		{Attempt: 2, LastFailure: "boom"},
	}, time.Now())

	require.Equal(t, []string{"retrying, attempt 2: boom"}, lines)
}

// TestAPendingActivityLineCannotBeSplitByItsOwnFailure covers a hole this
// function had before `flow timeline` was written, and which reviewing that
// command's rendering is what surfaced.
//
// A pending activity's `last_failure` is the workload's own sentence, and it
// was concatenated into a printed line bare. A newline in it makes what looks
// like a second retrying step, and an ANSI escape restyles the terminal from
// inside a `flow get` answer. Both surfaces that render this — `flow get` and
// `flow watch` — got it, because there is one renderer.
func TestAPendingActivityLineCannotBeSplitByItsOwnFailure(t *testing.T) {
	t.Parallel()

	lines := pendingActivityLines([]*v1.PendingActivity{{
		Attempt:     2,
		LastFailure: "boom\nretrying, attempt 9: totally fine\x1b[31m",
	}}, time.Now())

	require.Len(t, lines, 1, "one pending activity produced more than one line")

	assert.NotContains(t, lines[0], "\n",
		"a failure message with a newline invented a line that reads as another retrying step")
	assert.NotContains(t, lines[0], "\x1b",
		"a workload's failure text chose how the reader's terminal looks")

	// Escaping is not redaction: the diagnosis still has to be readable.
	assert.Contains(t, lines[0], "boom")
	assert.Contains(t, lines[0], "retrying, attempt 2")
}
