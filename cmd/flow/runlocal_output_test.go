package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A local run exists to tell an author what production will do, so the two drivers
// have to answer with the same document — and this is the file that says what "the
// same" means, because they did not.
//
// The disagreement was not in what either driver executed. It was in the sixteen
// characters of one renderer: `flow run` writes through [marshalJSON], which emits
// unpopulated fields, and `flow run local` wrote through a bare protojson.Marshal,
// which does not. `flow run local` also declared no `--output`, so the two formats
// that exist for programs were reachable only from the driver that needs a server.

// deniedWorkflow fails while running rather than while loading.
//
// The distinction is the whole point of the failure tests below: a file the
// validator refuses never starts, and a run that never started has no outcome to
// report. This one compiles, starts, and is stopped by the egress policy — which
// makes it hermetic as well, since nothing is dialled.
const deniedWorkflow = `edition: v2026.3
name: denied
steps:
  - id: fetch
    http:
      url: http://127.0.0.1:1/nope
`

// TestTheLocalAnswerNamesAFieldNobodyPopulated is the bug, stated as a property.
//
// `hello` produces no named values. The durable driver emitted an empty map for
// them and the local one omitted the key entirely, so a `jq` expression reaching
// for the step's values answered `{}` against production and `null` against the
// rehearsal. Those are the same question, and [marshalJSON]'s comment says why only
// one of them is answerable: a reader who finds a key missing cannot tell an absent
// value from a field this build has never heard of without going to the schema.
//
// The property outlived the spelling. The wrapper the two drivers disagreed about
// is gone from the document — a message whose only field is a map is that map now,
// see rundoc.go — so what is asserted is the same fact one level up: the *step* is
// named, with an empty object under it, rather than missing because it produced
// nothing.
//
// Asserted on the parsed document rather than on the bytes, so it stays a statement
// about what a consumer can index and not about how it was spelled.
func TestTheLocalAnswerNamesAFieldNobodyPopulated(t *testing.T) {
	t.Parallel()

	stdout, _, err := runLocal(t, narratingWorkflow)
	require.NoError(t, err)

	var outputs struct {
		Steps map[string]map[string]json.RawMessage `json:"steps"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &outputs),
		"stdout is not a single JSON document:\n%s", stdout)

	step, ran := outputs.Steps["hello"]
	require.True(t, ran, "the run did not report the step it ran:\n%s", stdout)

	assert.NotNil(t, step,
		"the local driver omitted a step that produced nothing, where the durable "+
			"driver names it with an empty object, so one jq expression cannot read "+
			"both:\n%s", stdout)
}

// TestALocalRunAnswersAMachineCallerWithTheWholeRun covers the format that did not
// exist here at all.
//
// `flow run local -o json` was `unknown shorthand flag: 'o'`. The document is a
// GetResponse because that is what the durable driver's machine formats emit, and
// the point of having one is that `.status` and `.outputs.steps` are the same two
// paths whichever driver ran the workload.
func TestALocalRunAnswersAMachineCallerWithTheWholeRun(t *testing.T) {
	t.Parallel()

	stdout, _, err := runLocal(t, narratingWorkflow, "--output", "json")
	require.NoError(t, err)

	var run map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &run),
		"stdout is not a single JSON document:\n%s", stdout)

	assert.Equal(t, "STATUS_COMPLETED", run["status"],
		"a machine caller cannot read how the run went, in the word every other verb uses")

	outputs, produced := run["outputs"].(map[string]any)
	require.True(t, produced, "the run's answer is not under the field the durable driver puts it under:\n%s", stdout)
	assert.Contains(t, outputs, "steps",
		"`.outputs.steps` does not resolve, so a jq expression written against "+
			"`flow run` does not work against `flow run local`")

	// Present and empty, which is the honest answer rather than an omission. A local
	// run is a process: there is no id to watch it by, and a caller that finds the
	// field missing cannot tell that from a build too old to report one.
	assert.Contains(t, run, "workflowId",
		"the identity fields are absent rather than empty, so a caller cannot tell "+
			"'this run has no durable identity' from 'this field does not exist'")
	assert.Equal(t, "", run["workflowId"],
		"a local run was given a durable identity it does not have")
}

// TestBothDriversAnswerWithTheSameFieldSet is the claim the README makes, checked
// rather than asserted in prose.
//
// The durable side is built here from the schema message directly, not from
// anything `run local` touched, so this compares two independent renderings of what
// a finished run is. A local driver that went back to emitting its own shape — the
// bare outputs, say, or a document invented for the occasion — loses or gains a
// top-level key and fails, which a test that only checked the fields it happened to
// care about would not.
//
// Keys rather than values, because the values are the two drivers' actual
// difference: one has a run id and the other cannot.
func TestBothDriversAnswerWithTheSameFieldSet(t *testing.T) {
	t.Parallel()

	stdout, _, err := runLocal(t, narratingWorkflow, "--output", "json")
	require.NoError(t, err)

	var local map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(stdout), &local))

	// What the durable driver hands its renderer: the server's own answer about a
	// run, with the same oneof arm a completed run carries.
	durable, err := marshalJSON(&v1.GetResponse{
		WorkflowId: "flowstate-workflow-3f7c",
		RunId:      "1d1b0d0e",
		Status:     v1.RunResponse_STATUS_COMPLETED,
		Kind:       &v1.GetResponse_Outputs{Outputs: &v1.Workflow_StepOutputs{}},
	}, true)
	require.NoError(t, err)

	var remote map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(durable, &remote))

	assert.ElementsMatch(t, slices.Collect(maps.Keys(remote)), slices.Collect(maps.Keys(local)),
		"the two drivers answer with different documents, so a caller has to know which "+
			"one ran the workload before it can read the result")
}

// TestALocalRunThatFailsStillAnswersAMachineCaller is the direction that costs a
// program the reason.
//
// `flow run -o json` on a run that fails writes a document carrying STATUS_FAILED
// and the failure, then exits non-zero. `flow run local -o json` wrote nothing at
// all, so a caller that had asked for JSON had to recover the reason by parsing
// prose off stderr — which is the thing asking for JSON is meant to avoid.
func TestALocalRunThatFailsStillAnswersAMachineCaller(t *testing.T) {
	t.Parallel()

	stdout, _, err := runLocal(t, deniedWorkflow, "--output", "json")
	require.Error(t, err, "a run the egress policy stopped was reported as having succeeded")

	var run map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &run),
		"a failed run answered a machine caller with nothing it could parse:\n%s", stdout)

	assert.Equal(t, "STATUS_FAILED", run["status"])

	failure, described := run["error"].(map[string]any)
	require.True(t, described, "the document does not say why the run failed:\n%s", stdout)
	assert.NotEmpty(t, failure["message"], "the failure carries no reason")

	// #241's P2: the classification behind the prose survives the same round
	// trip the message does — `flow run local -o json` through marshalJSON,
	// exactly the path an agent's own tooling reads.
	//
	// Checked as "some recognized kind arrived" rather than pinned to one
	// value: this test runs `flow run local` through the real CLI, sharing the
	// process-wide task registry with every other package test running
	// alongside it under `go test -race ./...` — a test elsewhere that installs
	// its own permissive http task for the duration of its run (see
	// applyEgressPolicy, which has no teardown) can make this exact URL fail on
	// a refused connection (Upstream) instead of a policy denial
	// (PolicyDenied). Both are real classifications a wire consumer can act on;
	// which one this particular race lands on is not what P2 is answering for.
	kind, recognized := v1.ParseErrorKind(fmt.Sprint(failure["kind"]))
	assert.True(t, recognized, "the failure's kind %q did not survive the round trip to -o json as a recognized ErrorKind", failure["kind"])
	assert.True(t, kind == v1.ErrorKindPolicyDenied || kind == v1.ErrorKindUpstream,
		"an http task refused to reach a loopback address should classify as PolicyDenied or, "+
			"under the registry race described above, Upstream — got %s", kind)

	// The negative direction, and the one that would let a reader act on nothing:
	// `kind` is a oneof, so a failed run must not also appear to have produced an
	// answer. A consumer that checked `.outputs` before `.status` would otherwise
	// read an empty result as a successful one.
	assert.NotContains(t, run, "outputs",
		"a run that failed also claimed outputs, so a reader checking `.outputs` "+
			"first sees a successful run that produced nothing")
}

// TestALocalRunThatFailsWritesNoDocumentForAPerson keeps the text shape as it was.
//
// Not an inconsistency with the test above. stdout in the text shape is the
// *answer*, and a failed run has none — an empty stdout is the meaningful value
// there, where `{}` would claim it produced none successfully. A machine format is
// a different contract: it is the run's state, and a failed run has one.
func TestALocalRunThatFailsWritesNoDocumentForAPerson(t *testing.T) {
	t.Parallel()

	stdout, _, err := runLocal(t, deniedWorkflow)
	require.Error(t, err)

	assert.Empty(t, stdout,
		"a run that produced no answer wrote something to the stream a pipe reads")
}

// TestALocalRunThatFailsHonoursRawWithoutDashOJSON is the regression for a
// Codex follow-on on #666: the failure-path guard in runLocal still asked
// format.Machine() alone after the success and task-run failure paths moved
// to runRendering.WantsDocument(), so --raw on a failed local run with the
// default text format wrote nothing, exactly the gap --raw's own help
// promises not to have.
func TestALocalRunThatFailsHonoursRawWithoutDashOJSON(t *testing.T) {
	t.Parallel()

	stdout, _, err := runLocal(t, deniedWorkflow, "--raw")
	require.Error(t, err, "a run the egress policy stopped was reported as having succeeded")

	var run map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &run),
		"--raw without -o json on a failed local run wrote nothing a program could parse:\n%s", stdout)
	assert.Equal(t, "STATUS_FAILED", run["status"])
}

// TestARunSomebodyStoppedIsNotReportedAsAFault is the distinction a machine
// consumer has only one field to make.
//
// ctrl+c cancels the command's context, `v1.Run` returns an error like any other,
// and reporting that as STATUS_FAILED tells a caller the workload broke when what
// happened is that an operator stopped it. The schema has a word for that which is
// not FAILED, and [statusTone] already says the same thing about colour: "a run
// somebody stopped on purpose is not a fault and must not be coloured as one".
func TestARunSomebodyStoppedIsNotReportedAsAFault(t *testing.T) {
	t.Parallel()

	stdout, _, err := runLocalUnder(t, cancelAfter(t, 200*time.Millisecond), nappingWorkflow, "--output", "json")
	require.Error(t, err, "a run that was cut short reported success")

	var run map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &run),
		"an interrupted run answered a machine caller with nothing it could parse:\n%s", stdout)

	assert.Equal(t, "STATUS_CANCELED", run["status"],
		"a run an operator stopped is reported as a workload failure")
}

// TestAFailureIsClassifiedByWhoStoppedTheRun is the direction that makes the check
// non-trivial, and the reason it reads the command's context rather than the error.
//
// Every one of these arrives as a non-nil error from `v1.Run`, and two of them carry
// the *same* sentinel for opposite reasons. A step's own `timeout:` expires an inner
// context, so its failure wraps [context.DeadlineExceeded] exactly as a command that
// ran out of time does — and an implementation that classified by walking the error
// chain, which is the obvious one, calls that run TIMED_OUT. It is not: what ran out
// of time is one step inside it, and a step that fails is a run that failed.
//
// Only the command's own context can separate them, because nothing inside the
// engine can reach it. Asserted here rather than through a workflow because the
// engine refuses `timeout:` on a waiting step, so the collision cannot be staged
// from a Flowfile — and a discrimination that is hard to provoke is exactly the one
// worth pinning where it is decided.
func TestAFailureIsClassifiedByWhoStoppedTheRun(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		runErr      error
		interrupted error
		want        v1.RunResponse_Status
	}{
		{
			name:        "an operator pressed ctrl+c",
			runErr:      fmt.Errorf(`step "nap": %w`, context.Canceled),
			interrupted: context.Canceled,
			want:        v1.RunResponse_STATUS_CANCELED,
		},
		{
			name:        "the command was given a deadline and reached it",
			runErr:      fmt.Errorf(`step "nap": %w`, context.DeadlineExceeded),
			interrupted: context.DeadlineExceeded,
			want:        v1.RunResponse_STATUS_TIMED_OUT,
		},
		{
			name: "a step ran out of its own time",
			// The same sentinel as the case above, and a different fact. The
			// command was never interrupted, so this is the run failing.
			runErr:      fmt.Errorf(`step "nap": %w`, context.DeadlineExceeded),
			interrupted: nil,
			want:        v1.RunResponse_STATUS_FAILED,
		},
		{
			name:   "the workload itself failed",
			runErr: errors.New(`step "fetch": denied by egress policy`),
			want:   v1.RunResponse_STATUS_FAILED,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			at := time.Unix(0, 0)
			run := localRun(nil, test.runErr, test.interrupted, at, at)

			assert.Equal(t, test.want, run.GetStatus())
			assert.NotEmpty(t, run.GetError().GetMessage(),
				"the reason was dropped, so a caller has a status and nothing to act on")
		})
	}
}

// nappingWorkflow runs long enough to be interrupted and does nothing else.
const nappingWorkflow = `edition: v2026.3
name: naps
steps:
  - id: nap
    sleep: 30s
`

// cancelAfter is a context that stands in for somebody pressing ctrl+c.
//
// Cancelled rather than deadlined, because those produce different statuses and
// this test is about the one an operator causes.
func cancelAfter(t *testing.T, after time.Duration) context.Context {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	timer := time.AfterFunc(after, cancel)
	t.Cleanup(func() {
		timer.Stop()
		cancel()
	})

	return ctx
}

// TestTheLineShapeIsOneLine is what `jsonl` promises, on a driver that produces one
// record.
//
// A local run has nothing to stream — it is one process producing one result — so
// the line shape here is the single-document shape written compactly. That is still
// worth honouring rather than refusing, because a caller looping over
// `flow run local ... | while read -r line` should not have to know which driver it
// asked for.
func TestTheLineShapeIsOneLine(t *testing.T) {
	t.Parallel()

	stdout, _, err := runLocal(t, narratingWorkflow, "--output", "jsonl")
	require.NoError(t, err)

	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	require.Len(t, lines, 1, "the line shape wrote more than one record for one run:\n%s", stdout)

	var run map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &run))
	assert.Equal(t, "STATUS_COMPLETED", run["status"])
}

// TestALocalRunRefusesAFormatItCannotHonour proves the flag is the shared one.
//
// The refusal, its wording, and the list of accepted values all come from
// [resolveOutputFormat] — so this failing is how anybody would find out that
// `run local` had been given a second, private notion of what `--output` means.
func TestALocalRunRefusesAFormatItCannotHonour(t *testing.T) {
	t.Parallel()

	stdout, _, err := runLocal(t, narratingWorkflow, "--output", "yaml")
	require.Error(t, err, "a format this CLI does not have was accepted")

	assert.Contains(t, err.Error(), "json",
		"the refusal does not say what is accepted, which is the question the caller is about to ask")
	assert.Empty(t, stdout, "a refused invocation still wrote to the stream a pipe reads")
}

// TestAMachineFormatIsNotAlsoNarratedInProse is the rule `flow run` already follows.
//
// A machine format carries the status inside the document it writes, so a pill
// saying it again on stderr is a second spelling of one fact — and two spellings is
// how they come to disagree. What stays on stderr either way is the workload's own
// narration, which is the run's account of itself and belongs to the run rather
// than to this command.
func TestAMachineFormatIsNotAlsoNarratedInProse(t *testing.T) {
	t.Parallel()

	_, machine, err := runLocal(t, narratingWorkflow, "--output", "json")
	require.NoError(t, err)

	assert.NotContains(t, machine, "COMPLETED",
		"the status was narrated on stderr as well as carried in the document")
	assert.Contains(t, machine, "hello from the workload",
		"asking for a machine format silenced the workload's own narration")

	// The other direction, so this cannot pass by the pill having been deleted.
	_, person, err := runLocal(t, narratingWorkflow)
	require.NoError(t, err)
	assert.Contains(t, person, "COMPLETED",
		"the text shape stopped saying how the run went")
}
