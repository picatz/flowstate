package main

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The lifecycle and schedule-mutation verbs used to be mute to anything that was not
// a person: prose on one stream, an exit code, and nothing addressable. An agent that
// cancelled a run had to `flow get` it afterwards to learn what it had just done.
//
// These cover the two halves of fixing that, and the second half is the one worth
// having. Parsing the document proves the fields are there; asserting that stdout
// carries the document and *nothing else* proves `| jq` works, which is the actual
// use. A single stray sentence on the wrong stream defeats the feature while every
// field-level assertion still passes.

// mutationKeys is the whole shape, in the order a reader of the help text meets it.
//
// Asserted as a set rather than field by field, so a field added here without a line
// in the help is a failure. The document is a contract with scripts, and a contract
// that grows silently is one nobody can rely on being complete.
var mutationKeys = []string{"result", "runId", "scheduleName", "signalName", "verb", "workflowId"}

// mutationDocument is the shape [mutationResult] promises, spelled out again here
// rather than reused from the source.
//
// Deliberately a second copy: a test that unmarshals into the very struct that
// marshalled it agrees with any renaming, including one that breaks every caller.
// This one is written the way a consumer would write it, from the help text.
type mutationDocument struct {
	Verb         string `json:"verb"`
	WorkflowID   string `json:"workflowId"`
	RunID        string `json:"runId"`
	ScheduleName string `json:"scheduleName"`
	SignalName   string `json:"signalName"`
	Result       string `json:"result"`
}

// runCLI drives the real command tree and returns the two streams apart.
//
// Through [newRootCommand] and argv rather than by calling the RunE function,
// because the flag is half of what is under test: an `--output` that parses and is
// then ignored passes every test written one level down. It is also the only way to
// see what a caller sees, which is two streams and an exit status.
func runCLI(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()

	root := newRootCommand()

	var out, errOut strings.Builder
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(args)

	err = root.Execute()

	return out.String(), errOut.String(), err
}

// requireOnlyDocument parses stdout and insists it is one JSON document and nothing
// besides.
//
// json.Unmarshal is the check for "nothing besides": it refuses trailing bytes, so a
// sentence printed before or after the document fails here rather than in whatever
// pipeline finds it in production.
func requireOnlyDocument(t *testing.T, stdout string) mutationDocument {
	t.Helper()

	var keys map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &keys),
		"stdout is not exactly one JSON document, so `| jq` would fail on it:\n%q", stdout)

	got := make([]string, 0, len(keys))
	for key := range keys {
		got = append(got, key)
	}
	slices.Sort(got)

	require.Equal(t, mutationKeys, got,
		"the result document's fields are not the ones the help text documents")

	var document mutationDocument
	require.NoError(t, json.Unmarshal([]byte(stdout), &document))

	return document
}

func TestCancelJSONIsTheOnlyThingOnStdout(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)

	stdout, stderr, err := runCLI(t, "cancel", "deploy-abc123", "--run-id", "run-7", "-o", "json")
	require.NoError(t, err)

	document := requireOnlyDocument(t, stdout)
	assert.Equal(t, "cancel", document.Verb)
	assert.Equal(t, "deploy-abc123", document.WorkflowID)
	assert.Equal(t, "run-7", document.RunID, "--run-id was not reported, so a caller cannot tell which attempt was asked")
	assert.Empty(t, document.ScheduleName)
	assert.Empty(t, document.SignalName)

	// The distinction this verb turns on. Cancellation is cooperative, so a
	// document saying "applied" would hand a script the very claim the prose has
	// always refused to make.
	assert.Equal(t, "requested", document.Result)

	require.Empty(t, stderr,
		"prose accompanied the machine format; a caller reading the document was told the same "+
			"facts again in a sentence it would have to parse")

	require.NotNil(t, fake.gotCancel, "nothing reached the server")
}

func TestTerminateJSONIsTheOnlyThingOnStdout(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)

	stdout, stderr, err := runCLI(t, "terminate", "deploy-abc123", "--reason", "wedged", "-o", "json")
	require.NoError(t, err)

	document := requireOnlyDocument(t, stdout)
	assert.Equal(t, "terminate", document.Verb)
	assert.Equal(t, "deploy-abc123", document.WorkflowID)

	// Applied rather than requested, and the pair with cancel above is the point:
	// two verbs that read alike from a terminal are different acts, and the
	// document is where a script learns which one it performed.
	assert.Equal(t, "applied", document.Result)

	require.Empty(t, stderr)
	require.NotNil(t, fake.gotTerminate, "nothing reached the server")
	require.Equal(t, "wedged", fake.gotTerminate.GetReason(), "--reason was dropped")
}

func TestSignalJSONIsTheOnlyThingOnStdout(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)

	stdout, stderr, err := runCLI(t,
		"signal", "deploy-abc123", "deploy-approved", "--data", `{"approved": true}`, "-o", "json")
	require.NoError(t, err)

	document := requireOnlyDocument(t, stdout)
	assert.Equal(t, "signal", document.Verb)
	assert.Equal(t, "deploy-abc123", document.WorkflowID)

	// Which signal was delivered is part of what happened rather than only of what
	// was asked: two signals to one run are two different acts, and a document that
	// named neither would leave a script unable to tell them apart in a log.
	assert.Equal(t, "deploy-approved", document.SignalName)
	assert.Equal(t, "applied", document.Result)

	// The verb whose prose has always gone to stdout, which is why this assertion
	// earns its place here more than anywhere else in the file: the human line and
	// the document would otherwise share a stream.
	require.Empty(t, stderr)
	require.NotContains(t, stdout, "delivered",
		"the human line reached stdout alongside the document")

	require.NotNil(t, fake.got, "nothing reached the server")
	require.True(t, fake.got.GetPayload().GetNamedValues()["approved"].GetLiteral().GetBoolValue())
}

// TestScheduleMutationsAnswerWithOneShape is the reason there is one envelope rather
// than four: a caller writes one expression and reads every verb with it.
func TestScheduleMutationsAnswerWithOneShape(t *testing.T) {
	for _, test := range []struct {
		verb   string
		args   []string
		result string
	}{
		{verb: "schedule delete", args: []string{"schedule", "delete", "nightly-report"}, result: "applied"},
		{verb: "schedule pause", args: []string{"schedule", "pause", "nightly-report"}, result: "applied"},
		{verb: "schedule resume", args: []string{"schedule", "resume", "nightly-report"}, result: "applied"},

		// Requested, not applied: the cluster starts the run after answering, which
		// is also why workflowId stays empty below.
		{verb: "schedule trigger", args: []string{"schedule", "trigger", "nightly-report"}, result: "requested"},
	} {
		t.Run(test.verb, func(t *testing.T) {
			serveFake(t, &fakeWorkflowService{})

			stdout, stderr, err := runCLI(t, append(slices.Clone(test.args), "-o", "json")...)
			require.NoError(t, err)

			document := requireOnlyDocument(t, stdout)
			assert.Equal(t, test.verb, document.Verb,
				"the document does not say which verb produced it")
			assert.Equal(t, "nightly-report", document.ScheduleName)
			assert.Equal(t, test.result, document.Result)

			// A schedule verb acts on no run, and the run fields are present and
			// empty rather than absent: a consumer indexing .workflowId should find
			// "" instead of null, because the two are the same question and only one
			// is answerable without knowing the shape in advance.
			assert.Empty(t, document.WorkflowID)
			assert.Empty(t, document.RunID)
			assert.Empty(t, document.SignalName)

			require.Empty(t, stderr)
		})
	}
}

// TestScheduleTriggerReachesTheServer keeps the shape test above honest.
//
// Every case there would pass against a CLI that printed a plausible document and
// made no request at all, which is the failure a mutation verb can least afford.
func TestScheduleTriggerReachesTheServer(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)

	_, _, err := runCLI(t, "schedule", "trigger", "nightly-report", "-o", "json")
	require.NoError(t, err)

	require.NotNil(t, fake.gotScheduleTrigger, "nothing reached the server")
	require.Equal(t, "nightly-report", fake.gotScheduleTrigger.GetName())
}

// TestMutationJSONLIsOneCompactLine pins the difference between the two machine
// forms, which everywhere else in this CLI is one document against one per line.
//
// A mutation is a single act, so jsonl has exactly one record. It is still accepted,
// because a script that formats every flow invocation the same way should not have to
// special-case these.
func TestMutationJSONLIsOneCompactLine(t *testing.T) {
	serveFake(t, &fakeWorkflowService{})

	stdout, stderr, err := runCLI(t, "schedule", "pause", "nightly-report", "-o", "jsonl")
	require.NoError(t, err)
	require.Empty(t, stderr)

	lines := strings.Split(strings.TrimSuffix(stdout, "\n"), "\n")
	require.Len(t, lines, 1, "jsonl wrote more than the one record a single act has")
	require.NotContains(t, lines[0], "\n")
	require.Equal(t, `{"verb":"schedule pause","workflowId":"","runId":"","scheduleName":"nightly-report","signalName":"","result":"applied"}`,
		lines[0], "the compact form is not compact, so a line-oriented reader gets a partial record")
}

// TestMutationTextOutputIsUnchanged is the other half of "additive".
//
// The verbs already had readers: a person watching a terminal, and every script
// already grepping what they print. `-o json` is worth nothing if adding it moved a
// byte of that, so the lines are pinned exactly rather than by substring, on the
// stream each has always used.
func TestMutationTextOutputIsUnchanged(t *testing.T) {
	for _, test := range []struct {
		name   string
		args   []string
		stdout string
		stderr string
	}{
		{
			name: "cancel",
			args: []string{"cancel", "deploy-abc123"},
			stderr: "asked deploy-abc123 to stop; it runs its cleanup before finishing, " +
				"so ask `flow get deploy-abc123` whether it has\n",
		},
		{
			name:   "terminate",
			args:   []string{"terminate", "deploy-abc123"},
			stderr: "terminated deploy-abc123; no cleanup ran\n",
		},
		{
			// On stdout, which is where this one has always gone. It is out of step
			// with the other six and moving it would change what an existing
			// `flow signal > file` collects, so it is pinned where it is and left
			// for a change that says so.
			name:   "signal",
			args:   []string{"signal", "deploy-abc123", "deploy-approved"},
			stdout: "delivered deploy-approved to deploy-abc123\n",
		},
		{
			name: "schedule delete",
			args: []string{"schedule", "delete", "nightly-report"},
			stderr: "deleted schedule nightly-report; runs it already started keep going, " +
				"and `flow cancel` is what stops one\n",
		},
		{
			name:   "schedule pause",
			args:   []string{"schedule", "pause", "nightly-report"},
			stderr: "paused schedule nightly-report; `flow schedule resume nightly-report` starts it firing again\n",
		},
		{
			name:   "schedule resume",
			args:   []string{"schedule", "resume", "nightly-report"},
			stderr: "resumed schedule nightly-report\n",
		},
		{
			name: "schedule trigger",
			args: []string{"schedule", "trigger", "nightly-report"},
			stderr: "asked schedule nightly-report to fire; `flow schedule describe nightly-report` " +
				"lists it under recent runs once it has\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			serveFake(t, &fakeWorkflowService{})

			stdout, stderr, err := runCLI(t, test.args...)
			require.NoError(t, err)

			require.Equal(t, test.stdout, stdout, "what a person reads on stdout changed")
			require.Equal(t, test.stderr, stderr, "what a person reads on stderr changed")
		})
	}
}

// TestMutationRefusesAnUnknownFormatBeforeActing is why the format is resolved at the
// top of each verb rather than at the point of rendering.
//
// A `--output yaml` noticed only when there was something to write would cancel the
// run and *then* report a usage error, which reads to a caller like the cancellation
// never happened. Nothing may reach the server.
func TestMutationRefusesAnUnknownFormatBeforeActing(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)

	stdout, _, err := runCLI(t, "cancel", "deploy-abc123", "-o", "yaml")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a format this understands")

	require.Nil(t, fake.gotCancel,
		"the run was cancelled and the invocation then reported an error, so a caller cannot "+
			"tell whether it happened")

	// Cobra writes the usage text for a refused invocation, which this does not
	// touch. What must not be there is a result document: a refused format that
	// still reported a result would be the worst of both answers.
	require.NotContains(t, stdout, `"result"`)
}
