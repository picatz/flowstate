package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"

	v1types "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// [FlowstateServer.reportedStarter] read directly, beside the end-to-end tests that go through a
// real server.
//
// The two answer different questions and neither covers the other. The
// integration tests prove that a run this build submits records a starter and
// that the handler answers with it; these prove the *branches* - what comes back
// for a memo an older writer produced, which no live server in this repo can be
// made to write any more, and what comes back for one that cannot be decoded at
// all.

// memoWithStarterValue builds a Describe response whose memo carries whatever
// payload is given under [starterMemoKey], which is how a test reaches shapes a
// current writer would never produce.
func memoWithStarterValue(t *testing.T, starter string) *workflowservice.DescribeWorkflowExecutionResponse {
	t.Helper()

	payload, err := converter.GetDefaultDataConverter().ToPayload(starter)
	require.NoError(t, err)

	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
			Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{starterMemoKey: payload}},
		},
	}
}

// TestReportedStarterReadsWhatTheCurrentWriterRecords is the positive direction,
// and the one a mutation has to be able to fail: dropping the population in the
// Get handler leaves the two integration tests failing and this one green, so
// this pins the derivation itself rather than the handler that calls it.
func TestReportedStarterReadsWhatTheCurrentWriterRecords(t *testing.T) {
	t.Parallel()

	starter := v1types.QualifiedSubject("https://issuer.example.com", "requester@example.com")

	assert.Equal(t, starter, mustNew(t, nil).reportedStarter(memoWithStarterValue(t, starter)),
		"a run whose memo records a starter reported something else, so a surface comparing this "+
			"against a signal policy rule would compare the wrong string")
}

// TestReportedStarterOnARunPredatingTheKeyReportsNothing is the old-writer,
// new-reader direction: a memo written before [starterMemoKey] existed has no
// such field, and this build must answer "nobody recorded one" rather than
// inventing a compatibility arm.
//
// It is the same absence [authorizeSignal] turns into a denial when a policy
// demands the comparison, and the reason there is no placeholder here: a reader
// handed one would have a string that compares equal to nothing real, which is
// worse than an empty one they can see is empty.
func TestReportedStarterOnARunPredatingTheKeyReportsNothing(t *testing.T) {
	t.Parallel()

	old := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
			// A memo with other fields in it, and no starter: the shape a writer
			// that predates the key produces, rather than a run with no memo at
			// all, which would also be answered by the nil path and would prove
			// less.
			Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{}},
		},
	}

	assert.Empty(t, mustNew(t, nil).reportedStarter(old),
		"a run started before the starter memo key existed reported a starter, so this build "+
			"invented one for a run that never recorded it")
	assert.False(t, v1types.LooksLikeQualifiedSubject(mustNew(t, nil).reportedStarter(old)),
		"the answer for a run with no recorded starter has the shape of a subject a policy rule "+
			"would accept")
}

// TestReportedStarterOnAnUnreadableMemoReportsNothing is the fail-closed
// direction for a *read*.
//
// A memo entry this build cannot decode says nothing about who started the run,
// and the only honest answers are the two that mean the same thing to a reader:
// absent. Nothing is authorized on the strength of this field - the handler that
// does authorize a delivery reads the memo itself and denies rather than
// proceeds - so answering absent here weakens nothing and inventing a value
// would.
func TestReportedStarterOnAnUnreadableMemoReportsNothing(t *testing.T) {
	t.Parallel()

	corrupt := &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{
			Memo: &commonpb.Memo{Fields: map[string]*commonpb.Payload{
				starterMemoKey: {Data: []byte("not a payload this converter can read")},
			}},
		},
	}

	assert.Empty(t, mustNew(t, nil).reportedStarter(corrupt),
		"a memo whose starter entry could not be decoded produced a starter anyway")
}

// TestReportedStarterOnAnUnauthenticatedSubmissionReportsNothing is the third
// absence, and the one a reader is most likely to actually meet: the memo is
// written unconditionally, so a run submitted with nobody authenticated records
// the qualified form of two empty strings - the bare separator, which names
// nobody.
//
// Reported as empty rather than passed through, because a reader handed "#"
// would have a string that is not a subject and compares equal to no rule. The
// authorization path keeps the distinction this drops, deliberately: see
// [FlowstateServer.reportedStarter].
func TestReportedStarterOnAnUnauthenticatedSubmissionReportsNothing(t *testing.T) {
	t.Parallel()

	recorded := starterMemoEntry(&v1types.WorkloadIdentity{})[starterMemoKey].(string)

	assert.Empty(t, mustNew(t, nil).reportedStarter(memoWithStarterValue(t, recorded)),
		"a run submitted by nobody reported %q as its starter", recorded)
}
