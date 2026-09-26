package server

import (
	"maps"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/converter"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The key's composition, without a cluster: what changes the digest, what does
// not, and what the memo carries.

func submissionFor(t *testing.T, namespace, requestID string, inputs map[string]*v1.Value) *submissionKey {
	t.Helper()

	key, err := newSubmissionKey(namespace, requestID, &v1.Workflow{Name: "w"}, inputs)
	require.NoError(t, err)
	require.NotNil(t, key)

	return key
}

func TestSubmissionKeyIsAbsentWithoutARequestID(t *testing.T) {
	t.Parallel()

	key, err := newSubmissionKey("team-a", "", &v1.Workflow{Name: "w"}, nil)
	require.NoError(t, err)
	require.Nil(t, key, "no request id is the byte-identical-to-today case, and composes nothing")
}

func TestSubmissionKeyIsDeterministicAndTenantScoped(t *testing.T) {
	t.Parallel()

	// Two keys from two maps built in different insertion orders: a map's wire
	// order is the encoder's choice unless the marshal is deterministic, and a
	// retry whose digest depended on it would be refused at random.
	forward := map[string]*v1.Value{"a": v1.NewLiteral("1"), "b": v1.NewLiteral("2")}
	backward := map[string]*v1.Value{}
	backward["b"] = v1.NewLiteral("2")
	backward["a"] = v1.NewLiteral("1")

	one := submissionFor(t, "team-a", "job-1", forward)
	same := submissionFor(t, "team-a", "job-1", backward)
	require.Equal(t, one, same)

	require.True(t, strings.HasPrefix(one.workflowID, requestWorkflowIDPrefix))
	require.Equal(t, requestWorkflowIDPrefix+one.request, one.workflowID)
	require.NotContains(t, one.workflowID, "job-1", "the id is a digest of the request id, never the value")

	otherTenant := submissionFor(t, "team-b", "job-1", forward)
	require.NotEqual(t, one.workflowID, otherTenant.workflowID, "the tenant is inside the digest")
	require.Equal(t, one.submission, otherTenant.submission, "the submission digest is about what was sent, not by whom")

	otherRequest := submissionFor(t, "team-a", "job-2", forward)
	require.NotEqual(t, one.workflowID, otherRequest.workflowID)

	otherInputs := submissionFor(t, "team-a", "job-1", map[string]*v1.Value{"a": v1.NewLiteral("1"), "b": v1.NewLiteral("3")})
	require.Equal(t, one.workflowID, otherInputs.workflowID, "the inputs do not change the address")
	require.NotEqual(t, one.submission, otherInputs.submission, "but they do change the submission")

	edited, err := newSubmissionKey("team-a", "job-1", &v1.Workflow{Name: "w", Labels: map[string]string{"k": "v"}}, forward)
	require.NoError(t, err)
	require.NotEqual(t, one.submission, edited.submission, "an edited specification is a different submission")
}

func TestSubmissionKeyMemoCarriesDigestsNotTheRequestID(t *testing.T) {
	t.Parallel()

	key := submissionFor(t, "team-a", "ci-run-4711", nil)
	memo := key.memo()

	require.Equal(t, key.request, memo[requestMemoKey])
	require.Equal(t, key.submission, memo[submissionMemoKey])
	for _, value := range memo {
		require.NotContains(t, value, "ci-run-4711", "the request id itself never reaches the run's memo")
	}
	require.NoError(t, v1.ValidateContentDigest(key.submission), "the submission digest is the tree's one digest spelling")
}

// TestDigestWorkflowIDJoinsUnambiguously is the boundary argument the tenant
// inside the digest rests on: the parts are separated, so shifting a byte from
// one part to its neighbour is a different id.
func TestDigestWorkflowIDJoinsUnambiguously(t *testing.T) {
	t.Parallel()

	require.NotEqual(t,
		digestWorkflowID("p-", "team", "a-key"),
		digestWorkflowID("p-", "team-a", "key"))
	require.NotEqual(t,
		digestWorkflowID("p-", "a", "bc"),
		digestWorkflowID("p-", "ab", "c"))

	// The webhook derivation is this function with its own prefix, so a delivery
	// and a request that happen to digest the same parts are still two ids.
	require.NotEqual(t,
		webhookWorkflowID("ns", "wf", "trigger", "key"),
		digestWorkflowID(requestWorkflowIDPrefix, "ns", "wf", "trigger", "key"))
	require.True(t, strings.HasPrefix(webhookWorkflowID("ns", "wf", "trigger", "key"), "flowstate-webhook-"))
}

// TestStartRequestIDIsTheSubmissionAndNeverTheRequestID is #1966's key: every
// attempt at one submission reissues under one Temporal request id, a
// different submission under the same request id does not, and the caller's
// own value never reaches Temporal.
func TestStartRequestIDIsTheSubmissionAndNeverTheRequestID(t *testing.T) {
	t.Parallel()

	inputs := map[string]*v1.Value{"cluster": v1.NewLiteral("checkout")}
	one := submissionFor(t, "team-a", "deploy-4711", inputs)
	again := submissionFor(t, "team-a", "deploy-4711", inputs)
	require.Equal(t, one.startRequestID(), again.startRequestID(), "a retry must reissue under the same request id")
	require.True(t, strings.HasPrefix(one.startRequestID(), startRequestIDPrefix))
	require.NotContains(t, one.startRequestID(), "deploy-4711")

	edited := submissionFor(t, "team-a", "deploy-4711", map[string]*v1.Value{"cluster": v1.NewLiteral("billing")})
	require.NotEqual(t, one.startRequestID(), edited.startRequestID(),
		"a different submission under the same request id must not be folded onto the first")

	otherTenant := submissionFor(t, "team-b", "deploy-4711", inputs)
	require.NotEqual(t, one.startRequestID(), otherTenant.startRequestID(), "the tenant is inside the digest")
}

// TestReissueStartedReadsTheNonceAndFailsClosed covers what the race tests
// cannot reach: a run that answered this submission's start request id but
// does not carry what this server writes on one.
func TestReissueStartedReadsTheNonceAndFailsClosed(t *testing.T) {
	t.Parallel()

	key := submissionFor(t, "", "deploy-4711", nil)

	run := func(t *testing.T, fields map[string]string) *workflowservice.DescribeWorkflowExecutionResponse {
		t.Helper()
		memo := mineMemo(t)
		for name, value := range fields {
			payload, err := converter.GetDefaultDataConverter().ToPayload(value)
			require.NoError(t, err)
			memo.Fields[name] = payload
		}
		return &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Execution: &commonpb.WorkflowExecution{WorkflowId: key.workflowID, RunId: "r-1"},
				Type:      &commonpb.WorkflowType{Name: flowstateRunWorkflowType},
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				Memo:      memo,
			},
		}
	}
	ours := map[string]string{requestMemoKey: key.request, submissionMemoKey: key.submission}

	for _, tc := range []struct {
		name    string
		fields  map[string]string
		started bool
		code    connect.Code
	}{
		{name: "this call's nonce", fields: withField(ours, startMemoKey, "mine"), started: true},
		{name: "another call's nonce", fields: withField(ours, startMemoKey, "theirs")},
		{name: "no nonce", fields: ours, code: connect.CodeInternal},
		{name: "another submission", fields: withField(withField(ours, submissionMemoKey, "other"), startMemoKey, "mine"), code: connect.CodeInternal},
		{name: "another request", fields: withField(withField(ours, requestMemoKey, "other"), startMemoKey, "mine"), code: connect.CodeInternal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := mustNew(t, &fakeRunClient{describe: run(t, tc.fields)})
			resp, started, err := s.reissueStarted(t.Context(), key.workflowID, "r-1", key, "mine")
			if tc.code != 0 {
				require.Equal(t, tc.code, connect.CodeOf(err), "%v", err)
				require.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.started, started)
			require.Equal(t, "r-1", resp.GetWorkflowExecutionInfo().GetExecution().GetRunId())
		})
	}
}

// withField is fields with one more, leaving fields itself as it was.
func withField(fields map[string]string, name, value string) map[string]string {
	out := maps.Clone(fields)
	out[name] = value
	return out
}
