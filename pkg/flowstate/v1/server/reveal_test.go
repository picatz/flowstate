package server_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const revealToken = "synthetic-token-5d0c"

// revealFlowfile declares a sensitive input, uses it where it reaches a
// declared-sensitive output, the step transcript, and a failure message, and
// declares one ordinary output beside it.
const revealFlowfile = `edition: v2026.3
name: reveal
inputs:
  token:
    type: string
    sensitive: true
  region:
    type: string
steps:
  - id: call
    continue_on_error: true
    http:
      url: "http://127.0.0.1:9/${inputs.token}"
outputs:
  echo:
    value: ${inputs.token}
    sensitive: true
  plain:
    value: ${inputs.region}
`

func revealWorkflow(t *testing.T, doc string) *v1.Workflow {
	t.Helper()
	wf, _, err := flowfile.Parse([]byte(doc))
	require.NoError(t, err)
	return wf
}

func caller(ctx context.Context, actions ...string) context.Context {
	p := auth.Principal{Issuer: "https://issuer.example", Subject: "reader"}
	if actions != nil {
		p.Actions = actions
	}
	return auth.ContextWithPrincipal(ctx, p)
}

func completedRun(t *testing.T, s interface {
	Get(context.Context, *connect.Request[v1.GetRequest]) (*connect.Response[v1.GetResponse], error)
}, workflowID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := s.Get(t.Context(), connect.NewRequest(&v1.GetRequest{WorkflowId: workflowID}))
		return err == nil && resp.Msg.GetStatus() != v1.RunResponse_STATUS_RUNNING
	}, 60*time.Second, 100*time.Millisecond)
}

// TestTheServerDecidesWhatSensitiveValuesLeaveIt is the disclosure matrix: a
// reader without the explicit action never receives a declared-sensitive
// value, in any field, whether or not they ask; one with it receives them
// only when they ask; and a run declaring nothing is untouched.
func TestTheServerDecidesWhatSensitiveValuesLeaveIt(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	s := mustNew(t, temporal)

	started, err := s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: revealWorkflow(t, revealFlowfile),
		Inputs: map[string]*v1.Value{
			"token":  v1.NewLiteral(revealToken),
			"region": v1.NewLiteral("eu-west-1"),
		},
	}))
	require.NoError(t, err)
	id := started.Msg.GetWorkflowId()
	completedRun(t, s, id)

	get := func(ctx context.Context, reveal bool) *v1.GetResponse {
		resp, err := s.Get(ctx, connect.NewRequest(&v1.GetRequest{WorkflowId: id, RevealSensitive: reveal}))
		require.NoError(t, err)
		return resp.Msg
	}
	leaks := func(resp *v1.GetResponse) bool {
		raw, err := proto.Marshal(resp)
		require.NoError(t, err)
		return strings.Contains(string(raw), revealToken)
	}

	for name, ctx := range map[string]context.Context{
		"no principal":            t.Context(),
		"no action list":          caller(t.Context()),
		"read only":               caller(t.Context(), "workload.read"),
		"read and payload decode": caller(t.Context(), "workload.read", "payload.decode"),
	} {
		for _, reveal := range []bool{false, true} {
			resp := get(ctx, reveal)
			require.Equal(t, v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD, resp.GetSensitiveDisclosure(), name)
			require.False(t, leaks(resp), "%s (reveal=%v): the token left the server", name, reveal)
			require.Equal(t, "[redacted: echo]", resp.GetRunOutputs().GetValues()["echo"].GetLiteral().GetStringValue(), name)
			require.Equal(t, "eu-west-1", resp.GetRunOutputs().GetValues()["plain"].GetLiteral().GetStringValue(),
				"%s: an ordinary output was withheld; the server redacts against the executed specification", name)
		}
	}

	revealed := get(caller(t.Context(), "workload.read", "workload.reveal_sensitive"), true)
	require.Equal(t, v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_REVEALED, revealed.GetSensitiveDisclosure())
	require.Equal(t, revealToken, revealed.GetRunOutputs().GetValues()["echo"].GetLiteral().GetStringValue())

	// Holding the action is not asking: the default is still withheld.
	notAsked := get(caller(t.Context(), "workload.read", "workload.reveal_sensitive"), false)
	require.Equal(t, v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD, notAsked.GetSensitiveDisclosure())
	require.False(t, leaks(notAsked))

	// The timeline's failure text quotes the URL the http task was given,
	// which carried the token.
	timeline, err := s.GetTimeline(caller(t.Context(), "workload.read"), connect.NewRequest(&v1.GetTimelineRequest{WorkflowId: id, RevealSensitive: true}))
	require.NoError(t, err)
	require.Equal(t, v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD, timeline.Msg.GetSensitiveDisclosure())
	raw, err := proto.Marshal(timeline.Msg)
	require.NoError(t, err)
	require.NotContains(t, string(raw), revealToken)
	sawFailure := false
	for _, entry := range timeline.Msg.GetEntries() {
		sawFailure = sawFailure || entry.GetFailure() != ""
	}
	require.True(t, sawFailure, "the timeline recorded no failure, so its redaction was not exercised")

	full, err := s.GetTimeline(caller(t.Context(), "workload.read", "workload.reveal_sensitive"),
		connect.NewRequest(&v1.GetTimelineRequest{WorkflowId: id, RevealSensitive: true}))
	require.NoError(t, err)
	raw, err = proto.Marshal(full.Msg)
	require.NoError(t, err)
	require.Contains(t, string(raw), revealToken, "the authorized reveal did not reveal the failure text")
}

func TestARunDeclaringNothingSensitiveIsUntouched(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	s := mustNew(t, temporal)

	plain := strings.ReplaceAll(revealFlowfile, "    sensitive: true\n", "")
	started, err := s.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: revealWorkflow(t, plain),
		Inputs:   map[string]*v1.Value{"token": v1.NewLiteral(revealToken), "region": v1.NewLiteral("eu-west-1")},
	}))
	require.NoError(t, err)
	id := started.Msg.GetWorkflowId()
	completedRun(t, s, id)

	resp, err := s.Get(caller(t.Context(), "workload.read"), connect.NewRequest(&v1.GetRequest{WorkflowId: id}))
	require.NoError(t, err)
	require.Equal(t, v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_NONE_DECLARED, resp.Msg.GetSensitiveDisclosure())
	require.Equal(t, revealToken, resp.Msg.GetRunOutputs().GetValues()["echo"].GetLiteral().GetStringValue())
}

// TestAContinuedRunIsDecidedByTheSegmentReported: after Continue-As-New, Get
// reports the latest segment, and the decision is read from that segment's
// own start input, which carries the same specification and inputs.
func TestAContinuedRunIsDecidedByTheSegmentReported(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	s := mustNew(t, temporal)

	wf := revealWorkflow(t, `edition: v2026.3
name: segmented
inputs:
  token:
    type: string
    sensitive: true
steps:
  - id: grow
    loop:
      as: acc
      init: 0
      update: ${acc + 1}
      until: ${acc >= 3}
      max_iterations: 10
      steps:
        - id: tick
          log:
            message: tick
outputs:
  echo:
    value: ${inputs.token}
    sensitive: true
`)
	run, err := temporal.ExecuteWorkflow(t.Context(),
		client.StartWorkflowOptions{
			TaskQueue: engine.RunTaskQueueName,
			// The default tenant's own, as the server records it on Run.
			Memo: map[string]any{"flowstate.namespace": ""},
		},
		engine.RunWorkflowType, &v1.RunState{
			Workflow:    wf,
			Inputs:      map[string]*v1.Value{"token": v1.NewLiteral(revealToken)},
			StepsBudget: 1,
		})
	require.NoError(t, err)
	firstRunID := run.GetRunID()
	require.NoError(t, run.Get(t.Context(), nil))

	resp, err := s.Get(caller(t.Context(), "workload.read"), connect.NewRequest(&v1.GetRequest{WorkflowId: run.GetID()}))
	require.NoError(t, err)
	require.NotEqual(t, firstRunID, resp.Msg.GetRunId(), "the run never continued as new, so a later segment was not exercised")
	require.Equal(t, v1.SensitiveDisclosure_SENSITIVE_DISCLOSURE_WITHHELD, resp.Msg.GetSensitiveDisclosure())
	require.Equal(t, "[redacted: echo]", resp.Msg.GetRunOutputs().GetValues()["echo"].GetLiteral().GetStringValue())
}
