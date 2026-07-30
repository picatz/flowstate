package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/go-cmp/cmp"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/testing/protocmp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

// TestParseSignalFlag covers the flag that makes an approval gate runnable on a
// laptop.
//
// Its payload becomes the waiting step's outputs, so what this produces is what a
// later step reads as ${approval.approved} — which makes a quoting mistake here
// indistinguishable from a workflow bug unless the error says otherwise.
func TestParseSignalFlag(t *testing.T) {
	t.Parallel()

	// The expected payloads are written out rather than built by handing the same
	// Go value to the same conversion the code under test uses, which would assert
	// only that a function agrees with itself.
	//
	// That mattered: this table previously compared key *presence* and never a
	// value, so replacing every nested map and list with null passed the whole
	// ./cmd/flow/ suite. Nothing else on the branch checks that a nested payload
	// survives, and a gate whose ${approval.meta.ticket} resolves to null is a gate
	// that looks answered and carries nothing.
	str := func(s string) *expr.Value {
		return &expr.Value{Kind: &expr.Value_StringValue{StringValue: s}}
	}

	tests := []struct {
		name    string
		flag    string
		want    *v1.Node_Outputs
		wantErr string
	}{
		{
			name: "a name and an object",
			flag: `deploy-approved={"approved": true, "by": "me@example.com"}`,
			want: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"approved": v1.NewLiteral(true),
				"by":       v1.NewLiteral("me@example.com"),
			}},
		},
		{
			name: "a nested payload",
			flag: `deploy-approved={"meta": {"ticket": "OPS-1"}}`,
			want: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"meta": {Kind: &v1.Value_Literal{Literal: &expr.Value{
					Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{
						Entries: []*expr.MapValue_Entry{{Key: str("ticket"), Value: str("OPS-1")}},
					}},
				}}},
			}},
		},
		{
			// A list nests differently from a mapping and is the other half of what
			// went unchecked.
			name: "a payload holding a list",
			flag: `deploy-approved={"reviewers": ["ana", "bo"]}`,
			want: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
				"reviewers": {Kind: &v1.Value_Literal{Literal: &expr.Value{
					Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{
						Values: []*expr.Value{str("ana"), str("bo")},
					}},
				}}},
			}},
		},
		{
			// A signal that carries nothing is a reasonable thing to send: the
			// wait completes and reports it did not time out.
			name: "no payload at all",
			flag: "deploy-approved=",
			want: &v1.Node_Outputs{NamedValues: map[string]*v1.Value{}},
		},
		{
			name:    "no payload separator",
			flag:    "deploy-approved",
			wantErr: "needs a name and a payload",
		},
		{
			name:    "no name",
			flag:    `={"approved": true}`,
			wantErr: "names no signal",
		},
		{
			name:    "not JSON",
			flag:    "deploy-approved=yes",
			wantErr: "not a JSON object",
		},
		{
			// The shell-quoting mistake this is most likely to be: a bare list
			// rather than an object.
			name:    "JSON that is not an object",
			flag:    `deploy-approved=[1, 2]`,
			wantErr: "not a JSON object",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			name, payload, err := parseSignalFlag(test.flag)

			if test.wantErr != "" {
				require.Error(t, err, "a malformed --signal was accepted")
				require.Contains(t, err.Error(), test.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, "deploy-approved", name)

			require.Empty(t,
				cmp.Diff(test.want, payload, protocmp.Transform()),
				"the payload a later step would read differs from what was sent:\n%s",
				cmp.Diff(test.want, payload, protocmp.Transform()))
		})
	}
}

// TestWithLocalSignalsDelivers checks that a supplied answer actually reaches a
// waiting step, buffered until the run gets there.
func TestWithLocalSignalsDelivers(t *testing.T) {
	t.Parallel()

	ctx, err := withLocalSignals(t.Context(), []string{
		`deploy-approved={"approved": true}`,
	})
	require.NoError(t, err)

	waiter, ok := v1.SignalWaiterFromContext(ctx)
	require.True(t, ok, "no signal waiter was attached to the run")

	// Already waiting when the run starts, which is what lets a gate reached later
	// find its answer rather than blocking on something that already happened.
	payload, err := waiter.WaitForSignal(t.Context(), "deploy-approved")
	require.NoError(t, err)
	require.True(t, payload.GetNamedValues()["approved"].GetLiteral().GetBoolValue())
}

// TestWithLocalSignalsAttachesAWaiterRegardless checks that a run with no answers
// still gets a waiter.
//
// Without one, reaching a gate fails with an error about local tooling instead of
// waiting the way production would — and the point of a local run is to behave like
// production.
func TestWithLocalSignalsAttachesAWaiterRegardless(t *testing.T) {
	t.Parallel()

	ctx, err := withLocalSignals(t.Context(), nil)
	require.NoError(t, err)

	_, ok := v1.SignalWaiterFromContext(ctx)
	require.True(t, ok, "a run with no --signal flags got no waiter, so a gate would fail rather than wait")
}

// TestReportUnansweredGates checks the warning, which is the difference between a
// terminal that looks broken and one that says what it is doing.
func TestReportUnansweredGates(t *testing.T) {
	t.Parallel()

	workflow := &v1.Workflow{
		Steps: []*v1.Node{
			{Id: "a", Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "needs-answer"}},
			}}},
			{Id: "b", Kind: &v1.Node_Wait{Wait: &v1.Wait{
				Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "answered"}},
			}}},
		},
	}

	var out strings.Builder
	reportUnansweredGates(&out, workflow, []string{`answered={"ok": true}`})

	require.Contains(t, out.String(), "needs-answer",
		"the gate with no answer was not reported")
	require.Contains(t, out.String(), "--signal needs-answer=",
		"the warning does not say what would answer it")
	require.NotContains(t, out.String(), `"answered"`,
		"a gate that was answered was reported as unanswered")
}

// A capability is not done until somebody can reach it.
//
// The tests above cover the parsing, which proves nothing about whether typing
// `flow signal` puts a signal on the wire. These cover the path a person actually
// takes: flags, through validation, to an RPC a server receives.

// fakeWorkflowService records what the CLI sent, and answers with what a test asks
// it to.
type fakeWorkflowService struct {
	flowstatev1connect.UnimplementedWorkflowServiceHandler

	got *v1.SignalRequest
	err error

	// The Get half, whose handler lives in get_test.go: one stand-in server
	// covers every verb the CLI has.
	gotGet      *v1.GetRequest
	getResponse *v1.GetResponse
	getErr      error

	// Starting a run, whose handler lives in watch_test.go beside the tests for
	// following one.
	gotRun      *v1.RunRequest
	runResponse *v1.RunResponse
	runErr      error

	// The lifecycle verbs, whose handlers live in lifecycle_test.go.
	gotCancel    *v1.CancelRequest
	gotTerminate *v1.TerminateRequest

	// listResponses is answered in order, so a test can describe a listing that
	// takes several pages — including a page that comes back empty with more to
	// find, which a bounded scan produces and a caller must not read as the end.
	listResponses []*v1.ListResponse
	listCalls     int
	lastListToken string
	listErr       error

	// listErrAfter makes List fail once this many calls have succeeded, which is
	// how a test describes a walk that breaks partway rather than at the start.
	listErrAfter int
}

// Cancel implements [flowstatev1connect.WorkflowServiceHandler].
func (f *fakeWorkflowService) Cancel(_ context.Context, req *connect.Request[v1.CancelRequest]) (*connect.Response[v1.CancelResponse], error) {
	f.gotCancel = req.Msg
	return connect.NewResponse(&v1.CancelResponse{}), nil
}

// Terminate implements [flowstatev1connect.WorkflowServiceHandler].
func (f *fakeWorkflowService) Terminate(_ context.Context, req *connect.Request[v1.TerminateRequest]) (*connect.Response[v1.TerminateResponse], error) {
	f.gotTerminate = req.Msg
	return connect.NewResponse(&v1.TerminateResponse{}), nil
}

// List implements [flowstatev1connect.WorkflowServiceHandler].
func (f *fakeWorkflowService) List(_ context.Context, req *connect.Request[v1.ListRequest]) (*connect.Response[v1.ListResponse], error) {
	f.lastListToken = req.Msg.GetPageToken()
	f.listCalls++

	if f.listErr != nil {
		return nil, f.listErr
	}
	if f.listErrAfter > 0 && f.listCalls > f.listErrAfter {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("page unavailable"))
	}

	if len(f.listResponses) == 0 {
		return connect.NewResponse(&v1.ListResponse{}), nil
	}

	next := f.listResponses[0]
	f.listResponses = f.listResponses[1:]

	return connect.NewResponse(next), nil
}

// Signal implements [flowstatev1connect.WorkflowServiceHandler].
func (f *fakeWorkflowService) Signal(_ context.Context, req *connect.Request[v1.SignalRequest]) (*connect.Response[v1.SignalResponse], error) {
	f.got = req.Msg
	if f.err != nil {
		return nil, f.err
	}
	return connect.NewResponse(&v1.SignalResponse{}), nil
}

// serveFake stands a fake Flowstate server up and points the CLI at it for the
// duration of one test.
func serveFake(t *testing.T, fake *fakeWorkflowService) {
	t.Helper()

	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(fake))

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	// The address is process-wide configuration, so it is restored rather than
	// left pointing at a server that has been closed.
	previous := flowstateAddress
	flowstateAddress = server.URL
	t.Cleanup(func() { flowstateAddress = previous })
}

// signalCommand builds the command runSignal expects, with its flags reset.
func signalCommand(t *testing.T) (*cobra.Command, *strings.Builder) {
	t.Helper()

	previousData, previousRunID := signalData, signalRunID
	t.Cleanup(func() { signalData, signalRunID = previousData, previousRunID })
	signalData, signalRunID = "", ""

	var out strings.Builder
	cmd := &cobra.Command{}
	cmd.SetContext(t.Context())
	cmd.SetOut(&out)

	return cmd, &out
}

// TestSignalReachesTheServer is the reachability proof: what a person types
// arrives as the request the server handles.
func TestSignalReachesTheServer(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)
	cmd, out := signalCommand(t)

	signalData = `{"approved": true, "by": "someone@example.com"}`
	signalRunID = "run-1"

	require.NoError(t, runSignal(cmd, []string{"deploy-abc123", "deploy-approved"}))

	require.NotNil(t, fake.got, "nothing reached the server")
	require.Equal(t, "deploy-abc123", fake.got.GetWorkflowId())
	require.Equal(t, "deploy-approved", fake.got.GetName())
	require.Equal(t, "run-1", fake.got.GetRunId(), "--run-id was dropped")

	// The payload is what a later step reads as ${approval.approved}, so its
	// shape surviving the trip is the whole point of sending it.
	values := fake.got.GetPayload().GetNamedValues()
	require.True(t, values["approved"].GetLiteral().GetBoolValue())
	require.Equal(t, "someone@example.com", values["by"].GetLiteral().GetStringValue())

	require.Contains(t, out.String(), "deploy-approved",
		"a delivered signal said nothing about what it delivered")
}

// TestSignalWithoutAPayloadSendsAnAbsentOne pins down how "carries nothing" is
// spelled on the wire.
//
// Node.Outputs.named_values is required, so an empty map is not something the
// schema lets a message say — sending one is refused before it leaves. A signal
// with no --data therefore travels with no payload at all, and the server turns
// that back into empty outputs so a later ${approval.timed_out} still resolves.
func TestSignalWithoutAPayloadSendsAnAbsentOne(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)
	cmd, _ := signalCommand(t)

	require.NoError(t, runSignal(cmd, []string{"deploy-abc123", "deploy-approved"}),
		"a signal carrying nothing is a reasonable thing to send and was refused")

	require.NotNil(t, fake.got, "nothing reached the server")
	require.Nil(t, fake.got.GetPayload(),
		"an empty payload was sent, which the schema forbids; absent is how a signal says it carries nothing")
}

// TestSignalRefusesAMalformedPayloadBeforeSending checks that a quoting mistake
// is caught here rather than delivered.
func TestSignalRefusesAMalformedPayloadBeforeSending(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)
	cmd, _ := signalCommand(t)

	// The shell-quoting mistake this is most likely to be: a bare list rather
	// than an object.
	signalData = `[1, 2]`

	err := runSignal(cmd, []string{"deploy-abc123", "deploy-approved"})
	require.ErrorContains(t, err, "not a JSON object")
	require.ErrorContains(t, err, "--data", "the error does not name the flag that was wrong")
	require.Nil(t, fake.got, "a malformed payload was sent anyway")
}

// TestSignalRefusesAnInvalidNameBeforeSending checks the schema's own rules run
// before the round trip.
//
// The rules are read off the descriptor rather than restated in the CLI, so this
// also fails if the two ever drift apart.
func TestSignalRefusesAnInvalidNameBeforeSending(t *testing.T) {
	fake := &fakeWorkflowService{}
	serveFake(t, fake)
	cmd, _ := signalCommand(t)

	err := runSignal(cmd, []string{"deploy-abc123", "not a signal name"})
	require.Error(t, err, "a signal name the schema forbids was accepted")
	require.Nil(t, fake.got, "an invalid signal name was sent anyway")
}

// TestSignalOnAnUnaddressableRunNamesEveryCause checks the message a person gets
// when the server refuses.
//
// The server deliberately cannot say whether the run is absent or someone else's,
// so a bare "no such run" reads as "you mistyped the id" and sends the reader to
// check the one thing that is probably fine. All three causes get named, including
// retention: a run that finished is still readable until it ages out, so "it
// finished" and "it aged out" are different answers and only one of them is here.
func TestSignalOnAnUnaddressableRunNamesEveryCause(t *testing.T) {
	fake := &fakeWorkflowService{
		err: connect.NewError(connect.CodeNotFound, errors.New(`no such run "deploy-abc123"`)),
	}
	serveFake(t, fake)
	cmd, _ := signalCommand(t)

	err := runSignal(cmd, []string{"deploy-abc123", "deploy-approved"})
	require.ErrorContains(t, err, "deploy-abc123")
	require.ErrorContains(t, err, "check the id")
	require.ErrorContains(t, err, "tenant")
	require.ErrorContains(t, err, "retention")
}
