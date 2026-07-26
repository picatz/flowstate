package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestParseSignalFlag covers the flag that makes an approval gate runnable on a
// laptop.
//
// Its payload becomes the waiting step's outputs, so what this produces is what a
// later step reads as ${approval.approved} — which makes a quoting mistake here
// indistinguishable from a workflow bug unless the error says otherwise.
func TestParseSignalFlag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		flag    string
		want    map[string]any
		wantErr string
	}{
		{
			name: "a name and an object",
			flag: `deploy-approved={"approved": true, "by": "me@example.com"}`,
			want: map[string]any{"approved": true, "by": "me@example.com"},
		},
		{
			name: "a nested payload",
			flag: `deploy-approved={"meta": {"ticket": "OPS-1"}}`,
			want: map[string]any{"meta": nil}, // presence is what matters here
		},
		{
			// A signal that carries nothing is a reasonable thing to send: the
			// wait completes and reports it did not time out.
			name: "no payload at all",
			flag: "deploy-approved=",
			want: map[string]any{},
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
			require.Len(t, payload.GetNamedValues(), len(test.want))

			for key := range test.want {
				require.Contains(t, payload.GetNamedValues(), key,
					"the payload lost the %q key", key)
			}
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
