package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// What #551 decided, and the two directions that can break it.
//
// The text format's stdout is machine-shaped on purpose, and that is a contract
// rather than an accident: `flow run local … | jq .stepValues.hello.namedValues`
// appears in this command's own help with no -o json in it. So the piped
// direction is asserted by comparing bytes, not by checking it still parses —
// a document that parses and has moved a key breaks that pipe just as
// thoroughly.
//
// The terminal direction is the new behaviour, and the assertion worth making
// there is an absence: nothing on stdout. Checking that the narration is present
// would pass just as happily with the JSON printed underneath it, which is the
// thing being removed.

// runForOutput is the response a workflow with one silent step produces: no
// declared outputs, one step that named nothing. It is the shape the issue
// opened with, and the one most workflows have.
func runForOutput(t *testing.T) *v1.GetResponse {
	t.Helper()

	return &v1.GetResponse{
		Kind: &v1.GetResponse_Outputs{
			Outputs: &v1.Workflow_StepOutputs{
				StepValues: map[string]*v1.Node_Outputs{
					"hello": {},
				},
			},
		},
	}
}

func TestAPipedRunStillWritesTheTranscript(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer
	piped := ui.Capabilities{Width: 80}
	surface := ui.ForCapabilities(&out, &errOut, piped, piped)

	require.NoError(t, writeRun(surface, FormatText, runForOutput(t)))

	require.NotEmpty(t, out.String(),
		"a pipe reads stdout, and every documented `flow run local … | jq` omits -o json")
	require.True(t, strings.HasPrefix(out.String(), `{"stepValues":`),
		"the transcript a jq expression addresses, byte for byte: got %q", out.String())
}

func TestATerminalRunWritesNothingToStdout(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer
	terminal := ui.Capabilities{Width: 80, TTY: true}
	surface := ui.ForCapabilities(&out, &errOut, terminal, terminal)

	require.NoError(t, writeRun(surface, FormatText, runForOutput(t)))

	require.Empty(t, out.String(),
		"a person got a machine document: %q", out.String())
}

// TestATerminalRunStillReportsDeclaredOutputs is the other half, and the one
// that would make this change a loss rather than a gain if it broke: the answer
// a workflow promised still reaches the person, on the stream this CLI puts its
// account of a run.
func TestATerminalRunStillReportsDeclaredOutputs(t *testing.T) {
	t.Parallel()

	response := runForOutput(t)
	response.RunOutputs = &v1.RunOutputs{
		Values: map[string]*v1.Value{
			"url": {Kind: &v1.Value_Literal{Literal: &expr.Value{
				Kind: &expr.Value_StringValue{StringValue: "https://example.com/releases/2026.9.0"},
			}}},
		},
	}

	var out, errOut bytes.Buffer
	terminal := ui.Capabilities{Width: 80, TTY: true}
	surface := ui.ForCapabilities(&out, &errOut, terminal, terminal)

	require.NoError(t, writeRun(surface, FormatText, response))

	require.Empty(t, out.String())
	require.Contains(t, errOut.String(), "url")
	require.Contains(t, errOut.String(), "https://example.com/releases/2026.9.0")
}

// TestAMachineFormatIgnoresTheTerminal pins that -o json is unaffected. It is
// the explicit override in the other direction, and a person who typed it on a
// terminal meant it.
func TestAMachineFormatIgnoresTheTerminal(t *testing.T) {
	t.Parallel()

	var out, errOut bytes.Buffer
	terminal := ui.Capabilities{Width: 80, TTY: true}
	surface := ui.ForCapabilities(&out, &errOut, terminal, terminal)

	require.NoError(t, writeRun(surface, FormatJSON, runForOutput(t)))

	require.NotEmpty(t, out.String(), "-o json on a terminal wrote nothing")
}
