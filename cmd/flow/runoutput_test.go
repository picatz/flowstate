package main

import (
	"bytes"
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

	response := runForOutput(t)
	require.NoError(t, writeRun(surface, FormatText, response))

	// Compared against what marshalJSON produces rather than against a literal,
	// and that is not fussiness. protojson deliberately randomizes its
	// whitespace, seeded per binary, so `{"stepValues":{` and `{"stepValues": {`
	// are both legal output from the same code on different builds — a literal
	// prefix here would pass all day and fail on somebody's rebuild. Marshalling
	// the same document the same way asks the real question, which is whether
	// stdout still carries the transcript a jq expression addresses.
	want, err := marshalJSON(response.GetOutputs(), false)
	require.NoError(t, err)
	require.Equal(t, string(want)+"\n", out.String(),
		"a pipe reads stdout, and every documented `flow run local … | jq` omits -o json")
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

// TestATerminalKeepsTheDocumentWhenTheSummaryIsLossy is the case Codex found on
// this change, and it is a good example of removing something whose absence
// invalidated a permission granted elsewhere.
//
// renderLiteral is allowed to be a summary — bytes are named as a length, and a
// CEL kind it does not render is named as its type — and the comment granting
// that permission says why: "the value itself is on stdout". Suppressing the
// document on a terminal took that away, so a declared bytes output became
// unreachable in the default invocation with nothing telling the person so.
//
// The document is therefore held back only when the summary carries everything.
func TestATerminalKeepsTheDocumentWhenTheSummaryIsLossy(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		value *expr.Value
	}{
		{
			name:  "bytes",
			value: &expr.Value{Kind: &expr.Value_BytesValue{BytesValue: []byte{0x01, 0x02}}},
		},
		{
			// Nested, because a lossy value hides just as well inside a list.
			name: "bytes inside a list",
			value: &expr.Value{Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{
				Values: []*expr.Value{{Kind: &expr.Value_BytesValue{BytesValue: []byte{0x03}}}},
			}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			response := runForOutput(t)
			response.RunOutputs = &v1.RunOutputs{
				Values: map[string]*v1.Value{
					"blob": {Kind: &v1.Value_Literal{Literal: test.value}},
				},
			}

			var out, errOut bytes.Buffer
			terminal := ui.Capabilities{Width: 80, TTY: true}
			surface := ui.ForCapabilities(&out, &errOut, terminal, terminal)

			require.NoError(t, writeRun(surface, FormatText, response))

			require.NotEmpty(t, out.String(),
				"the summary cannot carry this value, so the document must still be there")
		})
	}
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
