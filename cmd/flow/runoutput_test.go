package main

import (
	"bytes"
	"encoding/json"
	"maps"
	"slices"
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

	// Decoded and inspected by field name, which is the only form of this
	// assertion that holds the contract.
	//
	// Two weaker versions were tried first and both are recorded here because
	// each fails in an instructive way. A literal prefix asserts the encoder's
	// whitespace, which is not the contract — though the claim that protojson's
	// per-binary randomization could flake it was simply wrong: detrand injects
	// after a comma in compact output, never after the opening brace, so that
	// prefix was in fact stable. And comparing against marshalJSON's own output
	// is worse still, because the expectation then comes from the same helper
	// that produced the actual: setting UseProtoNames would rename every field
	// to step_values and the test would stay green while `| jq
	// .stepValues.hello.namedValues`, which this command's help documents, broke
	// completely. That path is the whole reason this test exists.
	var document map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &document),
		"stdout must be one JSON document a program can read: %q", out.String())

	require.Contains(t, document, "runOutputs")

	steps, ok := document["stepValues"].(map[string]any)
	require.True(t, ok, "`jq .stepValues` is in this command's help; got %v", slices.Sorted(maps.Keys(document)))

	hello, ok := steps["hello"].(map[string]any)
	require.True(t, ok, "the step's own entry, addressed by id")

	require.Contains(t, hello, "namedValues",
		"`jq .stepValues.hello.namedValues` is the exact path the help documents")
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
		value *v1.Value
	}{
		{
			name: "bytes",
			value: &v1.Value{Kind: &v1.Value_Literal{Literal: &expr.Value{
				Kind: &expr.Value_BytesValue{BytesValue: []byte{0x01, 0x02}},
			}}},
		},
		{
			// Nested, because a lossy value hides just as well inside a list.
			name: "bytes inside a list",
			value: &v1.Value{Kind: &v1.Value_Literal{Literal: &expr.Value{
				Kind: &expr.Value_ListValue{ListValue: &expr.ListValue{
					Values: []*expr.Value{{Kind: &expr.Value_BytesValue{BytesValue: []byte{0x03}}}},
				}},
			}}},
		},
		{
			// A non-literal Value has no CEL kind for the summary to render.
			name: "outer structure",
			value: &v1.Value{Kind: &v1.Value_Structure_{Structure: &v1.Value_Structure{
				Kind: &v1.Value_Structure_List_{List: &v1.Value_Structure_List{}},
			}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			response := runForOutput(t)
			response.RunOutputs = &v1.RunOutputs{
				Values: map[string]*v1.Value{
					"blob": test.value,
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
