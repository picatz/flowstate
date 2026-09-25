package flowtest_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// A scripted `signals:` delivery goes through [v1.LocalSignals.DeliverFrom],
// the door that holds a payload to [v1.MaxStructureDepth] exactly as the
// server's Signal door does (#1770). A rehearsal that delivered what
// production refuses would be the divergence invariant 3 forbids, so this
// pins the boundary from the rehearsal's side: at the bound the gate is
// answered, one past it the delivery is refused in the shared words and the
// gate lapses.
//
// Built in Go rather than parsed from YAML deliberately: a test file's own
// document bound refuses a payload nested this deep before any delivery is
// scripted, so the door is reachable from a rehearsal only through [flowtest.Run].

// nestedPayload nests a scripted payload's field levels deep around one leaf.
func nestedPayload(levels int) any {
	var value any = "leaf"
	for range levels {
		value = map[string]any{"k": value}
	}

	return value
}

func TestAScriptedSignalIsHeldToTheDepthBound(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: depth-gate
steps:
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
outputs:
  answered:
    value: ${!steps.gate.timed_out}
`)

	file := &flowtest.File{
		Tests: []flowtest.Test{
			{
				Name:     "at the bound",
				Workflow: "./workflow.yaml",
				Signals: []flowtest.SignalScript{{
					Name:    "go",
					Payload: map[string]any{"doc": nestedPayload(v1.MaxStructureDepth)},
				}},
				Expect: flowtest.Expectation{Outputs: map[string]any{"answered": true}},
			},
			{
				Name:     "past the bound",
				Workflow: "./workflow.yaml",
				Signals: []flowtest.SignalScript{{
					Name:    "go",
					Payload: map[string]any{"doc": nestedPayload(v1.MaxStructureDepth + 1)},
				}},
				// The refused delivery never reaches the gate, which lapses at
				// its own timeout — the same thing the run would observe in
				// production after the server refused the sender.
				Expect: flowtest.Expectation{Outputs: map[string]any{"answered": false}},
			},
		},
	}

	run := flowtest.Run(t.Context(), file, dir, flowtest.RunOptions{Label: "built"})
	require.Empty(t, run.Report.GetRefused())
	require.Len(t, run.Report.GetCases(), 2)
	for _, c := range run.Report.GetCases() {
		assert.True(t, c.GetPassed(), "%s: %v / %v", c.GetName(), c.GetError(), c.GetFailures())
	}

	require.Len(t, run.Transcripts, 2)
	atBound := transcriptText(run.Transcripts[0])
	assert.Contains(t, atBound, "signal go ", "the payload at the bound was not delivered")
	assert.NotContains(t, atBound, "refused")

	// The account trims a long line, so the shared sentence is asserted by
	// the head that survives: the field, the depth and the bound.
	pastBound := transcriptText(run.Transcripts[1])
	assert.Contains(t, pastBound, "signal go refused: ")
	assert.Contains(t, pastBound, fmt.Sprintf("signal payload field %q nests %d levels deep, over the %d levels",
		"doc", v1.MaxStructureDepth+1, v1.MaxStructureDepth),
		"the rehearsal refused in different words from the server's door")
}
