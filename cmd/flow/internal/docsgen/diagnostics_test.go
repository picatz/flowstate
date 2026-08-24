package docsgen

import (
	"errors"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestDiagnosticShapeExampleIsReal guards the one thing a reader cannot verify
// by eye: that the YAML fixture in [diagnosticShapeSection] actually produces
// the JSON shown beside it.
//
// A doc that shows a request and a response is a claim the pair are real, and
// this page has already gotten that claim wrong once — an earlier draft
// misspelled a task input (`log:`'s `message:`) rather than a step property,
// which `validateTaskInputs` reports without an edit, so the documented
// `edits` array could never actually come back. Extracting both fenced blocks
// from the same constant the generator renders, running the YAML through the
// real validator, and comparing is what CLAUDE.md means by testing by
// compiling the result rather than trusting a hand-written pair to agree.
func TestDiagnosticShapeExampleIsReal(t *testing.T) {
	yamlFence := regexp.MustCompile("(?s)```yaml\n(.*?)\n```")
	fixture := yamlFence.FindStringSubmatch(diagnosticShapeSection)
	require.Len(t, fixture, 2, "diagnosticShapeSection must contain one ```yaml fenced fixture")
	source := "edition: v2026.3\nname: shape-example\n" + fixture[1] + "\n"

	// The compiler reports an unknown step property as a compile failure, not as
	// a passable Diagnostics slice — see [flowfile.ValidateSource]'s own doc
	// comment. It is still exactly the Diagnostics an author or an agent reads,
	// unwrapped the same way `flow validate` unwraps it (cmd/flow/main.go's
	// diagnosticsError path): errors.As, not a nil check on err.
	var diags flowfile.Diagnostics
	_, err := flowfile.ValidateSource([]byte(source))
	require.Error(t, err)
	require.True(t, errors.As(err, &diags), "expected a flowfile.Diagnostics error, got %v", err)
	require.Len(t, diags, 1, "the fixture must produce exactly the one diagnostic the doc shows")

	d := diags[0]
	require.Equal(t, "notify", d.Step)
	require.Equal(t, `unknown key "retryy"; did you mean "retry"?`, d.Message)
	require.Len(t, d.Edits, 1, "the doc claims this diagnostic carries a suggested edit")

	edit := d.Edits[0]
	require.Equal(t, "rename to `retry`", edit.Title)
	require.Len(t, edit.Changes, 1)
	require.Equal(t, "retry", edit.Changes[0].NewText)

	// The line/column the doc hard-codes in its JSON block must match what the
	// fixture actually produces, not just what it produced when the prose was
	// written.
	require.Equal(t, 5, d.Line)
	require.Equal(t, 5, d.Column)
	require.Equal(t, uint32(5), edit.Changes[0].Range.StartLine)
	require.Equal(t, uint32(5), edit.Changes[0].Range.StartColumn)
	require.Equal(t, uint32(5), edit.Changes[0].Range.EndLine)
	require.Equal(t, uint32(11), edit.Changes[0].Range.EndColumn)
}
