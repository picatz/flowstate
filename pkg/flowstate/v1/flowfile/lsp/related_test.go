package lsp

import (
	"encoding/json"

	"github.com/sourcegraph/go-lsp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func relatedIn(t *testing.T, src string) map[string][]relatedInformation {
	t.Helper()
	doc := refsDoc(t, src)
	out := map[string][]relatedInformation{}
	for _, p := range publishable(doc, diagnoseCarried(doc)) {
		out[p.Message] = p.RelatedInformation
	}

	return out
}

func TestDuplicateIDPointsAtTheFirstDeclaration(t *testing.T) {
	t.Parallel()
	src := "edition: v2026.4\nname: d\nsteps:\n  - id: a\n    log:\n      message: one\n  - id: a\n    log:\n      message: two\n"

	var found bool
	for msg, rel := range relatedIn(t, src) {
		if len(msg) < 12 || msg[:12] != "duplicate id" {
			continue
		}
		found = true
		require.Len(t, rel, 1, msg)
		assert.Equal(t, `step "a" also declared here`, rel[0].Message)
		assert.Equal(t, 6, rel[0].Location.Range.Start.Line, "the second `id: a`, the diagnostic sits on the first")
	}
	assert.True(t, found, "the validator must still report the duplicate id this test pins")
}

func TestReferenceToALaterStepPointsAtItsDeclaration(t *testing.T) {
	t.Parallel()
	src := "edition: v2026.4\nname: d\nsteps:\n  - id: first\n    log:\n      message: ${string(steps.second.result)}\n  - id: second\n    log:\n      message: x\n"

	var found bool
	for msg, rel := range relatedIn(t, src) {
		if len(msg) < 15 || msg[:15] != "references step" {
			continue
		}
		found = true
		require.Len(t, rel, 1, msg)
		assert.Equal(t, `step "second" declared here`, rel[0].Message)
		assert.Equal(t, 6, rel[0].Location.Range.Start.Line)
	}
	assert.True(t, found, "the validator must still report the forward reference this test pins")
}

func TestUnknownStepHasNothingToPointAt(t *testing.T) {
	t.Parallel()
	src := "edition: v2026.4\nname: d\nsteps:\n  - id: first\n    log:\n      message: ${string(steps.nope.result)}\n"
	for msg, rel := range relatedIn(t, src) {
		assert.Empty(t, rel, msg)
	}
}

func TestPublishedDiagnosticMarshalsFlat(t *testing.T) {
	t.Parallel()
	b, err := json.Marshal(publishedDiagnostic{
		Diagnostic:         lsp.Diagnostic{Message: "m", Code: "c"},
		RelatedInformation: []relatedInformation{{Message: "r"}},
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"range":{"start":{"line":0,"character":0},"end":{"line":0,"character":0}},"code":"c","message":"m","relatedInformation":[{"location":{"uri":"","range":{"start":{"line":0,"character":0},"end":{"line":0,"character":0}}},"message":"r"}]}`, string(b))

	b, err = json.Marshal(publishedDiagnostic{Diagnostic: lsp.Diagnostic{Message: "m"}})
	require.NoError(t, err)
	assert.NotContains(t, string(b), "relatedInformation", "a diagnostic with nothing related carries no empty field")
}
