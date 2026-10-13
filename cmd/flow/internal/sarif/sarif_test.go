package sarif_test

import (
	"encoding/json"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/cmd/flow/internal/sarif"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

var tool = sarif.Tool{Name: "flow", Version: "v1.2.3", InformationURI: "https://example.test/flow"}

func decode(t *testing.T, reports []*v1.DiagnosticReport) map[string]any {
	t.Helper()

	data, err := sarif.Marshal(reports, tool)
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(data, &doc), string(data))

	return doc
}

func firstRun(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()

	runs, ok := doc["runs"].([]any)
	require.True(t, ok && len(runs) == 1, "want one run: %v", doc["runs"])

	return runs[0].(map[string]any)
}

func results(t *testing.T, run map[string]any) []map[string]any {
	t.Helper()

	var out []map[string]any
	for _, r := range run["results"].([]any) {
		out = append(out, r.(map[string]any))
	}

	return out
}

func TestMarshalWritesAValidSARIFEnvelope(t *testing.T) {
	t.Parallel()

	doc := decode(t, []*v1.DiagnosticReport{{File: "flows/a.yaml"}})

	assert.Equal(t, "2.1.0", doc["version"])
	assert.Equal(t, sarif.Schema, doc["$schema"])
	run := firstRun(t, doc)
	driver := run["tool"].(map[string]any)["driver"].(map[string]any)
	assert.Equal(t, "flow", driver["name"])
	assert.Equal(t, "v1.2.3", driver["version"])
	assert.Equal(t, []any{}, run["results"], "a clean run has an empty results array, not null: it was checked")
	assert.Equal(t, true, run["invocations"].([]any)[0].(map[string]any)["executionSuccessful"])
}

func TestMarshalProjectsADiagnostic(t *testing.T) {
	t.Parallel()

	doc := decode(t, []*v1.DiagnosticReport{{
		File: "flows/a.yaml",
		Diagnostics: []*v1.Diagnostic{{
			Line: 7, Column: 5, Message: "no such task", Code: string(v1.DiagnosticCodeUnknownTask),
			Path:    []string{"pages", "page"},
			Related: []*v1.RelatedLocation{{File: "flows/b.yaml", Line: 2, Column: 3, Message: "compiled because step calls it"}},
			Edits: []*v1.SuggestedEdit{{Title: "rename it", Changes: []*v1.TextChange{{
				Range:   &v1.SourceRange{StartLine: 7, StartColumn: 5, EndLine: 7, EndColumn: 9},
				NewText: "http",
			}}}},
		}},
	}})
	run := firstRun(t, doc)
	got := results(t, run)
	require.Len(t, got, 1)
	r := got[0]

	assert.Equal(t, string(v1.DiagnosticCodeUnknownTask), r["ruleId"])
	assert.Equal(t, "error", r["level"])
	assert.Equal(t, "no such task", r["message"].(map[string]any)["text"])

	rules := run["tool"].(map[string]any)["driver"].(map[string]any)["rules"].([]any)
	require.Len(t, rules, 1)
	assert.Equal(t, r["ruleId"], rules[0].(map[string]any)["id"])
	assert.EqualValues(t, 0, r["ruleIndex"])
	assert.NotEmpty(t, rules[0].(map[string]any)["shortDescription"].(map[string]any)["text"])

	location := r["locations"].([]any)[0].(map[string]any)
	physical := location["physicalLocation"].(map[string]any)
	assert.Equal(t, "flows/a.yaml", physical["artifactLocation"].(map[string]any)["uri"])
	assert.Equal(t, map[string]any{"startLine": 7.0, "startColumn": 5.0}, physical["region"])
	logical := location["logicalLocations"].([]any)[0].(map[string]any)
	assert.Equal(t, "page", logical["name"])
	assert.Equal(t, "pages/page", logical["fullyQualifiedName"])

	related := r["relatedLocations"].([]any)[0].(map[string]any)
	assert.Equal(t, "flows/b.yaml", related["physicalLocation"].(map[string]any)["artifactLocation"].(map[string]any)["uri"])
	assert.Equal(t, "compiled because step calls it", related["message"].(map[string]any)["text"])

	fix := r["fixes"].([]any)[0].(map[string]any)
	assert.Equal(t, "rename it", fix["description"].(map[string]any)["text"])
	replacement := fix["artifactChanges"].([]any)[0].(map[string]any)["replacements"].([]any)[0].(map[string]any)
	assert.Equal(t, map[string]any{"startLine": 7.0, "startColumn": 5.0, "endLine": 7.0, "endColumn": 9.0}, replacement["deletedRegion"])
	assert.Equal(t, "http", replacement["insertedContent"].(map[string]any)["text"])
}

// SARIF has no line zero. A diagnostic that only has an address is written with its
// logical location and no region, never a guessed line.
func TestMarshalOmitsAPositionItDoesNotHave(t *testing.T) {
	t.Parallel()

	doc := decode(t, []*v1.DiagnosticReport{{
		File:        "spec.json",
		Diagnostics: []*v1.Diagnostic{{Message: "bad", Path: []string{"second"}}, {Message: "whole document"}},
	}})
	got := results(t, firstRun(t, doc))
	require.Len(t, got, 2)

	for _, r := range got {
		physical := r["locations"].([]any)[0].(map[string]any)["physicalLocation"].(map[string]any)
		assert.NotContains(t, physical, "region", "no line is known")
		assert.Equal(t, "spec.json", physical["artifactLocation"].(map[string]any)["uri"])
	}
	assert.Equal(t, string(v1.DiagnosticCodeGeneral), got[1]["ruleId"], "an empty code is the general class")
}

func TestMarshalSpellsFileNamesAsURIReferences(t *testing.T) {
	t.Parallel()

	for name, want := range map[string]string{
		"a b/c#d.yaml":      "a%20b/c%23d.yaml",
		"/work/flow.yaml":   "file:///work/flow.yaml",
		"./relative/f.yaml": "./relative/f.yaml",
		"C:/work/f.yaml":    "file:///C:/work/f.yaml",
		"//host/share/f.y":  "file:////host/share/f.y",
	} {
		if runtime.GOOS == "windows" && name == "//host/share/f.y" {
			want = "file://host/share/f.y" // a local UNC path names its host
		}
		doc := decode(t, []*v1.DiagnosticReport{{File: name, Diagnostics: []*v1.Diagnostic{{Message: "x", Line: 1}}}})
		physical := results(t, firstRun(t, doc))[0]["locations"].([]any)[0].(map[string]any)["physicalLocation"].(map[string]any)
		assert.Equal(t, want, physical["artifactLocation"].(map[string]any)["uri"], name)
	}
}

func TestMarshalDescribesACodeTheRegistryDoesNotKnow(t *testing.T) {
	t.Parallel()

	doc := decode(t, []*v1.DiagnosticReport{{File: "f", Diagnostics: []*v1.Diagnostic{
		{Message: "a", Code: "from-a-newer-producer"},
		{Message: "b", Code: string(v1.DiagnosticCodeUnknownTask)},
	}}})
	run := firstRun(t, doc)
	rules := run["tool"].(map[string]any)["driver"].(map[string]any)["rules"].([]any)
	require.Len(t, rules, 2)
	for _, r := range results(t, run) {
		index := int(r["ruleIndex"].(float64))
		assert.Equal(t, r["ruleId"], rules[index].(map[string]any)["id"], "every ruleIndex names its rule")
	}
}

func TestMarshalBoundsTheResultsAndSaysSo(t *testing.T) {
	t.Parallel()

	many := make([]*v1.Diagnostic, sarif.MaxResults+7)
	for i := range many {
		many[i] = &v1.Diagnostic{Message: "x", Line: 1}
	}
	doc := decode(t, []*v1.DiagnosticReport{{File: "f", Diagnostics: many}})
	run := firstRun(t, doc)

	assert.Len(t, results(t, run), sarif.MaxResults)
	assert.Equal(t, "unicodeCodePoints", run["columnKind"], "Flowstate columns count code points")
	note := run["invocations"].([]any)[0].(map[string]any)["toolExecutionNotifications"].([]any)[0].(map[string]any)
	assert.Contains(t, note["message"].(map[string]any)["text"], "7 further results")
}

func TestMarshalBoundsTheTextAndSaysSo(t *testing.T) {
	t.Parallel()

	// Each message is 1 MiB: the byte budget, not the result count, stops the run.
	big := strings.Repeat("a", 1<<20)
	var diagnostics []*v1.Diagnostic
	for range 40 {
		diagnostics = append(diagnostics, &v1.Diagnostic{Message: big[:1<<20], Line: 1})
	}
	data, err := sarif.Marshal([]*v1.DiagnosticReport{{File: "f", Diagnostics: diagnostics}, {File: "g", Diagnostics: diagnostics}}, tool)
	require.NoError(t, err)
	assert.Less(t, len(data), sarif.MaxBytes+1<<20)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(data, &doc))
	run := firstRun(t, doc)
	assert.Len(t, results(t, run), 15, "the 16th MiB message passes the budget")
	note := run["invocations"].([]any)[0].(map[string]any)["toolExecutionNotifications"].([]any)[0].(map[string]any)
	assert.Contains(t, note["message"].(map[string]any)["text"], "65 further results", "the rest of both reports is counted, not projected")
}

func TestMarshalOffersNoFixWithoutAPosition(t *testing.T) {
	t.Parallel()

	edit := &v1.SuggestedEdit{Title: "t", Changes: []*v1.TextChange{{NewText: "x"}}}
	doc := decode(t, []*v1.DiagnosticReport{{File: "f", Diagnostics: []*v1.Diagnostic{{Message: "m", Edits: []*v1.SuggestedEdit{edit}}}}})

	assert.NotContains(t, results(t, firstRun(t, doc))[0], "fixes")
}

func TestMarshalIsDeterministic(t *testing.T) {
	t.Parallel()

	reports := []*v1.DiagnosticReport{{File: "f", Diagnostics: []*v1.Diagnostic{
		{Message: "a", Code: "zz"}, {Message: "b", Code: "aa"}, {Message: "c", Code: "mm"},
	}}}
	first, err := sarif.Marshal(reports, tool)
	require.NoError(t, err)
	for range 5 {
		again, err := sarif.Marshal(reports, tool)
		require.NoError(t, err)
		require.Equal(t, string(first), string(again))
	}
}
