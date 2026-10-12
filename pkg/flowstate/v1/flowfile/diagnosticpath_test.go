package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A diagnostic on a step names the step by the chain of ids the debugger's source
// map keys on, so a program can address it without a line number (#1568). The
// line and column keep being computed as they were.

const nestedDefect = `edition: v2026.4
name: nested-defect
steps:
  - id: pages
    for_each:
      items: ${["a"]}
      as: page
      steps:
        - id: fetch
          log:
            message: ${nosuchstep.output}
`

func pathDiagnostics(t *testing.T, src string) flowfile.Diagnostics {
	t.Helper()

	ds, err := flowfile.ValidateSource([]byte(src))
	if err != nil {
		var carried flowfile.Diagnostics
		require.ErrorAs(t, err, &carried)

		return carried
	}

	return ds
}

func TestDiagnosticPathNamesNestedStep(t *testing.T) {
	ds := pathDiagnostics(t, nestedDefect)

	var found *flowfile.Diagnostic
	for i := range ds {
		if ds[i].Step == "fetch" {
			found = &ds[i]
		}
	}
	require.NotNil(t, found, "want a diagnostic on step fetch, got %v", ds)

	assert.Equal(t, []string{"pages", "fetch"}, found.Path)
	assert.Equal(t, []string{"pages", "fetch"}, found.Proto().GetPath())
	// Position is untouched by the path: still the line the source puts it on.
	assert.Positive(t, found.Line)
	assert.Positive(t, found.Column)
	assert.Contains(t, strings.Split(nestedDefect, "\n")[found.Line-1], "message")
}

func TestDiagnosticPathEmptyWhenStepIDIsAmbiguous(t *testing.T) {
	src := `edition: v2026.4
name: ambiguous
steps:
  - id: first
    for_each:
      items: ${["a"]}
      as: item
      steps:
        - id: dup
          log:
            message: ${nosuchstep.output}
  - id: second
    for_each:
      items: ${["a"]}
      as: item
      steps:
        - id: dup
          log:
            message: ${nosuchstep.output}
`
	var seen int
	for _, d := range pathDiagnostics(t, src) {
		if d.Step == "dup" {
			seen++
			assert.Empty(t, d.Path, "a step id declared in two scopes names no unique site")
		}
	}
	require.Positive(t, seen, "the fixture must raise a diagnostic on the reused id")
}

func TestDiagnosticPathSurvivesProtoRoundTrip(t *testing.T) {
	d := flowfile.Diagnostic{Message: "m", Step: "b", Path: []string{"a", "b"}}

	report := flowfile.Diagnostics{d}.Report("f.yaml")
	require.Equal(t, []string{"a", "b"}, report.GetDiagnostics()[0].GetPath())

	d.Path[0] = "mutated"
	assert.Equal(t, []string{"a", "b"}, report.GetDiagnostics()[0].GetPath(), "the proto must not alias the Go slice")
}

func TestDiagnosticPathIsBounded(t *testing.T) {
	path := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = "s"
		}

		return out
	}
	require.NoError(t, v1.Validate(&v1.Diagnostic{Message: "m", Path: path(129)}))
	require.Error(t, v1.Validate(&v1.Diagnostic{Message: "m", Path: path(130)}), "130 ids exceed the bound")
	require.Error(t, v1.Validate(&v1.Diagnostic{Message: "m", Path: []string{strings.Repeat("x", 257)}}), "an id over 256 bytes")
}
