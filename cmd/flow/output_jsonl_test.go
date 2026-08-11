package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// picatz/flowstate#396: `-o jsonl` promises one JSON value per line, and the
// promise held for most of the sixteen verbs carrying `--output` but not for
// `flow tasks`, whose catalog and single-task views wrote the same pretty-printed
// document `-o json` does, with a hardcoded FormatJSON at the two call sites in
// runTasks rather than the format the caller asked for. `flow plugins` had the
// identical mistake, at both of its own call sites: the unconfigured-worker
// shortcut and the populated-catalog path.
//
// The pin below is deliberately the property from the issue rather than the
// mechanism: any line of jsonl output parses alone as JSON, so a fix that repairs
// tasks and plugins by any means, and a regression that reintroduces the mistake
// on any verb reachable without a server, are both caught here: see
// requireEachLineIsOneJSONValue.

// requireEachLineIsOneJSONValue is the golden test #396 asks for: every line of
// jsonl output parses alone as JSON, and there is at least one line, so a writer
// that emits nothing does not pass by vacuity.
func requireEachLineIsOneJSONValue(t *testing.T, stdout string) {
	t.Helper()

	trimmed := strings.TrimRight(stdout, "\n")
	require.NotEmpty(t, trimmed, "jsonl output was empty")

	lines := strings.Split(trimmed, "\n")
	for i, line := range lines {
		require.Truef(t, json.Valid([]byte(line)),
			"line %d of jsonl output is not a single JSON value on its own: %q", i+1, line)
	}
}

// TestTasksJSONLIsLineDelimited is the direct reproduction of the issue: `flow
// tasks -o jsonl` used to be byte-identical to `-o json` (570 pretty-printed
// lines for one catalog), rather than the same document compacted to one line.
//
// Mutation-proven: reverting runTasks's two writeJSON(surface, format, ...) call
// sites back to the hardcoded writeJSON(surface, FormatJSON, ...) they were
// before the fix makes this fail with more than one line.
func TestTasksJSONLIsLineDelimited(t *testing.T) {
	rendered := runTasksInto(t, "jsonl")

	requireEachLineIsOneJSONValue(t, rendered)

	lines := strings.Split(strings.TrimRight(rendered, "\n"), "\n")
	assert.Len(t, lines, 1,
		"the catalog is one document; jsonl should compact it to one line, not stream it")
}

// TestTasksNamedTaskJSONLIsLineDelimited covers the second call site in
// runTasks: naming one task (`flow tasks http -o jsonl`) narrows the document,
// but it is still a single document and carried the same hardcoded FormatJSON.
func TestTasksNamedTaskJSONLIsLineDelimited(t *testing.T) {
	rendered := runTasksInto(t, "jsonl", "http")

	requireEachLineIsOneJSONValue(t, rendered)

	lines := strings.Split(strings.TrimRight(rendered, "\n"), "\n")
	assert.Len(t, lines, 1,
		"one described task is one document; jsonl should be that document on one line")
}

// TestTasksJSONAndJSONLAreTheSameDocument pins the relationship #396 asks every
// single-document verb to hold: jsonl is not a different answer, it is the same
// one compacted.
func TestTasksJSONAndJSONLAreTheSameDocument(t *testing.T) {
	asJSON := runTasksInto(t, "json")
	asJSONL := runTasksInto(t, "jsonl")

	var fromJSON, fromJSONL any
	require.NoError(t, json.Unmarshal([]byte(asJSON), &fromJSON))
	require.NoError(t, json.Unmarshal([]byte(strings.TrimRight(asJSONL, "\n")), &fromJSONL))

	assert.Equal(t, fromJSON, fromJSONL,
		"`-o json` and `-o jsonl` disagree on the catalog's content")
}

// TestPluginsJSONLIsLineDelimitedWhenUnconfigured covers the shortcut path in
// runPlugins: no --plugin-dir configured answers with an empty
// *v1.PluginCatalog{}, which also carried the hardcoded FormatJSON.
func TestPluginsJSONLIsLineDelimitedWhenUnconfigured(t *testing.T) {
	t.Setenv(pluginSearchPathEnv, "")

	stdout, _, err := runPluginsInto(t, "jsonl")
	require.NoError(t, err)

	requireEachLineIsOneJSONValue(t, stdout)

	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	assert.Len(t, lines, 1,
		"the empty catalog is one document; jsonl should be that document on one line")
}

// TestPluginsJSONLIsLineDelimitedWithADirectory covers runPlugins's second call
// site: a configured, empty plugin directory, which reaches host.Catalog() and
// the second hardcoded writeJSON(surface, FormatJSON, catalog).
func TestPluginsJSONLIsLineDelimitedWithADirectory(t *testing.T) {
	dir := t.TempDir()

	stdout, _, err := runPluginsInto(t, "jsonl", "plugin-dir", dir)
	require.NoError(t, err)

	requireEachLineIsOneJSONValue(t, stdout)

	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	assert.Len(t, lines, 1,
		"a plugin catalog is one document; jsonl should be that document on one line")
}

// jsonlSurface is one verb reachable in-process (no Temporal, no network) whose
// jsonl output this test enumerates and checks, per #396's own instruction to
// sweep every verb rather than trust the one reported. addOutputFlag's sixteen
// call sites are cmd/flow/output.go:40-130's `-o` verbs; the ones omitted here
// (run, run local, get, signal, watch, lifecycle list/cancel/terminate, schedule
// create/list/describe/delete/pause/resume/trigger, server dev) all need a
// running server, a Temporal connection, or a live process and are covered
// instead by their own package's tests (compile_test.go, fixfmt_output_test.go,
// validate_output_test.go, mutation_output_test.go, runlocal_output_test.go,
// version_test.go, watch_test.go, lifecycle_test.go) exactly as listed in each
// comment there.
func TestJSONLSurfacesEnumerated(t *testing.T) {
	for _, test := range []struct {
		name   string
		render func(t *testing.T) string
	}{
		{"tasks catalog", func(t *testing.T) string {
			return runTasksInto(t, "jsonl")
		}},
		{"tasks named", func(t *testing.T) string {
			return runTasksInto(t, "jsonl", "http")
		}},
		{"plugins unconfigured", func(t *testing.T) string {
			t.Setenv(pluginSearchPathEnv, "")
			stdout, _, err := runPluginsInto(t, "jsonl")
			require.NoError(t, err)
			return stdout
		}},
		{"plugins configured", func(t *testing.T) string {
			dir := t.TempDir()
			stdout, _, err := runPluginsInto(t, "jsonl", "plugin-dir", dir)
			require.NoError(t, err)
			return stdout
		}},
		{"compile", func(t *testing.T) string {
			path := writeWorkflow(t, "jsonl-sweep.yaml", cleanWorkflow)
			asJSONL, _, err := compileOutput(t, path, "-o", "jsonl")
			require.NoError(t, err)
			return asJSONL
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireEachLineIsOneJSONValue(t, test.render(t))
		})
	}
}
