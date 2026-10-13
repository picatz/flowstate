package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValidateReportsCalleeUnderItsOwnFile pins #1555 at the command: a defect in
// a called file is printed and reported under that file, at its own line and
// column, and the caller says once that its callee has problems.
//
// callee/ is the shape whose default breaks its own `must:` (lib/callee.yaml:6:14,
// found by validating the compiled callee); callee-cycle/ is the one that also
// calls itself (lib/callee.yaml:10:11, found while compiling it, which stops the
// compile before validation could reach line 6).
func TestValidateReportsCalleeUnderItsOwnFile(t *testing.T) {
	type report struct {
		File        string `json:"file"`
		Diagnostics []struct {
			Line    int    `json:"line"`
			Column  int    `json:"column"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"diagnostics"`
	}
	type document struct {
		Files []report `json:"files"`
	}

	for _, tc := range []struct {
		fixture      string
		line, column int
		code         string
	}{
		{"callee", 6, 14, ""},
		{"callee-cycle", 10, 11, "call-cycle"},
	} {
		t.Run(tc.fixture, func(t *testing.T) {
			t.Chdir(filepath.Join("testdata", "validate", tc.fixture))

			text := runFlow(t, "validate", "workflow.yaml")
			assert.Equal(t, exitCodeFailure, text.ExitCode)
			assert.Contains(t, text.Stdout, "lib/callee.yaml:"+strconv.Itoa(tc.line)+":"+strconv.Itoa(tc.column)+": ",
				"the callee's defect is printed under the callee's file")
			assert.NotContains(t, text.Stdout, "workflow.yaml:"+strconv.Itoa(tc.line)+":",
				"a callee's position must not be printed under the caller's name")
			assert.Equal(t, 1, strings.Count(text.Stdout, "reported under lib/callee.yaml"),
				"the caller says once that its callee has problems: %s", text.Stdout)

			asJSON := runFlow(t, "validate", "workflow.yaml", "-o", "json")
			assert.Equal(t, exitCodeFailure, asJSON.ExitCode)
			var doc document
			require.NoError(t, json.Unmarshal([]byte(asJSON.Stdout), &doc), asJSON.Stdout)
			require.Len(t, doc.Files, 2, asJSON.Stdout)
			assert.Equal(t, "workflow.yaml", doc.Files[0].File)
			assert.Equal(t, "lib/callee.yaml", doc.Files[1].File)

			require.Len(t, doc.Files[0].Diagnostics, 1, "the caller's single callee-failed")
			failed := doc.Files[0].Diagnostics[0]
			assert.Equal(t, "callee-failed", failed.Code)
			assert.Equal(t, [2]int{5, 11}, [2]int{failed.Line, failed.Column})

			require.Len(t, doc.Files[1].Diagnostics, 1)
			callee := doc.Files[1].Diagnostics[0]
			assert.Equal(t, [2]int{tc.line, tc.column}, [2]int{callee.Line, callee.Column})
			if tc.code != "" {
				assert.Equal(t, tc.code, callee.Code)
			}

			asJSONL := runFlow(t, "validate", "workflow.yaml", "-o", "jsonl")
			assert.Equal(t, exitCodeFailure, asJSONL.ExitCode)
			lines := strings.Split(strings.TrimSpace(asJSONL.Stdout), "\n")
			require.Len(t, lines, 2, asJSONL.Stdout)
			var first, second report
			require.NoError(t, json.Unmarshal([]byte(lines[0]), &first))
			require.NoError(t, json.Unmarshal([]byte(lines[1]), &second))
			assert.Equal(t, "workflow.yaml", first.File)
			assert.Equal(t, "lib/callee.yaml", second.File)

			// Naming the callee as well merges into its own entry rather than
			// adding a second one.
			both := runFlow(t, "validate", "lib/callee.yaml", "workflow.yaml", "-o", "json")
			var merged document
			require.NoError(t, json.Unmarshal([]byte(both.Stdout), &merged), both.Stdout)
			names := make([]string, 0, len(merged.Files))
			for _, f := range merged.Files {
				names = append(names, f.File)
			}
			assert.Equal(t, []string{"lib/callee.yaml", "workflow.yaml"}, names)
		})
	}
}

// TestValidateMergesSymlinkedCalleeGivenByLinkName holds that a callee named on
// the command line through a symlink and the same file derived from its caller
// (named by the file actually read) are one report, under the name the user gave.
func TestValidateMergesSymlinkedCalleeGivenByLinkName(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join("testdata", "validate", "callee")
	for _, f := range []struct{ from, to string }{
		{"workflow.yaml", "workflow.yaml"},
		{filepath.Join("lib", "callee.yaml"), filepath.Join("shared", "real.yaml")},
	} {
		data, err := os.ReadFile(filepath.Join(src, f.from))
		require.NoError(t, err)
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(dir, f.to)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, f.to), data, 0o644))
	}
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "lib"), 0o755))
	require.NoError(t, os.Symlink(filepath.Join("..", "shared", "real.yaml"), filepath.Join(dir, "lib", "callee.yaml")))
	t.Chdir(dir)

	got := runFlow(t, "validate", "-o", "jsonl", "workflow.yaml", "lib/callee.yaml")
	assert.Equal(t, exitCodeFailure, got.ExitCode)
	var names []string
	for line := range strings.SplitSeq(strings.TrimSpace(got.Stdout), "\n") {
		var r struct {
			File string `json:"file"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &r), line)
		names = append(names, r.File)
	}
	assert.Equal(t, []string{"workflow.yaml", "lib/callee.yaml"}, names, "one report for the file, under the name given: %s", got.Stdout)
}
