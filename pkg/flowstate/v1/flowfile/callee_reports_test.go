package flowfile_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The two fixtures of #1555. Both callees declare `shards` with a default its own
// `must:` refuses (line 6, column 14 is the default); the first also calls itself
// (line 10), which stops the compile before validation can reach the default.
const (
	calleeHead = "edition: v2026.4\nname: callee\ninputs:\n  shards:\n    type: int\n    default: 0\n    must: this >= 1\nsteps:\n"

	selfCallingCallee = calleeHead + "  - id: loop\n    call: ./callee.yaml\n"
	plainCallee       = calleeHead + "  - id: work\n    log:\n      message: hi\n"

	issueCaller = "edition: v2026.4\nname: caller\nsteps:\n  - id: place\n    call: ./lib/callee.yaml\n"
)

// reportsOf validates the file at name, relative to dir, as `flow validate name`
// would from dir, and returns the reports grouped by file.
func reportsOf(t *testing.T, dir, name string) []*v1.DiagnosticReport {
	t.Helper()
	t.Chdir(dir)

	// A file that does not compile answers with its diagnostics as the error, as
	// `flow validate` reads them.
	ds, err := flowfile.ValidateSourceFile(name)
	if err != nil {
		require.ErrorAs(t, err, &ds, "only compile diagnostics are expected here: %v", err)
	}

	return ds.Reports(name)
}

func reportFor(t *testing.T, reports []*v1.DiagnosticReport, file string) *v1.DiagnosticReport {
	t.Helper()
	for _, r := range reports {
		if r.GetFile() == file {
			return r
		}
	}
	require.Failf(t, "no report", "no report for %s among %v", file, reportNames(reports))
	return nil
}

func reportNames(reports []*v1.DiagnosticReport) []string {
	names := make([]string, 0, len(reports))
	for _, r := range reports {
		names = append(names, r.GetFile())
	}
	return names
}

func codesOf(r *v1.DiagnosticReport) []string {
	codes := make([]string, 0, len(r.GetDiagnostics()))
	for _, d := range r.GetDiagnostics() {
		codes = append(codes, d.GetCode())
	}
	return codes
}

func validAgainstSchema(t *testing.T, reports []*v1.DiagnosticReport) {
	t.Helper()
	validator, err := protovalidate.New()
	require.NoError(t, err)
	for _, r := range reports {
		require.NoError(t, validator.Validate(r), "report for %s must satisfy the schema", r.GetFile())
	}
}

// TestCalleeDiagnosticsReportUnderCalleeFile pins #1555: a defect found while
// compiling or validating a callee is reported under the callee's file, at the
// callee's own line and column, with the call that reached it as its reason; the
// caller says only that its callee has problems.
func TestCalleeDiagnosticsReportUnderCalleeFile(t *testing.T) {
	t.Run("a self-calling callee fails to compile under its own file", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "workflow.yaml", issueCaller)
		writeFile(t, dir, "lib/callee.yaml", selfCallingCallee)

		reports := reportsOf(t, dir, "workflow.yaml")
		require.Equal(t, []string{"workflow.yaml", "lib/callee.yaml"}, reportNames(reports))
		validAgainstSchema(t, reports)

		callee := reportFor(t, reports, "lib/callee.yaml")
		require.Len(t, callee.GetDiagnostics(), 1)
		cycle := callee.GetDiagnostics()[0]
		require.Equal(t, "call-cycle", cycle.GetCode())
		require.Equal(t, [2]uint32{10, 11}, [2]uint32{cycle.GetLine(), cycle.GetColumn()})
		require.Equal(t, "loop", cycle.GetStep())
		require.Contains(t, cycle.GetMessage(), "calls itself,", "a direct self-call is not a chain of files")
		require.NotContains(t, cycle.GetMessage(), "through a chain")
		require.Len(t, cycle.GetRelated(), 1)
		require.Equal(t, "workflow.yaml", cycle.GetRelated()[0].GetFile())
		require.Equal(t, [2]uint32{5, 11}, [2]uint32{cycle.GetRelated()[0].GetLine(), cycle.GetRelated()[0].GetColumn()})

		caller := reportFor(t, reports, "workflow.yaml")
		require.Len(t, caller.GetDiagnostics(), 1, "one diagnostic, not a copy of the callee's text")
		failed := caller.GetDiagnostics()[0]
		require.Equal(t, "callee-failed", failed.GetCode())
		require.Equal(t, [2]uint32{5, 11}, [2]uint32{failed.GetLine(), failed.GetColumn()})
		require.Equal(t, "place", failed.GetStep())
		require.Contains(t, failed.GetMessage(), "lib/callee.yaml")
		require.NotContains(t, failed.GetMessage(), "calls itself")
		require.Len(t, failed.GetRelated(), 1)
		require.Equal(t, "lib/callee.yaml", failed.GetRelated()[0].GetFile())
		require.Equal(t, [2]uint32{10, 11}, [2]uint32{failed.GetRelated()[0].GetLine(), failed.GetRelated()[0].GetColumn()})
	})

	t.Run("a callee whose default breaks its own must is reported at that default", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "workflow.yaml", issueCaller)
		writeFile(t, dir, "lib/callee.yaml", plainCallee)

		reports := reportsOf(t, dir, "workflow.yaml")
		require.Equal(t, []string{"workflow.yaml", "lib/callee.yaml"}, reportNames(reports))
		validAgainstSchema(t, reports)

		callee := reportFor(t, reports, "lib/callee.yaml")
		require.Len(t, callee.GetDiagnostics(), 1, "%v", callee.GetDiagnostics())
		must := callee.GetDiagnostics()[0]
		require.Equal(t, [2]uint32{6, 14}, [2]uint32{must.GetLine(), must.GetColumn()})
		require.Equal(t, "inputs.shards.default", must.GetField(), "the callee's own field path survives")
		require.Contains(t, must.GetMessage(), "`this >= 1`")
		require.NotContains(t, must.GetMessage(), `workflow "callee":`, "no caller-side prefix once it sits under its own file")
		require.Len(t, must.GetRelated(), 1)
		require.Equal(t, "workflow.yaml", must.GetRelated()[0].GetFile())
		require.Equal(t, [2]uint32{5, 11}, [2]uint32{must.GetRelated()[0].GetLine(), must.GetRelated()[0].GetColumn()})
		require.Contains(t, must.GetRelated()[0].GetMessage(), `step "place"`)

		caller := reportFor(t, reports, "workflow.yaml")
		require.Equal(t, []string{"callee-failed"}, codesOf(caller))
		failed := caller.GetDiagnostics()[0]
		require.Equal(t, [2]uint32{5, 11}, [2]uint32{failed.GetLine(), failed.GetColumn()})
		require.Equal(t, "place", failed.GetStep())
		require.Contains(t, failed.GetMessage(), "reported under lib/callee.yaml")
		require.NotContains(t, failed.GetMessage(), "this >= 1", "a pointer, not a copy")
		require.Len(t, failed.GetRelated(), 1)
		require.Equal(t, "lib/callee.yaml", failed.GetRelated()[0].GetFile())
		require.Equal(t, [2]uint32{6, 14}, [2]uint32{failed.GetRelated()[0].GetLine(), failed.GetRelated()[0].GetColumn()})
	})

	t.Run("a clean callee adds no report", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "workflow.yaml", issueCaller)
		writeFile(t, dir, "lib/callee.yaml", strings.Replace(plainCallee, "default: 0", "default: 1", 1))

		reports := reportsOf(t, dir, "workflow.yaml")
		require.Equal(t, []string{"workflow.yaml"}, reportNames(reports))
		require.Empty(t, reports[0].GetDiagnostics())
	})

	for _, route := range []struct {
		name, callee string
		code         string
	}{
		{"compile route", selfCallingCallee, "call-cycle"},
		{"validate route", plainCallee, ""},
	} {
		t.Run("a diamond keeps one callee report with both call sites, "+route.name, func(t *testing.T) {
			dir := t.TempDir()
			caller := "edition: v2026.4\nname: caller\nsteps:\n" +
				"  - id: left\n    call: ./lib/callee.yaml\n" +
				"  - id: right\n    call: ./lib/callee.yaml\n"
			writeFile(t, dir, "workflow.yaml", caller)
			writeFile(t, dir, "lib/callee.yaml", route.callee)

			reports := reportsOf(t, dir, "workflow.yaml")
			require.Equal(t, []string{"workflow.yaml", "lib/callee.yaml"}, reportNames(reports))
			validAgainstSchema(t, reports)

			callee := reportFor(t, reports, "lib/callee.yaml")
			require.Len(t, callee.GetDiagnostics(), 1, "one finding however many sites call it: %v", callee.GetDiagnostics())
			related := callee.GetDiagnostics()[0].GetRelated()
			require.Len(t, related, 2)
			require.Equal(t, [2]uint32{5, 11}, [2]uint32{related[0].GetLine(), related[0].GetColumn()})
			require.Equal(t, [2]uint32{7, 11}, [2]uint32{related[1].GetLine(), related[1].GetColumn()})
			require.Contains(t, related[0].GetMessage(), `step "left"`)
			require.Contains(t, related[1].GetMessage(), `step "right"`)

			// Each site still says so itself.
			callerReport := reportFor(t, reports, "workflow.yaml")
			require.Equal(t, []string{"callee-failed", "callee-failed"}, codesOf(callerReport))
			require.Equal(t, []string{"left", "right"}, []string{callerReport.GetDiagnostics()[0].GetStep(), callerReport.GetDiagnostics()[1].GetStep()})
		})
	}
}

// TestSymlinkedCalleeReportsItsOwnCallsWhereTheyWereRead holds that a callee's
// own `call:` is named by the file the compiler read. lib/callee.yaml is a
// symlink to shared/real.yaml, so the `./helper.yaml` it calls is
// shared/helper.yaml, and a problem there must not be filed under lib/helper.yaml.
func TestSymlinkedCalleeReportsItsOwnCallsWhereTheyWereRead(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "workflow.yaml", issueCaller)
	writeFile(t, dir, "shared/real.yaml", calleeHead[:strings.Index(calleeHead, "inputs:")]+"steps:\n"+"  - id: help\n    call: ./helper.yaml\n")
	writeFile(t, dir, "shared/helper.yaml", plainCallee)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "lib"), 0o755))
	require.NoError(t, os.Symlink("../shared/real.yaml", filepath.Join(dir, "lib", "callee.yaml")))

	reports := reportsOf(t, dir, "workflow.yaml")

	names := reportNames(reports)
	require.Contains(t, names, "shared/helper.yaml", "reported where the compiler read it")
	require.NotContains(t, names, "lib/helper.yaml", "a file that does not exist")
	require.Len(t, reportFor(t, reports, "shared/helper.yaml").GetDiagnostics(), 1)
}

// TestMergeReportsDoesNotMutateDerivedReports holds the documented promise that
// neither input changes: a derived diagnostic merged into a given report must be
// copied, or a later derived report's call site would be added to it.
func TestMergeReportsDoesNotMutateDerivedReports(t *testing.T) {
	site := func(line uint32) *v1.RelatedLocation {
		return &v1.RelatedLocation{File: "w.yaml", Line: line, Message: "m"}
	}
	finding := func(related ...*v1.RelatedLocation) *v1.Diagnostic {
		return &v1.Diagnostic{Message: "bad", Line: 1, Column: 1, Related: related}
	}
	given := []*v1.DiagnosticReport{{File: "w.yaml"}}
	first := &v1.DiagnosticReport{File: "w.yaml", Diagnostics: []*v1.Diagnostic{finding(site(1))}}
	second := &v1.DiagnosticReport{File: "w.yaml", Diagnostics: []*v1.Diagnostic{finding(site(2))}}

	merged := flowfile.MergeReports(given, []*v1.DiagnosticReport{first, second})

	require.Len(t, merged[0].GetDiagnostics(), 1)
	require.Len(t, merged[0].GetDiagnostics()[0].GetRelated(), 2, "both sites are kept in the result")
	require.Len(t, first.GetDiagnostics()[0].GetRelated(), 1, "the derived input is left as it was")
}
