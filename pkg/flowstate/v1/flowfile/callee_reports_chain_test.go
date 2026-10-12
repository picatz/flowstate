package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// manySites is a caller whose n steps all call the one callee.
func manySites(n int) string {
	var b strings.Builder
	b.WriteString("edition: v2026.4\nname: caller\nsteps:\n")
	for i := range n {
		fmt.Fprintf(&b, "  - id: site%d\n    call: ./lib/callee.yaml\n", i)
	}
	return b.String()
}

// TestCalleeDiagnosticsReportBoundsRelatedLocations holds the bound invariant 5
// asks for: a callee called from more places than the schema allows keeps the
// first of them in source order, still reports once, and every call site still
// says so itself.
func TestCalleeDiagnosticsReportBoundsRelatedLocations(t *testing.T) {
	const sites = 20

	for _, route := range []struct{ name, callee string }{
		{"compile route", selfCallingCallee},
		{"validate route", plainCallee},
	} {
		t.Run(route.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, dir, "workflow.yaml", manySites(sites))
			writeFile(t, dir, "lib/callee.yaml", route.callee)

			reports := reportsOf(t, dir, "workflow.yaml")
			validAgainstSchema(t, reports)

			callee := reportFor(t, reports, "lib/callee.yaml")
			require.Len(t, callee.GetDiagnostics(), 1)
			related := callee.GetDiagnostics()[0].GetRelated()
			require.Len(t, related, 16, "bounded at the schema's max_items")
			for i, r := range related {
				require.Equal(t, uint32(5+2*i), r.GetLine(), "the first sixteen sites in source order")
				require.Contains(t, r.GetMessage(), fmt.Sprintf("step %q", fmt.Sprintf("site%d", i)))
			}

			caller := reportFor(t, reports, "workflow.yaml")
			require.Len(t, caller.GetDiagnostics(), sites, "every site still reports its own callee")
		})
	}

	t.Run("a callee is retained once however many sites call it", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "workflow.yaml", manySites(sites))
		writeFile(t, dir, "lib/callee.yaml", selfCallingCallee)
		t.Chdir(dir)

		var ds flowfile.Diagnostics
		_, _, err := flowfile.ParseFile("workflow.yaml")
		require.ErrorAs(t, err, &ds)

		held := 0
		for _, d := range ds {
			if d.File != "" {
				held++
			}
		}
		require.Equal(t, 1, held, "the compile itself keeps one copy, not one per site")
		require.Len(t, ds, sites+1)
	})

	t.Run("the schema refuses a seventeenth", func(t *testing.T) {
		d := &v1.Diagnostic{Message: "m"}
		for i := range 17 {
			d.Related = append(d.Related, &v1.RelatedLocation{File: "a.yaml", Line: uint32(i + 1), Message: "m"})
		}
		validator, err := protovalidate.New()
		require.NoError(t, err)
		require.Error(t, validator.Validate(d))

		d.Related = d.Related[:16]
		require.NoError(t, validator.Validate(d))
	})
}

// A three-deep chain reports each file once, nearest first, each problem under
// the file that holds it, and each with its immediate caller as the reason.
func TestCalleeDiagnosticsReportThroughAChain(t *testing.T) {
	for _, route := range []struct {
		name, leaf string
		line, col  uint32
	}{
		{"compile route", "edition: v2026.4\nname: leaf\nsteps:\n  - id: x\n    bogus_key: 1\n", 5, 16},
		{"validate route", plainCallee, 6, 14},
	} {
		t.Run(route.name, func(t *testing.T) {
			dir := t.TempDir()
			writeFile(t, dir, "a.yaml", "edition: v2026.4\nname: a\nsteps:\n  - id: toB\n    call: ./lib/b.yaml\n")
			writeFile(t, dir, "lib/b.yaml", "edition: v2026.4\nname: b\nsteps:\n  - id: toC\n    call: ./sub/c.yaml\n")
			writeFile(t, dir, "lib/sub/c.yaml", route.leaf)

			reports := reportsOf(t, dir, "a.yaml")
			require.Equal(t, []string{"a.yaml", "lib/b.yaml", "lib/sub/c.yaml"}, reportNames(reports))
			validAgainstSchema(t, reports)

			a, b, c := reports[0], reports[1], reports[2]
			require.Equal(t, []string{"callee-failed"}, codesOf(a))
			require.Equal(t, []string{"callee-failed"}, codesOf(b))
			require.Len(t, c.GetDiagnostics(), 1)

			require.Equal(t, [2]uint32{5, 11}, [2]uint32{a.GetDiagnostics()[0].GetLine(), a.GetDiagnostics()[0].GetColumn()})
			require.Equal(t, "toB", a.GetDiagnostics()[0].GetStep())
			require.Contains(t, a.GetDiagnostics()[0].GetMessage(), "reported under lib/b.yaml")

			// B's own call to C sits at B's line 5, and that is where it says so.
			require.Equal(t, [2]uint32{5, 11}, [2]uint32{b.GetDiagnostics()[0].GetLine(), b.GetDiagnostics()[0].GetColumn()})
			require.Equal(t, "toC", b.GetDiagnostics()[0].GetStep())
			require.Contains(t, b.GetDiagnostics()[0].GetMessage(), "reported under lib/sub/c.yaml")
			require.Equal(t, []string{"lib/sub/c.yaml", "a.yaml"}, []string{b.GetDiagnostics()[0].GetRelated()[0].GetFile(), b.GetDiagnostics()[0].GetRelated()[1].GetFile()}, "the first problem, then why B was compiled: A calls it")

			leaf := c.GetDiagnostics()[0]
			require.Equal(t, [2]uint32{route.line, route.col}, [2]uint32{leaf.GetLine(), leaf.GetColumn()})
			require.Len(t, leaf.GetRelated(), 1, "the immediate caller only")
			require.Equal(t, "lib/b.yaml", leaf.GetRelated()[0].GetFile())
			require.Equal(t, [2]uint32{5, 11}, [2]uint32{leaf.GetRelated()[0].GetLine(), leaf.GetRelated()[0].GetColumn()})
			require.Contains(t, leaf.GetRelated()[0].GetMessage(), `step "toC"`)
		})
	}
}

// A loop through other files closes inside the callee: B's report holds the
// cycle, and A, which called B, says B has problems.
func TestCalleeDiagnosticsReportTransitiveCycle(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.yaml", "edition: v2026.4\nname: a\nsteps:\n  - id: toB\n    call: ./b.yaml\n")
	writeFile(t, dir, "b.yaml", "edition: v2026.4\nname: b\nsteps:\n  - id: back\n    call: ./a.yaml\n")

	reports := reportsOf(t, dir, "a.yaml")
	require.Equal(t, []string{"a.yaml", "b.yaml"}, reportNames(reports))
	validAgainstSchema(t, reports)

	require.Equal(t, []string{"callee-failed"}, codesOf(reports[0]))
	require.Equal(t, []string{"call-cycle"}, codesOf(reports[1]))
	cycle := reports[1].GetDiagnostics()[0]
	require.Contains(t, cycle.GetMessage(), "through a chain of files", "a transitive cycle is a chain")
	require.Equal(t, [2]uint32{5, 11}, [2]uint32{cycle.GetLine(), cycle.GetColumn()})
	require.Equal(t, "a.yaml", cycle.GetRelated()[0].GetFile())
}

// The grouping is a fact of the compile: the same files give the same reports in
// the same order, every time.
func TestCalleeDiagnosticsReportIsDeterministic(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.yaml", "edition: v2026.4\nname: a\nsteps:\n"+
		"  - id: one\n    call: ./lib/x.yaml\n  - id: two\n    call: ./lib/y.yaml\n  - id: three\n    call: ./lib/x.yaml\n")
	writeFile(t, dir, "lib/x.yaml", "edition: v2026.4\nname: x\nsteps:\n  - id: x\n    bogus_key: 1\n")
	writeFile(t, dir, "lib/y.yaml", strings.ReplaceAll(selfCallingCallee, "./callee.yaml", "./y.yaml"))

	first := reportsOf(t, dir, "a.yaml")
	require.Equal(t, []string{"a.yaml", "lib/x.yaml", "lib/y.yaml"}, reportNames(first))
	for range 5 {
		again := reportsOf(t, dir, "a.yaml")
		require.Len(t, again, len(first))
		for i := range first {
			require.True(t, proto.Equal(first[i], again[i]), "report %d differs between compiles", i)
		}
	}
}

func TestMergeReports(t *testing.T) {
	rel := func(file string, line uint32) *v1.RelatedLocation {
		return &v1.RelatedLocation{File: file, Line: line, Message: "compiled because it calls it"}
	}
	diag := func(line uint32, msg string, related ...*v1.RelatedLocation) *v1.Diagnostic {
		return &v1.Diagnostic{Line: line, Column: 1, Message: msg, Code: "general", Related: related}
	}

	t.Run("a derived report merges into the file that was also given", func(t *testing.T) {
		given := []*v1.DiagnosticReport{
			{File: "a.yaml", Diagnostics: []*v1.Diagnostic{diag(1, "a's own")}},
			{File: "lib/b.yaml", Diagnostics: []*v1.Diagnostic{diag(6, "b's own"), diag(9, "shared")}},
			{File: "c.yaml"},
		}
		derived := []*v1.DiagnosticReport{
			{File: "a.yaml", Diagnostics: []*v1.Diagnostic{diag(5, "callee-failed")}},
			// Spelled differently, the same file once cleaned.
			{File: "./lib/b.yaml", Diagnostics: []*v1.Diagnostic{
				diag(9, "shared", rel("a.yaml", 5)),
				diag(12, "only in context", rel("a.yaml", 5)),
			}},
			{File: "lib/d.yaml", Diagnostics: []*v1.Diagnostic{diag(2, "d's")}},
		}

		before := proto.Clone(given[1])
		merged := flowfile.MergeReports(given, derived)

		require.Equal(t, []string{"a.yaml", "lib/b.yaml", "c.yaml", "lib/d.yaml"}, reportNames(merged), "given order first, then what is new")
		b := merged[1]
		require.Len(t, b.GetDiagnostics(), 3, "the shared finding is not said twice")
		require.Equal(t, "b's own", b.GetDiagnostics()[0].GetMessage())
		require.Equal(t, "shared", b.GetDiagnostics()[1].GetMessage())
		require.Len(t, b.GetDiagnostics()[1].GetRelated(), 1, "its reason is added to the finding already there")
		require.Equal(t, "only in context", b.GetDiagnostics()[2].GetMessage())
		require.Len(t, merged[0].GetDiagnostics(), 2)

		require.True(t, proto.Equal(before, given[1]), "the caller's report is not changed under it")
	})

	t.Run("related locations union without duplicates and stay bounded", func(t *testing.T) {
		var have, extra []*v1.RelatedLocation
		for i := range 10 {
			have = append(have, rel("a.yaml", uint32(i+1)))
			extra = append(extra, rel("a.yaml", uint32(i+6))) // overlaps 6..10
		}
		merged := flowfile.MergeReports(
			[]*v1.DiagnosticReport{{File: "b.yaml", Diagnostics: []*v1.Diagnostic{diag(1, "m", have...)}}},
			[]*v1.DiagnosticReport{{File: "b.yaml", Diagnostics: []*v1.Diagnostic{diag(1, "m", extra...)}}},
		)
		related := merged[0].GetDiagnostics()[0].GetRelated()
		require.Len(t, related, 15, "1..10 and 11..15, with 6..10 not repeated")
		for i, r := range related {
			require.Equal(t, uint32(i+1), r.GetLine())
		}
	})

	t.Run("nothing derived changes nothing", func(t *testing.T) {
		given := []*v1.DiagnosticReport{{File: "a.yaml", Diagnostics: []*v1.Diagnostic{diag(1, "m")}}}
		merged := flowfile.MergeReports(given, nil)
		require.Len(t, merged, 1)
		require.True(t, proto.Equal(given[0], merged[0]))
	})

	t.Run("compiled reports merge into a file validated on its own", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "workflow.yaml", issueCaller)
		writeFile(t, dir, "lib/callee.yaml", plainCallee)

		t.Chdir(dir)
		own, err := flowfile.ValidateSourceFile("lib/callee.yaml")
		require.NoError(t, err)
		require.Len(t, own, 1, "`flow validate lib/callee.yaml` finds the default on its own")
		ownReport := own.Report("lib/callee.yaml")
		require.Empty(t, ownReport.GetDiagnostics()[0].GetRelated(), "validated alone there is no caller")

		callerDs, err := flowfile.ValidateSourceFile("workflow.yaml")
		require.NoError(t, err)
		merged := flowfile.MergeReports(
			[]*v1.DiagnosticReport{callerDs.Report("workflow.yaml"), ownReport},
			callerDs.Reports("workflow.yaml")[1:],
		)

		require.Equal(t, []string{"workflow.yaml", "lib/callee.yaml"}, reportNames(merged))
		callee := reportFor(t, merged, "lib/callee.yaml")
		require.Len(t, callee.GetDiagnostics(), 1, "the same finding, found twice, is one")
		related := callee.GetDiagnostics()[0].GetRelated()
		require.Len(t, related, 1)
		require.Equal(t, "workflow.yaml", related[0].GetFile())
	})
}
