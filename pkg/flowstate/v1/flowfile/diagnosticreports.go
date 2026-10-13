package flowfile

import (
	"cmp"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// maxRelatedLocations is the bound on [Diagnostic.Related], the same number the
// schema enforces on [v1.Diagnostic.Related]. The locations come from the call
// sites of one callee, and the files that call it are another party's to write,
// so the list keeps the first sites in source order rather than growing with them.
const maxRelatedLocations = 16

// A callHop is one `call:` step a callee's diagnostic was raised through.
type callHop struct {
	call *v1.Call
	step string
}

// A calleeFrame is a compiled callee as the file that called it saw it: the name
// its diagnostics are reported under, and where its steps are written.
type calleeFrame struct {
	display   string
	positions *Positions
}

// A diagnosticKey is what makes two diagnostics the same finding: the same
// problem at the same place in the same file, whichever call site reached it.
type diagnosticKey struct {
	file, code, message, step, field, kind, value string
	line, column                                  int
}

func (d Diagnostic) key() diagnosticKey {
	return diagnosticKey{
		file: d.File, code: string(d.Code), message: d.Message,
		step: d.Step, field: d.Field, kind: d.Kind, value: d.Value,
		line: d.Line, column: d.Column,
	}
}

func protoKey(file string, d *v1.Diagnostic) diagnosticKey {
	return diagnosticKey{
		file: file, code: d.GetCode(), message: d.GetMessage(),
		step: d.GetStep(), field: d.GetField(), kind: d.GetKind(), value: d.GetValue(),
		line: int(d.GetLine()), column: int(d.GetColumn()),
	}
}

// unionRelated appends the locations of extra that dst does not already hold,
// stopping at [maxRelatedLocations].
func unionRelated(dst, extra []*v1.RelatedLocation) []*v1.RelatedLocation {
	for _, r := range extra {
		if len(dst) >= maxRelatedLocations {
			break
		}
		if !slices.ContainsFunc(dst, func(have *v1.RelatedLocation) bool {
			return have.GetFile() == r.GetFile() && have.GetLine() == r.GetLine() &&
				have.GetColumn() == r.GetColumn() && have.GetMessage() == r.GetMessage()
		}) {
			dst = append(dst, r)
		}
	}
	return dst
}

// calledFrom is the related location of a `call:` at line and column of file.
func calledFrom(file string, at Position, step string) []*v1.RelatedLocation {
	if file == "" {
		return nil
	}
	return []*v1.RelatedLocation{{
		File:    file,
		Line:    uint32(max(at.Line, 0)),
		Column:  uint32(max(at.Column, 0)),
		Message: fmt.Sprintf("compiled because step %q calls it", step),
	}}
}

// calleeDisplayName is the name a callee's diagnostics are reported under: the
// file actually read, as a path from the caller's real directory joined onto the
// directory of the name the caller is reported under. A callee reached through a
// symlink is therefore named by the file behind it, so the `call:` targets it
// makes in turn, which resolve against that file's real directory, are reported
// where the compiler read them. When the read path cannot be expressed relative
// to the caller's real directory it is the read path itself.
//
// A callee that does not exist has no real path, so it is read at the caller's
// directory as the caller spells it (callerFile's); when it lies under that
// directory it is named from there, rather than by a path from the real directory
// that would climb out of the caller's own spelling of it.
func calleeDisplayName(caller, callerFile, callerRealDir, read string) string {
	if rel, err := filepath.Rel(filepath.Dir(callerFile), read); err == nil && rel != ".." && !strings.HasPrefix(filepath.ToSlash(rel), "../") {
		return filepath.Clean(filepath.Join(filepath.Dir(caller), rel))
	}
	rel, err := filepath.Rel(callerRealDir, read)
	if err != nil || callerRealDir == "" {
		return read
	}

	return filepath.Clean(filepath.Join(filepath.Dir(caller), rel))
}

// Reports renders the diagnostics as one report per file they are in: file's own
// first, then each callee file in the order the compiler first reached it.
//
// File's report is always present, empty when file is clean, for the reason
// [Diagnostics.Report] says; a callee's is present only when it has problems. In a
// callee's report one finding appears once however many call sites reached it, with
// those sites in its Related, and is sorted by line, column, and message. File's
// own report keeps the order it was found in, exactly as Report gives it.
func (ds Diagnostics) Reports(file string) []*v1.DiagnosticReport {
	reports := []*v1.DiagnosticReport{ds.Report(file)}

	var order []string
	byFile := map[string]Diagnostics{}
	for _, d := range ds {
		if d.File == "" {
			continue
		}
		if _, seen := byFile[d.File]; !seen {
			order = append(order, d.File)
		}
		byFile[d.File] = append(byFile[d.File], d)
	}

	for _, name := range order {
		var merged Diagnostics
		at := map[diagnosticKey]int{}
		for _, d := range byFile[name] {
			if i, again := at[d.key()]; again {
				merged[i].Related = unionRelated(slices.Clone(merged[i].Related), d.Related)
				continue
			}
			at[d.key()] = len(merged)
			merged = append(merged, d)
		}
		slices.SortStableFunc(merged, func(a, b Diagnostic) int {
			return cmp.Or(cmp.Compare(a.Line, b.Line), cmp.Compare(a.Column, b.Column), cmp.Compare(a.Message, b.Message))
		})

		report := &v1.DiagnosticReport{File: name, Diagnostics: make([]*v1.Diagnostic, 0, len(merged))}
		for _, d := range merged {
			report.Diagnostics = append(report.Diagnostics, d.Proto())
		}
		reports = append(reports, report)
	}

	return reports
}

// MergeReports adds the reports a compile derived to the ones a caller already
// holds, one per file it was asked about.
//
// Files in given keep their order, and a derived report for one of them (the
// same key, see below) is merged into it, under the name given uses: what it adds is kept, what given
// already says is not said twice, and the call sites it carries are added to the
// matching diagnostic's Related. The rest of derived follows, in its own order.
// Neither input is modified.
//
// key names the file a report is about, so that two spellings of one file merge.
// A nil key is [filepath.Clean]; a caller that can reach the filesystem passes
// one that also resolves symlinks, because a derived report is named by the file
// actually read while a given one is named as the user spelled it.
func MergeReports(given, derived []*v1.DiagnosticReport, key func(name string) string) []*v1.DiagnosticReport {
	if key == nil {
		key = filepath.Clean
	}
	merged := make([]*v1.DiagnosticReport, 0, len(given)+len(derived))
	index := map[string]int{}
	for _, r := range given {
		index[key(r.GetFile())] = len(merged)
		merged = append(merged, r)
	}

	cloned := map[int]bool{}
	for _, r := range derived {
		name := key(r.GetFile())
		i, ok := index[name]
		if !ok {
			index[name] = len(merged)
			merged = append(merged, r)
			continue
		}

		// Cloned on first write, so that a report the caller holds is never
		// changed under it.
		if !cloned[i] {
			own := &v1.DiagnosticReport{File: merged[i].GetFile()}
			for _, d := range merged[i].GetDiagnostics() {
				own.Diagnostics = append(own.Diagnostics, proto.Clone(d).(*v1.Diagnostic))
			}
			merged[i] = own
			cloned[i] = true
		}

		own := merged[i]
		at := map[diagnosticKey]int{}
		for j, d := range own.GetDiagnostics() {
			at[protoKey("", d)] = j
		}
		for _, d := range r.GetDiagnostics() {
			if j, again := at[protoKey("", d)]; again {
				have := own.Diagnostics[j]
				have.Related = unionRelated(have.Related, d.GetRelated())
				continue
			}
			at[protoKey("", d)] = len(own.Diagnostics)
			// Cloned so a later merge's union into its Related cannot reach the
			// derived report the caller holds.
			own.Diagnostics = append(own.Diagnostics, proto.Clone(d).(*v1.Diagnostic))
		}
	}

	return merged
}
