package flowfile

import (
	"path/filepath"
	"sort"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// SourceLanguage is the [v1.DebugSourceDocument.language] a Flowfile's
// documents carry.
const SourceLanguage = "flowfile"

// maxSourceMapCallDepth bounds how far [SourceMap] follows `call:` into
// callee files, which is the engine's own nesting bound.
const maxSourceMapCallDepth = v1.MaxCallDepth

// SourceMap relates the steps of a compiled Flowfile to where they are written,
// for the step debugger (#1568).
//
// It is derived from the compiler's own position records, so the editor, the
// language server, and the debugger agree about where a step is; nothing here
// parses YAML a second time. Each document is bound to the exact bytes the map
// was computed from by its content digest, and the map as a whole to the
// compiled program by [v1.DebugSourceMap.ir_digest].
//
// A callee is included only when its file can be read beside its caller and
// its bytes still match the digest the compiler recorded on the `call:` step.
// A callee that moved or changed since compilation is left out rather than
// mapped to lines that no longer hold it, and its sites are then debugged by
// address alone.
func SourceMap(path string, source []byte, workflow *v1.Workflow, positions *Positions) *v1.DebugSourceMap {
	sourceMap := &v1.DebugSourceMap{IrDigest: IRDigest(workflow)}

	documents := map[string]int{}
	var add func(path string, source []byte, workflow *v1.Workflow, positions *Positions, depth int)
	add = func(path string, source []byte, workflow *v1.Workflow, positions *Positions, depth int) {
		index, seen := documents[path]
		if !seen {
			index = len(sourceMap.Documents)
			documents[path] = index
			sourceMap.Documents = append(sourceMap.Documents, &v1.DebugSourceDocument{
				Uri:      path,
				Digest:   v1.ContentDigest(source),
				Language: SourceLanguage,
			})
		}
		sourceMap.Entries = append(sourceMap.Entries, positions.siteEntries(workflow.GetName(), int32(index))...)

		if depth >= maxSourceMapCallDepth || path == "" {
			return
		}
		for _, call := range calls(workflow.GetSteps()) {
			calleePath := call.GetSource()
			if calleePath == "" || call.GetSourceDigest() == "" {
				continue
			}
			if !filepath.IsAbs(calleePath) {
				calleePath = filepath.Join(filepath.Dir(path), calleePath)
			}
			if _, seen := documents[calleePath]; seen {
				continue
			}
			data, err := readBoundedSource(calleePath)
			if err != nil || v1.ContentDigest(data) != call.GetSourceDigest() {
				continue
			}
			_, calleePositions, err := ParseAt(data, calleePath)
			if err != nil {
				continue
			}
			add(calleePath, data, call.GetWorkflow(), calleePositions, depth+1)
		}
	}
	add(path, source, workflow, positions, 0)

	return sourceMap
}

// IRDigest is [v1.WorkflowIRDigest], the identity a source map is bound to.
func IRDigest(workflow *v1.Workflow) string {
	return v1.WorkflowIRDigest(workflow)
}

// calls returns every `call:` step in nodes, descending into every container
// that runs in the same workflow.
func calls(nodes []*v1.Node) []*v1.Call {
	var found []*v1.Call
	for _, node := range nodes {
		switch kind := node.GetKind().(type) {
		case *v1.Node_Call:
			found = append(found, kind.Call)
		case *v1.Node_ForEach:
			found = append(found, calls(kind.ForEach.GetBody())...)
		case *v1.Node_Loop:
			found = append(found, calls(kind.Loop.GetBody())...)
		case *v1.Node_Parallel:
			for _, branch := range kind.Parallel.GetBranches() {
				found = append(found, calls(branch.GetSteps())...)
			}
		case *v1.Node_Switch:
			for _, arm := range kind.Switch.GetCases() {
				found = append(found, calls(arm.GetSteps())...)
			}
			found = append(found, calls(kind.Switch.GetDefault().GetSteps())...)
		}
	}

	return found
}

// siteEntries maps each step recorded in this document to its span, with the
// step's site path read from the ids of the step paths enclosing it.
func (p *Positions) siteEntries(workflow string, document int32) []*v1.DebugSourceEntry {
	if p == nil {
		return nil
	}

	paths := make([]string, 0, len(p.stepAt))
	for path := range p.stepAt {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	entries := make([]*v1.DebugSourceEntry, 0, len(paths))
	for _, path := range paths {
		span, ok := p.spans[path]
		if !ok {
			continue
		}

		var site []string
		for i := range len(path) {
			if path[i] != ']' {
				continue
			}
			if id, ok := p.stepAt[path[:i+1]]; ok && strings.HasSuffix(path[:i+1], "]") {
				site = append(site, id)
			}
		}

		entries = append(entries, &v1.DebugSourceEntry{
			Site: &v1.DebugSite{Workflow: workflow, Path: site},
			Location: &v1.DebugSourceLocation{
				Document: document,
				Range: &v1.SourceRange{
					StartLine:   uint32(span.Start.Line),
					StartColumn: uint32(max(span.Start.Column, 0)),
					EndLine:     uint32(max(span.End.Line, span.Start.Line)),
					EndColumn:   uint32(max(span.End.Column, 0)),
				},
			},
		})
	}

	return entries
}
