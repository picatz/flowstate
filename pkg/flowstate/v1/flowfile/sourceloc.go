package flowfile

import (
	"path/filepath"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// maxSourceFileBytes holds the file name a step location carries to the bound
// [v1.SourceLocation.File] declares.
const maxSourceFileBytes = 256

// AttachSources records on each of wf's steps where it is written, from the
// positions of the compilation that produced wf, so a failure the engine reports
// can point back at the file without the reader holding it.
//
// It is for a front end that is about to submit or run wf, not part of parsing:
// a parsed workflow stays the bare program, and two compilations of one program
// stay equal. The locations are advisory and cleared from every digest.
//
// A step is located only when its id names exactly one step in the file
// ([Positions.UniqueStepPath]): a failure is attributed by id, and sibling
// bodies may each declare one, so no location is better than the first
// declaration's. A called workflow's steps are not touched, since their
// positions belong to another file.
//
// file is the name to show. An absolute path is reduced to its base name so a
// home directory does not travel into a durable specification.
func AttachSources(wf *v1.Workflow, positions *Positions, file string) {
	if wf == nil || positions == nil {
		return
	}
	file = sourceFileName(file)
	unique := positions.UniqueStepPaths()

	v1.WalkNodes(wf.GetSteps(), v1.Walk{Node: func(node *v1.Node) {
		path, ok := unique[node.GetId()]
		if !ok {
			return
		}
		span, ok := positions.At(path)
		if !ok || !span.IsValid() {
			return
		}
		node.Source = &v1.SourceLocation{
			File:   file,
			Line:   int32(span.Start.Line),
			Column: int32(span.Start.Column),
		}
	}})
}

// sourceFileName is the name a location shows for path.
func sourceFileName(path string) string {
	name := filepath.ToSlash(path)
	if filepath.IsAbs(path) {
		name = filepath.Base(path)
	}
	if len(name) > maxSourceFileBytes {
		name = strings.ToValidUTF8(name[len(name)-maxSourceFileBytes:], "")
	}

	return name
}
