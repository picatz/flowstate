package main

import (
	"path/filepath"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// debugSourceMap relates a compiled Flowfile's steps to the lines they are
// written on, for a debugger to show and to break at.
//
// Nil when the file cannot be read again or no longer compiles: a debugger
// without a source map still addresses every step by name, and one with a map
// computed from different bytes would point at the wrong lines. The map is
// bound to workflow — the program actually run — and to the exact bytes read
// here, by digest.
func debugSourceMap(path string, workflow *v1.Workflow) *v1.DebugSourceMap {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil
	}
	source, err := readBoundedFile(absolute, "a Flowfile", maxFlowfileSourceBytes)
	if err != nil {
		return nil
	}
	_, positions, err := flowfile.ParseAt(source, absolute)
	if err != nil {
		return nil
	}

	return flowfile.SourceMap(absolute, source, workflow, positions)
}
