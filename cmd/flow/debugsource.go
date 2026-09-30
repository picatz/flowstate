package main

import (
	"errors"
	"fmt"
	"path/filepath"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// loadDebuggedWorkflow is [loadWorkflow] for a debugger: one read of the file
// compiles the program and gives the positions its source map is made from,
// so the lines a debugger shows are those of the bytes that were compiled. A
// second read could see a file saved in between, and a save that only moves
// lines compiles to the same program, so no digest could tell the two apart.
func loadDebuggedWorkflow(path string) (*v1.Workflow, *debugSource, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, nil, err
	}
	data, err := readBoundedFile(absolute, "a Flowfile", maxFlowfileSourceBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	workflow, positions, diagnostics, err := flowfile.ParseAndValidateSourceAt(data, absolute)
	if err != nil {
		if parsed, ok := errors.AsType[flowfile.Diagnostics](err); ok {
			return nil, nil, diagnosticsError(path, parsed)
		}

		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(diagnostics) > 0 {
		return nil, nil, diagnosticsError(path, diagnostics)
	}

	return workflow, &debugSource{path: absolute, data: data, positions: positions}, nil
}

// loadMappedWorkflow compiles the Flowfile at path for its source map alone,
// for an attach to a run that executes somewhere else. Compiled without
// validating it against what this process can run: a plugin task the
// deployment runs need not be registered here, and a compile that would pass
// there names the same program here. Whether it is the program the run
// executes is the program digest's to say ([flowdebug.Remote.SourceMapVerified]).
func loadMappedWorkflow(path string) (*v1.Workflow, *debugSource, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, nil, err
	}
	data, err := readBoundedFile(absolute, "a Flowfile", maxFlowfileSourceBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	workflow, positions, err := flowfile.ParseAt(data, absolute)
	if err != nil {
		if parsed, ok := errors.AsType[flowfile.Diagnostics](err); ok {
			return nil, nil, diagnosticsError(path, parsed)
		}

		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}

	return workflow, &debugSource{path: absolute, data: data, positions: positions}, nil
}

// debugSource is the read a debugged workflow was compiled from.
type debugSource struct {
	path      string
	data      []byte
	positions *flowfile.Positions
}

// sourceMap relates workflow's steps to the lines they are written on in this
// read, for a debugger to show and to break at. workflow is the program as
// run, plugins resolved, which is what the map is bound to by digest.
func (s *debugSource) sourceMap(workflow *v1.Workflow) *v1.DebugSourceMap {
	if s == nil {
		return nil
	}

	return flowfile.SourceMap(s.path, s.data, workflow, s.positions)
}
