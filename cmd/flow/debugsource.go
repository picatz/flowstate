package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/picatz/flowstate/cmd/flow/internal/debugtui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
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

// documents are the texts of the files a source map names, for the full-screen
// debugger to show beside the flow: the Flowfile as it was read for the
// compile, and each file it calls as it is on disk now. A file that cannot be
// read is left out, and one that changed is handed over as it is: the screen
// compares each with the digest the map records, and shows addresses instead of
// lines for one that differs.
func (s *debugSource) documents(sourceMap *v1.DebugSourceMap) []debugtui.Document {
	if s == nil {
		return nil
	}
	docs := []debugtui.Document{{URI: s.path, Text: s.data}}
	for _, document := range sourceMap.GetDocuments() {
		if len(docs) >= debugtui.MaxSourceDocuments {
			break
		}
		if document.GetUri() == s.path || document.GetUri() == "" {
			continue
		}
		data, err := readBoundedFile(document.GetUri(), "a called Flowfile", debugtui.MaxSourceBytes)
		if err != nil {
			continue
		}
		docs = append(docs, debugtui.Document{URI: document.GetUri(), Text: data})
	}

	return docs
}

// screenSource is the source map of workflow and the texts to show it with, for
// a screen over a run in this process: [debugSource.sourceMap] and
// [debugSource.documents] together, the two halves attach hands the screen. The
// map is verified by construction when workflow is the program that runs, and
// the pane still shows a line only where a text hashes to the digest the map
// records, so a callee saved since the compile is answered with the reason and
// not with lines that may be the wrong ones.
func (s *debugSource) screenSource(workflow *v1.Workflow) (*v1.DebugSourceMap, []debugtui.Document) {
	sourceMap := s.sourceMap(workflow)
	if sourceMap == nil {
		return nil, nil
	}

	return sourceMap, s.documents(sourceMap)
}

// mapDescribesRun reports whether the program target is running is the one
// sourceMap is bound to: the snapshot names its program's digest and the map
// names the digest it was made for, the rule a durable attach and a history walk
// apply before they show a line. A target that cannot answer, or answers
// without a digest, is not verified.
//
// It is for a front that does not hold the program it runs (`flow test --debug`
// plays a case that compiles its own), where the map can be checked only against
// what the run reports.
func mapDescribesRun(ctx context.Context, target flowdebug.Target, sourceMap *v1.DebugSourceMap) bool {
	if sourceMap == nil {
		return false
	}
	snapshot, err := target.Snapshot(ctx)
	if err != nil {
		return false
	}

	return snapshot.GetIrDigest() != "" && snapshot.GetIrDigest() == sourceMap.GetIrDigest()
}
