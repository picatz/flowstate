package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// validateTarget is one file `flow validate` checks, told apart by kind
// because a Flowfile and a Flowfile test validate under different schemas
// (#394): a workflow goes through [flowfile.ValidateSourceFile], a test goes
// through [flowtest.Load].
type validateTarget struct {
	path   string
	isTest bool
}

// isTestFilePath reports whether path names a Flowfile test by its
// conventional suffix, the same rule [collectTestFiles] uses for `flow test`.
func isTestFilePath(path string) bool {
	return strings.HasSuffix(path, ".test.yaml") || strings.HasSuffix(path, ".test.yml")
}

// collectValidateTargets expands the paths given into the files `flow
// validate` checks, on the same rule [collectFlowfiles] and [collectTestFiles]
// already use for their own verbs: a named file is taken as given, whatever
// it is called, and a directory is walked.
//
// `validate` is the one file-taking verb that used to refuse a directory
// outright, answering with raw `open`/`read` syscall prose instead of saying
// what it wanted (#394). This is what closes that gap: an author who has
// learned `flow test demo` or `flow fix demo` reasonably tries `flow validate
// demo` next, and every sibling verb already walks.
//
// A directory yields both kinds of file it may hold, sorted into workflows
// and tests by the same walk [flowfile.LooksLikeFlowfileTest] already tells a
// test apart from a workflow with elsewhere in this package, not by name
// alone, so a test file that does not happen to end in `.test.yaml` is still
// found and checked under its own schema rather than as a malformed
// workflow. A named path, by contrast, is trusted for what it is named:
// someone who typed `flow validate case.yaml` said what they meant, and only
// its suffix, not its shape, decides which schema checks it, the same rule
// [collectFlowfiles] applies to a named Flowfile.
func collectValidateTargets(paths []string) ([]validateTarget, error) {
	var out []validateTarget

	for _, path := range paths {
		// Opened rather than merely stat'd, so a missing path reads the same
		// "open <path>: ..." wording `loadWorkflow` already uses for `flow run
		// local`, one spelling for file-not-found across the verbs that take a
		// path, per #394.
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		info, err := f.Stat()
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if !info.IsDir() {
			out = append(out, validateTarget{path: path, isTest: isTestFilePath(path)})
			continue
		}

		err = filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if isTestFilePath(p) {
				out = append(out, validateTarget{path: p, isTest: true})
				return nil
			}
			switch filepath.Ext(p) {
			case ".yaml", ".yml":
				data, err := os.ReadFile(p)
				if err != nil {
					return fmt.Errorf("%s: %w", p, err)
				}
				switch {
				case flowfile.LooksLikeFlowfileTest(data):
					out = append(out, validateTarget{path: p, isTest: true})
				case flowfile.LooksLikeFlowfile(data) || flowfile.IsMalformedYAML(data):
					out = append(out, validateTarget{path: p, isTest: false})
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("walking %s: %w", path, err)
		}
	}

	return out, nil
}

// validateTestFile checks one Flowfile test under its own schema:
// [flowtest.Load], which parses the file and applies its own size and count
// bounds, without running any case or reaching the workflow it targets, the
// same "is this well-formed" question [flowfile.Validate] answers for a
// workflow.
//
// A load failure is reported as one unpositioned diagnostic rather than as an
// invocation error: [flowtest.Load] already distinguishes a missing file
// (returned to the caller before this is reached, exactly as a missing
// workflow file is) from one that exists and does not parse, which is a fact
// about the file and belongs in the same report a workflow's own diagnostics
// travel in.
func validateTestFile(path string) flowfile.Diagnostics {
	if _, err := flowtest.Load(path); err != nil {
		return flowfile.Diagnostics{{Message: err.Error()}}
	}
	return nil
}
