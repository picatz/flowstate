package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// stdinArg is the conventional spelling for "read this argument from stdin
// instead of a path"; see #397.
const stdinArg = "-"

// maxStdinBytes bounds how much of stdin `flow validate -` reads before
// refusing, matching flowfile's own document bound (see flowfile's unexported
// maxBytes and [flowtest.MaxTestFileBytes], both 1 MiB): stdin is untrusted
// input exactly like a file on disk, and CLAUDE.md's "bound anything that
// consumes untrusted input" applies to a pipe precisely as it does to an HTTP
// response body, one byte cap short of "read until the sender stops talking."
const maxStdinBytes = 1 << 20

// validateTarget is one file `flow validate` checks, told apart by kind
// because a Flowfile and a Flowfile test validate under different schemas
// (#394): a workflow goes through [flowfile.ValidateSourceFile], a test goes
// through [flowtest.Load].
//
// data carries the bytes read from stdin for the one target named "-"
// ([stdinArg]); every other target is read from path when it is checked, not
// held in memory ahead of time, which is what keeps a sweep over a directory
// of Flowfiles from holding all of them at once.
type validateTarget struct {
	path   string
	isTest bool
	data   []byte
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
//
// # Stdin
//
// A path spelled exactly "-" (see [stdinArg]) reads the document from stdin
// instead of a file: the convention every consumer piping a generated
// Flowfile into `flow validate` already expects, and the one this command
// answered with a bare `open -: no such file or directory` before this
// existed, as if the caller had misspelled a filename rather than invoked a
// shape this command simply had no name for (#397). It may not be combined
// with another path in the same invocation, because there is exactly one
// stdin to read and mixing it with a real path would make one of the two
// arguments a lie about where its bytes came from; every consumer piping a
// document in has one document to check anyway.
func collectValidateTargets(paths []string, stdin io.Reader) ([]validateTarget, error) {
	if err := checkStdinArg(paths); err != nil {
		return nil, err
	}

	if len(paths) == 1 && paths[0] == stdinArg {
		data, err := readStdinBounded(stdin)
		if err != nil {
			return nil, err
		}
		return []validateTarget{{
			path:   stdinArg,
			isTest: flowfile.LooksLikeFlowfileTest(data),
			data:   data,
		}}, nil
	}

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

// checkStdinArg refuses a mix of "-" and a real path in one invocation, with
// a message that says what the shape means instead of leaving a caller to
// find out from whichever half happened to fail first.
func checkStdinArg(paths []string) error {
	hasStdin := false
	for _, p := range paths {
		if p == stdinArg {
			hasStdin = true
			break
		}
	}
	if !hasStdin || len(paths) == 1 {
		return nil
	}
	return errors.New(`"-" (read stdin) cannot be combined with another path in one invocation; validate one document at a time`)
}

// readStdinBounded reads stdin up to [maxStdinBytes], refusing rather than
// truncating when the sender has more to say: a truncated document would be
// validated as something the caller never wrote, which is a worse answer
// than refusing outright. The +1 is what tells "exactly at the bound" apart
// from "more than the bound"; nothing else needs to be read to know which.
func readStdinBounded(stdin io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(stdin, maxStdinBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading stdin: %w", err)
	}
	if len(data) > maxStdinBytes {
		return nil, fmt.Errorf("stdin exceeds the %d byte limit for a Flowfile", maxStdinBytes)
	}
	return data, nil
}

// validateWorkflowTarget checks one workflow target: [flowfile.ValidateSourceFile]
// for a file on disk, or [flowfile.ValidateSource] for the one target read
// from stdin ([stdinArg]), the same split [validateTestFile] makes for a
// Flowfile test, and for the same reason: the bytes to validate already sit
// in memory for stdin, so re-reading target.path (which is just "-" and
// names nothing on disk) would validate the wrong thing.
func validateWorkflowTarget(target validateTarget) (flowfile.Diagnostics, error) {
	if target.data != nil {
		return flowfile.ValidateSource(target.data)
	}
	return flowfile.ValidateSourceFile(target.path)
}

// validateTestFile checks one Flowfile test under its own schema:
// [flowtest.Load] for a file on disk, or [flowtest.LoadSource] for the one
// target read from stdin ([stdinArg]), both parse the file and apply its own
// size and count bounds, without running any case or reaching the workflow it
// targets, the same "is this well-formed" question [flowfile.Validate]
// answers for a workflow.
//
// A load failure is reported as one unpositioned diagnostic rather than as an
// invocation error: [flowtest.Load] already distinguishes a missing file
// (returned to the caller before this is reached, exactly as a missing
// workflow file is) from one that exists and does not parse, which is a fact
// about the file and belongs in the same report a workflow's own diagnostics
// travel in.
func validateTestFile(target validateTarget) flowfile.Diagnostics {
	var err error
	if target.data != nil {
		_, err = flowtest.LoadSource(target.data)
	} else {
		_, err = flowtest.Load(target.path)
	}
	if err != nil {
		return flowfile.Diagnostics{{Message: err.Error()}}
	}
	return nil
}
