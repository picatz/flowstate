package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// stdinArg is the conventional spelling for "read this argument from stdin
// instead of a path"; see #397.
const stdinArg = "-"

// maxValidateDocumentBytes bounds how much of one document `flow validate`
// reads before refusing or skipping it, matching flowfile's own document
// bound (see flowfile's unexported maxBytes and [flowtest.MaxTestFileBytes],
// both 1 MiB). Two call sites share it rather than each picking a number: the
// stdin read in [readStdinBounded], and the classifying read a directory walk
// does in [collectValidateTargets], because both are the same question:
// "how much of untrusted input does this command hold in memory to decide
// what it is." CLAUDE.md's "bound anything that consumes untrusted
// input" applies to a file on disk exactly as it does to a pipe or an HTTP
// response body: one byte cap short of "read until the sender stops talking."
const maxValidateDocumentBytes = 1 << 20

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
				// Bounded the same way [readStdinBounded] bounds stdin,
				// rather than os.ReadFile-ing the whole file first and
				// discarding it once flowfile's own maxBytes check answers
				// false: a directory can hold a file of any size an attacker
				// chose, and reading it whole to classify it defeats the
				// bound before flowfile ever gets to apply it (CLAUDE.md,
				// "bound anything that consumes untrusted input"). A read
				// that hits the +1 sentinel is classified as neither a
				// workflow nor a test without ever holding the rest of the
				// file, which is also flowfile.LooksLikeFlowfile's own
				// verdict for anything over its bound, the same answer,
				// reached without the memory cost.
				data, truncated, err := readFileBounded(p)
				if err != nil {
					return fmt.Errorf("%s: %w", p, err)
				}
				if truncated {
					return nil
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

	// Empty, rather than a clean pass: a directory holding no Flowfile and no
	// Flowfile test is far more likely a typo'd path or a scaffold nobody
	// filled in yet than a deliberate "there is genuinely nothing here to
	// check," and the version of this that says nothing exits 0 and reads as
	// CI having validated a directory it never actually looked inside, the
	// same refusal [collectFlowfiles] and [collectTestFiles] already give
	// their own siblings for the identical shape.
	if len(out) == 0 {
		return nil, errors.New("no Flowfiles or Flowfile tests found in the paths given")
	}

	return out, nil
}

// readFileBounded reads p up to [maxValidateDocumentBytes], reporting
// truncated=true rather than the file's full contents once that bound is
// reached, the same shape [readStdinBounded] uses for a pipe. The two
// differ only in what a caller does with "too big": stdin refuses the whole
// command, because it named exactly one document to check, while a
// directory-walk read simply drops the one file that would not classify as a
// Flowfile at that size anyway, and continues sweeping the rest.
func readFileBounded(p string) (data []byte, truncated bool, err error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	data, err = io.ReadAll(io.LimitReader(f, maxValidateDocumentBytes+1))
	if err != nil {
		return nil, false, err
	}
	if len(data) > maxValidateDocumentBytes {
		return nil, true, nil
	}
	return data, false, nil
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

// readStdinBounded reads stdin up to [maxValidateDocumentBytes], refusing rather than
// truncating when the sender has more to say: a truncated document would be
// validated as something the caller never wrote, which is a worse answer
// than refusing outright. The +1 is what tells "exactly at the bound" apart
// from "more than the bound"; nothing else needs to be read to know which.
func readStdinBounded(stdin io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(stdin, maxValidateDocumentBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading stdin: %w", err)
	}
	if len(data) > maxValidateDocumentBytes {
		return nil, fmt.Errorf("stdin exceeds the %d byte limit for a Flowfile", maxValidateDocumentBytes)
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

// validatePluginRequirements checks a workflow's `plugins:` block against the
// catalog the launched plugins actually formed, and reports a version or
// availability mismatch as a diagnostic (#835 review).
//
// It is the validator agreeing with the two execution drivers, which is the
// invariant CLAUDE.md states as "both execution drivers must agree" extended to
// the surface that exists to tell an author what they will do. `flow run local`
// resolves a file's `plugins:` requirements against the launched catalog through
// [v1.ResolvePlugins] (runlocal.go), and the durable submission does the same on
// the server; before this, `flow validate --plugin-dir` launched the plugins,
// registered their task schemas, and then said nothing about a requirement that
// both drivers would refuse — a `plugins: {example: v99.0.0}` against a v0.1.0
// binary validated clean and failed at the first real run. That is the false
// green this whole branch set out to end, wearing the requirement block instead
// of the task name.
//
// Only under --plugin-dir, which is the whole of why this is legitimate rather
// than a deployment's decision reported as the file's. With no catalog it does
// nothing and returns nothing: whether a plugin is installed, and at what
// version, is the deployment's call, and the validator stays silent on it — the
// same line [runValidate] holds everywhere else. The author who passed
// --plugin-dir has said "check against this toolchain", so the versions in it
// are theirs to be checked against.
//
// nil catalog, a target read from stdin, or a file that did not parse are each a
// no-op: there is nothing to resolve against, nothing on disk to re-read, or no
// workflow to resolve. A parse failure has already been reported by
// [validateWorkflowTarget]; re-reporting the parse here would double it.
func validatePluginRequirements(target validateTarget, catalog *v1.PluginCatalog) flowfile.Diagnostics {
	// No launched plugins, or a document that is not a file on disk to re-parse
	// for its requirements and its positions — stdin's bytes were validated in
	// memory and carry no path a callee `call:` could resolve against.
	if catalog == nil || target.data != nil {
		return nil
	}

	wf, pos, err := flowfile.ParseFile(target.path)
	if err != nil {
		// The compile already failed and [validateWorkflowTarget] reported it
		// with positions. There is no workflow to resolve, and saying so again
		// here is the double-diagnostic #384 warns against.
		return nil
	}

	// Nothing declared, nothing to check — and skipping keeps a file with no
	// `plugins:` block from paying for a resolution that would select the empty
	// set. [v1.ResolvePlugins] overwrites ResolvedPlugins, so it runs on the
	// freshly parsed workflow, which is thrown away here: validate selects
	// nothing, it only checks that a selection is possible.
	if len(wf.GetPluginRequirements()) == 0 {
		return nil
	}

	if err := v1.ResolvePlugins(wf, catalog); err != nil {
		// Positioned at the `plugins:` block, which is what the requirement is a
		// property of. The resolver names the plugin, the version the file asked
		// for, and the version the deployment has — the triple an author acts on
		// — but returns one error rather than a per-entry path, so the block is
		// the finest position honestly available without re-deriving which entry
		// failed.
		d := flowfile.Diagnostic{Message: err.Error()}
		if span, ok := pos.At("plugins"); ok {
			d.Line = span.Start.Line
			d.Column = span.Start.Column
		}

		return flowfile.Diagnostics{d}
	}

	return nil
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
//
// When the test file is on disk and a case scripts signals, the workflow it
// names is compiled and the signal names are checked against it — the same
// check [flowtest.CheckSignalNames] applies at run time, extended to the
// validate surface so a misspelled gate is caught before `flow test` (#1443).
// If the workflow fails to compile (plugin tasks, missing files), the signal
// check is skipped: the workflow's own diagnostics are the workflow's concern.
func validateTestFile(target validateTarget) flowfile.Diagnostics {
	var (
		file *flowtest.File
		err  error
	)
	if target.data != nil {
		file, err = flowtest.LoadSource(target.data)
	} else {
		file, err = flowtest.Load(target.path)
	}
	if err != nil {
		return flowfile.Diagnostics{{Message: err.Error()}}
	}

	if target.path == "" || target.data != nil {
		return nil
	}

	var diags flowfile.Diagnostics
	for i := range file.Tests {
		test := &file.Tests[i]
		if len(test.Signals) == 0 || test.Workflow == "" {
			continue
		}
		wfPath := flowtest.WorkflowPath(target.path, test)
		spec, _, parseErr := flowfile.ParseFile(wfPath)
		if parseErr != nil {
			continue
		}
		if err := flowtest.CheckSignalNames(test.Signals, spec); err != nil {
			diags = append(diags, flowfile.Diagnostic{Message: fmt.Sprintf("test %q: %s", test.Name, err)})
		}
	}

	return diags
}
