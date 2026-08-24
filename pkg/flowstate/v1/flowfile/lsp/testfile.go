package lsp

import (
	"regexp"
	"strings"

	"github.com/sourcegraph/go-lsp"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// A `*.test.yaml` in the editor, checked by the loader `flow test` runs
// (#1110 slice 1): the same refusal, at the keystroke instead of the run.
//
// The check is [flowtest.LoadSourceAt] against the live buffer — unsaved
// edits are exactly what a diagnostic has to reflect — with the document's
// own path deciding the semantics Load's path decides: the directory's
// `testdefaults.yaml` folds in, and a case's `workflow:` is required. One
// loader, two callers, so the editor can never disagree with `flow test`
// about the same file.
//
// What slice 1 does not do: the loader's semantic errors carry prose and no
// position, so they anchor best-effort — at the named test's `name:` line
// where the message names exactly one, at the top of the document otherwise.
// Slice 2 is the loader owning positions (#1110), and it deletes the
// heuristic rather than growing it.

// codeTestFile marks a diagnostic produced by the flowtest loader.
const codeTestFile = "flowtest-load"

// diagnoseTestDocument reports what `flow test` would refuse about a test
// document, and nothing a workflow's checks would say about it.
func diagnoseTestDocument(doc *document) []carriedDiagnostic {
	set := &diagnosticSet{}

	if doc.kind == docTestDefaults {
		// The shared fixture file gets syntax feedback only in slice 1 — its
		// semantic rules read the directory off disk, which is not this
		// buffer. What it must never get is a workflow's diagnostics.
		if doc.parseErr != nil {
			set.add(yamlDiagnostic(doc, doc.parseErr, codeYAMLSyntax))
		}
		return set.sorted()
	}

	var err error
	path, hasPath := doc.filesystemPath()
	if hasPath {
		_, err = flowtest.LoadSourceAt([]byte(doc.text), path)
	} else {
		// An untitled buffer has no directory for testdefaults.yaml or a
		// relative `workflow:` to mean anything against, so it gets the
		// byte-door semantics — the same answer a `call:` step gets from an
		// untitled workflow.
		_, err = flowtest.LoadSource([]byte(doc.text))
	}
	if err == nil {
		return set.sorted()
	}

	// One loader error is the whole report, exactly as one syntax error is
	// for a workflow: everything the loader checks after the failure point
	// never ran, and diagnostics for checks that never ran would be guesses.
	d := yamlDiagnostic(doc, err, codeTestFile)
	if hasPath {
		// The loader prefixes its errors with the path for a terminal reader;
		// the editor is already looking at the file.
		d.Message = strings.TrimPrefix(d.Message, path+": ")
	}
	if d.Range == documentStart {
		if rng, ok := anchorAtNamedTest(doc, d.Message); ok {
			d.Range = rng
		}
	}
	set.add(d)

	return set.sorted()
}

// namedTestPattern finds the test a loader message names: every semantic
// refusal about one case spells it `test "<name>"`.
var namedTestPattern = regexp.MustCompile(`test "((?:[^"\\]|\\.)+)"`)

// anchorAtNamedTest places an unpositioned loader message on the `name:` line
// of the test it names, when the document has exactly one.
//
// A heuristic, deliberately timid: it anchors only on an unambiguous match,
// because a diagnostic on the wrong line is worse than one at the top. It
// reads the raw lines rather than a parsed model — the model may be exactly
// what failed to build.
func anchorAtNamedTest(doc *document, message string) (lsp.Range, bool) {
	m := namedTestPattern.FindStringSubmatch(message)
	if m == nil {
		return lsp.Range{}, false
	}
	name := m[1]

	found := lsp.Range{}
	matches := 0
	for i, line := range strings.Split(doc.text, "\n") {
		key := strings.Index(line, "name:")
		if key < 0 {
			continue
		}
		value := strings.Trim(strings.TrimSpace(line[key+len("name:"):]), `"'`)
		if value != name {
			continue
		}
		matches++
		start := key + len("name:")
		for start < len(line) && line[start] == ' ' {
			start++
		}
		found = lsp.Range{
			Start: lsp.Position{Line: i, Character: start},
			End:   lsp.Position{Line: i, Character: len(line)},
		}
	}
	if matches != 1 {
		return lsp.Range{}, false
	}

	return found, true
}
