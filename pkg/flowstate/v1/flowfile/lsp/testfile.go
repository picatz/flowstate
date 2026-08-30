package lsp

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sourcegraph/go-lsp"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// codeTestFile marks a diagnostic produced by the flowtest loader.
const codeTestFile = "flowtest-load"

// A diagnosticPublication is one LSP document's share of a test-suite load.
// A suite can produce diagnostics owned by its sibling testdefaults.yaml, and
// LSP puts the URI on the publication rather than on each diagnostic.
type diagnosticPublication struct {
	uri         lsp.DocumentURI
	diagnostics []lsp.Diagnostic
}

// diagnoseTestDocument is the same-document projection used by diagnose and
// package-level tests. Protocol publication uses diagnoseTestPublications so a
// problem written in testdefaults.yaml is sent to that URI rather than mapped
// onto an unrelated line in the suite.
func diagnoseTestDocument(doc *document) []carriedDiagnostic {
	for _, publication := range diagnoseTestPublications(doc, nil) {
		if publication.uri != doc.uri {
			continue
		}
		out := make([]carriedDiagnostic, 0, len(publication.diagnostics))
		for _, d := range publication.diagnostics {
			out = append(out, carriedDiagnostic{published: d})
		}
		return out
	}
	return []carriedDiagnostic{}
}

// diagnoseTestPublications runs the flowtest loader on the live suite buffer.
// includedDefaults, when non-nil, is the open sibling defaults buffer; otherwise
// LoadSourceAt reads the bounded file from disk. No test grammar is implemented
// here: the loader supplies the problems, positions, and source ownership.
func diagnoseTestPublications(doc, includedDefaults *document) []diagnosticPublication {
	if doc.tooLarge {
		return []diagnosticPublication{{uri: doc.uri, diagnostics: []lsp.Diagnostic{{
			Range: documentStart, Severity: lsp.Warning, Source: diagnosticSource, Code: codeTooLarge,
			Message: fmt.Sprintf("file is %d bytes, larger than the %d byte limit this server analyzes; it is not being checked",
				len(doc.text), maxDocumentBytes),
		}}}}
	}

	// A defaults document is validated semantically when a suite includes it;
	// on its own there is no effective case to fold it into. Its strict shape
	// still goes through the loader, not the LSP's YAML parser alone.
	if doc.kind == docTestDefaults {
		path, ok := doc.filesystemPath()
		if !ok {
			path = string(doc.uri)
		}
		err := flowtest.LoadDirDefaultsSource([]byte(doc.text), path)
		if err == nil {
			return []diagnosticPublication{{uri: doc.uri, diagnostics: []lsp.Diagnostic{}}}
		}
		var defaultsErr *flowtest.DirDefaultsError
		if errors.As(err, &defaultsErr) {
			err = defaultsErr.Err
		}
		code := codeTestFile
		if doc.parseErr != nil {
			code = codeYAMLSyntax
		}
		return []diagnosticPublication{{uri: doc.uri, diagnostics: []lsp.Diagnostic{
			yamlDiagnostic(doc, err, code),
		}}}
	}

	path, hasPath := doc.filesystemPath()
	var err error
	switch {
	case !hasPath:
		_, err = flowtest.LoadSource([]byte(doc.text))
	case includedDefaults != nil:
		_, err = flowtest.LoadSourceAtWithDefaults([]byte(doc.text), path, []byte(includedDefaults.text))
	default:
		_, err = flowtest.LoadSourceAt([]byte(doc.text), path)
	}
	if err == nil {
		return []diagnosticPublication{{uri: doc.uri, diagnostics: []lsp.Diagnostic{}}}
	}

	var problems *flowtest.Diagnostics
	if errors.As(err, &problems) {
		byURI := map[lsp.DocumentURI][]lsp.Diagnostic{doc.uri: {}}
		for _, problem := range problems.Problems {
			uri := doc.uri
			if includedDefaults != nil && sameTestSource(problem.File, includedDefaults) {
				uri = includedDefaults.uri
			} else if problem.File != "" && (!hasPath || filepath.Clean(problem.File) != filepath.Clean(path)) {
				uri = fileURI(problem.File)
			}
			source := sourceForTestDiagnostic(doc, includedDefaults, uri, problem.File)
			byURI[uri] = append(byURI[uri], lsp.Diagnostic{
				Range:    testProblemRange(source, problem.Line, problem.Column),
				Severity: lsp.Error,
				Source:   diagnosticSource,
				Code:     codeTestFile,
				Message:  problem.Message,
			})
		}
		if problems.Total > len(problems.Problems) {
			byURI[doc.uri] = append(byURI[doc.uri], lsp.Diagnostic{
				Range: documentStart, Severity: lsp.Warning, Source: diagnosticSource, Code: codeTestFile,
				Message: fmt.Sprintf("%d more test-file problems were found and %d are shown",
					problems.Total-len(problems.Problems), len(problems.Problems)),
			})
		}
		return sortedTestPublications(doc.uri, byURI)
	}

	// A sibling defaults syntax/read refusal predates the structured semantic
	// collection, but its typed owner and goccy token are still authoritative.
	var defaultsErr *flowtest.DirDefaultsError
	if errors.As(err, &defaultsErr) {
		uri := fileURI(defaultsErr.Path)
		if includedDefaults != nil && sameTestSource(defaultsErr.Path, includedDefaults) {
			uri = includedDefaults.uri
		}
		source := sourceForTestDiagnostic(doc, includedDefaults, uri, defaultsErr.Path)
		owner := newDocument(uri, 0, source, doc.tasks)
		d := yamlDiagnostic(owner, defaultsErr.Err, codeTestFile)
		return []diagnosticPublication{
			{uri: doc.uri, diagnostics: []lsp.Diagnostic{}},
			{uri: uri, diagnostics: []lsp.Diagnostic{d}},
		}
	}

	// File-size and filesystem errors have no source position to claim.
	message := err.Error()
	if hasPath {
		message = strings.TrimPrefix(message, path+": ")
	}
	return []diagnosticPublication{{uri: doc.uri, diagnostics: []lsp.Diagnostic{{
		Range: documentStart, Severity: lsp.Error, Source: diagnosticSource, Code: codeTestFile, Message: message,
	}}}}
}

func sameTestSource(path string, doc *document) bool {
	docPath, ok := doc.filesystemPath()
	return ok && filepath.Clean(path) == filepath.Clean(docPath)
}

func sortedTestPublications(own lsp.DocumentURI, byURI map[lsp.DocumentURI][]lsp.Diagnostic) []diagnosticPublication {
	other := make([]lsp.DocumentURI, 0, len(byURI)-1)
	for uri := range byURI {
		if uri != own {
			other = append(other, uri)
		}
	}
	slices.Sort(other)
	out := []diagnosticPublication{{uri: own, diagnostics: byURI[own]}}
	for _, uri := range other {
		out = append(out, diagnosticPublication{uri: uri, diagnostics: byURI[uri]})
	}
	return out
}

func sourceForTestDiagnostic(doc, defaults *document, uri lsp.DocumentURI, path string) string {
	if uri == doc.uri {
		return doc.text
	}
	if defaults != nil && uri == defaults.uri {
		return defaults.text
	}
	data, ok := readCalleeSource(path)
	if !ok {
		return ""
	}
	return string(data)
}

// testProblemRange converts the loader's one-based rune column to LSP's
// zero-based UTF-16 column. With no source text it keeps the exact line and a
// conservative point; it never guesses an enclosing token.
func testProblemRange(source string, line, column int) lsp.Range {
	if line <= 0 {
		return documentStart
	}
	line--
	character := max(column-1, 0)
	if source == "" {
		character = 0
	}
	lines := strings.Split(source, "\n")
	if line < len(lines) {
		runes := []rune(lines[line])
		character = min(character, len(runes))
		character = utf16Len(string(runes[:character]))
	}
	start := lsp.Position{Line: line, Character: character}
	return lsp.Range{Start: start, End: start}
}
