// Command citations checks every `path.go:NNN` citation the hand-written
// documentation makes against the tree it describes.
//
// The house style asks a claim to point at the line that makes it true, and
// that is the best thing about reading these documents. It is also the first
// thing to rot: a file is renamed, a function moves forty lines, and the
// sentence keeps pointing where the code was. Nothing structural refuses that,
// so this does, the way tools/vacuity and tools/agentconfig turn a convention
// into a test (#1760).
//
// Three checks, in the order a reader would notice them:
//
//   - the cited path exists, spelled from the repository root;
//   - the cited line, or the end of the cited range, is inside the file;
//   - when the same line of prose names an identifier in backticks near the
//     citation, the cited range contains that identifier. This is the check
//     that catches the common drift, where the file survives and the function
//     moves: the path resolves, the line exists, and it holds something else.
//
// What it does not check is whether the cited lines still say what the
// sentence claims; no path check can. `docs/DSL.md`'s "Since written" notes
// remain the manual answer to that.
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Citation is one `path.go:NNN` or `path.go:NNN-MMM` in a document.
type Citation struct {
	// Doc and Line are where the citation is written, for the finding.
	Doc  string
	Line int

	// Text is the citation as written, backticks and all.
	Text string

	// Path is the cited file as written; From and To the cited range, both
	// inclusive, equal for a single line.
	Path     string
	From, To int

	// Symbols are the identifiers named in backticks on the same line of
	// prose within [symbolReach] characters of the citation: what the
	// sentence says the cited lines hold.
	Symbols []string
}

// Finding is one citation that does not hold.
type Finding struct {
	Citation Citation
	Problem  string
}

// String renders a finding the way a compiler names a position, so an editor
// can jump to the document line that needs fixing.
func (f Finding) String() string {
	return fmt.Sprintf("%s:%d: %s: %s", f.Citation.Doc, f.Citation.Line, f.Citation.Text, f.Problem)
}

// symbolReach is how far, in characters, an identifier may sit from the
// citation on the same line and still be read as naming what the cited
// lines hold. A sentence is about eighty characters; further than that the
// identifier belongs to the next claim.
const symbolReach = 80

var (
	// citationPattern is a backticked `path.go:NNN` or `path.go:NNN-MMM`.
	// Go files only: those are the citations the house style asks for, and a
	// `Makefile:12` or a `workflow.yaml:7` names a line in a file whose
	// shape no symbol check applies to.
	citationPattern = regexp.MustCompile("`([A-Za-z0-9_./-]+\\.go):([0-9]+)(?:-([0-9]+))?`")

	// backtickPattern is any backticked span on a line.
	backtickPattern = regexp.MustCompile("`([^`]+)`")

	// identifierPattern is what a backticked span has to look like to be read
	// as a Go identifier, with at most one qualifier: `sdk.Main`, `Task.Fn`,
	// `checkLiteralHost`. The last segment is the identifier looked for.
	identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?$`)
)

// Documents lists the hand-written Markdown this checks, relative to root:
// the root-level documents and everything under docs/ except the two trees
// that are not claims about the current tree. docs/reference is generated
// and drift-tested on its own; docs/plans holds dated proposals and reviews
// whose citations are pinned to the revision they were written against, and
// re-pointing those would rewrite what a plan said at the time.
func Documents(root string) ([]string, error) {
	var docs []string
	for _, name := range []string{"AGENTS.md", "README.md", "CONTRIBUTING.md", "SECURITY.md", "THREAT_MODEL.md"} {
		if _, err := os.Stat(filepath.Join(root, name)); err == nil {
			docs = append(docs, name)
		}
	}
	err := filepath.WalkDir(filepath.Join(root, "docs"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if rel == "docs/reference" || rel == "docs/plans" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(rel, ".md") {
			docs = append(docs, rel)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(docs)
	return docs, nil
}

// Extract reads every citation out of one document's bytes. A line the
// scanner cannot hold is an error rather than a silent stop, since a document
// half-read is a document half-checked.
func Extract(doc string, data []byte) ([]Citation, error) {
	var citations []Citation
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	line := 0
	for scanner.Scan() {
		line++
		text := scanner.Text()
		for _, m := range citationPattern.FindAllStringSubmatchIndex(text, -1) {
			c := Citation{
				Doc:  doc,
				Line: line,
				Text: text[m[0]:m[1]],
				Path: text[m[2]:m[3]],
			}
			c.From, _ = strconv.Atoi(text[m[4]:m[5]])
			c.To = c.From
			if m[6] >= 0 {
				c.To, _ = strconv.Atoi(text[m[6]:m[7]])
			}
			c.Symbols = symbolsNear(text, m[0], m[1])
			citations = append(citations, c)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("%s: after line %d: %w", doc, line, err)
	}
	return citations, nil
}

// symbolsNear is every backticked identifier on the line within
// [symbolReach] of the citation spanning [start, end), by its last segment,
// deduplicated. In a table row the first cell is the row's subject, so its
// identifier counts whatever the distance: a row's reference column is at
// the far end of a sentence about the field in its first column. A plain
// lowercase word is left out: `env` or `main` in backticks is as likely a
// flag or a package as an identifier, and a check that fires on prose is one
// people learn to ignore.
func symbolsNear(text string, start, end int) []string {
	var symbols []string
	tableRow := strings.HasPrefix(strings.TrimSpace(text), "|")
	for i, m := range backtickPattern.FindAllStringSubmatchIndex(text, -1) {
		if m[0] == start {
			continue
		}
		distance := 0
		switch {
		case m[1] <= start:
			distance = start - m[1]
		case m[0] >= end:
			distance = m[0] - end
		}
		if distance > symbolReach && !(tableRow && i == 0) {
			continue
		}
		span := text[m[2]:m[3]]
		if !identifierPattern.MatchString(span) {
			continue
		}
		name := span[strings.LastIndex(span, ".")+1:]
		if strings.ToLower(name) == name {
			continue
		}
		if !slices.Contains(symbols, name) {
			symbols = append(symbols, name)
		}
	}
	return symbols
}

// Check reads every document, resolves every citation against root, and
// reports the ones that do not hold, with the number of citations checked.
func Check(root string, docs []string) ([]Finding, int, error) {
	var (
		findings []Finding
		total    int
		files    = map[string][]string{}
	)
	for _, doc := range docs {
		data, err := os.ReadFile(filepath.Join(root, doc))
		if err != nil {
			return nil, 0, err
		}
		citations, err := Extract(doc, data)
		if err != nil {
			return nil, 0, err
		}
		for _, c := range citations {
			total++
			if problem := check(root, c, files); problem != "" {
				findings = append(findings, Finding{Citation: c, Problem: problem})
			}
		}
	}
	return findings, total, nil
}

// check is the three checks for one citation, returning the first problem or
// "" when the citation holds. files caches each cited file's lines.
func check(root string, c Citation, files map[string][]string) string {
	if strings.HasPrefix(c.Path, "/") || strings.HasPrefix(c.Path, ".") {
		return "cite a path from the repository root"
	}
	if isModulePath(c.Path) {
		// A line in a dependency, `connectrpc.com/otelconnect/instruments.go:44`:
		// spelled with its module so a reader can find it, and checked
		// nowhere, since the module cache is not the tree.
		return ""
	}
	lines, ok := files[c.Path]
	if !ok {
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(c.Path)))
		if err != nil {
			if os.IsNotExist(err) {
				if hint := suffixMatches(root, c.Path); len(hint) > 0 {
					return fmt.Sprintf("no such file from the repository root; did you mean %s", strings.Join(hint, " or "))
				}
				return "no such file from the repository root"
			}
			return err.Error()
		}
		lines = strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
		files[c.Path] = lines
	}
	if c.From < 1 || c.From > c.To {
		return "the range is not a line, or runs backwards"
	}
	if c.To > len(lines) {
		return fmt.Sprintf("the file has %d lines", len(lines))
	}
	// One of the identifiers the sentence names has to be in the cited
	// lines. One rather than all: a sentence citing the `SecretInputs` field
	// may say what `Fn` receives in the same breath, and the citation is
	// for the field.
	cited := strings.Join(lines[c.From-1:c.To], "\n")
	if len(c.Symbols) == 0 {
		return ""
	}
	for _, symbol := range c.Symbols {
		if wordPattern(symbol).MatchString(cited) {
			return ""
		}
	}
	var hints []string
	for _, symbol := range c.Symbols {
		if at := findSymbol(lines, symbol); at > 0 {
			hints = append(hints, fmt.Sprintf("%s is at line %d", symbol, at))
		}
	}
	problem := fmt.Sprintf("the cited lines hold none of %s, which the sentence names", strings.Join(c.Symbols, ", "))
	if len(hints) > 0 {
		return problem + "; " + strings.Join(hints, ", ")
	}
	return problem + ", and nothing in the file does"
}

// wordPattern matches symbol as a whole word.
func wordPattern(symbol string) *regexp.Regexp {
	return regexp.MustCompile(`\b` + regexp.QuoteMeta(symbol) + `\b`)
}

// isModulePath reports whether a cited path starts with a module path rather
// than a directory of this repository: its first segment carries a dot, as
// `connectrpc.com` or `go.temporal.io` do and `cmd`, `pkg` and `docs` do not.
func isModulePath(path string) bool {
	first, _, hasDir := strings.Cut(path, "/")
	return hasDir && strings.Contains(first, ".")
}

// findSymbol is the first line of the file declaring or naming symbol, or 0.
// A declaration is preferred over a use, so the hint sends a reader to the
// function rather than to its first caller.
func findSymbol(lines []string, symbol string) int {
	word := wordPattern(symbol)
	decl := regexp.MustCompile(`^(func(\s+\([^)]*\))?\s+|type\s+|var\s+|const\s+)` + regexp.QuoteMeta(symbol) + `\b`)
	first := 0
	for i, line := range lines {
		if decl.MatchString(line) {
			return i + 1
		}
		if first == 0 && word.MatchString(line) {
			first = i + 1
		}
	}
	return first
}

// suffixMatches lists the Go files under root whose path ends with the cited
// one, for a citation written relative to a package rather than the root.
func suffixMatches(root, cited string) []string {
	var matches []string
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if rel == cited || strings.HasSuffix(rel, "/"+cited) {
			matches = append(matches, rel)
		}
		return nil
	})
	slices.Sort(matches)
	return matches
}
