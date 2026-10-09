package lsp

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/sourcegraph/go-lsp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A `use:` entry makes another file's declarations nameable here as `alias.Name`.
// This file is the editor's half of that: it finds the qualified names a document
// writes, follows each into the module that declares it, and offers the edits that
// span the two files.
//
// There is one resolver. Which file an alias means is answered by
// [flowfile.ResolveCallTarget], the function the compiler resolves a `use:` path
// with, so a module the compiler would refuse (absolute, climbing out of the
// importing file's directory, escaping through a symlink) is never navigated to,
// completed from or renamed in. Every read is bounded the way a `call:` target's
// is, and every question that cannot be answered exactly is answered with nothing.
//
// What reads the workspace is bounded by [maxWorkspaceFiles] and
// [maxWorkspaceVisits], and a bound that is reached refuses a rename instead of
// completing it over a partial view: an edit that misses an importer leaves a file
// that no longer compiles.

const (
	// maxWorkspaceFiles is the most Flowfiles one workspace walk reads.
	maxWorkspaceFiles = 256

	// maxWorkspaceVisits is the most directory entries one walk looks at, files or
	// not, so a tree of a million small files costs a bounded walk.
	maxWorkspaceVisits = 8192

	// maxWorkspaceSymbols is the most symbols one workspace/symbol answers with.
	maxWorkspaceSymbols = 200

	// maxAddUseActions is the most modules one unresolved name is offered.
	maxAddUseActions = 5
)

// A usedModule is one `use:` entry that resolves to a file this document may read.
type usedModule struct {
	// alias is the word the module is named by here.
	alias string

	// path is the module file as the compiler resolves it.
	path string

	// written is the `path:` as the document wrote it.
	written string
}

// usedModules lists the document's `use:` entries that resolve, at most
// [v1.MaxUsesPerFile] of them, the bound a compile holds a file to.
func usedModules(doc *document) []usedModule {
	if doc.parsed == nil {
		return nil
	}
	callerPath, ok := doc.filesystemPath()
	if !ok {
		return nil
	}

	var out []usedModule
	for _, e := range doc.parsed.entries {
		if e.key != "use" {
			continue
		}
		for _, alias := range nestedEntries(e) {
			if len(out) == v1.MaxUsesPerFile {
				return out
			}
			for _, field := range nestedEntries(alias) {
				if field.key != "path" {
					continue
				}
				target, err := flowfile.LiteralText(field.valueText())
				if target == "" || err != nil {
					continue
				}
				located := flowfile.ResolveCallTarget(callerPath, target)
				if located.Refusal != flowfile.CallTargetResolved {
					continue
				}
				out = append(out, usedModule{alias: alias.key, path: located.Path, written: target})
			}
		}
	}

	return out
}

// loadModule reads and parses the file at path, bounded by [maxDocumentBytes], and
// reports false for anything that is not a regular file of that size.
func loadModule(path string) (*document, bool) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, false
	}
	data, ok := readCalleeSource(path)
	if !ok {
		return nil, false
	}
	doc := newDocument(fileURI(path), 0, string(data), nil)
	if doc.parsed == nil {
		return nil, false
	}

	return doc, true
}

// A declaration is a type, error or function a module declares.
type declaration struct {
	block string // "types", "errors" or "functions"
	kind  lsp.SymbolKind
	entry *entry
}

// declarationsNamed lists what doc declares under name, across the three blocks.
func declarationsNamed(doc *document, name string) []declaration {
	var out []declaration
	for _, e := range doc.parsed.entries {
		for _, block := range declarationBlocks {
			if e.key != block.key {
				continue
			}
			for _, d := range nestedEntries(e) {
				if d.key == name {
					out = append(out, declaration{block: block.key, kind: block.kind, entry: d})
				}
			}
		}
	}

	return out
}

// field is the scalar text of the nested key of an entry, or "".
func field(e *entry, key string) string {
	for _, f := range nestedEntries(e) {
		if f.key == key {
			return f.valueText()
		}
	}

	return ""
}

// A qualifiedToken is `alias.name` found in a run of source.
type qualifiedToken struct {
	alias, name string
	// start, nameStart and end are byte offsets: the alias, the name, and the end.
	start, nameStart, end int
}

// qualifiedTokens finds every `alias.name` in s whose alias is accepted.
//
// A token must start a word: preceded by an identifier byte, a dot (a member of
// something else: `steps.x.ids.y`) or a slash (a path: `./lib/ids.yaml`) it is
// not one. With skipStrings a CEL string literal is not read, which is how a
// fence is scanned; a raw scan of a document reads everything.
func qualifiedTokens(s string, accept func(alias string) bool, skipStrings bool) []qualifiedToken {
	var out []qualifiedToken
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case skipStrings && (c == '"' || c == '\''):
			i = skipStringLiteral(s, i)
		case isIdentStart(c) && (i == 0 || !isTokenBoundary(s[i-1])):
			j := i
			for j < len(s) && isIdentPart(s[j]) {
				j++
			}
			alias := s[i:j]
			if j+1 < len(s) && s[j] == '.' && isIdentStart(s[j+1]) && accept(alias) {
				k := j + 1
				for k < len(s) && isIdentPart(s[k]) {
					k++
				}
				out = append(out, qualifiedToken{alias: alias, name: s[j+1 : k], start: i, nameStart: j + 1, end: k})
				j = k
			}
			i = j
		default:
			i++
		}
	}

	return out
}

func isTokenBoundary(c byte) bool { return isIdentPart(c) || c == '.' || c == '/' }

// A qualifiedSite is one written `alias.name` the model can place.
type qualifiedSite struct {
	module      usedModule
	alias, name string
	// rng is the whole `alias.name`; nameRng is the name alone.
	rng, nameRng lsp.Range
}

// qualifiedSites is every qualified name of a used module the document writes,
// with how many the model found and could not place (in a folded block scalar).
type qualifiedSites struct {
	sites []qualifiedSite

	// unplaced names the qualified names found and not placed (in a folded block
	// scalar, or written with an escape), one entry per occurrence.
	unplaced []string
}

// proseKeys are the keys whose scalar values are text for a reader, not names.
var proseKeys = []string{"description", "message", "name"}

// collectQualified finds the qualified names of the document's used modules in the
// places a name is written: inside `${...}` fences and in plain scalars (a `type:`,
// a `fail: error:`, a function's `params:`).
func collectQualified(doc *document, mods []usedModule) qualifiedSites {
	var out qualifiedSites
	byAlias := map[string]usedModule{}
	for _, m := range mods {
		byAlias[m.alias] = m
	}
	accept := func(a string) bool { _, ok := byAlias[a]; return ok }

	add := func(tok qualifiedToken, span func(start, end int) (lsp.Range, bool)) {
		whole, ok1 := span(tok.start, tok.end)
		name, ok2 := span(tok.nameStart, tok.end)
		if !ok1 || !ok2 {
			out.unplaced = append(out.unplaced, tok.name)
			return
		}
		out.sites = append(out.sites, qualifiedSite{module: byAlias[tok.alias], alias: tok.alias, name: tok.name, rng: whole, nameRng: name})
	}

	var walk func(v *value)
	var walkEntries func(entries []*entry)
	walk = func(v *value) {
		if v == nil {
			return
		}
		walkEntries(v.entries)
		for _, item := range v.items {
			walk(item)
		}
		if v.kind != kindScalar {
			return
		}
		if len(v.fences) > 0 {
			for _, f := range v.fences {
				for _, tok := range qualifiedTokens(f.source, accept, true) {
					add(tok, func(s, e int) (lsp.Range, bool) { return v.fenceSpan(doc.index, f, s, e) })
				}
			}
			return
		}
		for _, tok := range qualifiedTokens(v.text, accept, false) {
			add(tok, func(s, e int) (lsp.Range, bool) { return v.textSpan(doc.index, s, e) })
		}
	}
	walkEntries = func(entries []*entry) {
		for _, e := range entries {
			// Literal prose names nothing; the same key holding a `${...}` is an
			// expression and is read like any other.
			prose := slices.Contains(proseKeys, e.key) && e.value != nil && e.value.kind == kindScalar && len(e.value.fences) == 0
			if e.key == "use" || prose {
				continue
			}
			walk(e.value)
		}
	}
	walkEntries(doc.parsed.entries)

	return out
}

// writtenTokens counts the `alias.name` spellings of the accepted aliases in the
// document's text outside whole-line comments: the number a rename must have
// placed every one of.
func writtenTokens(text string, accept func(string) bool, name string) int {
	n := 0
	for line := range strings.SplitSeq(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		for _, tok := range qualifiedTokens(line, accept, false) {
			if tok.name == name {
				n++
			}
		}
	}

	return n
}

// qualifiedAt is the qualified name the cursor is on, with the module it names.
func qualifiedAt(doc *document, pos lsp.Position) (qualifiedSite, bool) {
	if doc.parsed == nil || !doc.speaksFlowfile() {
		return qualifiedSite{}, false
	}
	mods := usedModules(doc)
	if len(mods) == 0 {
		return qualifiedSite{}, false
	}
	for _, s := range collectQualified(doc, mods).sites {
		if contains(s.rng, pos) {
			return s, true
		}
	}

	return qualifiedSite{}, false
}

// qualifiedDefinition is go-to-definition on `alias.Name`: the key that declares
// Name in the module the alias names. Nil when the module cannot be read or does
// not declare the name, never a guess.
func qualifiedDefinition(doc *document, pos lsp.Position) []lsp.Location {
	site, ok := qualifiedAt(doc, pos)
	if !ok {
		return nil
	}
	module, ok := loadModule(site.module.path)
	if !ok {
		return nil
	}
	var out []lsp.Location
	for _, d := range declarationsNamed(module, site.name) {
		out = append(out, lsp.Location{URI: module.uri, Range: d.entry.keyRange})
	}

	return out
}

// qualifiedHover describes a type or error a module declares, on its qualified
// name. A module's function is described by the hover that already answers for
// every declared function, so it is left to that.
func qualifiedHover(doc *document, pos lsp.Position) *lsp.Hover {
	site, ok := qualifiedAt(doc, pos)
	if !ok {
		return nil
	}
	module, ok := loadModule(site.module.path)
	if !ok {
		return nil
	}
	decls := declarationsNamed(module, site.name)
	if len(decls) != 1 || decls[0].block == "functions" {
		return nil
	}
	d := decls[0]

	var b strings.Builder
	noun := "a type"
	if d.block == "errors" {
		noun = "an error"
	}
	fmt.Fprintf(&b, "**`%s.%s`** — %s declared by the module `%s` (`%s@%s`).",
		site.alias, site.name, noun, site.alias, site.module.written, v1.ContentDigest([]byte(module.text)))
	if base := field(d.entry, "type"); base != "" {
		fmt.Fprintf(&b, "\n\nBase type `%s`.", base)
	}
	if must := field(d.entry, "must"); must != "" {
		fmt.Fprintf(&b, "\n\nMust satisfy `%s`.", must)
	}
	if desc := field(d.entry, "description"); desc != "" {
		fmt.Fprintf(&b, "\n\n%s", desc)
	}

	return markdownHover(b.String(), site.rng)
}

// qualifiedValueCandidates completes `alias.|` in a value that names a type or an
// error: the declarations of the module the alias names. Nil when the cursor is not
// after a used alias and a dot, or the key is not one that holds such a name.
func qualifiedValueCandidates(doc *document, path []string, key, before string, pos lsp.Position) []lsp.CompletionItem {
	var block string
	switch {
	case key == "error":
		block = "errors"
	case key == "type" || key == "returns" || (len(path) > 0 && path[len(path)-1] == "params"):
		block = "types"
	default:
		return nil
	}
	word := trailingWord(before, func(c byte) bool { return isIdentPart(c) || c == '.' })
	alias, prefix, ok := strings.Cut(word, ".")
	if !ok || strings.Contains(prefix, ".") {
		return nil
	}
	i := slices.IndexFunc(usedModules(doc), func(m usedModule) bool { return m.alias == alias })
	if i < 0 {
		return nil
	}
	module, ok := loadModule(usedModules(doc)[i].path)
	if !ok {
		return nil
	}

	var items []lsp.CompletionItem
	for _, e := range module.parsed.entries {
		if e.key != block {
			continue
		}
		for _, d := range nestedEntries(e) {
			if len(items) == maxWorkspaceSymbols {
				break
			}
			detail := field(d, "type")
			if block == "errors" {
				detail = "error"
			}
			items = append(items, lsp.CompletionItem{
				Label:         d.key,
				Kind:          lsp.CIKStruct,
				Detail:        detail,
				Documentation: field(d, "description"),
				TextEdit:      &lsp.TextEdit{Range: rangeBack(pos, prefix), NewText: d.key},
			})
		}
	}

	return items
}

// A workspace is the folders the client opened and a way to prefer an open buffer
// over the file on disk.
type workspace struct {
	roots []string
	open  func(path string) (*document, bool)
}

// document is the file at path as the editor sees it: the open buffer when there
// is one, else the file read within the usual bounds.
//
// An open buffer that does not parse answers false and never falls back to the
// disk copy: the disk is not what the editor holds, and an edit computed from it
// would be sent for the buffer's URI.
func (w workspace) document(path string) (*document, bool) {
	if doc, open := w.openBuffer(path); open {
		return doc, doc.parsed != nil
	}

	return loadModule(path)
}

// openBuffer is the open document for path, found under the path as written and
// under its canonical spelling.
func (w workspace) openBuffer(path string) (*document, bool) {
	if w.open == nil {
		return nil, false
	}
	if doc, ok := w.open(path); ok {
		return doc, true
	}
	if canon := canonicalPath(path); canon != path {
		return w.open(canon)
	}

	return nil, false
}

// canonicalPath is path with symlinks followed, or path itself when they cannot
// be: the one spelling a file is keyed by, so a workspace reached through a
// symlinked directory yields each file once and finds its open buffer.
func canonicalPath(path string) string {
	if real, err := filepath.EvalSymlinks(path); err == nil {
		return real
	}

	return filepath.Clean(path)
}

// flowfiles lists the Flowfile-shaped YAML files under the roots, in walk order.
//
// One budget covers the whole request: [maxWorkspaceVisits] directory entries and
// [maxWorkspaceFiles] files however many roots there are. Roots are followed
// through symlinks once (they are the folders the client authorised), nested and
// duplicate roots are walked once, and below a root a symlink is never followed.
// `.git` and `node_modules` are not entered, a hidden directory is entered only
// when hidden is set, and a test suite is not a Flowfile.
//
// incomplete reports that the listing may be missing files: a budget was spent or
// an entry could not be read. The callers that edit refuse on it.
func flowfiles(roots []string, hidden bool) (paths []string, incomplete bool) {
	var dirs []string
	for _, root := range roots {
		dirs = append(dirs, canonicalPath(root))
	}
	slices.Sort(dirs)
	dirs = slices.Compact(dirs)
	dirs = slices.DeleteFunc(dirs, func(d string) bool {
		return slices.ContainsFunc(dirs, func(o string) bool { return o != d && strings.HasPrefix(d, o+string(filepath.Separator)) })
	})

	visits := 0
	for _, dir := range dirs {
		_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				incomplete = true
				return nil
			}
			if visits++; visits > maxWorkspaceVisits {
				incomplete = true
				return filepath.SkipAll
			}
			name := d.Name()
			if d.IsDir() {
				if path != dir && (name == ".git" || name == "node_modules" || (!hidden && strings.HasPrefix(name, "."))) {
					return filepath.SkipDir
				}
				return nil
			}
			ext := filepath.Ext(name)
			if !d.Type().IsRegular() || (ext != ".yaml" && ext != ".yml") ||
				strings.HasSuffix(name, ".test"+ext) || name == "testdefaults.yaml" {
				return nil
			}
			if len(paths) == maxWorkspaceFiles {
				incomplete = true
				return filepath.SkipAll
			}
			paths = append(paths, path)

			return nil
		})
		if visits > maxWorkspaceVisits || len(paths) == maxWorkspaceFiles && incomplete {
			break
		}
	}

	return paths, incomplete
}

// isModule reports whether doc is a declarations-only file: it declares something
// and has no steps.
func isModule(doc *document) bool {
	if doc.parsed == nil || len(doc.parsed.steps) > 0 {
		return false
	}

	return len(declarationSymbols(doc)) > 0
}

// moduleFiles lists the modules under dir, cheaply rejecting a file that declares
// nothing before parsing it.
func (w workspace) moduleFiles(roots []string) (modules []*document, truncated bool) {
	paths, truncated := flowfiles(roots, false)
	for _, path := range paths {
		doc, ok := w.document(path)
		if !ok || !(declaresKey(doc.text, "types") || declaresKey(doc.text, "errors") || declaresKey(doc.text, "functions")) {
			continue
		}
		if isModule(doc) {
			modules = append(modules, doc)
		}
	}

	return modules, truncated
}

// workspaceSymbols answers workspace/symbol over the modules in the workspace
// folders: every type, error and function they declare whose name contains the
// query, case-insensitively. Bounded by [maxWorkspaceSymbols]; a truncated walk
// returns what it found, since a symbol picker is better partial than empty.
func workspaceSymbols(w workspace, query string) []lsp.SymbolInformation {
	out := []lsp.SymbolInformation{}
	seen := map[string]bool{}
	query = strings.ToLower(query)
	{
		modules, _ := w.moduleFiles(w.roots)
		for _, module := range modules {
			if seen[string(module.uri)] {
				continue
			}
			seen[string(module.uri)] = true
			label := cmpName(module)
			for _, sym := range declarationSymbols(module) {
				if !strings.Contains(strings.ToLower(sym.Name), query) {
					continue
				}
				sym.ContainerName = label + " " + sym.ContainerName
				if out = append(out, sym); len(out) == maxWorkspaceSymbols {
					return out
				}
			}
		}
	}

	return out
}

// cmpName is the name a module goes by in a list: its `name:`, else its file name.
func cmpName(module *document) string {
	if module.parsed.nameEntry != nil && module.parsed.nameEntry.valueText() != "" {
		return module.parsed.nameEntry.valueText()
	}

	return strings.TrimSuffix(filepath.Base(string(module.uri)), filepath.Ext(string(module.uri)))
}

// addUseActions offers, for a qualified name whose alias this file does not use,
// to add the `use:` entry for a module in the file's own directory tree that
// declares the name. That is the only place a `use:` can reach, because a path may
// not climb above the importing file; the path written is checked back through the
// compiler's resolver before it is offered.
func addUseActions(doc *document, params codeActionParams) []codeAction {
	callerPath, ok := doc.filesystemPath()
	if !ok || doc.parsed == nil {
		return nil
	}
	used := usedModules(doc)
	accept := func(a string) bool {
		return v1.ValidModuleAlias(a) == nil &&
			!slices.ContainsFunc(used, func(m usedModule) bool { return m.alias == a })
	}

	// The tokens an editor squiggles are the ones to repair, so only a name under a
	// diagnostic the request covers is considered.
	var diagnostics []lsp.Range
	for _, c := range diagnoseCarried(doc) {
		if overlaps(c.published.Range, params.Range) {
			diagnostics = append(diagnostics, c.published.Range)
		}
	}
	if len(diagnostics) == 0 {
		return nil
	}

	var unresolved []qualifiedToken
	for i := 0; i < doc.index.lineCount(); i++ {
		if i < params.Range.Start.Line || i > params.Range.End.Line {
			continue
		}
		line := doc.index.line(i)
		for _, tok := range qualifiedTokens(line, accept, false) {
			rng := lsp.Range{
				Start: lsp.Position{Line: i, Character: utf16Len(line[:tok.start])},
				End:   lsp.Position{Line: i, Character: utf16Len(line[:tok.end])},
			}
			if slices.ContainsFunc(diagnostics, func(d lsp.Range) bool { return overlaps(d, rng) }) {
				unresolved = append(unresolved, tok)
			}
		}
	}
	if len(unresolved) == 0 {
		return nil
	}

	dir := filepath.Dir(callerPath)
	modules, _ := workspace{}.moduleFiles([]string{dir})
	var actions []codeAction
	for _, tok := range unresolved {
		for _, module := range modules {
			target, ok := module.filesystemPath()
			if !ok || target == callerPath || len(declarationsNamed(module, tok.name)) == 0 {
				continue
			}
			rel, err := filepath.Rel(dir, target)
			if err != nil {
				continue
			}
			written := "./" + filepath.ToSlash(rel)
			if located := flowfile.ResolveCallTarget(callerPath, written); located.Refusal != flowfile.CallTargetResolved {
				continue
			}
			edit, ok := useEdit(doc, tok.alias, written)
			if !ok {
				continue
			}
			actions = append(actions, codeAction{
				Title: fmt.Sprintf("Add `use: {%s: %s}` for `%s.%s`", tok.alias, written, tok.alias, tok.name),
				Kind:  lsp.CAKQuickFix,
				Edit:  &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{string(doc.uri): {edit}}},
			})
			if len(actions) == maxAddUseActions {
				return actions
			}
		}
	}

	return actions
}

// useEdit is the insertion that adds `alias: {path}` to the document's `use:`
// block, or a new block after `name:` when there is none. False when the block is
// written in a shape the edit cannot extend (flow style) or there is nowhere to
// put it.
func useEdit(doc *document, alias, path string) (lsp.TextEdit, bool) {
	eol := "\n"
	if strings.Contains(doc.text, "\r\n") {
		eol = "\r\n"
	}
	var block *entry
	for _, e := range doc.parsed.entries {
		if e.key == "use" {
			block = e
		}
	}
	if block == nil {
		after := doc.parsed.nameEntry
		if after == nil || after.value == nil {
			return lsp.TextEdit{}, false
		}
		at := lsp.Position{Line: after.value.rng.End.Line + 1}

		return lsp.TextEdit{
			Range:   lsp.Range{Start: at, End: at},
			NewText: fmt.Sprintf("use:%[3]s  %[1]s:%[3]s    path: %[2]s%[3]s", yamlScalar(alias), yamlScalar(path), eol),
		}, true
	}

	entries := nestedEntries(block)
	if len(entries) == 0 || entries[0].value == nil || block.keyRange.Start.Line == entries[0].keyRange.Start.Line {
		return lsp.TextEdit{}, false
	}
	last := entries[len(entries)-1]
	if last.value == nil || last.value.kind != kindMapping || len(last.value.entries) == 0 {
		return lsp.TextEdit{}, false
	}
	indent := strings.Repeat(" ", entries[0].keyRange.Start.Character)
	inner := strings.Repeat(" ", last.value.entries[0].keyRange.Start.Character)
	if last.value.entries[0].keyRange.Start.Line == last.keyRange.Start.Line {
		return lsp.TextEdit{}, false // flow style: `ids: {path: ...}`
	}
	end := last.value.rng.End
	for _, f := range last.value.entries {
		if f.value != nil && f.value.rng.End.Line > end.Line {
			end = f.value.rng.End
		}
	}
	at := lsp.Position{Line: end.Line + 1}

	return lsp.TextEdit{
		Range:   lsp.Range{Start: at, End: at},
		NewText: fmt.Sprintf("%s%s:%s%spath: %s%s", indent, yamlScalar(alias), eol, inner, yamlScalar(path), eol),
	}, true
}

// plainScalar is a word that is a plain YAML scalar of the same text everywhere:
// letters, digits and the punctuation of a relative path, starting with a letter,
// a dot or an underscore.
var plainScalar = regexp.MustCompile(`^[A-Za-z_.][A-Za-z0-9_./-]*$`)

// yamlScalar renders s as a YAML scalar that reads back as exactly s: plain when
// that is certain, double-quoted otherwise (`yes`, `./lib/a # b.yaml`).
func yamlScalar(s string) string {
	if plainScalar.MatchString(s) && !yamlAmbiguous(s) && s != "~" {
		return s
	}

	return strconv.Quote(s)
}

// repinActions offers to re-stamp the digests of the `use:` entries when a
// diagnostic the request covers is a pin mismatch.
//
// The rewrite is [flowfile.RepinUses], the function behind `flow fix --repin`, so
// the editor and the command stamp the same bytes the same way. It is offered as a
// quick-fix a person picks after reading the module, never as a fix-all: a pin is
// the author saying "I read these bytes", and no save should say it for them. Like
// every migration here it carries the whole document, because the rewrite reports
// no ranges.
func repinActions(doc *document, params codeActionParams) []codeAction {
	path, ok := doc.filesystemPath()
	if !ok {
		return nil
	}
	var covered []lsp.Diagnostic
	for _, c := range diagnoseCarried(doc) {
		if c.published.Code == string(v1.DiagnosticCodeModulePinMismatch) && overlaps(c.published.Range, params.Range) {
			covered = append(covered, c.published)
		}
	}
	if len(covered) == 0 {
		return nil
	}
	result, err := flowfile.RepinUses(path, []byte(doc.text))
	if err != nil || len(result.Changes) == 0 || string(result.Source) == doc.text {
		return nil
	}

	return []codeAction{{
		Title:       fmt.Sprintf("Repin the module digests after reading the change (%s, rewrites the whole file)", plural(len(result.Changes), "digest")),
		Kind:        lsp.CAKQuickFix,
		Diagnostics: covered,
		Edit: &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{
			string(doc.uri): {{Range: wholeDocumentRange(doc), NewText: string(result.Source)}},
		}},
	}}
}

// prepareRenameQualified is prepareRename on `alias.Name`: the name part is the
// placeholder, because the alias is the importer's own word.
func prepareRenameQualified(doc *document, pos lsp.Position) (lsp.Range, string, bool) {
	site, ok := qualifiedAt(doc, pos)
	if !ok {
		return lsp.Range{}, "", false
	}

	return site.nameRng, site.name, true
}

// renameQualified renames a declaration of a module from a qualified name of it:
// the key in the module and every `alias.Name` in every importer in the workspace.
//
// handled is false when the cursor is not on a qualified name, so the caller falls
// through to the single-file renames. Once handled, every doubt is a refusal, and a
// refusal names the reason, because a rename that leaves one importer stale breaks
// a file nobody opened:
//
//   - no workspace folder, or a walk that hit a bound, means importers could be
//     missed;
//   - the module must declare the name exactly once and not the new name;
//   - the module's own bare mentions of the name (a function body calling a
//     function, a type naming another) are not tracked, so any other mention
//     refuses;
//   - an importer spelling the name somewhere the model cannot place (a comment
//     beside code, a string, a folded block scalar) refuses.
//
// Test suites are not Flowfiles and are not edited; `flow test` reports a stale
// name in one.
func renameQualified(doc *document, w workspace, pos lsp.Position, newName string) (edit *lsp.WorkspaceEdit, handled bool, err error) {
	site, ok := qualifiedAt(doc, pos)
	if !ok {
		return nil, false, nil
	}
	refuse := func(format string, args ...any) (*lsp.WorkspaceEdit, bool, error) {
		return nil, true, renameError(fmt.Sprintf(format, args...))
	}

	module, ok := w.document(site.module.path)
	if !ok {
		return refuse("the module `%s` cannot be read", site.alias)
	}
	decls := declarationsNamed(module, site.name)
	if len(decls) != 1 {
		return refuse("the module `%s` does not declare `%s` exactly once", site.alias, site.name)
	}
	if newName == site.name {
		return refuse("%q is already the name", newName)
	}
	switch {
	case decls[0].block == "functions" && (!v1.IsCELIdentifier(newName) || flowfile.IsCELReservedIdentifier(newName)):
		return refuse("%q is not a valid function name", newName)
	case decls[0].block != "functions" && !v1.IsModuleName(v1.QualifiedName(site.alias, newName)):
		return refuse("%q is not a valid %s name: start with a capital letter, then letters, digits and underscores", newName, strings.TrimSuffix(decls[0].block, "s"))
	case len(declarationsNamed(module, newName)) > 0:
		return refuse("the module `%s` already declares `%s`", site.alias, newName)
	}
	if n := wordMentions(module.text, site.name) - 1; n > 0 {
		return refuse("the module itself mentions `%s` %d more time(s), in code the server does not track; rename by hand", site.name, n)
	}
	if len(w.roots) == 0 {
		return refuse("no workspace folder is open, so the importers of `%s` cannot be found", site.module.written)
	}

	changes := map[string][]lsp.TextEdit{}
	key := newName
	if yamlAmbiguous(newName) {
		key = `"` + newName + `"`
	}
	changes[string(module.uri)] = []lsp.TextEdit{{Range: decls[0].entry.keyRange, NewText: key}}

	var candidates []*document
	seen := map[string]bool{}
	consider := func(d *document) {
		if p, ok := d.filesystemPath(); ok && !seen[canonicalPath(p)] {
			seen[canonicalPath(p)] = true
			candidates = append(candidates, d)
		}
	}
	consider(doc)
	paths, incomplete := flowfiles(w.roots, true)
	if incomplete {
		return refuse("the workspace could not be listed completely (more than %d Flowfiles or %d entries, or an unreadable folder); open a narrower folder so every importer of `%s` is seen", maxWorkspaceFiles, maxWorkspaceVisits, site.module.written)
	}
	for _, p := range paths {
		if seen[canonicalPath(p)] {
			continue
		}
		d, ok := w.document(p)
		if buffer, open := w.openBuffer(p); open && !ok {
			if strings.Contains(buffer.text, site.name) {
				return refuse("the open buffer %s mentions `%s` but does not parse, so it cannot be renamed safely; fix it or rename by hand", buffer.uri, site.name)
			}
			continue
		}
		if !ok {
			// A file the server cannot read as a Flowfile might still be an
			// importer, so it is a reason unless it provably is not one.
			raw, readable := readCalleeSource(p)
			switch {
			case !readable:
				return refuse("%s cannot be read within the %d byte bound, so it cannot be ruled out as an importer of `%s`", p, maxDocumentBytes, site.module.written)
			case strings.Contains(string(raw), site.name):
				return refuse("%s mentions `%s` but does not parse, so it cannot be renamed safely; fix it or rename by hand", p, site.name)
			}
			continue
		}
		// Every loaded file is analysed, not only those whose raw text spells the
		// name: a name written with an escape decodes to it without containing it.
		consider(d)
	}

	edited := map[string]bool{canonicalPath(site.module.path): true}
	for _, importer := range candidates {
		mods := usedModules(importer)
		aliases := map[string]bool{}
		for _, m := range mods {
			if m.path == site.module.path {
				aliases[m.alias] = true
			}
		}
		if len(aliases) == 0 {
			continue
		}
		found := collectQualified(importer, mods)
		var edits []lsp.TextEdit
		for _, s := range found.sites {
			if s.name == site.name && aliases[s.alias] {
				edits = append(edits, lsp.TextEdit{Range: s.nameRng, NewText: newName})
			}
		}
		if slices.Contains(found.unplaced, site.name) {
			return refuse("`%s` is written in %s where the server cannot place it (a folded block scalar or an escape); rename by hand", site.name, importer.uri)
		}
		if written := writtenTokens(importer.text, func(a string) bool { return aliases[a] }, site.name); written != len(edits) {
			return refuse("%d spelling(s) of `%s` in %s are in text the server does not track; renaming would leave them stale", written-len(edits), site.name, importer.uri)
		}
		if len(edits) > 0 {
			changes[string(importer.uri)] = edits
			if p, ok := importer.filesystemPath(); ok {
				edited[canonicalPath(p)] = true
			}
		}
	}

	// An edit changes the bytes of every file it touches, so a `digest:` pin on any
	// of them goes stale. The server does not stamp pins (a pin is the author's
	// statement that they read the bytes), so it refuses and leaves the order to
	// `flow fix --repin` after the rename.
	for _, d := range candidates {
		path, ok := d.filesystemPath()
		if !ok {
			continue
		}
		pins, err := flowfile.CallPins([]byte(d.text))
		if err != nil {
			return refuse("%s could not be read for pins: %s", d.uri, err)
		}
		for _, pin := range pins {
			if located := flowfile.ResolveCallTarget(path, pin.Call); pin.Alias != "" && located.Refusal == flowfile.CallTargetResolved && edited[canonicalPath(located.Path)] {
				return refuse("%s pins `%s` by digest, and a rename changes that file; rename by hand and repin with `flow fix --repin`", d.uri, pin.Call)
			}
		}
	}

	return &lsp.WorkspaceEdit{Changes: changes}, true, nil
}

// wordMentions counts whole-word occurrences of word in text outside whole-line
// comments.
func wordMentions(text, word string) int {
	n := 0
	for line := range strings.SplitSeq(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		for i := 0; ; {
			j := strings.Index(line[i:], word)
			if j < 0 {
				break
			}
			at := i + j
			end := at + len(word)
			if (at == 0 || !isIdentPart(line[at-1])) && (end == len(line) || !isIdentPart(line[end])) {
				n++
			}
			i = end
		}
	}

	return n
}
