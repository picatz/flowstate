package lsp

import (
	"fmt"
	"slices"
	"strings"

	"github.com/sourcegraph/go-lsp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Find references, document highlight and rename all answer one question: where
// is this step's id spelled? A step id is written once, as `id:`, and read by
// every `${steps.<id>.<output>}` that resolves to it, so the answer is the
// declaration plus the references [parsedFile.stepVisibleFrom] resolves to the
// same step. Go to definition asks the same resolver from the other end, which
// is what keeps the four features from disagreeing about what a name refers to.
//
// Only step ids have this today. A loop's iterator is bound by `as:` and read as
// a bare name, a different lookup that none of these three answer yet; the
// request on one answers nothing rather than guessing.

// stepSite is one place a step id is spelled: the id itself, as the characters
// an edit would replace, or a reference to it.
type stepSite struct {
	rng         lsp.Range
	declaration bool
}

// stepSites is every place the model can tell a step id is spelled.
type stepSites struct {
	target *parsedStep
	sites  []stepSite

	// unplaced counts references the model found but cannot place in the
	// document, because the value holding them was folded and its bytes are not
	// at a fixed distance from the document's. A rename that skipped them would
	// leave a broken reference, so it refuses; a list of references reports the
	// ones it has.
	unplaced int
}

// stepSitesAt resolves the step id the cursor is on, either as `id:` or as a
// reference to it, and returns every site of that step. It reports false when the
// cursor is on neither.
func stepSitesAt(doc *document, pos lsp.Position) (stepSites, bool) {
	if !doc.speaksFlowfile() || doc.parsed == nil {
		return stepSites{}, false
	}
	pos = clampPosition(pos)

	target := stepDeclaredAt(doc, pos)
	if target == nil {
		target = stepReferencedAt(doc, pos)
	}
	if target == nil {
		return stepSites{}, false
	}
	return collectStepSites(doc, target), true
}

// stepDeclaredAt is the step whose `id:` value the cursor is inside.
func stepDeclaredAt(doc *document, pos lsp.Position) *parsedStep {
	for _, s := range doc.parsed.steps {
		if s.idEntry != nil && s.idEntry.value != nil && s.id != "" && contains(s.idEntry.value.rng, pos) {
			return s
		}
	}
	return nil
}

// stepReferencedAt is the step a `steps.<id>` reference under the cursor
// resolves to. The cursor may be anywhere in the reference, `steps.` and the
// output name included; which part is the id is [siteContaining]'s business.
func stepReferencedAt(doc *document, pos lsp.Position) *parsedStep {
	var target *parsedStep
	forEachExpression(doc, func(from *parsedStep, ls loopScope, v *value) {
		if target != nil {
			return
		}
		f, cursor, ok := v.fenceAt(doc.index, pos)
		if !ok {
			return
		}
		// A reference the scanner finds, not the word under the cursor:
		// `steps.web` inside a string literal is text, and renaming it would be
		// editing a message.
		for _, ref := range stepRefsIn(f.source) {
			if cursor >= ref.span[0] && cursor <= ref.span[1] {
				target = resolveStepFrom(doc, from, ls, ref.step)
				return
			}
		}
	})
	return target
}

// resolveStepFrom resolves a step id written in an expression of from. A
// top-level expression (from nil) sees only top-level steps, because a block's
// body outputs do not escape it.
func resolveStepFrom(doc *document, from *parsedStep, ls loopScope, id string) *parsedStep {
	if from != nil {
		return doc.parsed.stepVisibleFrom(id, from, ls)
	}
	for _, s := range doc.parsed.steps {
		if s.id == id && len(s.scope) == 0 {
			return s
		}
	}
	return nil
}

// forEachExpression visits every value that may hold a fence, with the step it
// is evaluated in and the scope within that step. The workflow's own `outputs:`
// is visited with a nil step.
func forEachExpression(doc *document, fn func(from *parsedStep, ls loopScope, v *value)) {
	for _, s := range doc.parsed.steps {
		for _, e := range s.expressionEntries() {
			ls := s.loopScopeOf(e)
			walkValues(e.value, func(v *value) { fn(s, ls, v) })
		}
	}
	for _, e := range doc.parsed.entries {
		if e.key == "outputs" {
			walkValues(e.value, func(v *value) { fn(nil, loopScopeNone, v) })
		}
	}
}

// collectStepSites gathers the declaration of target and every reference that
// resolves to it.
func collectStepSites(doc *document, target *parsedStep) stepSites {
	out := stepSites{target: target}

	if rng, ok := idNameRange(doc, target); ok {
		out.sites = append(out.sites, stepSite{rng: rng, declaration: true})
	}
	forEachExpression(doc, func(from *parsedStep, ls loopScope, v *value) {
		for _, f := range v.fences {
			for _, ref := range stepRefsIn(f.source) {
				if ref.step != target.id || resolveStepFrom(doc, from, ls, ref.step) != target {
					continue
				}
				start := ref.span[0] + len(v1.StepsRoot) + 1
				rng, ok := v.fenceSpan(doc.index, f, start, start+len(ref.step))
				if !ok {
					out.unplaced++
					continue
				}
				if !slices.ContainsFunc(out.sites, func(s stepSite) bool { return s.rng == rng }) {
					out.sites = append(out.sites, stepSite{rng: rng})
				}
			}
		}
	})
	slices.SortFunc(out.sites, func(a, b stepSite) int {
		if c := a.rng.Start.Line - b.rng.Start.Line; c != 0 {
			return c
		}
		return a.rng.Start.Character - b.rng.Start.Character
	})
	return out
}

// siteContaining is the site whose characters the cursor is on or just after,
// which is what an editor sends when the caret sits at the end of a word.
func (ss stepSites) siteContaining(pos lsp.Position) (stepSite, bool) {
	for _, s := range ss.sites {
		if contains(s.rng, pos) || s.rng.End == pos {
			return s, true
		}
	}
	return stepSite{}, false
}

// idNameRange is the characters of a step's id: the `id:` value without the
// quotes a quoted scalar carries. It declines when the text there is not the id,
// as it would be if the scalar used an escape or an alias.
func idNameRange(doc *document, s *parsedStep) (lsp.Range, bool) {
	if s.idEntry == nil || s.idEntry.value == nil || s.id == "" {
		return lsp.Range{}, false
	}
	rng := s.idEntry.value.rng
	start, end := doc.index.offsetOfPosition(rng.Start), doc.index.offsetOfPosition(rng.End)
	if start < 0 || end > len(doc.text) || start >= end {
		return lsp.Range{}, false
	}
	text := doc.text[start:end]
	if n := len(text); n >= 2 && (text[0] == '"' || text[0] == '\'') && text[n-1] == text[0] {
		start, end, text = start+1, end-1, text[1:n-1]
	}
	if text != s.id {
		return lsp.Range{}, false
	}
	return doc.index.rangeOfOffsets(start, end), true
}

// stepRefsIn lists the rooted step references written in an expression, in
// order, skipping what is inside a string literal. It works on text, as
// [referenceAt] does, because an expression being edited often does not parse.
func stepRefsIn(src string) []reference {
	var out []reference
	prefix := v1.StepsRoot + "."
	for i := 0; i < len(src); {
		switch c := src[i]; {
		case c == '"' || c == '\'':
			i = skipStringLiteral(src, i)
		case strings.HasPrefix(src[i:], prefix) && (i == 0 || !isReferenceByte(src[i-1])):
			ref := referenceAt(src, i)
			if ref.step != "" && ref.span[0] == i {
				out = append(out, ref)
				i = ref.span[1]
				continue
			}
			i++
		default:
			i++
		}
	}
	return out
}

// isReferenceByte is a byte that can be part of a dotted name, the set
// [referenceAt] reads a word with.
func isReferenceByte(c byte) bool {
	return c == '_' || c == '.' ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// skipStringLiteral returns the index after the string literal opening at i. An
// unterminated literal runs to the end of the source, which keeps a half-typed
// string from being read as code.
func skipStringLiteral(src string, i int) int {
	quote := src[i]
	if strings.HasPrefix(src[i:], strings.Repeat(string(quote), 3)) {
		end := strings.Index(src[i+3:], strings.Repeat(string(quote), 3))
		if end < 0 {
			return len(src)
		}
		return i + 3 + end + 3
	}
	for j := i + 1; j < len(src); j++ {
		switch src[j] {
		case '\\':
			j++
		case quote:
			return j + 1
		}
	}
	return len(src)
}

// referencesAt answers textDocument/references.
func referencesAt(doc *document, pos lsp.Position, includeDeclaration bool) []lsp.Location {
	ss, ok := stepSitesAt(doc, pos)
	if !ok {
		return nil
	}
	var out []lsp.Location
	for _, s := range ss.sites {
		if s.declaration && !includeDeclaration {
			continue
		}
		out = append(out, lsp.Location{URI: doc.uri, Range: s.rng})
	}
	return out
}

// highlightsAt answers textDocument/documentHighlight: the declaration as a
// write and every reference as a read.
func highlightsAt(doc *document, pos lsp.Position) []lsp.DocumentHighlight {
	ss, ok := stepSitesAt(doc, pos)
	if !ok {
		return nil
	}
	out := make([]lsp.DocumentHighlight, 0, len(ss.sites))
	for _, s := range ss.sites {
		kind := lsp.Read
		if s.declaration {
			kind = lsp.Write
		}
		out = append(out, lsp.DocumentHighlight{Range: s.rng, Kind: kind})
	}
	return out
}

// prepareRenameAt answers textDocument/prepareRename: the id under the cursor,
// or false where there is nothing to rename. The output name and `steps.` of a
// reference are not the id, so a cursor there declines.
func prepareRenameAt(doc *document, pos lsp.Position) (lsp.Range, string, bool) {
	ss, ok := stepSitesAt(doc, pos)
	if !ok {
		return lsp.Range{}, "", false
	}
	site, ok := ss.siteContaining(clampPosition(pos))
	if !ok {
		return lsp.Range{}, "", false
	}
	return site.rng, ss.target.id, true
}

// renameError is a refusal to rename, carrying the reason the editor shows.
type renameError string

func (e renameError) Error() string { return string(e) }

// renameAt answers textDocument/rename with one edit per site.
//
// It refuses rather than renames partway. The edit is only right if every
// reference moves with the declaration, so any of these stops it with the
// reason: a name that is not a usable step id, a name another step already
// holds, a reference the model cannot place, or a mention of `steps.<id>` the
// model did not find — a description, a comment-adjacent value, a position the
// language gains tomorrow — which a rename would silently leave stale.
func renameAt(doc *document, pos lsp.Position, newName string) (*lsp.WorkspaceEdit, error) {
	ss, ok := stepSitesAt(doc, pos)
	if !ok {
		return nil, renameError("there is no step id to rename here")
	}
	if _, ok := ss.siteContaining(clampPosition(pos)); !ok {
		return nil, renameError("put the cursor on the step id to rename it")
	}
	old := ss.target.id

	switch {
	case newName == old:
		return &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{}}, nil
	case !v1.IsCELIdentifier(newName):
		return nil, renameError(fmt.Sprintf("%q is not a valid step id: use letters, digits and underscores, starting with a letter or underscore", newName))
	case v1.IsDeclarationRoot(newName):
		return nil, renameError(v1.ShadowsRootMessage("step", newName))
	case v1.IsCELUnusableStepID(newName):
		return nil, renameError(fmt.Sprintf("%q is punctuation in CEL rather than a name, so a reference to it cannot be parsed", newName))
	}
	for _, s := range doc.parsed.steps {
		if s.id == newName {
			return nil, renameError(fmt.Sprintf("a step is already named %q", newName))
		}
	}
	if ss.unplaced > 0 {
		return nil, renameError(fmt.Sprintf("%d reference(s) to %q sit in a folded block scalar the server cannot place; rename by hand", ss.unplaced, old))
	}
	if !slices.ContainsFunc(ss.sites, func(s stepSite) bool { return s.declaration }) {
		return nil, renameError(fmt.Sprintf("the id of %q is written in a form the server cannot edit safely", old))
	}
	// Same-named steps in sibling scopes own references of their own; they are
	// accounted for so only the references nobody resolved remain.
	known := len(ss.sites) - 1
	for _, other := range doc.parsed.steps {
		if other != ss.target && other.id == old {
			known += len(collectStepSites(doc, other).sites) - 1
		}
	}
	if extra := untrackedReferences(doc.text, old) - known; extra > 0 {
		return nil, renameError(fmt.Sprintf("%d other reference(s) to steps.%s are in expressions the server does not track; renaming would leave them stale", extra, old))
	}

	edits := make([]lsp.TextEdit, 0, len(ss.sites))
	for _, s := range ss.sites {
		text := newName
		if s.declaration && yamlAmbiguous(newName) {
			text = `"` + newName + `"`
			if r, ok := idScalarRange(doc, ss.target); ok {
				s.rng = r
			}
		}
		edits = append(edits, lsp.TextEdit{Range: s.rng, NewText: text})
	}
	return &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{string(doc.uri): edits}}, nil
}

// idScalarRange is the whole `id:` scalar, quotes included, for the rare name
// that has to be quoted to stay a string.
func idScalarRange(doc *document, s *parsedStep) (lsp.Range, bool) {
	if s.idEntry == nil || s.idEntry.value == nil {
		return lsp.Range{}, false
	}
	return s.idEntry.value.rng, true
}

// yamlAmbiguous is a name YAML 1.1 reads as something other than a string when
// it is written bare.
func yamlAmbiguous(name string) bool {
	switch strings.ToLower(name) {
	case "y", "n", "yes", "no", "on", "off":
		return true
	}
	return false
}

// untrackedReferences counts the references to a step id in every `${...}` fence
// of a document's text, outside lines that are only a comment. The model's sites
// are a subset of these; the difference is the expressions it does not walk, such
// as a position the language gains tomorrow. Prose that merely says `steps.web`
// is not a fence and is not counted: a stale sentence is not a broken reference.
func untrackedReferences(text, id string) int {
	n := 0
	for _, body := range fenceBodies(text) {
		for _, ref := range stepRefsIn(body) {
			if ref.step == id {
				n++
			}
		}
	}
	return n
}

// fenceBodies returns the source of every `${...}` fence in text, skipping the
// escaped `$${` and full-line comments. A fence runs to its matching brace,
// counting nested braces and skipping string literals; one that never closes is
// dropped, as a half-typed fence has no reference to count.
func fenceBodies(text string) []string {
	var lines []string
	for line := range strings.SplitSeq(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			line = ""
		}
		lines = append(lines, line)
	}
	text = strings.Join(lines, "\n")

	var out []string
	for i := 0; i < len(text); {
		at := strings.Index(text[i:], exprOpen)
		if at < 0 {
			break
		}
		at += i
		if at > 0 && text[at-1] == '$' {
			i = at + len(exprOpen)
			continue
		}
		depth, j := 1, at+len(exprOpen)
		for j < len(text) && depth > 0 {
			switch text[j] {
			case '"', '\'':
				j = skipStringLiteral(text, j)
				continue
			case '{':
				depth++
			case '}':
				depth--
			}
			j++
		}
		if depth == 0 {
			out = append(out, text[at+len(exprOpen):j-1])
		}
		i = j
	}
	return out
}
