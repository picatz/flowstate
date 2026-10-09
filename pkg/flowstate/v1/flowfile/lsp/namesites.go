package lsp

import (
	"fmt"
	"slices"
	"strings"

	"github.com/sourcegraph/go-lsp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Step ids are not the only names an author declares and reads back. A loop binds
// its iterator with `as:` and the body reads it bare; the workflow's `vars:` block
// declares names every expression reads as `vars.<name>`. References, highlight
// and rename answer for both here, through the same site model as step ids
// ([stepSite]), so one set of editor requests covers all three kinds of name.
//
// Narrower than a step id on purpose, in the same direction: a name is renamed
// only where the server can see every read of it. Steps' own `vars:` are bare
// names that other bindings can shadow, so they are not covered.

// nameKind says which kind of declaration a [nameSites] belongs to.
type nameKind int

const (
	nameIterator nameKind = iota
	nameVar
)

func (k nameKind) String() string {
	if k == nameIterator {
		return "loop variable"
	}

	return "var"
}

// nameSites is every place the model can tell a declared name is spelled.
type nameSites struct {
	kind  nameKind
	name  string
	sites []stepSite

	// loop is the step whose `as:` declares an iterator.
	loop *parsedStep

	// spelled holds every variable name written in a fence that reads the
	// iterator, bound or free. Renaming to one of them would capture a read or
	// be captured by a comprehension's binder.
	spelled map[string]bool

	// unplaced counts reads the model found but cannot place, as [stepSites] does.
	unplaced int

	// total and visited are how many reads of the name the whole document spells
	// and how many of those the model visited; fewer visited means a rename would
	// leave one stale.
	total, visited int
}

// nameSitesAt resolves the loop iterator or workflow var the cursor is on, as
// its declaration or as a read.
func nameSitesAt(doc *document, pos lsp.Position) (nameSites, bool) {
	if !doc.speaksFlowfile() || doc.parsed == nil {
		return nameSites{}, false
	}
	pos = clampPosition(pos)

	if loop := iteratorDeclaredAt(doc, pos); loop != nil {
		return collectIteratorSites(doc, loop), true
	}
	if name := varDeclaredAt(doc, pos); name != "" {
		return collectVarSites(doc, name), true
	}

	var (
		found nameSites
		ok    bool
	)
	forEachExpression(doc, func(from *parsedStep, ls loopScope, v *value) {
		if ok {
			return
		}
		f, cursor, fenceOK := v.fenceAt(doc.index, pos)
		if !fenceOK {
			return
		}
		if name, hit := varMemberUnder(f.source, cursor); hit {
			found, ok = collectVarSites(doc, name), true
			return
		}
		if _, text, hit := freeNameAtCursor(f.source, cursor); hit {
			if loop := resolveIterator(from, ls, text); loop != nil {
				found, ok = collectIteratorSites(doc, loop), true
			}
		}
	})

	return found, ok
}

// freeNameAtCursor is the free variable token the cursor touches.
func freeNameAtCursor(src string, cursor int) (celToken, string, bool) {
	for _, t := range lexCEL(src) {
		if t.start > cursor {
			break
		}
		if t.kind == tokVariable && !t.shadowed && cursor <= t.end {
			return t, src[t.start:t.end], true
		}
	}

	return celToken{}, "", false
}

// varMemberUnder reports the var name when the cursor is on the member of a
// `vars.<name>` read.
func varMemberUnder(src string, cursor int) (string, bool) {
	for _, m := range varReadsIn(src) {
		if cursor >= m.start && cursor <= m.end {
			return src[m.start:m.end], true
		}
	}

	return "", false
}

// varUse is one free `vars` root in an expression, with the member it selects
// when it selects a plain one.
type varUse struct {
	// member is the span of `<name>` in `vars.<name>` or `vars.?<name>`, and is
	// nil for every other use: indexed, bare, or a method call on the map.
	member *celToken
}

// varUsesIn lists every free `vars` root in src. A use with no member is one the
// server cannot attribute to a name, so a rename must not proceed past it.
func varUsesIn(src string) []varUse {
	var out []varUse
	toks := lexCEL(src)
	for i, t := range toks {
		if t.kind != tokVariable || t.shadowed || src[t.start:t.end] != v1.VarsRoot {
			continue
		}
		use := varUse{}
		rest := strings.TrimLeft(src[t.end:], " \t")
		if strings.HasPrefix(rest, ".") {
			after := len(src) - len(rest) + 1
			if strings.HasPrefix(src[after:], "?") { // optional selection, `vars.?name`
				after++
			}
			for j := i + 1; j < len(toks); j++ {
				if toks[j].start < after {
					continue
				}
				if strings.TrimSpace(src[after:toks[j].start]) == "" &&
					!strings.HasPrefix(strings.TrimLeft(src[toks[j].end:], " \t"), "(") {
					use.member = &celToken{start: toks[j].start, end: toks[j].end}
				}
				break
			}
		}
		out = append(out, use)
	}

	return out
}

// varReadsIn lists the spans of the `<name>` in each plain `vars.<name>` read.
func varReadsIn(src string) []celToken {
	var out []celToken
	for _, u := range varUsesIn(src) {
		if u.member != nil {
			out = append(out, *u.member)
		}
	}

	return out
}

// resolveIterator is the loop whose iterator a bare name in an expression of
// from reads, or nil. The order is [definitionAt]'s: a loop's own `until:` and
// `update:` read its carried state, then the nearest enclosing binder wins.
func resolveIterator(from *parsedStep, ls loopScope, name string) *parsedStep {
	if from == nil {
		return nil
	}
	if ls == loopScopeAfterBody && from.loopEntry != nil && name == from.iteratorName() {
		return from
	}
	for _, loop := range from.iteratorsInScope() {
		if loop.iteratorName() == name {
			return loop
		}
	}

	return nil
}

// asEntry is the `as:` entry of a loop or for_each step.
func asEntry(s *parsedStep) *entry {
	for _, block := range []*entry{s.forEachEntry, s.loopEntry} {
		if block == nil || block.value == nil {
			continue
		}
		for _, e := range block.value.entries {
			if e.key == "as" && e.value != nil && e.valueText() != "" {
				return e
			}
		}
	}

	return nil
}

// iteratorDeclaredAt is the loop whose written `as:` value the cursor is inside.
func iteratorDeclaredAt(doc *document, pos lsp.Position) *parsedStep {
	for _, s := range doc.parsed.steps {
		if e := asEntry(s); e != nil && contains(e.value.rng, pos) {
			return s
		}
	}

	return nil
}

// varDeclaredAt is the workflow var whose key the cursor is on.
func varDeclaredAt(doc *document, pos lsp.Position) string {
	vars := doc.parsed.varsEntry
	if vars == nil || vars.value == nil {
		return ""
	}
	for _, e := range vars.value.entries {
		if contains(e.keyRange, pos) {
			return e.key
		}
	}

	return ""
}

// collectIteratorSites gathers an iterator's declaration and every read that
// resolves to its loop.
func collectIteratorSites(doc *document, loop *parsedStep) nameSites {
	name := loop.iteratorName()
	out := nameSites{kind: nameIterator, name: name, loop: loop, spelled: map[string]bool{}}
	seen := map[lsp.Range]struct{}{}

	if e := asEntry(loop); e != nil {
		out.sites = append(out.sites, stepSite{rng: e.value.rng, declaration: true})
		seen[e.value.rng] = struct{}{}
	}
	forEachExpression(doc, func(from *parsedStep, ls loopScope, v *value) {
		for _, f := range v.fences {
			toks := lexCEL(f.source)
			reads := false
			for _, t := range toks {
				if t.kind != tokVariable || t.shadowed || f.source[t.start:t.end] != name {
					continue
				}
				out.visited++
				if resolveIterator(from, ls, name) != loop {
					continue
				}
				reads = true
				rng, ok := v.fenceSpan(doc.index, f, t.start, t.end)
				if !ok {
					out.unplaced++
					continue
				}
				if _, dup := seen[rng]; !dup {
					seen[rng] = struct{}{}
					out.sites = append(out.sites, stepSite{rng: rng})
				}
			}
			if reads {
				for _, t := range toks {
					if t.kind == tokVariable {
						out.spelled[f.source[t.start:t.end]] = true
					}
				}
			}
		}
	})
	for _, body := range fenceBodies(doc.text) {
		for _, t := range lexCEL(body) {
			if t.kind == tokVariable && !t.shadowed && body[t.start:t.end] == name {
				out.total++
			}
		}
	}
	sortSites(out.sites)

	return out
}

// collectVarSites gathers a workflow var's key and every `vars.<name>` read.
func collectVarSites(doc *document, name string) nameSites {
	out := nameSites{kind: nameVar, name: name}
	seen := map[lsp.Range]struct{}{}

	if e := varEntry(doc.parsed.varsEntry, name); e != nil {
		out.sites = append(out.sites, stepSite{rng: e.keyRange, declaration: true})
		seen[e.keyRange] = struct{}{}
	}
	forEachExpression(doc, func(_ *parsedStep, _ loopScope, v *value) {
		for _, f := range v.fences {
			for _, m := range varReadsIn(f.source) {
				if f.source[m.start:m.end] != name {
					continue
				}
				out.visited++
				rng, ok := v.fenceSpan(doc.index, f, m.start, m.end)
				if !ok {
					out.unplaced++
					continue
				}
				if _, dup := seen[rng]; !dup {
					seen[rng] = struct{}{}
					out.sites = append(out.sites, stepSite{rng: rng})
				}
			}
		}
	})
	// Every read of this name, plus every use of `vars` that names nothing the
	// server can read back (`vars[k]`, a bare `vars`): the latter are never
	// visited, so they leave total above visited and the rename is refused.
	for _, body := range fenceBodies(doc.text) {
		for _, u := range varUsesIn(body) {
			if u.member == nil || body[u.member.start:u.member.end] == name {
				out.total++
			}
		}
	}
	sortSites(out.sites)

	return out
}

func sortSites(sites []stepSite) {
	slices.SortFunc(sites, func(a, b stepSite) int {
		if a.rng.Start.Line != b.rng.Start.Line {
			return a.rng.Start.Line - b.rng.Start.Line
		}

		return a.rng.Start.Character - b.rng.Start.Character
	})
}

// siteContaining is the site the cursor is inside.
func (ns nameSites) siteContaining(pos lsp.Position) (stepSite, bool) {
	for _, s := range ns.sites {
		if contains(s.rng, pos) {
			return s, true
		}
	}

	return stepSite{}, false
}

// checkRename returns the reason a rename of the name to newName must be refused.
func (ns nameSites) checkRename(doc *document, newName string) error {
	switch {
	case !v1.IsCELIdentifier(newName):
		return renameError(fmt.Sprintf("%q is not a valid %s name: use letters, digits and underscores, starting with a letter or underscore", newName, ns.kind))
	case flowfile.IsCELReservedIdentifier(newName):
		return renameError(fmt.Sprintf("%q is a reserved word in CEL and cannot name a %s", newName, ns.kind))
	case v1.IsDeclarationRoot(newName):
		return renameError(v1.ShadowsRootMessage(ns.kind.String(), newName))
	}
	if ns.unplaced > 0 {
		return renameError(fmt.Sprintf("%d read(s) of %q sit in a folded block scalar the server cannot place; rename by hand", ns.unplaced, ns.name))
	}
	if !slices.ContainsFunc(ns.sites, func(s stepSite) bool { return s.declaration }) {
		return renameError(fmt.Sprintf("the %s %q has no written declaration to rename", ns.kind, ns.name))
	}
	if extra := ns.total - ns.visited; extra > 0 {
		return renameError(fmt.Sprintf("%d other read(s) of %q are in expressions the server does not track; renaming would leave them stale", extra, ns.name))
	}

	switch ns.kind {
	case nameVar:
		if varEntry(doc.parsed.varsEntry, newName) != nil {
			return renameError(fmt.Sprintf("a var is already named %q", newName))
		}
	case nameIterator:
		if ns.spelled[newName] {
			return renameError(fmt.Sprintf("%q is already written in an expression that reads %q; renaming would change what it refers to", newName, ns.name))
		}
		// A bare name another binder owns would capture the renamed reads or
		// have its own captured by them.
		for _, s := range doc.parsed.steps {
			if s.iteratorName() == newName && (s == ns.loop || slices.Contains(s.iteratorsInScope(), ns.loop) || slices.Contains(ns.loop.iteratorsInScope(), s)) {
				return renameError(fmt.Sprintf("another loop in scope already binds %q", newName))
			}
			if varEntry(s.varsEntry, newName) != nil {
				return renameError(fmt.Sprintf("a step declares a var named %q", newName))
			}
		}
	}

	return nil
}

// renameEdit is the edit set for renaming the name to newName.
func (ns nameSites) renameEdit(doc *document, newName string) *lsp.WorkspaceEdit {
	edits := make([]lsp.TextEdit, 0, len(ns.sites))
	for _, s := range ns.sites {
		text := newName
		if s.declaration && yamlAmbiguous(newName) {
			text = `"` + newName + `"`
		}
		edits = append(edits, lsp.TextEdit{Range: s.rng, NewText: text})
	}

	return &lsp.WorkspaceEdit{Changes: map[string][]lsp.TextEdit{string(doc.uri): edits}}
}

// hasDeclaration reports whether the name is declared somewhere an edit can reach.
func (ns nameSites) hasDeclaration() bool {
	return slices.ContainsFunc(ns.sites, func(s stepSite) bool { return s.declaration })
}
