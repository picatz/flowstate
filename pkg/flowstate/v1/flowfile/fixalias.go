package flowfile

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	"github.com/goccy/go-yaml/ast"
)

// Inlining a whole-value alias, which is the migration path across the refusal
// strict.go records.
//
// The grammar refuses anchors, aliases and merge keys. That leaves an author
// holding a file that compiled yesterday with a diagnostic and no automated way
// across, which is the half of #653 that did not ship with the refusal. This is
// that half, and it is deliberately the smaller of the two shapes the construct
// comes in.
//
// # What is mechanical, and what is a judgment
//
// An alias standing for a *whole value* — `retry: *backoff`, `- *step` — means
// exactly the bytes the anchor names, in one place, with nothing to decide. It is
// copied and the alias line is rebuilt around it. That is a rewrite `flow fix` may
// make under the charter rule that it never invents anything.
//
// A merge key is not that. `<<: *base` followed by sibling keys is a *precedence*
// rule: which of two spellings of one key survives is a question about what the
// author meant, and a rewriter that answers it is exercising judgment on a file it
// was asked only to reformat. So a merge key stays refused, in the words strict.go
// already uses ([strictMergeMessage]), and the file is left byte for byte alone.
//
// # Why this counts nodes
//
// Inlining *is* the expansion a billion-laughs document abuses. The refusal in
// strict.go is safe precisely because it never follows an alias; this code follows
// every one of them, so it is the one place in the front end that pays the cost the
// bound exists for. Every expansion is charged against [maxNodes] — the same total
// the compiler bounds a document by, so a file this rewrites is a file that
// compiles rather than one the compiler then refuses for its size — and a chain is
// bounded by [maxAliasDepth] with the anchor names being expanded held on a stack,
// so a cycle is named rather than followed. Reaching either bound refuses: the file
// is left alone with a positioned diagnostic, because a `flow fix` that writes a
// document larger than the compiler will read is a `flow fix` that breaks the file
// it was fixing.
//
// # Why every refusal refuses the whole file
//
// [FixResult.Complete] already says a document is rewritten entirely or not at all.
// That matters more here than anywhere else: half-inlined aliases leave a file whose
// anchors are partly gone, so the anchor a surviving alias names may no longer be
// declared — a document neither edition describes. So anything this cannot copy
// byte-safely leaves the whole file untouched, and the diagnostic says which
// construct and why.

// inlineWholeValueAliases rewrites every whole-value alias in file into the source
// bytes of the value its anchor names, and drops the anchor markers left behind.
//
// The bool reports whether the whole document could be rewritten. False means
// nothing was: the returned result carries the original bytes and one positioned
// refusal per construct that could not be copied.
func inlineWholeValueAliases(data []byte, file *ast.File) (FixResult, bool) {
	in := &aliasInliner{
		f: &fixer{
			lines:           splitLines(data),
			trailingNewline: bytes.HasSuffix(data, []byte("\n")),
			terminator:      lineTerminator(data),
		},
		anchors: map[string]*ast.AnchorNode{},
		byLine:  map[int]aliasSite{},
	}

	for _, doc := range file.Docs {
		in.collectAnchors(doc.Body)
		in.nodes += countNodes(doc.Body)
	}
	for _, doc := range file.Docs {
		in.collect(doc.Body, false)
	}
	in.checkEveryConstructClassified(file)

	if len(in.refusals) == 0 {
		in.rewrite()
	}
	if len(in.refusals) > 0 {
		return FixResult{Source: data, Refusals: in.refusals}, false
	}

	// The node budget bounds how many *values* the rewrite writes out; this bounds
	// the bytes, which is a different resource and the one the next pass reads
	// against. [fixOnce] refuses to read a document larger than maxBytes at all, so
	// a rewrite that crossed it would hand the fixed-point loop a file it cannot
	// parse — `flow fix` breaking the file it was fixing, in the one direction
	// counting nodes cannot see (a few long scalars aliased many times).
	out := in.f.apply()
	if len(out) > maxBytes {
		at := strictFinding{line: 1, column: 1}
		if len(in.sites) > 0 {
			at.line, at.column = in.sites[0].alias.Start.Position.Line, in.sites[0].alias.Start.Position.Column
		}
		in.refuseAt(at.line, at.column,
			"writing these aliases out would produce %d bytes, larger than the %d byte limit a Flowfile is read up to; nothing was rewritten",
			len(out), maxBytes)

		return FixResult{Source: data, Refusals: in.refusals}, false
	}

	return FixResult{Source: out, Changes: in.f.changes}, true
}

// An aliasSite is one alias this rewrite may replace: an alias written as the
// whole value of a mapping entry, or as a whole element of a block sequence.
//
// Nothing else is a site. An alias written as a *key*, inside flow style, or
// anywhere this walk does not recognise is refused rather than recorded, because
// the replacement is a splice into a line and those three shapes have no line to
// splice into that means what the author wrote.
type aliasSite struct {
	alias *ast.AliasNode

	// key is the mapping key the alias is the value of, or nil for a sequence
	// element. Carried to check that the key and its alias are written on one
	// line, which is what makes the replacement a single-line edit.
	key ast.Node

	// sequence marks the element form, whose replacement keeps the `- ` and puts
	// the value's first line beside it.
	sequence bool
}

// An aliasInliner accumulates the edits [inlineWholeValueAliases] makes, over one
// parsed file.
type aliasInliner struct {
	// f carries the source lines and the edits, so this rewrite is applied the
	// same way every other one in fix.go is — line edits recorded against line
	// numbers, applied at the end, the rest of the document copied through byte
	// for byte.
	f *fixer

	anchors     map[string]*ast.AnchorNode
	anchorNodes []*ast.AnchorNode

	sites  []aliasSite
	byLine map[int]aliasSite

	refusals []Diagnostic

	// nodes is what the document holds once every alias is expanded, charged
	// against [maxNodes]. It starts at what the document holds as written and
	// grows by the anchored value's own size at every expansion, which is exactly
	// how a billion-laughs document multiplies.
	nodes int
}

// collectAnchors records every anchor in the document, walking with [ast.Walk] so
// that this pass and [strictYAMLRefusals] traverse identically — see
// [aliasInliner.checkEveryConstructClassified], which compares their counts.
func (in *aliasInliner) collectAnchors(root ast.Node) {
	v := &anchorCollector{in: in}
	ast.Walk(v, root)
}

// anchorCollector is the [ast.Visitor] collectAnchors walks with.
type anchorCollector struct{ in *aliasInliner }

func (v *anchorCollector) Visit(n ast.Node) ast.Visitor {
	anchor, ok := n.(*ast.AnchorNode)
	if !ok {
		return v
	}
	v.in.anchorNodes = append(v.in.anchorNodes, anchor)

	name := anchorName(anchor)
	if name == "" {
		v.in.refuseAt(anchor.Start.Position.Line, anchor.Start.Position.Column,
			"this anchor has no name this rewrite can read, so an alias to it cannot be replaced by its value; write the value out where it is used by hand")
		return v
	}
	if _, twice := v.in.anchors[name]; twice {
		v.in.refuseAt(anchor.Start.Position.Line, anchor.Start.Position.Column,
			"the anchor `&%s` is declared more than once, so which value each `*%s` means depends on where it is written, "+
				"and copying one of them would be a guess; give them distinct names and run this again, or write the values out by hand",
			name, name)
		return v
	}
	v.in.anchors[name] = anchor

	return v
}

// collect finds every alias in the document and classifies it: a site this can
// rewrite, or a refusal naming why it cannot.
//
// flow says the walk is inside flow style (`{…}`, `[…]`). It is carried down
// rather than read off the node holding the alias because flow style nests — a
// block mapping's value may be a flow sequence whose elements are flow mappings —
// and every one of those has no line structure to splice a block value into.
func (in *aliasInliner) collect(n ast.Node, flow bool) {
	switch node := n.(type) {
	case nil:
		return

	case *ast.MappingNode:
		flow = flow || node.IsFlowStyle
		for _, v := range node.Values {
			in.collect(v, flow)
		}

	case *ast.MappingValueNode:
		if _, isMerge := node.Key.(*ast.MergeKeyNode); isMerge {
			// The one construct this rewrite deliberately does not make. Refused in
			// the compiler's own words so an author reading `flow fix`'s output and
			// `flow validate`'s output is not told two different things about one
			// line.
			span := spanOfNode(node.Key)
			in.refuseAt(span.Start.Line, span.Start.Column, "%s", strictMergeMessage())

			return
		}
		if alias, isAlias := node.Key.(*ast.AliasNode); isAlias {
			in.refuseAlias(alias,
				"this alias is written as a mapping key, where replacing it would rewrite the key itself rather than a value; write the key out by hand")

			return
		}
		in.collect(node.Key, flow)
		if alias, isAlias := node.Value.(*ast.AliasNode); isAlias {
			in.recordSite(aliasSite{alias: alias, key: node.Key}, flow)

			return
		}
		in.collect(node.Value, flow)

	case *ast.SequenceNode:
		flow = flow || node.IsFlowStyle
		for _, v := range node.Values {
			if alias, isAlias := v.(*ast.AliasNode); isAlias {
				in.recordSite(aliasSite{alias: alias, sequence: true}, flow)

				continue
			}
			in.collect(v, flow)
		}

	case *ast.AnchorNode:
		in.collect(node.Value, flow)

	case *ast.AliasNode:
		// Reached outside the two shapes above: a document whose whole body is an
		// alias, or an anchor whose value is one. Neither is a value written beside
		// a key or a dash, which is the only thing this splices.
		in.refuseAlias(node,
			"this alias is not written as the whole value of a key or as a list item, so there is no line to write the value it names into; write it out by hand")

	default:
	}
}

// recordSite keeps one alias to rewrite, or refuses it where the line it is
// written on cannot carry the value.
func (in *aliasInliner) recordSite(site aliasSite, flow bool) {
	if flow {
		in.refuseAlias(site.alias,
			"this alias is written in flow style (`{…}` or `[…]`), which has no line structure to write a value into; "+
				"write the mapping or list across lines and run this again, or write the value out by hand")

		return
	}

	line := site.alias.Start.Position.Line
	if _, taken := in.byLine[line]; taken {
		// Two aliases on one line is a flow-style shape the branch above already
		// refuses; this is the belt to that brace, because the replacement is a
		// whole-line edit and two of them on one line would lose one.
		in.refuseAlias(site.alias,
			"more than one alias is written on this line, and each is replaced by rewriting the whole line; write them out by hand")

		return
	}

	in.sites = append(in.sites, site)
	in.byLine[line] = site
}

// checkEveryConstructClassified refuses a document holding an anchor or alias this
// walk did not account for.
//
// [strictYAMLRefusals] is the one collector that decides what the grammar refuses,
// and this rewrite is the path across that refusal — so the two have to see the
// same set. A node kind this walk's switch does not name would otherwise be
// silently left in place: the rewrite would report success, `flow fix` would exit
// zero, and `flow validate` would refuse the file for the construct still in it.
// That is the exact failure mode CLAUDE.md records twice, so it is checked rather
// than assumed. Counting is enough — this walk cannot find a construct the strict
// walk does not, since it only ever classifies the same three node kinds.
func (in *aliasInliner) checkEveryConstructClassified(file *ast.File) {
	if len(in.refusals) > 0 {
		// Something is already refused, so the counts legitimately disagree: a
		// merge key is one strict finding this walk records as a refusal rather
		// than a site, and a refused subtree is not descended into.
		return
	}

	var findings []strictFinding
	for _, doc := range file.Docs {
		findings = append(findings, strictYAMLRefusals(doc.Body)...)
	}
	if len(findings) == len(in.sites)+len(in.anchorNodes) {
		return
	}

	at := strictFinding{line: 1, column: 1}
	if len(findings) > 0 {
		at = findings[0]
	}
	in.refuseAt(at.line, at.column,
		"this document holds an anchor or alias written in a shape this rewrite does not recognise, "+
			"so it was left alone rather than half rewritten; write the values out by hand")
}

// rewrite records an edit for every site, then drops the anchor markers.
//
// The order is load-bearing. Every replacement is computed from the source lines
// as the author wrote them, and dropping an anchor marker *edits* one of those
// lines — a `&name` sits to the left of the value it names, so removing it moves
// every column on that line. Computing first and stripping second means no
// replacement is ever read through a line that has already moved.
func (in *aliasInliner) rewrite() {
	for _, site := range in.sites {
		replacement, ok := in.replacement(site, nil)
		if !ok {
			return
		}

		name := aliasName(site.alias)
		line := site.alias.Start.Position.Line
		in.f.record(line, line, replacement,
			fmt.Sprintf("alias `*%s` replaced with the value `&%s` names", name, name),
			fmt.Sprintf("alias `*%s` would be replaced with the value `&%s` names", name, name))
	}

	for _, anchor := range in.anchorNodes {
		if !in.dropMarker(anchor) {
			return
		}
	}
}

// replacement returns the lines one site's line becomes.
//
// stack is the anchor names already being expanded on the way here, outermost
// first: an alias inside an anchored value is expanded too, which is what makes a
// chain work and a cycle findable.
func (in *aliasInliner) replacement(site aliasSite, stack []string) ([]string, bool) {
	name := aliasName(site.alias)
	if name == "" {
		in.refuseAlias(site.alias, "this alias names nothing this rewrite can read; write the value out by hand")

		return nil, false
	}
	if slices.Contains(stack, name) {
		in.refuseAlias(site.alias,
			"the anchor `&%s` reaches itself through this alias, so writing its value out would never finish; break the cycle by hand",
			name)

		return nil, false
	}
	if len(stack) >= maxAliasDepth {
		in.refuseAlias(site.alias,
			"this alias is more than %d anchors deep in a chain of them, which is deeper than a Flowfile is meant to go; write the value out by hand",
			maxAliasDepth)

		return nil, false
	}

	anchor, declared := in.anchors[name]
	if !declared {
		in.refuseAlias(site.alias,
			"this alias names an anchor (`&%s`) this document does not declare, so there is no value to write in its place; write it out by hand",
			name)

		return nil, false
	}
	if containsAnchor(anchor.Value) {
		in.refuseAlias(site.alias,
			"the value `&%s` names declares an anchor of its own, so copying it here would declare that anchor twice; write the value out by hand",
			name)

		return nil, false
	}

	// Charged before the value is read, so a document that would explode is refused
	// at the first expansion that crosses the line rather than after the memory has
	// been spent. See this file's comment on why this is the one place in the front
	// end that pays for expansion at all.
	in.nodes += countNodes(anchor.Value)
	if in.nodes > maxNodes {
		in.refuseAlias(site.alias,
			"writing these aliases out would hold more than %d values, which is more than a Flowfile is meant to hold — "+
				"an alias multiplies what it names at every level; nothing was rewritten",
			maxNodes)

		return nil, false
	}

	prefix, suffix, ok := in.split(site)
	if !ok {
		return nil, false
	}

	switch value := anchor.Value.(type) {
	case *ast.MappingNode:
		if value.IsFlowStyle {
			return in.spliceScalar(site, prefix, suffix, anchor)
		}

		return in.spliceBlock(site, prefix, suffix, anchor, append(slices.Clone(stack), name))
	case *ast.SequenceNode:
		if value.IsFlowStyle {
			return in.spliceScalar(site, prefix, suffix, anchor)
		}

		return in.spliceBlock(site, prefix, suffix, anchor, append(slices.Clone(stack), name))
	case *ast.MappingValueNode:
		// A mapping written with one entry arrives unwrapped. It is a block either
		// way — the flow spelling of one entry is a *ast.MappingNode.
		return in.spliceBlock(site, prefix, suffix, anchor, append(slices.Clone(stack), name))
	case *ast.StringNode, *ast.IntegerNode, *ast.FloatNode, *ast.BoolNode:
		return in.spliceScalar(site, prefix, suffix, anchor)
	default:
		in.refuseAlias(site.alias,
			"the value `&%s` names is %s, which this rewrite cannot copy into another line safely; write it out by hand",
			name, describeNode(anchor.Value))

		return nil, false
	}
}

// split returns the text of a site's line before and after the alias, and checks
// that both are shapes the replacement can be built around.
//
// The prefix is what the value is written after — `  retry: ` or `  - ` — and the
// suffix is whatever followed the alias, which may only be a comment. Anything
// else there means the line holds more than this one value, and a whole-line edit
// would lose it.
func (in *aliasInliner) split(site aliasSite) (prefix, suffix string, ok bool) {
	pos := site.alias.Start.Position
	text := in.f.line(pos.Line)

	at, located := byteOffsetOfColumn(text, pos.Column)
	if !located {
		in.refuseAlias(site.alias, "this alias is not written where it was read, so it cannot be replaced safely; write the value out by hand")

		return "", "", false
	}

	want := "*" + aliasName(site.alias)
	if at+len(want) > len(text) || text[at:at+len(want)] != want {
		in.refuseAlias(site.alias, "this alias is not written on its line the way it was read, so it cannot be replaced safely; write the value out by hand")

		return "", "", false
	}

	prefix, suffix = text[:at], text[at+len(want):]

	if trimmed := strings.TrimSpace(suffix); trimmed != "" && !strings.HasPrefix(trimmed, "#") {
		in.refuseAlias(site.alias,
			"something other than a comment is written after this alias, so the line holds more than the one value; write the value out by hand")

		return "", "", false
	}

	if site.sequence {
		// `<spaces>- <spaces>` and nothing else: the dash has to be on this line for
		// its column to be the one a copied block indents under.
		dash := strings.TrimLeft(prefix, " ")
		if !strings.HasPrefix(dash, "- ") || strings.TrimSpace(dash[1:]) != "" {
			in.refuseAlias(site.alias,
				"this list item is not written as `- ` followed by the alias, so the value cannot be written in its place; write it out by hand")

			return "", "", false
		}

		return prefix, suffix, true
	}

	keySpan := spanOfNode(site.key)
	if !keySpan.IsValid() || keySpan.Start.Line != pos.Line {
		in.refuseAlias(site.alias,
			"this alias is written on a line of its own rather than beside the key it is the value of, so there is no `key:` to write the value under; write it out by hand")

		return "", "", false
	}
	if !strings.HasSuffix(strings.TrimRight(prefix, " "), ":") {
		in.refuseAlias(site.alias,
			"this alias is not written directly after its key's colon, so it cannot be replaced safely; write the value out by hand")

		return "", "", false
	}

	return prefix, suffix, true
}

// spliceScalar replaces an alias with a value written on one line: a scalar, or a
// mapping or sequence in flow style.
//
// The value's own source text is copied, so its quoting is whatever the author
// chose rather than whatever a re-render would produce.
func (in *aliasInliner) spliceScalar(site aliasSite, prefix, suffix string, anchor *ast.AnchorNode) ([]string, bool) {
	span := spanOfNode(anchor.Value)
	if !span.IsValid() || span.Start.Line != span.End.Line {
		in.refuseAlias(site.alias,
			"the value `&%s` names is not written on one line, so it cannot be written after `%s`; write it out by hand",
			anchorName(anchor), strings.TrimSpace(prefix))

		return nil, false
	}

	text := in.f.line(span.Start.Line)
	from, located := byteOffsetOfColumn(text, span.Start.Column)
	if !located {
		in.refuseAlias(site.alias, "the value `&%s` names is not written where it was read; write it out by hand", anchorName(anchor))

		return nil, false
	}
	through, ended := byteOffsetOfColumn(text, span.End.Column)
	if !ended || through < from {
		in.refuseAlias(site.alias, "the value `&%s` names is not written where it was read; write it out by hand", anchorName(anchor))

		return nil, false
	}

	value := text[from:through]
	if strings.TrimSpace(value) == "" {
		in.refuseAlias(site.alias,
			"the anchor `&%s` names no value, so there is nothing to write in this alias's place; write the value out by hand",
			anchorName(anchor))

		return nil, false
	}

	return []string{prefix + value + suffix}, true
}

// spliceBlock replaces an alias with a value written as a block: a mapping or a
// sequence across lines.
//
// The block's own source lines are copied and shifted as one, so the comments
// among them travel with them and the relative shape of what was written is kept.
// Only the indentation changes, and only by the fixed amount that puts the block
// where the alias was.
func (in *aliasInliner) spliceBlock(site aliasSite, prefix, suffix string, anchor *ast.AnchorNode, stack []string) ([]string, bool) {
	name := anchorName(anchor)

	span := spanOfNode(anchor.Value)
	if !span.IsValid() {
		in.refuseAlias(site.alias, "the value `&%s` names is not written where it was read; write it out by hand", name)

		return nil, false
	}

	first := span.Start.Line
	anchorLine := anchor.Start.Position.Line
	base := indentWidth(in.f.line(first))
	if first <= anchorLine || base <= indentWidth(in.f.line(anchorLine)) {
		// Either the block opens on the anchor's own line — `- &s id: x`, whose
		// first line is not a whole line of the block — or it is written at or left
		// of the anchor's own indentation, which is legal YAML for a sequence and
		// gives the copy no indentation to measure a shift from. Both are shapes
		// where "copy these lines" is not what the value is.
		in.refuseAlias(site.alias,
			"the value `&%s` names does not open on its own line under the anchor, so its lines cannot be copied as a block; write it out by hand",
			name)

		return nil, false
	}

	last := in.f.blockEnd(first-1, base-1)
	raw, ok := in.expandRange(first, last, stack)
	if !ok {
		return nil, false
	}

	// Where the copied block goes: under the key, or beside the dash.
	indent := indentWidth(prefix) + 2
	if site.sequence {
		indent = len(prefix)
	}

	shifted := make([]string, 0, len(raw))
	for _, line := range raw {
		if strings.TrimSpace(line) == "" {
			shifted = append(shifted, "")

			continue
		}
		if indentWidth(line) < base {
			in.refuseAlias(site.alias,
				"a line of the value `&%s` names is indented less than the value itself, so the block cannot be moved as a whole; write it out by hand",
				name)

			return nil, false
		}
		shifted = append(shifted, strings.Repeat(" ", indent)+line[base:])
	}
	if len(shifted) == 0 {
		in.refuseAlias(site.alias, "the anchor `&%s` names no value, so there is nothing to write in this alias's place; write the value out by hand", name)

		return nil, false
	}

	if site.sequence {
		if strings.TrimSpace(suffix) != "" {
			in.refuseAlias(site.alias,
				"a comment is written after this alias, and the value `&%s` names is a block whose first line goes where the comment is; move the comment above the item and run this again",
				name)

			return nil, false
		}

		// The dash keeps its line and the block's first line sits beside it, which
		// is where a list item's mapping is written. `indent` is the dash's prefix
		// width by construction, so the two fit together exactly.
		out := []string{prefix + shifted[0][indent:]}

		return append(out, shifted[1:]...), true
	}

	// The key keeps its line — with its comment, if it had one — and the block goes
	// underneath it, which is the only place a block value can be written.
	out := []string{strings.TrimRight(prefix, " ") + suffix}

	return append(out, shifted...), true
}

// expandRange returns the source lines first..last with every alias written among
// them replaced by the value it names.
//
// This is what makes a chain work: an anchored value holding an alias of its own
// is copied with that alias already written out, so one pass settles the whole
// chain rather than one link per round. stack carries the anchors being expanded
// on the way here, so the recursion cannot follow a cycle.
func (in *aliasInliner) expandRange(first, last int, stack []string) ([]string, bool) {
	var out []string
	for n := first; n <= last; n++ {
		site, isSite := in.byLine[n]
		if !isSite {
			out = append(out, in.f.line(n))

			continue
		}

		replacement, ok := in.replacement(site, stack)
		if !ok {
			return nil, false
		}
		out = append(out, replacement...)
	}

	return out, true
}

// dropMarker removes one `&name` from the line it is written on.
//
// Every anchor goes, referenced or not: the grammar refuses the marker itself, so
// a file that kept one would be a file `flow validate` refuses after `flow fix`
// reported success — the "`flow fix . && git commit` succeeds on a file `flow
// validate` rejects" outcome the command exists to avoid.
//
// The value stays where it was written. An anchor is a name *on* a value, not a
// declaration of one, so `retry: &backoff` is still `retry:` with its block under
// it once the name is gone.
func (in *aliasInliner) dropMarker(anchor *ast.AnchorNode) bool {
	pos := anchor.Start.Position
	text := in.f.line(pos.Line)

	at, located := byteOffsetOfColumn(text, pos.Column)
	if !located {
		in.refuseAt(pos.Line, pos.Column,
			"this anchor is not written where it was read, so it cannot be removed safely; remove it by hand")

		return false
	}

	want := "&" + anchorName(anchor)
	if at+len(want) > len(text) || text[at:at+len(want)] != want {
		in.refuseAt(pos.Line, pos.Column,
			"this anchor is not written on its line the way it was read, so it cannot be removed safely; remove it by hand")

		return false
	}

	// One space after the name goes with it, so `retry: &backoff 3` becomes
	// `retry: 3` rather than `retry:  3`. When nothing follows, the space *before*
	// it goes instead, so `retry: &backoff` becomes `retry:` with no trailing
	// whitespace — a byte nobody asked to have added.
	rest := strings.TrimPrefix(text[at+len(want):], " ")
	rewritten := text[:at] + rest
	if strings.TrimSpace(rest) == "" {
		rewritten = strings.TrimRight(rewritten, " ")
	}

	in.f.lines[pos.Line-1] = rewritten
	in.f.substituted = true
	in.f.changes = append(in.f.changes, FixChange{
		Line:    pos.Line,
		Message: fmt.Sprintf("anchor `%s` removed, now that its value is written where it was used", want),
		Pending: fmt.Sprintf("anchor `%s` would be removed, now that its value is written where it was used", want),
	})

	return true
}

// refuseAlias records a refusal positioned at an alias.
func (in *aliasInliner) refuseAlias(alias *ast.AliasNode, format string, args ...any) {
	span := spanOfToken(alias.Start)
	in.refuseAt(span.Start.Line, span.Start.Column, format, args...)
}

// refuseAt records a refusal at a line and column.
func (in *aliasInliner) refuseAt(line, column int, format string, args ...any) {
	in.refusals = append(in.refusals, Diagnostic{
		Line:    line,
		Column:  column,
		Message: fmt.Sprintf(format, args...),
	})
}

// containsAnchor reports whether a subtree declares an anchor of its own.
//
// An anchored value holding an anchor cannot be copied, because every copy would
// declare that inner name again — and a document declaring one name twice is the
// shape [anchorCollector] already refuses to guess about.
func containsAnchor(n ast.Node) bool {
	v := &anchorFinder{}
	ast.Walk(v, n)

	return v.found
}

// anchorFinder is the [ast.Visitor] [containsAnchor] walks with.
type anchorFinder struct{ found bool }

func (v *anchorFinder) Visit(n ast.Node) ast.Visitor {
	if _, isAnchor := n.(*ast.AnchorNode); isAnchor {
		v.found = true
	}

	return v
}

// countNodes returns how many values a subtree holds, counted the way
// [compiler.enter] counts them: one per value walked, without following an alias.
//
// It is what charges an expansion against [maxNodes]. Following the alias here
// would be doing the expansion in order to measure it, which is the cost the bound
// exists to refuse to pay.
func countNodes(n ast.Node) int {
	if n == nil {
		return 0
	}

	count := 0
	switch node := n.(type) {
	case *ast.MappingNode:
		count = 1
		for _, v := range node.Values {
			count += countNodes(v)
		}
	case *ast.MappingValueNode:
		count = 1 + countNodes(node.Key) + countNodes(node.Value)
	case *ast.SequenceNode:
		count = 1
		for _, v := range node.Values {
			count += countNodes(v)
		}
	case *ast.AnchorNode:
		count = 1 + countNodes(node.Value)
	default:
		count = 1
	}

	return count
}
