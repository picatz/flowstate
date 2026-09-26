package flowfile

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/token"
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
	in, out, ok := runAliasInliner(data, file)
	if !ok {
		return FixResult{Source: data, Refusals: in.refusals}, false
	}

	return FixResult{Source: out, Changes: in.f.changes}, true
}

// runAliasInliner does [inlineWholeValueAliases]'s actual work and hands back
// the inliner itself, rather than the [FixResult] built from it — so this
// package's own tests can read [aliasInliner.bytes] and the [fixer.edits] it
// charged against directly, which is what the accounting oracle #2045 asks
// for needs to compare against each other. [inlineWholeValueAliases] is the
// thin wrapper every other caller uses.
//
// out is the applied bytes on success, computed once here rather than left
// for the caller to rebuild with a second [fixer.apply] call — that
// assembles the whole document from its lines and edits again, and this
// function already has to pay for one copy to check it against [maxBytes].
// nil on refusal, alongside the false every other caller already reads as
// "the document did not change."
func runAliasInliner(data []byte, file *ast.File) (*aliasInliner, []byte, bool) {
	in := &aliasInliner{
		f: &fixer{
			lines:           splitLines(data),
			trailingNewline: bytes.HasSuffix(data, []byte("\n")),
			terminator:      lineTerminator(data),
		},
		anchors:  map[string]*ast.AnchorNode{},
		byLine:   map[int]aliasSite{},
		bytes:    len(data),
		inputLen: len(data),
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
		return in, nil, false
	}

	// A backstop, not the bound: [aliasInliner.bytes] already refused before
	// any expansion past [maxBytes] could be built (see
	// [aliasInliner.appendLine]), so this measures what that accounting
	// already guaranteed rather than discovering it here for the first time.
	// Kept because [fixer.apply] is what actually assembles the final bytes —
	// the trailing newline and every untouched line included, which
	// [aliasInliner.bytes] does not itself walk — and [fixOnce] refuses to
	// read a document larger than maxBytes at all, so a rewrite that crossed
	// it would hand the fixed-point loop a file it cannot parse. A second,
	// cheap check on the one value that actually matters is worth keeping
	// even once the path to it is bounded.
	out := in.f.apply()
	if len(out) > maxBytes {
		at := strictFinding{line: 1, column: 1}
		if len(in.sites) > 0 {
			at.line, at.column = in.sites[0].alias.Start.Position.Line, in.sites[0].alias.Start.Position.Column
		}
		in.refuseAt(at.line, at.column,
			"writing these aliases out would produce %d bytes, larger than the %d byte limit a Flowfile is read up to; nothing was rewritten",
			len(out), maxBytes)

		return in, nil, false
	}

	return in, out, true
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

	// bytes is what the rewrite's output holds once every alias is expanded,
	// charged against [maxBytes] — see [aliasInliner.appendLine], the one place
	// a produced line enters any replacement this rewrite builds, for why this
	// bounds the resource [aliasInliner.nodes] does not (#2045).
	//
	// It starts at the source's own length, for the same reason [aliasInliner.nodes]
	// starts at the document's own node count: what a rewrite that touched
	// nothing would already cost is not free, and a budget that only counted
	// the growth would let a large file's last few bytes of headroom be spent
	// on an expansion this rewrite never priced.
	//
	// Only output is charged here. [aliasInliner.chargeScan] charges
	// *scanning* source text separately, against [aliasInliner.scanned] and
	// [maxScanned] — folding it in here once double-counted the same bytes
	// this field's own seed already prices, refusing legitimate documents
	// nowhere near [maxBytes] on their own (#2075).
	bytes int

	// inputLen is the source's own length, recorded once so [maxScanned]
	// can use it without threading it through every call to
	// [aliasInliner.chargeScan] — the same value [aliasInliner.bytes] seeds
	// from, kept separately because that field no longer stays fixed at it.
	inputLen int

	// scanned is the total size of every source-text read this rewrite has
	// charged through [aliasInliner.chargeScan] — a token's Origin, a line,
	// or a block's extent — charged against [maxScanned] rather than
	// [aliasInliner.bytes] (#2075). Caching still keeps this to roughly the
	// document's own size for a legitimate document (each anchor or alias
	// scanned once, not once per site); this field is what a test reads to
	// check that directly, without conflating it with what
	// [aliasInliner.appendLine] charges for output.
	scanned int

	// rawScannedBytes is incremented in the wrappers that perform this
	// rewrite's per-anchor and per-alias scans — [spanOfNode]/[tokenText] on
	// an anchor's value (in [aliasInliner.spanOf]) and on a site's own key
	// (in [split]), [byteOffsetOfColumn] (in [aliasInliner.scalarValueOf]
	// and [split]), and [indentWidth] (in [aliasInliner.spliceBlock]'s
	// [aliasInliner.blockBases] block) — never by [aliasInliner.chargeScan]
	// itself. A call that bypasses its wrapper is not counted here; the
	// cache-population test is what catches that. Two kinds of read are not
	// counted either: those the output charge bounds (the shifting loop's
	// trims, [indentWidth] on a site's prefix), and [dropMarkers]'s scan,
	// which walks each line a small constant number of times regardless of
	// how many anchors share it ([dropMarkersFromLine]'s own doc comment has
	// the reasoning) and so is bounded by the input either way.
	// [fixer.blockEndBytesScanned] is this field's
	// counterpart for [fixer.blockEnd]: incremented inside the scan's own
	// loop, so neither
	// counter can be satisfied by charging without actually scanning, the
	// way reading only [aliasInliner.scanned] could be (#2075's own review:
	// three mutants that kept every cache but reverted one round's charge to
	// call its underlying scan directly, uncached, all passed a test that
	// only read what was charged). Test-only in the sense that nothing in
	// this file's own logic reads it back, not in the sense that its
	// updates are conditional on a test running.
	rawScannedBytes int

	// blockEnds caches [fixer.blockEnd]'s answer for an anchor's block
	// extent, keyed by the anchor itself, so an anchor many sites alias is
	// scanned once rather than once per site (#2075). Nothing moves the
	// lines a cached answer was computed from while sites are still being
	// expanded — [aliasInliner.rewrite] only ever edits [fixer.lines] itself
	// in [aliasInliner.dropMarkers], after every site's replacement is
	// already recorded — so the same [first, last] line range answers every
	// site that opens on this anchor. The scan itself is charged through
	// [aliasInliner.chargeScan] once, at the point this cache is filled.
	blockEnds map[*ast.AnchorNode]int

	// blockBases caches [indentWidth] of an anchor's block's first line,
	// keyed by the anchor, for the same reason [blockEnds] does: YAML
	// indentation is a run of spaces an author (or an attacker) chooses the
	// width of, not bounded by how deeply the document nests, so
	// [aliasInliner.spliceBlock] re-measuring it once per site would be the
	// same shape [blockEnds] and [anchorSpans] already close, just on a
	// third line of the anchor's own (#2075).
	blockBases map[*ast.AnchorNode]int

	// anchorSpans caches [spanOfNode]'s answer for an anchor's value, keyed
	// by the anchor: both [aliasInliner.spliceScalar] and
	// [aliasInliner.spliceBlock] call it before they ever reach their own
	// caches below, so without this one an anchor's value is re-spanned —
	// walking every token via [eachToken] and scanning each one's `Origin`
	// via [tokenText] — once per site regardless of whether the value
	// itself is cached. [aliasInliner.spanOf] charges that scan through
	// [aliasInliner.chargeScan] before it runs, using each token's `Origin`
	// length, which [originLen] gets in O(1) per token without doing the
	// scan itself (#2075).
	anchorSpans map[*ast.AnchorNode]Span

	// scalarValues caches the source text [aliasInliner.spliceScalar] reads
	// off an anchor's own line, for the same reason [blockEnds] does:
	// [byteOffsetOfColumn] rescans that line from its start on every call,
	// and a scalar anchor many sites alias would otherwise pay that rescan
	// again at every site rather than once (#2075). The scan is charged
	// through [aliasInliner.chargeScan] in [aliasInliner.scalarValueOf].
	scalarValues map[*ast.AnchorNode]string

	// splits caches [aliasInliner.split]'s answer for one alias, keyed by
	// the alias node itself, plus the two derived forms
	// [aliasInliner.spliceBlock] rebuilds on every visit —
	// `strings.TrimRight(prefix, " ")` and whether `suffix` is blank once
	// trimmed — so those are not re-scanned either just because prefix and
	// suffix themselves came from a cache. A site nested inside a block
	// that many outer sites re-expand is not itself re-expanded once per
	// outer site — it is [aliasInliner.replacement]'s own recursion through
	// [aliasInliner.expandRange] that visits it again for every outer
	// site — so without this cache, [split] rescans that inner alias's own
	// line (byte offset, trailing comment, key-colon checks) once per
	// outer site rather than once for the alias, the same shape [blockEnds]
	// and [scalarValues] close for an anchor's own line (#2075). The scan
	// is charged through [aliasInliner.chargeScan] in [split] itself.
	splits map[*ast.AliasNode]splitResult

	// markerScanSteps sums [byteOffsetsOfColumns]'s own loop iterations
	// across every line [aliasInliner.dropMarkers] has removed markers
	// from — not charged through [aliasInliner.chargeScan] for the same
	// reason [aliasInliner.rawScannedBytes]'s own doc comment gives for
	// [dropMarkersFromLine]'s scan generally: it is bounded by the line's
	// own length regardless of how many anchors share it, so nothing here
	// multiplies with a site count the way every scan [chargeScan] guards
	// does. A test reads this directly, the same way
	// [aliasInliner.rawScannedBytes] is read, to prove that bound holds
	// rather than merely claiming it (#2106, round two: k anchors sharing
	// one line of length L used to cost O(k·L) here, each one relocating
	// its own marker by rescanning the line from column one).
	markerScanSteps int
}

// splitResult is one alias's cached [aliasInliner.split] answer, plus the
// two forms [aliasInliner.spliceBlock] derives from it.
type splitResult struct {
	prefix, suffix string

	// trimmedPrefix is `strings.TrimRight(prefix, " ")`, precomputed once:
	// it is bounded by prefix's own length, already charged when [prefix]
	// was split from its line, so computing it here costs nothing this
	// rewrite has not already paid for — but recomputing it on every visit
	// to a cached alias, as [aliasInliner.spliceBlock]'s mapping form used
	// to, would still cost that much again each time (#2075).
	trimmedPrefix string

	// suffixBlank is `strings.TrimSpace(suffix) == ""`, precomputed for the
	// same reason: [aliasInliner.spliceBlock]'s sequence form checks this on
	// every visit to decide whether a trailing comment survived the split.
	suffixBlank bool
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

	if !in.dropMarkers() {
		return
	}
}

// dropMarkers removes every anchor's `&name` marker, one line at a time.
//
// Every anchor goes, referenced or not: the grammar refuses the marker itself,
// so a file that kept one would be a file `flow validate` refuses after
// `flow fix` reported success — the "`flow fix . && git commit` succeeds on a
// file `flow validate` rejects" outcome the command exists to avoid. The
// value stays where it was written: an anchor is a name *on* a value, not a
// declaration of one, so `retry: &backoff` is still `retry:` with its block
// under it once the name is gone.
//
// Anchors are grouped by the line they are written on — a marker's removal
// never reads or writes any other line — and every marker sharing one line
// is then found and removed in a single pass over that line's own bytes,
// through [dropMarkersFromLine]. That is what makes this different from
// simply calling a per-anchor `dropMarker` in the right order — this
// rewrite's first answer to #2106, and no longer a symbol this file
// defines: right-to-left order alone was
// correct but still cost O(k·L) for k anchors sharing a line of length L —
// unbudgeted, since [aliasInliner.chargeScan]'s own doc comment already
// names this scan as the one this file trusts to be bounded by the input —
// because each anchor relocated its own marker by rescanning the line from
// column 1 ([byteOffsetOfColumn]) and then rebuilt the whole line from
// scratch. Batching every anchor on a line into one call finding every
// column in one forward pass ([byteOffsetsOfColumns]) and rewriting the
// line in one pass ([dropMarkersFromLine]) costs O(L+k) per line instead
// (#2106, round two).
func (in *aliasInliner) dropMarkers() bool {
	byLine := map[int][]*ast.AnchorNode{}
	var lines []int
	for _, anchor := range in.anchorNodes {
		line := anchor.Start.Position.Line
		if _, seen := byLine[line]; !seen {
			lines = append(lines, line)
		}
		byLine[line] = append(byLine[line], anchor)
	}
	slices.Sort(lines)

	for _, line := range lines {
		anchors := byLine[line]
		slices.SortFunc(anchors, func(a, b *ast.AnchorNode) int {
			return cmp.Compare(a.Start.Position.Column, b.Start.Position.Column)
		})
		if !in.dropMarkersOnLine(line, anchors) {
			return false
		}
	}

	return true
}

// dropMarkersOnLine removes every one of anchors' markers from the one line
// they are all written on, and records the changes.
//
// anchors is sorted ascending by column, which [dropMarkersFromLine] needs to
// find every one of them in a single forward pass, and every one of them is
// on this same line, which is what makes a single rewrite of the line answer
// for all of them at once.
func (in *aliasInliner) dropMarkersOnLine(line int, anchors []*ast.AnchorNode) bool {
	text := in.f.line(line)

	columns := make([]int, len(anchors))
	names := make([]string, len(anchors))
	for i, anchor := range anchors {
		columns[i] = anchor.Start.Position.Column
		names[i] = anchorName(anchor)
	}

	rewritten, badIndex, notLocated, steps, ok := dropMarkersFromLine(text, columns, names)
	in.markerScanSteps += steps
	if !ok {
		if notLocated {
			in.refuseAt(line, columns[badIndex],
				"this anchor is not written where it was read, so it cannot be removed safely; remove it by hand")
		} else {
			in.refuseAt(line, columns[badIndex],
				"this anchor is not written on its line the way it was read, so it cannot be removed safely; remove it by hand")
		}

		return false
	}

	in.f.lines[line-1] = rewritten
	in.f.substituted = true
	for _, name := range names {
		want := "&" + name
		in.f.changes = append(in.f.changes, FixChange{
			Line:    line,
			Message: fmt.Sprintf("anchor `%s` removed, now that its value is written where it was used", want),
			Pending: fmt.Sprintf("anchor `%s` would be removed, now that its value is written where it was used", want),
		})
	}

	return true
}

// dropMarkersFromLine removes every `&name` in names from text, each one
// written at the matching, ascending column in columns — [byteOffsetOfColumn]'s
// own convention (1-based, counting characters).
//
// badIndex, notLocated, and a false ok report the first marker (by position
// in columns/names, which is ascending column order) that could not be
// removed safely: notLocated true for a column past what the line holds,
// false for a column that holds something other than the marker it should.
// Nothing is rewritten on a refusal; the caller leaves the line alone.
// steps is [byteOffsetsOfColumns]'s own count, passed through so a caller
// can add it to [aliasInliner.markerScanSteps] without a second call.
//
// # One pass to find every column, one to rewrite
//
// [byteOffsetsOfColumns] finds every marker's byte offset in one forward walk
// of text's runes, rather than one restart-from-column-one scan per marker —
// the O(k·L) cost #2106's second round found in the single-anchor form this
// replaces. Once every offset and every marker's own width is known, the
// rewritten line is built in one further pass: copy the text before each
// marker, skip the marker itself and one adjacent space after it, and move
// on. Total cost is the line's own length plus the anchor count, once each.
//
// # The one thing that still needs a marker's *removal*, not just its bytes
//
// The rule this preserves: one space after the marker's name goes with it,
// so `retry: &backoff 3` becomes `retry: 3` rather than `retry:  3`; when
// nothing follows on the line, the space *before* the marker goes instead,
// so `retry: &backoff` becomes `retry:` with no trailing whitespace nobody
// asked to have added. "Nothing follows" depends on what the rest of the
// *rewritten* line looks like once every marker to the right is already
// gone — not on the original text, which still has every marker's own bytes
// in it. blank[i] answers that for marker i without re-scanning the line
// once per marker: computed right to left, it folds in whether the gap to
// the next marker (or to the line's end, for the last one) is blank with
// whatever was already true of the marker to its right — a marker's own
// bytes are always fully removed regardless of blank[i], so they never
// count against blankness, and the two adjacent markers sharing a line this
// rewrite exists to handle are always separated by the value or punctuation
// between them, which does.
func dropMarkersFromLine(text string, columns []int, names []string) (rewritten string, badIndex int, notLocated bool, steps int, ok bool) {
	offsets, bad, steps := byteOffsetsOfColumns(text, columns)
	if bad >= 0 {
		return text, bad, true, steps, false
	}

	type marker struct{ at, w int }
	markers := make([]marker, len(columns))
	for i, at := range offsets {
		want := "&" + names[i]
		if at+len(want) > len(text) || text[at:at+len(want)] != want {
			return text, i, false, steps, false
		}
		markers[i] = marker{at: at, w: len(want)}
	}

	blank := make([]bool, len(markers))
	tailBlank := true
	for i := len(markers) - 1; i >= 0; i-- {
		next := len(text)
		if i+1 < len(markers) {
			next = markers[i+1].at
		}
		gap := text[markers[i].at+markers[i].w : next]
		tailBlank = tailBlank && strings.TrimSpace(gap) == ""
		blank[i] = tailBlank
	}

	out := make([]byte, 0, len(text))
	pos := 0
	for i, m := range markers {
		out = append(out, text[pos:m.at]...)
		next := len(text)
		if i+1 < len(markers) {
			next = markers[i+1].at
		}
		switch {
		case blank[i]:
			// The whole gap to the next marker (or to the line's end, for
			// the last one) is blank, so none of it survives — not just the
			// one adjacent space the non-blank case below drops. Trimming
			// the prefix and skipping straight to next matches what
			// sequentially trimming `text[:at]+rest` down to its last
			// non-space character would leave, for the same reason
			// [dropMarkersFromLine]'s own doc comment gives: whitespace
			// contributes nothing either way, and every marker's own bytes
			// in the gap are already excluded from it.
			out = bytes.TrimRight(out, " ")
			pos = next
		case m.at+m.w < len(text) && text[m.at+m.w] == ' ':
			pos = m.at + m.w + 1
		default:
			pos = m.at + m.w
		}
	}
	out = append(out, text[pos:]...)

	return string(out), -1, false, steps, true
}

// byteOffsetsOfColumns returns the byte offset of every column in columns —
// which must be given in ascending order, the order [aliasInliner.dropMarkers]
// already sorts a line's anchors into — found in one forward pass over
// text's runes, rather than one [byteOffsetOfColumn]-style restart-from-column-one
// scan per column.
//
// bad is the index of the first column [byteOffsetOfColumn] would itself have
// refused (less than 1, or past one column beyond the line's last rune),
// or -1 when every one of them was found; offsets past that index are left
// zero and unused by any caller. steps counts this function's own loop
// iterations — at most len(text)+1 regardless of how many columns are
// asked for, which is the property [TestDropMarkersScanIsLinearInLineLength]
// checks directly rather than by wall time (#2106, round two).
func byteOffsetsOfColumns(text string, columns []int) (offsets []int, bad, steps int) {
	offsets = make([]int, len(columns))
	if len(columns) == 0 {
		return offsets, -1, 0
	}
	if columns[0] < 1 {
		return offsets, 0, 0
	}

	idx, col := 0, 1
	for offset := range text {
		steps++
		for idx < len(columns) && columns[idx] == col {
			offsets[idx] = offset
			idx++
		}
		if idx == len(columns) {
			return offsets, -1, steps
		}
		col++
	}
	// One past the last rune is still a valid position — the end of the
	// line, where a marker may legitimately start when nothing follows it —
	// matching [byteOffsetOfColumn]'s own final check.
	for idx < len(columns) {
		if columns[idx] != col {
			return offsets, idx, steps
		}
		offsets[idx] = len(text)
		idx++
	}

	return offsets, -1, steps
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

	prefix, suffix, ok := in.splitOf(site)
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

// splitOf returns [aliasInliner.split]'s answer for site, computed once per
// alias and cached in [aliasInliner.splits] along with the two forms
// [aliasInliner.spliceBlock] derives from it: a site nested inside a block
// is re-split (and re-derived) every time [aliasInliner.replacement]'s own
// recursion through [aliasInliner.expandRange] visits it again for another
// outer site, and without this cache that repeats [split]'s scan of the
// alias's own line — potentially long — once per outer site rather than
// once for the alias itself (#2075).
func (in *aliasInliner) splitOf(site aliasSite) (prefix, suffix string, ok bool) {
	if cached, hit := in.splits[site.alias]; hit {
		return cached.prefix, cached.suffix, true
	}

	prefix, suffix, ok = in.split(site)
	if !ok {
		return "", "", false
	}

	if in.splits == nil {
		in.splits = map[*ast.AliasNode]splitResult{}
	}
	in.splits[site.alias] = splitResult{
		prefix:        prefix,
		suffix:        suffix,
		trimmedPrefix: strings.TrimRight(prefix, " "),
		suffixBlank:   strings.TrimSpace(suffix) == "",
	}

	return prefix, suffix, true
}

// split returns the text of a site's line before and after the alias, and checks
// that both are shapes the replacement can be built around.
//
// The prefix is what the value is written after — `  retry: ` or `  - ` — and the
// suffix is whatever followed the alias, which may only be a comment. Anything
// else there means the line holds more than this one value, and a whole-line edit
// would lose it.
//
// The whole line is charged through [aliasInliner.chargeScan] before any of
// it is scanned: [byteOffsetOfColumn] below reads up to the alias's own
// column, and every `strings.Trim*` call further down reads a piece of what
// this charges, so one charge here covers this function's own scan in full
// (#2075).
func (in *aliasInliner) split(site aliasSite) (prefix, suffix string, ok bool) {
	pos := site.alias.Start.Position
	text := in.f.line(pos.Line)
	if !in.chargeScan(site.alias, len(text)) {
		return "", "", false
	}

	// Counted here rather than inside [aliasInliner.chargeScan]:
	// [byteOffsetOfColumn] below actually reads text, and every
	// `strings.Trim*` further down reads a piece of it, so this is what a
	// test checks stays flat per alias rather than growing per outer site
	// (#2075).
	in.rawScannedBytes += len(text)

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

	// Charged separately from the line above: [spanOfNode] walks
	// site.key's own tokens and [tokenText] scans each one's `Origin`,
	// which is not bounded by this line's own length — a key's Origin
	// captures every raw byte since the previous token, so a key that
	// merely follows a long run of blank lines or comments elsewhere in
	// the document carries that whole run in its own Origin (found via
	// this file's own probe for the unrelated block-blank-tail shape,
	// #2075).
	keySize := originLen(site.key)
	if !in.chargeScan(site.alias, keySize) {
		return "", "", false
	}

	// Counted here, not inside [aliasInliner.chargeScan]: [spanOfNode]
	// right below is the actual scan [keySize] prices (#2075).
	in.rawScannedBytes += keySize

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

// spanOf returns [spanOfNode]'s answer for an anchor's value, computed once
// per anchor and cached in [aliasInliner.anchorSpans]. Both
// [aliasInliner.spliceScalar] and [aliasInliner.spliceBlock] call this
// before they ever reach their own value caches, so without this one an
// anchor's value would be re-spanned once per site regardless of whether
// the value itself is cached: [spanOfNode] walks every token in the value
// via [eachToken], and [tokenText] scans each token's `Origin` — the raw
// source bytes the parser keeps to reproduce the document byte for byte,
// including any padding written before the token — with
// [strings.TrimSpace]. [originLen] gets the same total [tokenText] would
// scan without doing the scan itself (each token's `Origin` already knows
// its own length), which is what lets this charge for the scan through
// [aliasInliner.chargeScan] before it runs rather than after (#2075).
func (in *aliasInliner) spanOf(alias *ast.AliasNode, anchor *ast.AnchorNode) (Span, bool) {
	if span, cached := in.anchorSpans[anchor]; cached {
		return span, true
	}

	size := originLen(anchor.Value)
	if !in.chargeScan(alias, size) {
		return Span{}, false
	}

	// Counted here, next to the call [size] prices, rather than inside
	// [aliasInliner.chargeScan]: this line runs exactly when [spanOfNode]
	// is about to walk every token in anchor.Value and scan each one's
	// `Origin`, which is the actual cost this file's own tests check stays
	// bounded — a mutant that kept the charge above but skipped
	// [aliasInliner.anchorSpans] would still show it growing per site here
	// (#2075).
	in.rawScannedBytes += size
	span := spanOfNode(anchor.Value)
	if in.anchorSpans == nil {
		in.anchorSpans = map[*ast.AnchorNode]Span{}
	}
	in.anchorSpans[anchor] = span

	return span, true
}

// originLen returns the total length of every token's `Origin` in a
// subtree — the same total [tokenText] would scan with [strings.TrimSpace]
// on each one — without doing that scan: a string's own length is O(1) to
// read, so walking the tokens with [eachToken] and summing `len(tok.Origin)`
// (or `len(tok.Value)`, [tokenText]'s own fallback when a token carries no
// `Origin`) costs one [len] per token rather than one scan per token's
// worth of source bytes.
func originLen(n ast.Node) int {
	total := 0
	eachToken(n, func(tok *token.Token) {
		if tok == nil {
			return
		}
		if tok.Origin != "" {
			total += len(tok.Origin)
			return
		}
		total += len(tok.Value)
	})

	return total
}

// charge adds n to the bytes an expansion has spent on *output* and refuses
// once that crosses [maxBytes], exactly where [countNodes]'s own comment
// says a charge belongs: at the point of spend, before the memory is
// allocated, rather than after. [aliasInliner.appendLine] is the only
// caller — a line this rewrite produces — since [aliasInliner.chargeScan]
// charges source text this rewrite scans against its own, separate budget
// rather than this one (#2075).
func (in *aliasInliner) charge(alias *ast.AliasNode, n int) bool {
	in.bytes += n
	if in.bytes > maxBytes {
		in.refuseAlias(alias,
			"writing these aliases out would copy more than %d bytes, more than a Flowfile is meant to hold; nothing was rewritten",
			maxBytes)

		return false
	}

	return true
}

// maxScanned bounds [aliasInliner.scanned] for a document inputLen bytes
// long — [scanBudgetMultiple] times [maxBytes], plus inputLen itself as a
// floor. inputLen is a floor rather than the resource being priced: every
// scan this rewrite makes reads a subset of what the parser already read
// once, so a document's own length is the least any of them could cost, not
// an estimate of what several of them touching the same region will add up
// to. [scanBudgetMultiple]'s own comment has the reasoning for the
// multiplier (#2075).
func maxScanned(inputLen int) int {
	return scanBudgetMultiple*maxBytes + inputLen
}

// chargeScan is [aliasInliner.charge]'s shape for source text this rewrite
// is about to scan (or, where the exact size is only known once the scan is
// already done, has just finished scanning) rather than for a line it is
// about to produce — but charged against [aliasInliner.scanned] and
// [maxScanned], a separate budget from [aliasInliner.bytes] and [maxBytes].
// Charging both into one budget double-counted the same bytes (this field's
// own history: an earlier version of this rewrite did exactly that, and
// review found it refused legitimate documents nowhere near [maxBytes] on
// their own — see [scanBudgetMultiple]'s comment).
//
// Every per-expansion read this file's own caches exist for goes through
// this, from the wrapper that owns the cache rather than from the scanning
// call itself: [split]'s own line-charge (wrapping [byteOffsetOfColumn]
// and every `strings.Trim*` that reads the same line) and its key-charge
// (wrapping [spanOfNode] on the site's own key), [aliasInliner.spanOf]
// (wrapping [spanOfNode]/[tokenText] on an anchor's whole value),
// [aliasInliner.scalarValueOf] (wrapping two [byteOffsetOfColumn] calls),
// [aliasInliner.spliceBlock]'s [aliasInliner.blockBases] block (wrapping
// two [indentWidth] calls), and [aliasInliner.spliceBlock]'s own read of
// [fixer.blockEndBytesScanned] — so a read added later that forgets to
// cache its answer is still bounded by [maxScanned], not bounded only by
// whichever cache someone remembered to write for it. Two reads in
// [aliasInliner.spliceBlock] are not routed here at all: `indentWidth(prefix)`,
// computing where the shifted block goes, and the shifting loop's own
// `strings.TrimSpace`/[indentWidth] over each line of the range
// [aliasInliner.blockEnds] already bounded. Both read text
// [aliasInliner.appendLine] already charges as output on the same call —
// prefix is the current site's own line, already charged in [split]; the
// range's lines are charged as they are copied into the replacement — so
// their size is bounded by [aliasInliner.bytes] rather than needing a
// second charge here.
//
// This bound alone does not prove a read is actually cached, only that it
// cannot run away unbounded either way — a wrapper that charges through
// this function on every call, cache or not, still passes. See
// [aliasInliner.rawScannedBytes], which a test reads instead, for the
// cache itself.
func (in *aliasInliner) chargeScan(alias *ast.AliasNode, n int) bool {
	in.scanned += n
	if limit := maxScanned(in.inputLen); in.scanned > limit {
		in.refuseAlias(alias,
			"writing these aliases out would scan more than %d bytes, more than a Flowfile is meant to require reading; nothing was rewritten",
			limit)

		return false
	}

	return true
}

// appendLine is the one place a line this rewrite produces is added to a
// replacement, and every splice and copy in this file builds its lines by
// calling it — which is what makes a new copy site forgetting to charge
// structurally hard rather than merely documented (#2045).
//
// # What #2045 found, and why this closes all of it at once
//
// Four rounds of independent review each found a different copy site the
// previous attempt's per-site charges did not cover: an indentation shift in
// [aliasInliner.spliceBlock] that grew a copied line for free, a splice that
// charged only the aliased value and not the line it was rebuilt into, a
// trailing-comment reconstruction nothing charged at all, and — the one a
// five-row corpus of *shapes* still missed, because it varies bytes per line
// and this varies lines per byte — a blank line inside a copied block, which
// used to cost nothing no matter how many of them there were.
//
// Charging the finished line here, in the one function that actually grows
// every returned slice, closes the first three by construction: whatever a
// splice built — indent, prefix, suffix, comment — is priced as the line it
// became rather than as whichever of its ingredients a charge remembered to
// name. The fourth is the `+len(in.f.terminator)` floor: every line pays at
// least its terminator, blank or not, so a thousand blank lines cost a
// thousand terminators rather than zero.
//
// The line and slot overhead a Go string header and a slice element carry —
// on the order of tens of bytes each, unpriced here — is a second resource
// this does not meter separately. At [maxBytes]'s own ceiling and this
// function's one-terminator floor per line, the worst case is bounded by the
// same budget: at most maxBytes lines, tens of megabytes of header and slot
// overhead alongside the megabyte actually charged. Accepted rather than
// separately bounded, because a second, narrower limit on top of this one
// would be a second mechanism for a resource this file already prices —
// invariant 2 — for a cost an order of magnitude under what an already-bounded
// rewrite risks elsewhere.
func (in *aliasInliner) appendLine(alias *ast.AliasNode, out []string, line string) ([]string, bool) {
	if !in.charge(alias, len(line)+len(in.f.terminator)) {
		return out, false
	}

	return append(out, line), true
}

// spliceScalar replaces an alias with a value written on one line: a scalar, or a
// mapping or sequence in flow style.
//
// The value's own source text is copied, so its quoting is whatever the author
// chose rather than whatever a re-render would produce.
func (in *aliasInliner) spliceScalar(site aliasSite, prefix, suffix string, anchor *ast.AnchorNode) ([]string, bool) {
	span, ok := in.spanOf(site.alias, anchor)
	if !ok {
		return nil, false
	}
	if !span.IsValid() || span.Start.Line != span.End.Line {
		in.refuseAlias(site.alias,
			"the value `&%s` names is not written on one line, so it cannot be written after `%s`; write it out by hand",
			anchorName(anchor), strings.TrimSpace(prefix))

		return nil, false
	}

	value, cached := in.scalarValues[anchor]
	if !cached {
		var found bool
		value, found = in.scalarValueOf(site, anchor, span)
		if !found {
			return nil, false
		}
		if in.scalarValues == nil {
			in.scalarValues = map[*ast.AnchorNode]string{}
		}
		in.scalarValues[anchor] = value
	}

	// The whole rebuilt line, not just value: prefix carries this site's own
	// indentation and key, which #2045 found could dwarf a tiny aliased value
	// once nested deep enough — appendLine charges what this function is
	// actually about to hand back, not one ingredient of it.
	return in.appendLine(site.alias, nil, prefix+value+suffix)
}

// scalarValueOf reads the source text of an anchor's one-line value, span
// already validated by [aliasInliner.spliceScalar]. It exists to be called
// once per anchor and cached in [aliasInliner.scalarValues]: [byteOffsetOfColumn]
// rescans the anchor's own line from its start every time it is called, and a
// scalar anchor many sites alias would otherwise pay that rescan again at
// every site rather than once — the same shape [aliasInliner.blockEnds]
// caches for a block value (#2075). The whole line is charged through
// [aliasInliner.chargeScan] before either [byteOffsetOfColumn] call below
// reads any of it.
func (in *aliasInliner) scalarValueOf(site aliasSite, anchor *ast.AnchorNode, span Span) (string, bool) {
	text := in.f.line(span.Start.Line)
	if !in.chargeScan(site.alias, len(text)) {
		return "", false
	}

	// Counted here rather than inside [aliasInliner.chargeScan]: both
	// [byteOffsetOfColumn] calls below actually read text, up to twice its
	// own length (once per call), so this is what a test checks stays flat
	// per anchor rather than growing per site (#2075).
	in.rawScannedBytes += len(text)

	from, located := byteOffsetOfColumn(text, span.Start.Column)
	if !located {
		in.refuseAlias(site.alias, "the value `&%s` names is not written where it was read; write it out by hand", anchorName(anchor))

		return "", false
	}
	through, ended := byteOffsetOfColumn(text, span.End.Column)
	if !ended || through < from {
		in.refuseAlias(site.alias, "the value `&%s` names is not written where it was read; write it out by hand", anchorName(anchor))

		return "", false
	}

	value := text[from:through]
	if strings.TrimSpace(value) == "" {
		in.refuseAlias(site.alias,
			"the anchor `&%s` names no value, so there is nothing to write in this alias's place; write the value out by hand",
			anchorName(anchor))

		return "", false
	}

	return value, true
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

	span, ok := in.spanOf(site.alias, anchor)
	if !ok {
		return nil, false
	}
	if !span.IsValid() {
		in.refuseAlias(site.alias, "the value `&%s` names is not written where it was read; write it out by hand", name)

		return nil, false
	}

	first := span.Start.Line
	anchorLine := anchor.Start.Position.Line

	// [indentWidth] only counts leading spaces, but YAML indentation is
	// exactly as attacker-sized as anything else this file charges for —
	// nesting *depth* is not the resource at risk here, the width of one
	// line's own indent is — so [aliasInliner.blockBases] caches the answer
	// per anchor the same way [aliasInliner.anchorSpans] and
	// [aliasInliner.blockEnds] do, rather than re-measuring both lines once
	// per site (#2075).
	base, baseCached := in.blockBases[anchor]
	if !baseCached {
		firstLine, anchorLineText := in.f.line(first), in.f.line(anchorLine)
		if !in.chargeScan(site.alias, len(firstLine)+len(anchorLineText)) {
			return nil, false
		}

		// Counted here, not inside [aliasInliner.chargeScan]: the two
		// [indentWidth] calls right below are the actual scans this charge
		// prices (#2075).
		in.rawScannedBytes += len(firstLine) + len(anchorLineText)

		base = indentWidth(firstLine)
		if first <= anchorLine || base <= indentWidth(anchorLineText) {
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

		if in.blockBases == nil {
			in.blockBases = map[*ast.AnchorNode]int{}
		}
		in.blockBases[anchor] = base
	}

	last, blockCached := in.blockEnds[anchor]
	if !blockCached {
		before := in.f.blockEndBytesScanned
		last = in.f.blockEnd(first-1, base-1)

		// The scan already ran — [fixer.blockEnd] has no way to charge
		// mid-scan without a budget-shaped hook on a type every other
		// caller in this package also shares — so this charges its exact
		// cost immediately after, before the range is used to build a
		// replacement, into the same budget every other scan in this file
		// charges before running. A document with many anchors each
		// opening a large block is still bounded by [maxScanned], not
		// [maxBytes] — this is a scan, not output — the same way one
		// anchor aliased by many sites already is (#2075).
		if !in.chargeScan(site.alias, in.f.blockEndBytesScanned-before) {
			return nil, false
		}

		if in.blockEnds == nil {
			in.blockEnds = map[*ast.AnchorNode]int{}
		}
		in.blockEnds[anchor] = last
	}

	raw, ok := in.expandRange(site.alias, first, last, stack)
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
			// Charged even though nothing is written: an unwritten blank line
			// still occupies one in the copy, and this is where #2045's finding
			// 4 lived — a blank line costing nothing no matter how many of
			// them a copied block held. See [aliasInliner.appendLine].
			var ok bool
			shifted, ok = in.appendLine(site.alias, shifted, "")
			if !ok {
				return nil, false
			}

			continue
		}
		if indentWidth(line) < base {
			in.refuseAlias(site.alias,
				"a line of the value `&%s` names is indented less than the value itself, so the block cannot be moved as a whole; write it out by hand",
				name)

			return nil, false
		}

		var ok bool
		shifted, ok = in.appendLine(site.alias, shifted, strings.Repeat(" ", indent)+line[base:])
		if !ok {
			return nil, false
		}
	}
	if len(shifted) == 0 {
		in.refuseAlias(site.alias, "the anchor `&%s` names no value, so there is nothing to write in this alias's place; write the value out by hand", name)

		return nil, false
	}

	if site.sequence {
		if !in.splits[site.alias].suffixBlank {
			in.refuseAlias(site.alias,
				"a comment is written after this alias, and the value `&%s` names is a block whose first line goes where the comment is; move the comment above the item and run this again",
				name)

			return nil, false
		}

		// The dash keeps its line and the block's first line sits beside it, which
		// is where a list item's mapping is written. `indent` is the dash's prefix
		// width by construction, so the two fit together exactly — checked rather
		// than assumed: a block's first line is non-blank by construction (the
		// blank-line arm above never produces shifted[0]) and every nested
		// replacement's first line is non-empty, but that reasoning crosses two
		// functions and nothing before this asserted it (found on the way, #2045).
		if len(shifted[0]) < indent {
			in.refuseAlias(site.alias,
				"the value `&%s` names does not indent under this list item the way this rewrite expected; write it out by hand",
				name)

			return nil, false
		}

		out, ok := in.appendLine(site.alias, nil, prefix+shifted[0][indent:])
		if !ok {
			return nil, false
		}

		return append(out, shifted[1:]...), true
	}

	// The key keeps its line — with its comment, if it had one — and the block goes
	// underneath it, which is the only place a block value can be written.
	out, ok := in.appendLine(site.alias, nil, in.splits[site.alias].trimmedPrefix+suffix)
	if !ok {
		return nil, false
	}

	return append(out, shifted...), true
}

// expandRange returns the source lines first..last with every alias written among
// them replaced by the value it names.
//
// This is what makes a chain work: an anchored value holding an alias of its own
// is copied with that alias already written out, so one pass settles the whole
// chain rather than one link per round. stack carries the anchors being expanded
// on the way here, so the recursion cannot follow a cycle.
//
// against is the alias whose expansion this range is part of, named so a
// refusal here reads at that alias rather than nowhere: this range's own
// site is not itself a line, so nothing else in scope can attribute the
// charge for a line it merely copies through. A line untouched by any alias
// still costs a copy — #2045's finding 4 was a blank line inside a copied
// block costing nothing no matter how many of them the block held — so the
// plain-copy arm below charges every such line explicitly. A replacement
// line already pays for itself inside [aliasInliner.replacement]'s own
// splice and is not charged again here.
func (in *aliasInliner) expandRange(against *ast.AliasNode, first, last int, stack []string) ([]string, bool) {
	var out []string
	for n := first; n <= last; n++ {
		site, isSite := in.byLine[n]
		if !isSite {
			var ok bool
			out, ok = in.appendLine(against, out, in.f.line(n))
			if !ok {
				return nil, false
			}

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
