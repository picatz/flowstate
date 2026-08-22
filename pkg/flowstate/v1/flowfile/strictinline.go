package flowfile

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
)

// Anchors, aliases and merge keys are not part of the grammar (strict.go). The
// compiler refuses them, and so does `flow fix` — but a refusal leaves an author
// holding a file that no longer compiles and no migration for it: they spell the
// construct out by hand first, and get the rewrite second. This is the half that
// buys that back, and it is deliberately the smallest half that can be argued
// byte for byte (#653, #841).
//
// # What is inlined, and why only this
//
// Two shapes, both of which are a splice inside one line and nothing else:
//
//   - a whole-value alias — `message: *greeting`, or `- *greeting` under a block
//     sequence — replaced by the source text of the scalar its anchor names;
//   - the anchor that names that scalar — `greeting: &greeting hi` — with the
//     `&greeting ` dropped and the value's bytes left exactly where they were.
//
// An anchor is dropped only when its value is a single-line scalar written on
// the anchor's own line, which is precisely the shape an alias to it could be
// inlined from. That keeps what the rewriter does with these constructs to one
// sentence rather than two, and keeps every edit to one shape: a replacement of
// a run of bytes inside a line whose other bytes, comment included, are copied
// through untouched. Nothing is re-indented, no line is added, moved or removed,
// and so no comment can change what it is attached to (#862, #866).
//
// Everything else stays refused, byte for byte, exactly as before:
//
//   - a merge key (`<<:`). Splicing one in is not a copy: `<<:` loses to keys
//     written on the mapping itself, two merged mappings resolve left to right,
//     and reproducing that is deciding which of two spellings of a key wins. That
//     is judgement, and `flow fix` does not exercise judgement.
//   - an alias inside a flow collection (`[*a, 1]`, `{k: *a}`), which the fixer
//     already refuses to rewrite at all for want of line structure.
//   - an anchor or alias on a mapping *key*, which is not a value position.
//   - an anchor whose value is a block mapping, a sequence, a block scalar, or
//     another alias — anything whose text is not one line of one scalar. Copying
//     a block into a position at another indentation is a re-serialization, and a
//     re-serialization is where a rewriter that does not know what the grammar
//     binds corrupts a file.
//   - an alias naming an anchor that does not exist, that is defined twice, or
//     that is written later in the file than the alias that uses it.
//   - a file of more than one document, since an anchor is document-scoped.
//
// # What bounds it
//
// An inliner is an expander, and expansion is the resource an author of a
// hostile file controls: a billion-laughs document is a handful of anchors
// referenced from many aliases, multiplying breadth at every level, which is why
// this package bounds expansion by *total nodes* rather than by depth
// ([maxNodes], and CLAUDE.md's rule that a depth bound cannot see a breadth
// explosion). The same bound applies here, in the same spelling: every site
// inlines exactly one node — the anchored value is a single-line scalar or the
// site is not inlinable — so the nodes this rewrite would add is the number of
// sites, and past [maxNodes] of them the file is refused and left alone. The
// nested shape a bomb needs cannot arise at all, because an anchor whose value
// holds an alias is refused before any of this: the bound is the second answer,
// not the first.
//
// # What makes it safe to be wrong
//
// The walk below decides what it can inline. It does not have to be complete for
// the result to be sound: the rewritten bytes are re-parsed and re-inspected by
// [strictYAMLRefusalsIn] before they are returned, so a construct the walk failed
// to understand — a node shape a later goccy release introduces, say — leaves a
// refusal standing in the output and the whole rewrite is discarded. A file is
// inlined entirely or not at all, the same all-or-nothing rule the rest of
// [Fix] follows.

// an inlineSplice is one replacement of a run of bytes inside one line.
//
// Recorded rather than applied as it walks, for the reason the fixer records its
// line edits: an edit applied mid-walk moves every offset found after it.
type inlineSplice struct {
	// line is 1-based, and from/to are byte offsets into that line, half-open.
	line     int
	from, to int

	// text replaces line[from:to]. Empty for the anchor drop.
	text string

	change FixChange
}

// an anchorSite is one `&name` and the value it names.
type anchorSite struct {
	name  string
	node  *ast.AnchorNode
	value ast.Node
}

// an aliasSite is one `*name` in a whole-value position.
type aliasSite struct {
	name string
	node *ast.AliasNode
}

// an inliner collects the anchors and aliases of one document and decides
// whether all of them can be spliced away.
type inliner struct {
	lines []string

	anchors map[string]*anchorSite
	aliases []*aliasSite

	// blocked records that some construct cannot be inlined. It is a single flag
	// rather than a list because the refusal an author sees is the one
	// [strictYAMLRefusalsIn] already builds — positioned at every construct, in
	// the same words the compiler uses. A second explanation here would be the
	// fixer and the compiler naming one thing in two voices, which strict.go's
	// one-collector rule exists to prevent.
	blocked bool
}

// inlineStrictYAML rewrites every anchor and alias in data into the text it
// stands for, and reports whether it could do so for all of them.
//
// When it reports false nothing was written: the caller leaves the document byte
// for byte alone and refuses it. See this file's comment for the shapes that are
// inlined and the ones that are not.
func inlineStrictYAML(data []byte, file *ast.File) ([]byte, []FixChange, bool) {
	if len(file.Docs) != 1 {
		// An anchor is scoped to its document, and a Flowfile is one document
		// (`compile` says so). A multi-document file is refused elsewhere; here it
		// is simply not a file this rewrite reasons about.
		return nil, nil, false
	}

	in := &inliner{lines: splitLines(data), anchors: map[string]*anchorSite{}}
	in.walk(file.Docs[0].Body, positionValue)
	splices, ok := in.plan()
	if !ok {
		return nil, nil, false
	}

	out := applyInlineSplices(in.lines, splices, lineTerminator(data), bytes.HasSuffix(data, []byte("\n")))

	// The post-condition, checked rather than argued: whatever the walk did or did
	// not understand, what comes back holds none of the three constructs and still
	// parses. A rewrite that fails this is discarded whole.
	reparsed, err := parser.ParseBytes(out, parser.ParseComments)
	if err != nil || len(strictYAMLRefusalsIn(reparsed)) > 0 {
		return nil, nil, false
	}

	changes := make([]FixChange, 0, len(splices))
	for _, splice := range splices {
		changes = append(changes, splice.change)
	}
	return out, changes, true
}

// A position is where in the grammar a node sits, which is what decides whether
// an anchor or an alias there is one this rewriter can splice.
//
// The distinction is the whole safety argument for the alias case: `key: *a` is
// a value written in one place, and replacing it with the text of a scalar
// changes nothing else about the line. `*a: value` is a *key*, and `[*a]` is
// inside a collection with no line structure to splice.
type position int

const (
	// positionValue is a whole value in block style: a mapping value, a block
	// sequence entry, or a document body.
	positionValue position = iota

	// positionOther is everywhere else — a mapping key, anything inside a flow
	// collection, anything under an anchor or a tag.
	positionOther
)

// walk records the anchors and aliases of a subtree, and blocks on any construct
// this rewriter does not splice.
func (in *inliner) walk(n ast.Node, at position) {
	switch node := n.(type) {
	case nil:
		return
	case *ast.MappingNode:
		child := at
		if node.IsFlowStyle {
			child = positionOther
		}
		for _, v := range node.Values {
			in.walkMappingValue(v, child)
		}
	case *ast.MappingValueNode:
		in.walkMappingValue(node, at)
	case *ast.SequenceNode:
		child := positionValue
		if node.IsFlowStyle {
			child = positionOther
		}
		for _, v := range node.Values {
			in.walk(v, child)
		}
	case *ast.AnchorNode:
		if at != positionValue {
			in.blocked = true
			return
		}
		in.recordAnchor(node)
		// The anchored value in positionOther: an anchor whose value is itself an
		// alias or another anchor is not a single-line scalar, so it is refused
		// there rather than followed here — which is also what keeps a nested
		// expansion from existing to be bounded.
		in.walk(node.Value, positionOther)
	case *ast.AliasNode:
		if at != positionValue {
			in.blocked = true
			return
		}
		name, ok := scalarText(node.Value)
		if !ok {
			in.blocked = true
			return
		}
		in.aliases = append(in.aliases, &aliasSite{name: name, node: node})
	case *ast.MergeKeyNode:
		in.blocked = true
	default:
		// Scalars and the nodes that wrap them. Nothing to record — and nothing
		// missed either, because a construct hiding under a shape this switch does
		// not know is caught by the re-inspection of the output in
		// [inlineStrictYAML].
		for _, child := range childNodes(n) {
			in.walk(child, positionOther)
		}
	}
}

// walkMappingValue walks one `key: value` entry: the key is never a position an
// anchor or alias may be spliced from, the value is when the mapping is written
// in block style.
func (in *inliner) walkMappingValue(node *ast.MappingValueNode, at position) {
	if node == nil {
		return
	}
	value := at
	if node.IsFlowStyle {
		value = positionOther
	}
	in.walk(node.Key, positionOther)
	in.walk(node.Value, value)
}

// childNodes returns the children of a node whose shape this rewriter does not
// otherwise walk, so that a construct under one is still seen.
func childNodes(n ast.Node) []ast.Node {
	switch node := n.(type) {
	case *ast.TagNode:
		return []ast.Node{node.Value}
	case *ast.DocumentNode:
		return []ast.Node{node.Body}
	default:
		return nil
	}
}

// recordAnchor stores an anchor, blocking on a name declared twice.
//
// Two anchors of one name are legal YAML — an alias binds to the nearest
// preceding one — and deciding which of them a given alias meant is exactly the
// kind of resolution a splice must not be doing on the author's behalf.
func (in *inliner) recordAnchor(node *ast.AnchorNode) {
	name, ok := scalarText(node.Name)
	if !ok {
		in.blocked = true
		return
	}
	if _, seen := in.anchors[name]; seen {
		in.blocked = true
		return
	}
	in.anchors[name] = &anchorSite{name: name, node: node, value: node.Value}
}

// plan turns the collected sites into the splices that would remove all of them,
// or reports that they cannot all be removed.
func (in *inliner) plan() ([]inlineSplice, bool) {
	if in.blocked {
		return nil, false
	}
	if len(in.anchors) == 0 && len(in.aliases) == 0 {
		return nil, false
	}

	// The expansion bound, in the spelling the rest of the package uses. Each site
	// inlines one node, so the count of sites is the count of nodes this rewrite
	// would add — see this file's comment.
	if len(in.aliases) > maxNodes {
		return nil, false
	}

	var splices []inlineSplice
	for _, alias := range in.aliases {
		anchor, known := in.anchors[alias.name]
		if !known {
			return nil, false
		}
		text, ok := in.anchoredText(anchor)
		if !ok {
			return nil, false
		}

		span := spanOfToken(alias.node.Start)
		if !span.IsValid() || !before(spanOfToken(anchor.node.Start).Start, span.Start) {
			// An alias written above the anchor it names. YAML does not accept it,
			// but a rewriter that splices by position must not be the thing that
			// finds out.
			return nil, false
		}
		line, from, to, ok := in.locate(span.Start, "*"+alias.name)
		if !ok {
			return nil, false
		}
		splices = append(splices, inlineSplice{
			line: line, from: from, to: to, text: text,
			change: FixChange{
				Line:    line,
				Message: fmt.Sprintf("alias `*%s` replaced with the value `&%s` names", alias.name, alias.name),
				Pending: fmt.Sprintf("alias `*%s` would be replaced with the value `&%s` names", alias.name, alias.name),
			},
		})
	}

	for _, anchor := range in.anchors {
		if _, ok := in.anchoredText(anchor); !ok {
			return nil, false
		}
		drop, ok := in.anchorDrop(anchor)
		if !ok {
			return nil, false
		}
		splices = append(splices, drop)
	}

	// In source order, because that is what [FixResult.Changes] promises a caller
	// reporting to an author — and because the anchors are collected in a map,
	// whose iteration order is not an order at all.
	slices.SortFunc(splices, func(a, b inlineSplice) int {
		if a.line != b.line {
			return a.line - b.line
		}
		return a.from - b.from
	})
	return splices, true
}

// anchoredText returns the source text of an anchor's value, when that value is
// a single-line scalar written on the anchor's own line.
//
// The text is taken from the line rather than rebuilt from the parsed value, so
// an author's own spelling — the quotes they wrote, or the absence of them — is
// what lands at the alias. Re-quoting a value is a decision, and it is one that
// changes what a plain `yes` or `0o777` means.
func (in *inliner) anchoredText(anchor *anchorSite) (string, bool) {
	if !inlinableScalar(anchor.value) {
		return "", false
	}
	tok := anchor.value.GetToken()
	span := spanOfToken(tok)
	anchorAt := spanOfToken(anchor.node.Start)
	if !span.IsValid() || !anchorAt.IsValid() {
		return "", false
	}
	if span.Start.Line != span.End.Line || span.Start.Line != anchorAt.Start.Line {
		return "", false
	}

	line := in.line(span.Start.Line)
	from, ok := byteOffsetOfColumn(line, span.Start.Column)
	if !ok {
		return "", false
	}
	to, ok := byteOffsetOfColumn(line, span.End.Column)
	if !ok || to <= from || to > len(line) {
		return "", false
	}
	text := line[from:to]
	if text != tokenText(tok) {
		// The line does not hold the value where the token says it does, which is
		// what a block or folded scalar looks like from here. Refuse rather than
		// splice bytes that were not read.
		return "", false
	}
	return text, true
}

// inlinableScalar reports whether a node is a scalar whose source text is one
// line that can stand unchanged in another value position.
//
// An allowlist, not a deny-list, because the question is which shapes are
// *provably* copyable and every answer this does not know is a refusal.
func inlinableScalar(n ast.Node) bool {
	switch n.(type) {
	case *ast.StringNode, *ast.IntegerNode, *ast.FloatNode, *ast.BoolNode, *ast.NullNode, *ast.InfinityNode, *ast.NanNode:
		return true
	default:
		// *ast.LiteralNode (`|`, `>`) in particular: its text is the lines below
		// it, indented to where it was written.
		return false
	}
}

// anchorDrop is the splice removing `&name ` from an anchor's own line, leaving
// the value it named exactly where it was.
func (in *inliner) anchorDrop(anchor *anchorSite) (inlineSplice, bool) {
	at := spanOfToken(anchor.node.Start)
	value := spanOfToken(anchor.value.GetToken())
	if !at.IsValid() || !value.IsValid() || at.Start.Line != value.Start.Line {
		return inlineSplice{}, false
	}

	line := in.line(at.Start.Line)
	from, ok := byteOffsetOfColumn(line, at.Start.Column)
	if !ok {
		return inlineSplice{}, false
	}
	to, ok := byteOffsetOfColumn(line, value.Start.Column)
	if !ok || to <= from || to > len(line) {
		return inlineSplice{}, false
	}
	// What is being deleted is `&name` and the whitespace up to the value, and
	// nothing else on the line: read back and checked, so a position that is not
	// where the file actually spells the anchor deletes nothing.
	cut := line[from:to]
	spelling := "&" + anchor.name
	if !strings.HasPrefix(cut, spelling) || strings.TrimLeft(cut[len(spelling):], " \t") != "" {
		return inlineSplice{}, false
	}

	return inlineSplice{
		line: at.Start.Line, from: from, to: to,
		change: FixChange{
			Line:    at.Start.Line,
			Message: fmt.Sprintf("anchor `&%s` removed; the value it named stays where it was written", anchor.name),
			Pending: fmt.Sprintf("anchor `&%s` would be removed; the value it named stays where it was written", anchor.name),
		},
	}, true
}

// locate returns the line and byte range holding want at a position, and reports
// whether the line really holds it there.
func (in *inliner) locate(at Position, want string) (line, from, to int, ok bool) {
	if !at.IsValid() {
		return 0, 0, 0, false
	}
	src := in.line(at.Line)
	from, ok = byteOffsetOfColumn(src, at.Column)
	if !ok {
		return 0, 0, 0, false
	}
	to = from + len(want)
	if to > len(src) || src[from:to] != want {
		return 0, 0, 0, false
	}
	return at.Line, from, to, true
}

// line returns a 1-based source line, or empty past the end.
func (in *inliner) line(n int) string {
	if n < 1 || n > len(in.lines) {
		return ""
	}
	return in.lines[n-1]
}

// applyInlineSplices writes the splices into the lines and reassembles the
// document.
//
// Splices on one line are applied from the right, so an earlier one's offsets
// still address the text they were read from. Every other byte — indentation,
// comments, line endings, the presence or absence of a final newline — is copied
// through, which is what makes the result comparable to the input byte for byte
// everywhere the rewrite did not act.
func applyInlineSplices(lines []string, splices []inlineSplice, terminator string, trailingNewline bool) []byte {
	byLine := map[int][]inlineSplice{}
	for _, splice := range splices {
		byLine[splice.line] = append(byLine[splice.line], splice)
	}

	out := make([]string, len(lines))
	copy(out, lines)
	for n, onLine := range byLine {
		line := out[n-1]
		slices.SortFunc(onLine, func(a, b inlineSplice) int { return b.from - a.from })
		for _, splice := range onLine {
			line = line[:splice.from] + splice.text + line[splice.to:]
		}
		out[n-1] = line
	}

	var b strings.Builder
	for _, line := range out {
		b.WriteString(line)
		b.WriteString(terminator)
	}
	text := b.String()
	if !trailingNewline {
		text = strings.TrimSuffix(text, terminator)
	}
	return []byte(text)
}
