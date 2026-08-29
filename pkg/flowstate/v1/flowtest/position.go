package flowtest

import (
	"strconv"
	"strings"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/token"
)

// Where a `*.test.yaml` wrote the thing a diagnostic is about (#923 step 1).
//
// The loader decodes into the Go structs on this file's other side, and a
// decode throws the source away: by the time a check can see that a stub names
// neither a task nor a step, the line it was written on is gone. So the
// document tree is kept beside the decoded value, and a diagnostic asks it
// where a path was written — the same thing `flowfile`'s compiler does while it
// builds a workflow (see that package's position.go and [flowfile.Positions]),
// with two differences worth stating because they are decisions rather than
// omissions:
//
//   - It resolves a path on demand instead of recording every node's span on
//     the way past. Nothing here compiles the tree — the decoder does — so
//     there is no pass to hang a recording on, and an index over a document
//     bounded only by [MaxTestFileBytes] is memory an outside party sizes. A
//     lookup costs one bounded walk, and at most [MaxLoadProblems] of them
//     happen.
//   - A path is a list of steps rather than a rendered string. `flowfile`'s own
//     doc records the cost of the string form: "a key containing a dot or a
//     bracket makes its path ambiguous". A `secrets:` key is `vault:prod/db`
//     and an `allow_unreached` key is `on_event:case[0]`, so here the ambiguous
//     form would be the common one.
//
// This is the same *algorithm* as flowfile's — resolve anchors and aliases,
// honour `<<:` merge precedence, and take a node's position from its earliest
// token — against machinery this package owns, for the reason [bounds.go]
// already states about the expansion walk: that machinery is unexported over
// there, tied to the workflow grammar, and copying it without being able to run
// it against the tests it is proven by would risk a subtly different answer that
// only looks like the real one.

// These three are second walls, and it is worth saying plainly that no test
// reaches them: [checkExpansionBounds] has already refused anything that could,
// and it refuses first because the loader runs it before the decode. A document
// whose alias chain is long enough to exhaust [maxAliasDepth] costs about two
// levels of expansion depth per hop, so it passes [maxExpansionDepth] only up
// to roughly half of this bound; the same arithmetic covers the other two. They
// exist so that this walk terminates on its own terms rather than on another
// function's — a bound that lives only in the caller is a bound that moves when
// somebody edits the caller — and reaching one degrades to "position not known",
// which is the honest answer and never a wrong line.
const (
	// maxLookupSteps bounds the work one position lookup may do. A lookup
	// follows aliases and expands merge keys, which is the same breadth an
	// expansion bomb multiplies.
	maxLookupSteps = 100_000

	// maxAliasDepth bounds how far one alias chain is followed while resolving
	// a node, matching flowfile's own bound of the same name. A cycle
	// (`a: &x {b: *x}`) is what this stops: every pass through it increases
	// depth, so it ends in a few dozen steps rather than by exhausting the
	// stack.
	maxAliasDepth = 32

	// maxMergeDepth bounds how deep a chain of `<<:` merges is followed
	// looking for one key, for the same reason.
	maxMergeDepth = 32
)

// A position is one point in a test file's source.
//
// Line and column are 1-based and the column counts characters rather than
// bytes, which is what the YAML parser reports and what
// [flowfile.Position] documents for the same value.
type position struct {
	line   int
	column int
}

// valid reports whether the position names a line.
func (p position) valid() bool { return p.line > 0 }

// earlier reports whether a comes before b in the source.
func earlier(a, b position) bool {
	if a.line != b.line {
		return a.line < b.line
	}
	return a.column < b.column
}

// A pathStep is one hop into a document: a mapping key, or an index into a
// sequence.
type pathStep struct {
	name    string
	index   int
	indexed bool
}

// A loc addresses one value in a test document by the keys and indices an
// author wrote, so a diagnostic can be positioned at the thing it is about.
//
// The zero value is the document itself. A nil loc is "nowhere in particular",
// which is what a check passes when the value it refuses was not written in
// this document at all — see [problems.record], where that is the whole rule.
type loc []pathStep

// at starts a path at a top-level key.
func at(name string) loc { return loc(nil).field(name) }

// field extends a path with a mapping key.
func (l loc) field(name string) loc { return l.push(pathStep{name: name}) }

// item extends a path with a sequence index.
func (l loc) item(i int) loc { return l.push(pathStep{index: i, indexed: true}) }

// push returns l with one more step, always over a fresh slice: two paths
// grown from one parent must not write into each other's backing array.
func (l loc) push(s pathStep) loc {
	out := make(loc, len(l)+1)
	copy(out, l)
	out[len(l)] = s

	return out
}

// String renders the path the way this package's prose already names one —
// `tests[0].stubs[1].returns` — for [Diagnostic.Field].
func (l loc) String() string {
	var b strings.Builder
	for _, s := range l {
		if s.indexed {
			b.WriteString("[")
			b.WriteString(strconv.Itoa(s.index))
			b.WriteString("]")

			continue
		}
		if b.Len() > 0 {
			b.WriteString(".")
		}
		b.WriteString(s.name)
	}

	return b.String()
}

// A document is the parsed source a decoded [File] came from, kept only to
// answer where something was written.
//
// A nil *document answers every question with "not known", so a caller that
// has no source — the Go door, which builds a [File] rather than parsing one —
// needs no special case.
type document struct {
	body    ast.Node
	anchors map[string]ast.Node
}

// newDocument holds the first document in a parsed file, plus every anchor any
// document in it declares.
//
// The first body is the one the decoder reads, so it is the one whose
// positions describe the decoded value. Anchors are collected across all of
// them because that is what [collectAnchors] and the expansion bound already
// do, and an alias resolving to nothing simply reports no position.
func newDocument(file *ast.File) *document {
	if file == nil {
		return nil
	}

	d := &document{anchors: map[string]ast.Node{}}
	for _, doc := range file.Docs {
		if doc == nil || doc.Body == nil {
			continue
		}
		collectAnchors(doc.Body, d.anchors)
		if d.body == nil {
			d.body = doc.Body
		}
	}

	return d
}

// positionOf returns where the value at path was written.
//
// Exact or nothing: a path this document did not write reports nothing rather
// than falling back to an enclosing node. That is the rule that keeps a
// position honest in a format where values are inherited — a case's stub can
// come from `defaults:`, a row's `trigger:` from its entry, a suite's defaults
// from a sibling `testdefaults.yaml` — and an enclosing-node fallback would
// underline a case that is correct because something it inherited is not.
// CLAUDE.md's standard is that a false diagnostic is worse than an unplaced
// one; here it would be a false *position* on a true diagnostic, which sends an
// author to correct working text.
func (d *document) positionOf(path loc) (position, bool) {
	_, value, ok := d.find(path)
	if !ok {
		return position{}, false
	}
	budget := maxLookupSteps

	return startOf(d.resolve(value, &budget))
}

// positionOfKey is [document.positionOf] pointing at the key rather than the
// value, for a diagnostic whose subject is the name an author wrote: a var
// whose name CEL could never read back, an `allow_unreached` entry with no
// reason, a `secrets:` reference that does not parse.
func (d *document) positionOfKey(path loc) (position, bool) {
	key, _, ok := d.find(path)
	if !ok || key == nil {
		return position{}, false
	}

	return startOf(key)
}

// find walks path from the document body, returning the key node and the value
// node of its last step.
//
// The key is nil where the last step is a sequence index, which has no key.
func (d *document) find(path loc) (key, value ast.Node, ok bool) {
	if d == nil || d.body == nil || len(path) == 0 {
		return nil, nil, false
	}

	budget := maxLookupSteps
	node := d.body
	for _, step := range path {
		node = d.resolve(node, &budget)
		if node == nil {
			return nil, nil, false
		}
		if step.indexed {
			seq, isSequence := node.(*ast.SequenceNode)
			if !isSequence || step.index < 0 || step.index >= len(seq.Values) {
				return nil, nil, false
			}
			key, node = nil, seq.Values[step.index]

			continue
		}
		k, v, found := d.entry(node, step.name, &budget, 0)
		if !found {
			return nil, nil, false
		}
		key, node = k, v
	}

	return key, node, true
}

// entry finds one key of a mapping, honouring YAML's merge precedence: a key
// the mapping writes for itself is not shadowed by one a `<<:` merges in, which
// is the same order [flowfile]'s compiler reads entries in.
//
// With this decoder that precedence decides nothing, and the order is written
// out anyway. goccy refuses a mapping that writes a key its `<<:` also supplies
// — `duplicate key "name"` — so the document where the two orders would differ
// never reaches a check, let alone a position. The order is here because this
// walk has to answer the question the decoder answered, and "the decoder
// happens to refuse the disagreement" is a property of a dependency rather than
// of the format.
func (d *document) entry(n ast.Node, name string, budget *int, depth int) (key, value ast.Node, ok bool) {
	if depth > maxMergeDepth {
		return nil, nil, false
	}

	var values []*ast.MappingValueNode
	switch node := n.(type) {
	case *ast.MappingNode:
		values = node.Values
	case *ast.MappingValueNode:
		values = []*ast.MappingValueNode{node}
	default:
		return nil, nil, false
	}

	for _, v := range values {
		*budget--
		if *budget <= 0 {
			return nil, nil, false
		}
		if _, merged := v.Key.(*ast.MergeKeyNode); merged {
			continue
		}
		if written, named := keyName(v.Key); named && written == name {
			return v.Key, v.Value, true
		}
	}

	for _, v := range values {
		if _, merged := v.Key.(*ast.MergeKeyNode); !merged {
			continue
		}
		// A merge value is one mapping or a sequence of them, tried in the
		// order written — YAML's own rule, and the order the decoder applied
		// when it built the value this position is being asked about.
		source := d.resolve(v.Value, budget)
		if sequence, isSequence := source.(*ast.SequenceNode); isSequence {
			for _, element := range sequence.Values {
				if k, val, found := d.entry(d.resolve(element, budget), name, budget, depth+1); found {
					return k, val, true
				}
			}

			continue
		}
		if k, val, found := d.entry(source, name, budget, depth+1); found {
			return k, val, true
		}
	}

	return nil, nil, false
}

// keyName returns the name a mapping key spells, or false when it spells none.
// A key is a string and nothing else here: an explicit `? key` is legal YAML
// this loader has no position vocabulary for, and reports none rather than
// guessing.
func keyName(n ast.Node) (string, bool) {
	scalar, isString := n.(*ast.StringNode)
	if !isString {
		return "", false
	}

	return scalar.Value, true
}

// resolve follows anchors and aliases to the node that was actually written.
//
// An alias that names nothing this document anchored resolves to itself, so the
// position reported is where the alias sits — which is where the author has to
// look, and is also what the decoder will refuse a moment later.
func (d *document) resolve(n ast.Node, budget *int) ast.Node {
	for depth := 0; depth <= maxAliasDepth; depth++ {
		*budget--
		if *budget <= 0 {
			return nil
		}
		switch node := n.(type) {
		case nil:
			return nil
		case *ast.AnchorNode:
			n = node.Value
		case *ast.AliasNode:
			target, anchored := d.anchors[node.Value.String()]
			if !anchored {
				return n
			}
			n = target
		default:
			return n
		}
	}

	return nil
}

// startOf returns the earliest source position anything in a node's subtree was
// written at: the first key of a mapping, the dash of a sequence, the scalar
// itself.
//
// The earliest rather than the node's own token, because a block mapping's own
// token is its first *value's* separator — pointing a diagnostic about a case
// at the case's first key is what a reader expects, and is what
// `flowfile`'s spanOfNode answers for the same shapes.
func startOf(n ast.Node) (position, bool) {
	var out position
	eachToken(n, func(tok *token.Token) {
		if tok == nil || tok.Position == nil {
			return
		}
		p := position{line: tok.Position.Line, column: tok.Position.Column}
		if !p.valid() {
			return
		}
		if !out.valid() || earlier(p, out) {
			out = p
		}
	})

	return out, out.valid()
}

// eachToken calls fn for every token in a node's subtree.
//
// Aliases are not followed: an alias is written where it appears, and its
// position is the alias rather than the anchor it names. The recursion is
// bounded by the document's own nesting, which [checkExpansionBounds] has
// already refused past [maxExpansionDepth] by the time anything here runs.
func eachToken(n ast.Node, fn func(*token.Token)) {
	if n == nil {
		return
	}
	switch node := n.(type) {
	case *ast.MappingNode:
		for _, v := range node.Values {
			eachToken(v, fn)
		}
	case *ast.MappingValueNode:
		eachToken(node.Key, fn)
		eachToken(node.Value, fn)
	case *ast.SequenceNode:
		fn(node.Start)
		for _, v := range node.Values {
			eachToken(v, fn)
		}
	case *ast.AnchorNode:
		fn(node.Start)
		eachToken(node.Name, fn)
		eachToken(node.Value, fn)
	case *ast.TagNode:
		fn(node.Start)
		eachToken(node.Value, fn)
	case *ast.LiteralNode:
		fn(node.Start)
		eachToken(node.Value, fn)
	default:
		fn(n.GetToken())
	}
}
