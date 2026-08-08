package flowfile

import (
	"fmt"
	"slices"
	"strings"

	"github.com/goccy/go-yaml/ast"

	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Reading a Flowfile means reading mappings: what keys are here, which of them are
// known, and what type each one's value has to be. This is that layer — the part
// that turns "this node should be a duration" into either a duration or a
// diagnostic naming the line it is on.

// maxAliasDepth bounds how far an alias chain is followed. A cycle is refused
// before any of this runs, so reaching the limit means an unreasonably long chain
// rather than a loop.
const maxAliasDepth = 32

// A field is one key and its value within a mapping.
type field struct {
	name  string
	key   ast.Node
	value ast.Node

	// merged is true when this entry reached the mapping through a `<<:` rather
	// than being written in it.
	//
	// Carried for one reason, and it is a rewriting reason: key and value then
	// point into the *anchor*, which may be merged into any number of other
	// mappings. Replacing the source that key covers would edit the anchor,
	// which is a change to every site the anchor reaches and not to the one the
	// diagnostic names. Nothing that reads a merged entry's value cares, which
	// is why the flag says how it arrived rather than where it lives.
	merged bool
}

// A fieldSet is the fields of one mapping, addressable by name.
type fieldSet struct {
	list  []field
	index map[string]int
}

// get returns the field with the given name.
func (fs *fieldSet) get(name string) (field, bool) {
	if fs == nil {
		return field{}, false
	}
	i, ok := fs.index[name]
	if !ok {
		return field{}, false
	}
	return fs.list[i], true
}

// fields reads a mapping and reports any key that is not one of known.
func (c *compiler) fields(n ast.Node, path string, r ref, known []string) (*fieldSet, bool) {
	entries, ok := c.entries(n, path, r)
	if !ok {
		return nil, false
	}
	return c.check(entries, r, known), true
}

// check reports any entry whose key is not one of known, and returns the rest.
func (c *compiler) check(entries []entry, r ref, known []string) *fieldSet {
	fs := &fieldSet{index: make(map[string]int, len(entries))}
	for _, e := range entries {
		if !slices.Contains(known, e.name) {
			// A key that used to be grammar is a different mistake from one that never
			// was, and gets a different sentence. "unknown key" with a nearest-match
			// suggestion describes a typo an author did not make, and sends them
			// looking for one — while the thing they actually need is the new spelling
			// and the command that writes it for them.
			if advice, retired := retiredKeys[e.name]; retired && slices.Contains(known, advice.now) {
				c.report(spanOfNode(e.key), r, "%s", advice.message(e.name))

				continue
			}
			c.reportWith(spanOfNode(e.key), r, c.renameKeyEdit(e, entries, known),
				"unknown key %q; %s", e.name, expectedKeys(e.name, known))
			continue
		}
		fs.index[e.name] = len(fs.list)
		fs.list = append(fs.list, e)
	}
	return fs
}

// renameKeyEdit offers the rename an unknown key's nearest match describes, or
// nothing when this key is not one a rewriter may safely touch.
//
// The suggestion itself is [expectedKeys]'s: the same nearest match the sentence
// names, so the edit and the prose cannot say different things. What this adds is
// the judgement the schema's edits field demands of whoever fills it, which for a
// key rename is three questions, all of them about the *source* rather than about
// the suggestion:
//
//   - Would the rename write a key the mapping already has? Then applying it
//     turns one problem into a duplicate key, which YAML resolves by silently
//     dropping one of them. A file that loses a value it wrote is a worse
//     outcome than the diagnostic it started with.
//   - Did the entry arrive through a `<<:`? Then its key lives in the anchor,
//     and replacing that source edits every mapping the anchor is merged into
//     rather than the one being reported. See [field.merged].
//   - Does the key's source sit inside any `&anchor`'s value? Every `*alias`
//     of that anchor reads the same text, so the rename that repairs the
//     reporting site rewrites every reading site, and an alias can sit in a
//     context where the key being renamed away is legal. The merge-key flag
//     cannot see this case: a mapping written whole as an anchor and read
//     through a bare alias arrives here with `merged` false, because
//     [compiler.resolve] unwraps the anchor before the entries are built.
//   - Is the source the bare name? A quoted key's span covers the quotes, so
//     replacing it with a bare name deletes them, and a key that needed quoting
//     stops being the key it was. Rather than reason about which quoting styles
//     survive, the edit is offered only where the text and the name are the same
//     string, which is the overwhelmingly common spelling and the one that needs
//     no reasoning at all.
//
// Nothing here checks whether the *suggestion* is a sensible thing to write,
// because a key is a key: a grammar mapping's keys bind no names in CEL, which is
// what keeps this clear of the four bare names the two `flow fix` corruptions
// turned on (CLAUDE.md, "A rewriter has to know what the grammar binds").
func (c *compiler) renameKeyEdit(e entry, entries []entry, known []string) []*v1.SuggestedEdit {
	suggestion, ok := nearest(e.name, known)
	if !ok {
		return nil
	}
	if e.merged {
		return nil
	}
	if c.inAnchoredSource(spanOfNode(e.key)) {
		return nil
	}
	for _, other := range entries {
		if other.name == suggestion {
			return nil
		}
	}
	if tokenText(nodeToken(e.key)) != e.name {
		return nil
	}

	edit := replaceSpan(fmt.Sprintf("rename to `%s`", suggestion), spanOfNode(e.key), suggestion)
	if edit == nil {
		return nil
	}
	return []*v1.SuggestedEdit{edit}
}

// inAnchoredSource reports whether a span lies inside the value of any anchor
// in the document.
//
// [compiler.anchors] is filled by a collection pass before any mapping is
// checked, so by the time a diagnostic is being built the set is complete. The
// comparison is by position: anchors share the one source text with everything
// else, so a key whose span sits inside an anchor value's span is a key whose
// bytes an alias elsewhere reads.
func (c *compiler) inAnchoredSource(span Span) bool {
	if !span.IsValid() {
		return true
	}
	for _, target := range c.anchors {
		enclosing := spanOfNode(target)
		if !enclosing.IsValid() || !enclosing.End.IsValid() {
			continue
		}
		if positionBefore(span.Start, enclosing.Start) {
			continue
		}
		if positionBefore(enclosing.End, span.End) {
			continue
		}
		return true
	}
	return false
}

// positionBefore reports whether a comes strictly before b in the document,
// comparing the line first and the column within it.
func positionBefore(a, b Position) bool {
	if a.Line != b.Line {
		return a.Line < b.Line
	}
	return a.Column < b.Column
}

// expectedKeys says what could have been written instead, naming the nearest
// known key when there is one because a misspelling is the common case.
func expectedKeys(got string, known []string) string {
	if suggestion, ok := nearest(got, known); ok {
		return fmt.Sprintf("did you mean %q?", suggestion)
	}
	switch len(known) {
	case 1:
		return fmt.Sprintf("the only key here is %s", known[0])
	default:
		return fmt.Sprintf("the keys here are %s, and %s",
			strings.Join(known[:len(known)-1], ", "), known[len(known)-1])
	}
}

// nearest returns the known name closest to got, when one is close enough to be
// worth suggesting.
func nearest(got string, known []string) (string, bool) {
	best, bestDistance := "", 0
	for _, name := range known {
		distance := editDistance(got, name)
		// A suggestion is only helpful when it is plausibly what was meant: at
		// most a third of the name wrong, and never more than two edits.
		limit := min(len(name)/3+1, 2)
		if distance > limit {
			continue
		}
		if best == "" || distance < bestDistance {
			best, bestDistance = name, distance
		}
	}
	return best, best != ""
}

// editDistance returns the Levenshtein distance between two names.
func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = min(prev[j]+1, curr[j-1]+1, prev[j-1]+cost)
		}
		prev, curr = curr, prev
	}
	return prev[len(b)]
}

// An entry is one key and value of a mapping, after merge keys have been expanded
// and aliases resolved.
type entry = field

// entries returns the entries of a mapping.
func (c *compiler) entries(n ast.Node, path string, r ref) ([]entry, bool) {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil, false
	}

	var values []*ast.MappingValueNode
	switch node := n.(type) {
	case *ast.MappingNode:
		values = node.Values
	case *ast.MappingValueNode:
		values = []*ast.MappingValueNode{node}
	default:
		c.report(spanOfNode(n), r, "must be a mapping of keys to values, but %s was written here", describeNode(n))
		return nil, false
	}

	// Keys written here are claimed before anything is merged in, because a merged
	// mapping must not shadow one — that is the merge semantics YAML defines, and
	// the reason `<<:` is usable for sharing step boilerplate at all.
	// Which names the mapping writes for itself, so a merged key does not
	// override one written directly.
	//
	// Silent, because this is a first pass over keys the second pass reads again:
	// reporting here made a bad key produce two identical diagnostics at the same
	// position, one from each pass. An editor draws that as two squiggles on one
	// character.
	written := make(map[string]bool, len(values))
	for _, v := range values {
		if _, isMerge := v.Key.(*ast.MergeKeyNode); isMerge {
			continue
		}
		if name, ok := keyNameOf(v.Key); ok {
			written[name] = true
		}
	}

	var (
		out  []entry
		seen = make(map[string]bool, len(values))
	)
	for _, v := range values {
		if _, isMerge := v.Key.(*ast.MergeKeyNode); isMerge {
			merged, ok := c.entries(v.Value, path, r)
			if !ok {
				return nil, false
			}

			// Merged entries are counted against the document's value budget, which
			// nothing here used to do.
			//
			// The bound that exists is [compiler.enter]'s, and it counts values the
			// compiler *descends into*. Merging does not descend: one anchored
			// mapping of N keys merged into D steps is N×D entries produced from a
			// file of size N+D, so the work is quadratic in something the document
			// does not have to be large to express. Measured, a 43 KiB file of that
			// shape took 22 seconds and never reached the limit, because the limit
			// counts steps.
			//
			// This is the shape the bound was written for — an alias may be
			// referenced many times, so a short document expands into an enormous
			// one — and it was being enforced on the walk rather than on the
			// expansion. Counting here closes that, and matters most for the language
			// server, which runs this on whatever an editor opens.
			c.nodes += len(merged)
			if c.nodes > maxNodes {
				if !c.overflowed {
					c.overflowed = true
					c.report(spanOfToken(nodeToken(v.Key)), r,
						"holds more than %d values once aliases are expanded, which is more than a Flowfile is meant to hold", maxNodes)
				}
				return nil, false
			}

			for _, e := range merged {
				if written[e.name] || seen[e.name] {
					continue
				}
				seen[e.name] = true
				e.merged = true
				out = append(out, e)
			}
			continue
		}
		name, ok := c.keyName(v.Key, r)
		if !ok {
			return nil, false
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, entry{name: name, key: v.Key, value: v.Value})
	}
	return out, true
}

// keyNameOf returns the name a mapping key spells, or false when it spells none.
//
// The single definition of what a key is. [compiler.keyName] is this plus a
// diagnostic, rather than a second switch, because the two answering differently
// is a bug with no symptom at the point it is written: the merge-precedence pass
// in [compiler.entries] uses this one to decide which names a mapping claims for
// itself, and a name it fails to see is a name a merged mapping silently
// overrides.
//
// A key is a string and nothing else. It is never a *ast.LiteralNode — a block
// scalar cannot open a mapping key, so every `? |-` reaches the parser wrapped in
// a *ast.MappingKeyNode — and the branch that used to claim otherwise made these
// two look divergent while being unreachable.
func keyNameOf(n ast.Node) (string, bool) {
	scalar, ok := n.(*ast.StringNode)
	if !ok {
		return "", false
	}
	return scalar.Value, true
}

// keyName returns the name a mapping key spells.
func (c *compiler) keyName(n ast.Node, r ref) (string, bool) {
	if name, ok := keyNameOf(n); ok {
		return name, true
	}
	if _, explicit := n.(*ast.MappingKeyNode); explicit {
		// Valid YAML, and every spelling of it — `? a`, `? |-` — arrives here. The
		// generic message would call it "a mappingkey", which is the parser's word
		// and not the author's, and would say keys must be strings about a key that
		// is one.
		c.report(spanOfNode(n), r,
			"an explicit key (`? key` on its own line) is not written here; use `key: value`")
		return "", false
	}
	c.report(spanOfNode(n), r, "keys must be strings, but %s was written here", describeNode(n))
	return "", false
}

// resolve follows anchors and aliases to the node that was actually written.
func (c *compiler) resolve(n ast.Node, path string, r ref) ast.Node {
	for depth := 0; ; depth++ {
		if depth > maxAliasDepth {
			c.report(spanOfNode(n), r, "alias refers to itself, directly or through another alias")
			return nil
		}
		switch node := n.(type) {
		case nil:
			return nil
		case *ast.AnchorNode:
			n = node.Value
		case *ast.AliasNode:
			name := node.Value.String()
			target, ok := c.anchors[name]
			if !ok {
				c.report(spanOfNode(n), r,
					"unknown alias *%s; an alias must name an anchor written earlier as &%s", name, name)
				return nil
			}
			n = target
		default:
			return n
		}
	}
}

// resolveQuiet follows anchors and aliases without reporting a problem, for the
// passes that only need to look at a value's shape.
func (c *compiler) resolveQuiet(n ast.Node) ast.Node {
	for depth := 0; depth <= maxAliasDepth; depth++ {
		switch node := n.(type) {
		case nil:
			return nil
		case *ast.AnchorNode:
			n = node.Value
		case *ast.AliasNode:
			target, ok := c.anchors[node.Value.String()]
			if !ok {
				return n
			}
			n = target
		default:
			return n
		}
	}
	return nil
}

// text reads a value that the schema types as a plain string.
//
// An expression is refused rather than accepted as the literal text of one: a task
// name of "${steps.a.name}" is not a task, and quietly compiling it into one that
// does not exist trades a compile error for a run-time one.
func (c *compiler) text(n ast.Node, path string, r ref) (string, bool) {
	n = c.resolve(n, path, r)
	if n == nil {
		return "", false
	}
	c.pos.record(path, spanOfNode(n))

	var value string
	switch node := n.(type) {
	case *ast.StringNode:
		value = node.Value
	case *ast.LiteralNode:
		value = blockText(node)
	default:
		c.report(spanOfNode(n), r, "must be a string, but %s was written here", describeNode(n))
		return "", false
	}

	if _, fenced := SplitFence(value); fenced {
		c.report(spanOfNode(n), r,
			"cannot be an expression; it is read when the workflow is compiled, so write the value out")
		return "", false
	}
	if err := fenceError(value); err != nil {
		c.report(spanOfNode(n), r, "%s", err)
		return "", false
	}
	return value, true
}

// duration reads a duration, written the way durations are written everywhere else
// in Go tooling.
func (c *compiler) duration(n ast.Node, path string, r ref) (*durationpb.Duration, bool) {
	n = c.resolve(n, path, r)
	if n == nil {
		return nil, false
	}
	c.pos.record(path, spanOfNode(n))

	text, ok := n.(*ast.StringNode)
	if !ok {
		c.report(spanOfNode(n), r,
			"must be a duration written as a string, like 30s, 5m, 1h, or 7d, but %s was written here", describeNode(n))
		return nil, false
	}

	d, err := parseDuration(text.Value)
	if err != nil {
		c.report(spanOfNode(n), r, "%q is not a duration; write it as 30s, 5m, 1h, or 7d", text.Value)
		return nil, false
	}
	if d <= 0 {
		c.report(spanOfNode(n), r, "%q must be greater than zero; remove it to leave the step unbounded", text.Value)
		return nil, false
	}
	return durationpb.New(d), true
}

// integer reads a whole number within the range the schema allows.
func (c *compiler) integer(n ast.Node, path string, r ref, low, high int64) (int32, bool) {
	n = c.resolve(n, path, r)
	if n == nil {
		return 0, false
	}
	c.pos.record(path, spanOfNode(n))

	number, ok := n.(*ast.IntegerNode)
	if !ok {
		c.report(spanOfNode(n), r, "must be a whole number, but %s was written here", describeNode(n))
		return 0, false
	}

	var value int64
	switch v := number.Value.(type) {
	case int64:
		value = v
	case uint64:
		if v > uint64(high) {
			c.report(spanOfNode(n), r, "must be between %d and %d", low, high)
			return 0, false
		}
		value = int64(v)
	}
	if value < low || value > high {
		c.report(spanOfNode(n), r, "must be between %d and %d, but is %d", low, high, value)
		return 0, false
	}
	return int32(value), true
}

// unsignedWhole reads a non-negative whole number, for the schema's uint64
// bound fields — min_len, max_len, min_items, max_items — where a fraction or
// a negative count is a mistake worth naming rather than truncating.
func (c *compiler) unsignedWhole(n ast.Node, path string, r ref) (uint64, bool) {
	n = c.resolve(n, path, r)
	if n == nil {
		return 0, false
	}
	c.pos.record(path, spanOfNode(n))

	number, ok := n.(*ast.IntegerNode)
	if !ok {
		c.report(spanOfNode(n), r, "must be a whole number, but %s was written here", describeNode(n))
		return 0, false
	}

	switch v := number.Value.(type) {
	case uint64:
		return v, true
	case int64:
		if v < 0 {
			c.report(spanOfNode(n), r, "must not be negative, but %d was written here", v)
			return 0, false
		}
		return uint64(v), true
	}
	return 0, false
}

// number reads a value the schema types as a floating-point number, accepting the
// whole numbers YAML would otherwise give a different type.
func (c *compiler) number(n ast.Node, path string, r ref) (float64, bool) {
	n = c.resolve(n, path, r)
	if n == nil {
		return 0, false
	}
	c.pos.record(path, spanOfNode(n))

	switch node := n.(type) {
	case *ast.FloatNode:
		return node.Value, true
	case *ast.IntegerNode:
		switch v := node.Value.(type) {
		case int64:
			return float64(v), true
		case uint64:
			return float64(v), true
		}
	}
	c.report(spanOfNode(n), r, "must be a number, but %s was written here", describeNode(n))
	return 0, false
}

// boolean reads a true or false.
func (c *compiler) boolean(n ast.Node, path string, r ref) (bool, bool) {
	n = c.resolve(n, path, r)
	if n == nil {
		return false, false
	}
	c.pos.record(path, spanOfNode(n))

	value, ok := n.(*ast.BoolNode)
	if !ok {
		c.report(spanOfNode(n), r, "must be true or false, but %s was written here", describeNode(n))
		return false, false
	}
	return value.Value, true
}

// heldForLater reports any entry whose key is a word the grammar is holding for a
// version that does not exist yet, and returns the rest.
//
// Held back from the key check for the same reason a retired spelling is: the
// generic check offers the nearest known key and then lists the others, which for
// a reserved word describes a misspelling the author did not make and sends them
// looking for a typo that is not there.
//
// It runs wherever a key can be written, not only on a step. Reporting a word as
// reserved in one position and as an unknown key one line further out would be the
// tool disagreeing with itself about the same word, which is worse than either
// message alone.
//
// [liveElsewhere] names the positions where a reserved word is already grammar,
// because a word arrives one position at a time. A block the design places at two
// positions may have one of them built and not the other, and an author who writes it
// at the unbuilt one is neither making a typo nor writing something permanently
// refused — they are one position early, and the message that helps says where it does
// work. A word absent from that map is reserved everywhere.
func (c *compiler) heldForLater(entries []entry, r ref, available []string) []entry {
	kept := make([]entry, 0, len(entries))
	for _, e := range entries {
		if !v1.IsFutureStepKey(e.name) || slices.Contains(available, e.name) {
			kept = append(kept, e)

			continue
		}

		if where, live := liveElsewhere[e.name]; live {
			c.report(spanOfNode(e.key), r,
				"`%s:` is not built here yet; write it at %s, where this build reads it",
				e.name, where)

			continue
		}

		c.report(spanOfNode(e.key), r,
			"`%s:` is reserved for a later version of the grammar; nothing in this build reads it, "+
				"so writing it here does nothing", e.name)
	}
	return kept
}

// liveElsewhere maps a reserved word to the position where this build already reads
// it, for the diagnostic above.
//
// Empty today, which is the resting state rather than an oversight. An entry says a
// word works *somewhere and not here*, and that is only true while a block is arriving
// one position at a time. `vars` was the entry and is now grammar at both of its
// positions, so the advice it carried would send an author to a level that no longer
// needs it.
//
// Entries are therefore added when a multi-position block lands its first position and
// removed when it lands its last. TestALiveElsewhereEntryIsTrue keeps the map honest in
// between: an entry whose word is a future key nowhere is advice about a position that
// does not exist.
var liveElsewhere = map[string]string{}

// retiredKeys are spellings the grammar has replaced, and what replaced them.
//
// Guarded by the *position*, not only the word: an entry fires where its replacement is
// a key, so `iterator:` written on a step rather than inside a `for_each` still reads as
// an unknown key there — which it is. A table keyed on the word alone would answer "run
// `flow fix`" for a file `flow fix` will not touch, which is the one answer worse than
// no answer.
//
// An entry lives for exactly one edition. The grammar carries one spelling at a time and
// `flow fix` rewrites the other, so what an entry buys is a good sentence for a file
// written before the sweep — not a second spelling the parser accepts.
var retiredKeys = map[string]retiredKey{
	"iterator": {
		now:  "as",
		note: "it names the binding rather than the mechanism, and reads as the sentence it is: *for each item as name*",
	},
}

// A retiredKey is one replaced spelling.
type retiredKey struct {
	// now is the key that replaced it, and the position guard: the advice is only
	// given where this is a legal key.
	now string

	// note says why, in one clause. Worth carrying because a rename with no reason
	// reads as churn, and an author who understands the reason writes the new
	// spelling from memory next time instead of looking it up.
	note string
}

// message renders the advice for one retired key.
func (k retiredKey) message(was string) string {
	return fmt.Sprintf("`%s:` is now `%s:` — %s; run `flow fix` to rewrite this file", was, k.now, k.note)
}
