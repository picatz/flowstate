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
			c.report(spanOfNode(e.key), r, "unknown key %q; %s", e.name, expectedKeys(e.name, known))
			continue
		}
		fs.index[e.name] = len(fs.list)
		fs.list = append(fs.list, e)
	}
	return fs
}

// expectedKeys says what could have been written instead, naming the nearest
// known key when there is one because a misspelling is the common case.
func expectedKeys(got string, known []string) string {
	if suggestion, ok := nearest(got, known); ok {
		return fmt.Sprintf("did you mean %q?", suggestion)
	}
	quoted := make([]string, 0, len(known))
	for _, name := range known {
		quoted = append(quoted, name)
	}
	switch len(quoted) {
	case 1:
		return fmt.Sprintf("the only key here is %s", quoted[0])
	default:
		return fmt.Sprintf("the keys here are %s, and %s",
			strings.Join(quoted[:len(quoted)-1], ", "), quoted[len(quoted)-1])
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
// live names the positions where a reserved word is already grammar, because a word
// arrives one position at a time. `vars` is a block the design places at the workflow
// level and on a step; the workflow level is built and the step level is not, so an
// author who writes it there is not making a typo and is not writing something
// permanently refused — they are one position early, and the message that helps says
// where it does work. A word absent from this map is reserved everywhere.
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
// Entries are removed as the remaining positions land — an entry here is a statement
// that the word works *somewhere and not here*, so one that outlives its own build is
// a message that sends an author to a position which no longer needs the advice.
var liveElsewhere = map[string]string{
	"vars": "the workflow level",
}
