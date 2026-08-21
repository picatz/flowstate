package flowfile

import (
	"fmt"
	"slices"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
)

// Reading the `digest:` pins a file wrote, from the file rather than from what
// it compiles to.
//
// A pin is not part of [v1.Workflow] — [v1.Call.SourceDigest] is set whether or
// not an author wrote one — so anything that wants to know which calls were
// pinned has to read the source. Two things do, and they must agree, which is
// why one collector serves both:
//
//   - `flow fmt` carries a pin across the rewrite (see [Format]), because a
//     formatter that drops one silently turns off a security check.
//   - `flow fix` reports a pin its own run invalidated, because a run that
//     rewrites a callee's bytes makes every pin on that callee stale, and the
//     tool that caused the staleness is the one that should say so (#640).
//
// # Reading a pin the way the compiler reads one
//
// The keys of a mapping are not only the keys written in it. A step may reach
// `call:` and `digest:` through a `<<:` merge key, or be written whole as an
// `&anchor` and reused through an `*alias` — all of which fields.go resolves
// before the compiler ever sees a step, and all of which a collector reading
// only a mapping's own written keys is blind to. That blindness was #639's
// documented gap: a pin merged in through `<<: *shared` formatted as though it
// had never been written, silently, which is the same security check going
// missing that #339 fixed for the ordinary spelling. So this resolves anchors,
// aliases and merge keys the way [compiler.entries] does — written keys claim a
// name before anything merged can, and the first merge to offer a name wins —
// rather than knowing less about the grammar than the language does.
//
// # What bounds it
//
// Resolving an alias is expansion, and expansion is the thing a short document
// can multiply. One anchored mapping merged into many siblings is breadth the
// bytes do not spell out, so a walk that follows merges is bounded by the same
// total node count the compiler bounds its own expansion by ([maxNodes]) rather
// than by depth alone — a depth bound does not stop a breadth explosion.
//
// An alias that names no anchor, or a merge whose value is not a mapping, stops
// the walk there rather than refusing the document. Neither shape compiles: the
// compiler reports an unknown alias and refuses a merge that is not a mapping,
// so [Format] — whose contract is that its source already compiled — never sees
// one, while [CallPins] deliberately runs over files that do *not* compile yet
// (that is what `flow fix` is for) and must report what it can see rather than
// fail on the first thing it cannot.

// A CallPin is one `digest:` pin written in a Flowfile, with the call it pins.
//
// Read from source, so Call and Digest are exactly the text the author wrote —
// the target as written rather than as resolved, and the digest verbatim rather
// than lower-cased — because the two things that read this either write the text
// back out unchanged or quote it back to the author.
type CallPin struct {
	// Step is the `id:` of the step holding the call, empty when it wrote none.
	Step string

	// Call is the `call:` target as written, relative to the file that wrote it.
	Call string

	// Digest is the pin itself, verbatim.
	Digest string

	// Line and Column are where the `digest:` key sits, 1-based. For a pin that
	// reaches a step through a `<<:` they are the position inside the anchor,
	// which is where the text a reader has to change actually lives.
	Line, Column int
}

// CallPins reports every `digest:` pin in source, in the order they appear.
//
// It reads a document that need not compile: `flow fix` runs over files written
// in an older edition, and a pin is legible in one of those exactly as it is in
// a current one. An error means the document could not be read at all — it is
// not YAML, or it expands past what a Flowfile may hold — never that some part
// of it was unfamiliar.
func CallPins(source []byte) ([]CallPin, error) {
	pins, err := sourcePins(source)
	if err != nil {
		return nil, err
	}

	out := make([]CallPin, 0, len(pins))
	for _, pin := range pins {
		out = append(out, CallPin{
			Step:   pin.step,
			Call:   pin.call,
			Digest: pin.text,
			Line:   pin.line,
			Column: pin.column,
		})
	}

	// In source order, because the map they came out of has none, and a report
	// listing the same pins in a different order each run is a report nothing
	// can be diffed against.
	slices.SortStableFunc(out, func(a, b CallPin) int {
		if a.Line != b.Line {
			return a.Line - b.Line
		}
		return a.Column - b.Column
	})
	return out, nil
}

// A sourcePin is a `digest:` value found beside a `call:` in source, along
// with what it pins and where it was written — for a diagnostic if it cannot be
// carried across, and for the staleness report `flow fix` builds from it.
type sourcePin struct {
	text         string
	call         string
	step         string
	line, column int
}

// sourcePins collects every `digest:` pin in a document, keyed by the path of
// the mapping it sits in — the same path a comment on that mapping's own
// container gets, since a pin and a container comment are both properties of
// the mapping rather than of one entry in it.
//
// Only a mapping that also has a `call:` key is recorded. The grammar refuses
// `digest:` anywhere else (parse.go), so this is not a filter this package
// invents; it means a hand-built AST fed to [Format] (which, unlike a parsed
// Flowfile, its exported contract does not get to assume is well-formed) cannot
// make it write a pin that names nothing.
func sourcePins(source []byte) (map[string]sourcePin, error) {
	file, err := parser.ParseBytes(source, parser.ParseComments)
	if err != nil {
		// The caller compiled this source, so it parses. Refusing rather than
		// carrying on is the fail-closed reading, the same one sourceComments
		// takes: unable to see the pins is not the same as knowing there are
		// none.
		return nil, fmt.Errorf("the source could not be read to collect its call pins: %w", err)
	}

	out := map[string]sourcePin{}
	for _, doc := range file.Docs {
		collector := pinCollector{anchors: map[string]ast.Node{}, out: out}
		if err := collector.collectAnchors(doc.Body, 0); err != nil {
			return nil, err
		}
		if err := collector.collect(doc.Body, "", 0); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// pinCollector resolves anchors, aliases and merge keys the same way the
// compiler does. Anchors are document-scoped in YAML, so sourcePins creates one
// collector per document.
type pinCollector struct {
	anchors map[string]ast.Node
	out     map[string]sourcePin

	// nodes is what the walk has visited, bounded by [maxNodes]. Counted rather
	// than assumed from the byte length: following a merge key expands one
	// anchored mapping into every mapping that merges it, which is breadth the
	// bytes do not spell out.
	nodes int
}

func (c *pinCollector) collectAnchors(n ast.Node, depth int) error {
	if depth > maxDepth {
		return fmt.Errorf("nests more than %d levels deep, which is deeper than a Flowfile is meant to go", maxDepth)
	}
	switch node := n.(type) {
	case nil:
		return nil
	case *ast.AnchorNode:
		if name, ok := scalarText(node.Name); ok {
			c.anchors[name] = node.Value
		}
		// An anchor is a label on the value it wraps, not a level of nesting:
		// `k: &a {…}` and `k: {…}` are the same structure to the compiler. So
		// descend at the same depth, or this walk refuses an anchored document
		// one level shallower than the identical unanchored one — a `flow fmt`
		// failure on a file that compiles, caused solely by the `&a`.
		return c.collectAnchors(node.Value, depth)
	case *ast.MappingNode:
		// A mapping and its entries are one level, not two — the same
		// accounting [collectComments] uses, for the same reason (#691).
		for _, value := range node.Values {
			if err := c.collectAnchors(value, depth); err != nil {
				return err
			}
		}
	case *ast.MappingValueNode:
		if err := c.collectAnchors(node.Key, depth+1); err != nil {
			return err
		}
		return c.collectAnchors(node.Value, depth+1)
	case *ast.SequenceNode:
		for _, value := range node.Values {
			if err := c.collectAnchors(value, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// resolve follows anchors and aliases to the node that was actually written,
// the way [compiler.resolve] does, and reports whether it arrived anywhere.
func (c *pinCollector) resolve(n ast.Node) (ast.Node, bool) {
	for depth := 0; depth <= maxAliasDepth; depth++ {
		switch node := n.(type) {
		case nil:
			return nil, false
		case *ast.AnchorNode:
			n = node.Value
		case *ast.AliasNode:
			target, ok := c.anchors[node.Value.String()]
			if !ok {
				return nil, false
			}
			n = target
		default:
			return n, true
		}
	}
	return nil, false
}

func (c *pinCollector) scalar(n ast.Node) (string, bool) {
	resolved, ok := c.resolve(n)
	if !ok {
		return "", false
	}
	return scalarText(resolved)
}

// count accounts for one more node visited, and reports the same refusal the
// compiler reports when a document expands past what a Flowfile may hold.
func (c *pinCollector) count() error {
	c.nodes++
	if c.nodes > maxNodes {
		return fmt.Errorf("holds more than %d values once aliases are expanded, which is more than a Flowfile is meant to hold", maxNodes)
	}
	return nil
}

// A pinEntry is one key and value of a mapping, after merge keys have been
// expanded and aliases resolved — [entry] as this walk needs it, without a
// compiler to report through.
type pinEntry struct {
	name  string
	key   ast.Node
	value ast.Node
}

// entries returns a mapping's entries the way [compiler.entries] reads them:
// keys written here claim their names before anything a `<<:` merges in, and
// the first merge to offer a name wins.
//
// A merge whose value does not resolve to a mapping contributes nothing rather
// than failing the walk — see this file's header for why that is the right
// answer for both of the things reading it.
func (c *pinCollector) entries(mapping *ast.MappingNode, depth int) ([]pinEntry, error) {
	if depth > maxDepth {
		return nil, fmt.Errorf("nests more than %d levels deep, which is deeper than a Flowfile is meant to go", maxDepth)
	}

	written := make(map[string]bool, len(mapping.Values))
	for _, v := range mapping.Values {
		if _, isMerge := v.Key.(*ast.MergeKeyNode); isMerge {
			continue
		}
		if name, ok := keyNameOf(v.Key); ok {
			written[name] = true
		}
	}

	var (
		out  []pinEntry
		seen = make(map[string]bool, len(mapping.Values))
	)
	for _, v := range mapping.Values {
		if err := c.count(); err != nil {
			return nil, err
		}
		if _, isMerge := v.Key.(*ast.MergeKeyNode); isMerge {
			merged, err := c.mergedEntries(v.Value, depth+1)
			if err != nil {
				return nil, err
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
		// [keyNameOf] answers for a string key and nothing else, which is every
		// key the grammar has. A key of some other kind does not compile, but it
		// is still structure this walk has to descend through to reach whatever
		// is under it, so it is addressed by the same token text [keyStep] uses
		// and left to the compiler to complain about.
		name, ok := keyNameOf(v.Key)
		if !ok {
			name = keyStep(v)
		}
		if seen[name] {
			continue
		}
		seen[name] = true
		out = append(out, pinEntry{name: name, key: v.Key, value: v.Value})
	}
	return out, nil
}

// mergedEntries reads the entries a `<<:` brings in, following the alias it is
// almost always written as.
func (c *pinCollector) mergedEntries(n ast.Node, depth int) ([]pinEntry, error) {
	resolved, ok := c.resolve(n)
	if !ok {
		return nil, nil
	}
	switch node := resolved.(type) {
	case *ast.MappingNode:
		return c.entries(node, depth)
	case *ast.MappingValueNode:
		return c.entries(&ast.MappingNode{Values: []*ast.MappingValueNode{node}}, depth)
	default:
		return nil, nil
	}
}

// collect walks a source document the way [collectComments] does — same node
// kinds, same path arithmetic — because a pin is placed by [placePins] finding
// the same path later, and a shape one of them knows about and the other does
// not is a pin that goes missing.
//
// The one place the two walks differ is the one this file exists for: this one
// resolves anchors, aliases and merge keys, because a pin can arrive through
// any of them and the rendered document [placePins] walks has them all expanded
// already.
func (c *pinCollector) collect(n ast.Node, path string, depth int) error {
	if n == nil {
		return nil
	}
	if depth > maxDepth {
		return fmt.Errorf("nests more than %d levels deep, which is deeper than a Flowfile is meant to go", maxDepth)
	}
	if err := c.count(); err != nil {
		return err
	}

	resolved, ok := c.resolve(n)
	if !ok {
		return nil
	}

	switch x := resolved.(type) {
	case *ast.MappingNode:
		return c.collectMapping(x, path, depth)

	case *ast.MappingValueNode:
		return c.collectMapping(&ast.MappingNode{Values: []*ast.MappingValueNode{x}}, path, depth)

	case *ast.SequenceNode:
		for i, value := range x.Values {
			element := childPath(path, indexStep(i))
			if err := c.collect(value, element, depth+1); err != nil {
				return err
			}
		}
	}

	return nil
}

// collectMapping records the pin one mapping carries, if it carries one, and
// walks what is under it.
func (c *pinCollector) collectMapping(mapping *ast.MappingNode, path string, depth int) error {
	entries, err := c.entries(mapping, depth)
	if err != nil {
		return err
	}

	byName := make(map[string]pinEntry, len(entries))
	for _, e := range entries {
		byName[e.name] = e
	}

	call, hasCall := byName["call"]
	digest, hasDigest := byName["digest"]
	if hasCall && hasDigest {
		// The compiler resolves anchors and aliases before reading this
		// scalar, so the formatter must do the same. If a hand-built or
		// otherwise mismatched source cannot be resolved, refuse rather
		// than silently treating a security pin as absent.
		text, ok := c.scalar(digest.value)
		if !ok {
			// Name the position, the way every other diagnostic here
			// does: this refuses to format a file, so the author needs
			// to be told which `digest:` it could not read.
			where := ""
			if token := digest.key.GetToken(); token != nil && token.Position != nil {
				where = fmt.Sprintf("%d:%d: ", token.Position.Line, token.Position.Column)
			}
			return fmt.Errorf("%sdigest: pins the call here, but its value could not be read as text; write the digest as a scalar, or as an alias of one", where)
		}

		pin := sourcePin{text: text}
		// The target and the step's id are read best-effort: neither decides
		// whether the pin is carried across, and a call written as something
		// other than a scalar is the compiler's to refuse rather than this
		// walk's.
		pin.call, _ = c.scalar(call.value)
		if id, ok := byName["id"]; ok {
			pin.step, _ = c.scalar(id.value)
		}
		if token := digest.key.GetToken(); token != nil && token.Position != nil {
			pin.line, pin.column = token.Position.Line, token.Position.Column
		}
		c.out[path] = pin
	}

	for _, e := range entries {
		if err := c.collect(e.value, childPath(path, e.name), depth+1); err != nil {
			return err
		}
	}
	return nil
}
