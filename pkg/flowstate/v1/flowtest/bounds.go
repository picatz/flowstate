package flowtest

import (
	"fmt"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
)

// maxExpandedNodes and maxExpansionDepth bound a *.test.yaml the same shape
// pkg/flowstate/v1/flowfile bounds a Flowfile's own alias expansion (that
// package's unexported maxNodes/maxDepth, enforced by its compiler's own
// walk) — a test file is untrusted input like any Flowfile (CLAUDE.md,
// "bound anything that consumes untrusted input"), and an alias may be
// referenced many times, so a short document can expand into an enormous one
// — a billion-laughs document has a depth of one per alias and multiplies
// breadth at every level, which is exactly the shape [checkExpansionBounds]
// exists to catch: counted by total nodes reached, not by how deep any one
// chain of references goes.
//
// This is a different implementation from flowfile's, not a copy: that
// machinery lives on an unexported *compiler carrying its own diagnostic
// positions, its own YAML-merge-key handling (`<<:`), and its own anchor
// bookkeeping, none of which this package's much smaller grammar needs, and
// none of which is exported for reuse. Duplicating flowfile's exact walk
// without being able to run it against the same tests it is proven against
// would risk a subtly different bound that only looks like the real one —
// worse than admitting the reuse does not exist. So this is the same
// *algorithm* (walk the parsed AST, resolve every alias back to the anchor it
// names, and count every node reached, so a reference used many times costs
// what it actually costs) against a bound owned entirely by this package.
const (
	maxExpandedNodes  = 100_000
	maxExpansionDepth = 64
)

// checkExpansionBounds parses data only as far as the AST — never as far as
// yaml.Unmarshal, which resolves every alias into the destination value
// before any check written against that destination gets a chance to run —
// and refuses a document whose alias expansion would exceed
// [maxExpandedNodes] or [maxExpansionDepth].
//
// A parse failure here is not reported: [Load]'s own yaml.Unmarshal call
// reports the same malformed-YAML error in the shape a caller already
// expects, and reporting it twice, once from each of two parsers, would be
// the same fact said two different ways depending on which one happened to
// notice first.
func checkExpansionBounds(data []byte) error {
	file, err := parser.ParseBytes(data, 0)
	if err != nil {
		return nil
	}

	return checkExpansionBoundsIn(file)
}

// checkExpansionBoundsIn is [checkExpansionBounds] for a caller that has
// already parsed the document — the suite loader, which keeps the tree to
// answer where a diagnostic belongs (position.go) and would otherwise parse the
// same bytes twice, once per reader, on every load.
func checkExpansionBoundsIn(file *ast.File) error {
	anchors := map[string]ast.Node{}
	for _, doc := range file.Docs {
		collectAnchors(doc.Body, anchors)
	}

	count := 0
	for _, doc := range file.Docs {
		if err := walkBounded(doc.Body, anchors, 0, &count); err != nil {
			return err
		}
	}
	return nil
}

// collectAnchors records every `&name` anchor's node by name, so an
// `*name` alias reached later can be resolved back to what it names — an
// [ast.AliasNode]'s own Value is the alias's literal name, not the anchored
// content itself.
func collectAnchors(n ast.Node, anchors map[string]ast.Node) {
	switch node := n.(type) {
	case nil:
		return
	case *ast.AnchorNode:
		if node.Name != nil {
			anchors[node.Name.String()] = node.Value
		}
		collectAnchors(node.Value, anchors)
	case *ast.MappingNode:
		for _, v := range node.Values {
			collectAnchors(v, anchors)
		}
	case *ast.MappingValueNode:
		collectAnchors(node.Key, anchors)
		collectAnchors(node.Value, anchors)
	case *ast.SequenceNode:
		for _, v := range node.Values {
			collectAnchors(v, anchors)
		}
	}
}

// walkBounded descends into n, following every alias back to its anchor and
// counting each node reached into count, refusing once count exceeds
// [maxExpandedNodes] or depth exceeds [maxExpansionDepth].
//
// The depth bound is what stops a self-referential anchor
// (`a: &x {b: *x}`) from recursing forever: each pass through the cycle
// still increases depth, so it is caught in a few dozen steps rather than
// by exhausting the stack — the same reason a depth bound and a breadth
// bound are both needed and neither substitutes for the other (CLAUDE.md).
func walkBounded(n ast.Node, anchors map[string]ast.Node, depth int, count *int) error {
	if n == nil {
		return nil
	}
	if depth > maxExpansionDepth {
		return fmt.Errorf(
			"nests more than %d levels deep (following alias references), which is deeper than a test file is meant to go",
			maxExpansionDepth)
	}

	*count++
	if *count > maxExpandedNodes {
		return fmt.Errorf(
			"holds more than %d values once aliases are expanded, which is more than a test file is meant to hold",
			maxExpandedNodes)
	}

	switch node := n.(type) {
	case *ast.MappingNode:
		for _, v := range node.Values {
			if err := walkBounded(v, anchors, depth+1, count); err != nil {
				return err
			}
		}
	case *ast.MappingValueNode:
		if err := walkBounded(node.Key, anchors, depth+1, count); err != nil {
			return err
		}
		if err := walkBounded(node.Value, anchors, depth+1, count); err != nil {
			return err
		}
	case *ast.SequenceNode:
		for _, v := range node.Values {
			if err := walkBounded(v, anchors, depth+1, count); err != nil {
				return err
			}
		}
	case *ast.AnchorNode:
		if err := walkBounded(node.Value, anchors, depth+1, count); err != nil {
			return err
		}
	case *ast.AliasNode:
		name := node.Value.String()
		target, ok := anchors[name]
		if !ok {
			// An alias to nothing this document ever anchored is a different
			// problem — malformed input, not an expansion bomb — and is left
			// for yaml.Unmarshal's own parser to report in its own words.
			return nil
		}
		if err := walkBounded(target, anchors, depth+1, count); err != nil {
			return err
		}
	}
	// Every other node kind (string/int/bool/float/null scalars, comments,
	// document/tag wrappers) is a leaf as far as expansion goes: already
	// counted above, nothing further to descend into.

	return nil
}
