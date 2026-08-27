package flowfile

import (
	"fmt"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/token"
)

// The Flowfile grammar is a strict subset of YAML. Three constructs YAML allows
// are refused: anchors (`&name`), aliases (`*name`), and the merge key (`<<:`).
// The compiler refuses them at the document tree before anything is resolved or
// expanded; `flow fix` carries a file across that refusal where it can do so
// mechanically — a whole-value alias becomes the bytes of the value its anchor
// names — and refuses in the same words where it cannot, which is a merge key and
// anything else it cannot copy byte-safely. See fixalias.go.
//
// # Why refuse rather than support
//
// They are used zero times across the corpus and cost more than anything else
// the front end inherited from YAML: a cycle-detection pass and a depth bound on
// alias chains, a total-node budget that exists only because a billion-laughs
// document has depth one per alias so a depth bound cannot see it, edit
// suppression in `flow fix`, refusal paths in the formatter, merge-key
// precedence logic, and a measured quadratic-expansion incident (43 KiB of
// input, 22 seconds). A feature no author writes is not worth the machinery that
// makes it safe. See #653 and docs/DSL.md.
//
// # Why this refusal precedes expansion, and must
//
// The attack an alias enables is breadth, not depth: a billion-laughs document
// references one small anchor from many aliases, and each level multiplies the
// last, so a short file expands into an enormous one. A bound that fires *after*
// expansion has already paid the cost it was meant to prevent. This walk reads
// the un-expanded document tree — one node per construct as the author wrote it,
// linear in the file's own size — and refuses on the *presence* of the
// construct, so a document that would explode is rejected before a single alias
// is followed. In the compiler that is why it runs before
// [compiler.collectAnchors] and before any call to [compiler.entries], the two
// places resolution and merge expansion happen.
//
// The bounds it makes redundant *on this path* (maxAliasDepth, the merge branch
// of maxNodes, the cycle-detection pass, the formatter's merge/anchor handling)
// are left in place for now: closing the door is this change; removing the
// corridor behind it is follow-up cleanup that a reviewer should read on its own.
//
// Redundant here is not redundant everywhere, and the difference is load-bearing
// for anyone reading those bounds as dead. [CallPins] and [Format] share
// [pinCollector], which resolves anchors, aliases and merge keys on a document
// that need not compile — `flow fix` reads the pins of every file in a tree to
// report the staleness it caused, including files this refusal left alone — so
// maxNodes and maxAliasDepth are still driven by input an outside party chooses,
// with no refusal in front of them. bounds_test.go exercises both there.
//
// `flow fix` mechanically inlining a whole-value alias — so an author is not left
// to spell it out by hand — is the migration path across this refusal, and it
// lives in fixalias.go. It is the one place in the front end that *does* follow an
// alias, which is why it charges every expansion against maxNodes rather than
// relying on this refusal running first.

// strictFinding is one refused construct: its position, and the message naming
// it and what to write instead.
type strictFinding struct {
	line, column int
	message      string
}

// strictYAMLRefusals returns a finding for every anchor, alias, and merge key in
// root, each positioned at the construct itself.
//
// It walks the tree the parser built rather than resolving anything, so a
// document whose aliases would expand into millions of nodes is inspected at the
// cost of reading the nodes actually written — the breadth explosion never
// happens because nothing is ever followed. Every occurrence is reported, so an
// author fixing one construct sees the others in the same pass rather than one
// per recompile.
//
// The one collector both the compiler and [flow fix] use, so the two cannot come
// to name the same construct in two different sentences (the one-value-written-
// twice rule).
func strictYAMLRefusals(root ast.Node) []strictFinding {
	v := &strictVisitor{}
	ast.Walk(v, root)
	return v.findings
}

// strictVisitor is the [ast.Visitor] strictYAMLRefusals walks with.
type strictVisitor struct{ findings []strictFinding }

// Visit records n when it is one of the three refused constructs and returns the
// same visitor so the walk continues into every child — an anchor's value may
// itself hold an alias, and both are worth naming.
func (v *strictVisitor) Visit(n ast.Node) ast.Visitor {
	switch node := n.(type) {
	case *ast.AnchorNode:
		v.record(node.Start, strictAnchorMessage(anchorName(node)))
	case *ast.AliasNode:
		v.record(node.Start, strictAliasMessage(aliasName(node)))
	case *ast.MergeKeyNode:
		v.record(node.Token, strictMergeMessage())
	}
	return v
}

// record appends one finding, positioned at tok.
func (v *strictVisitor) record(tok *token.Token, message string) {
	span := spanOfToken(tok)
	v.findings = append(v.findings, strictFinding{
		line:    span.Start.Line,
		column:  span.Start.Column,
		message: message,
	})
}

// The three messages, each naming the construct, that it is not part of the
// grammar, and the concrete thing to write instead — the diagnostics-as-a-feature
// standard. Built here so the compiler and the fixer speak with one voice.

func strictAnchorMessage(name string) string {
	return fmt.Sprintf(
		"an anchor (`&%s`) is not part of the Flowfile grammar, which is a strict subset of YAML: "+
			"write the value out where it is used rather than naming it here for an alias to reuse", name)
}

func strictAliasMessage(name string) string {
	return fmt.Sprintf(
		"an alias (`*%s`) is not part of the Flowfile grammar, which is a strict subset of YAML: "+
			"write the value out here rather than referring to one written elsewhere", name)
}

func strictMergeMessage() string {
	return "a merge key (`<<:`) is not part of the Flowfile grammar, which is a strict subset of YAML: " +
		"write each key it would merge in directly on this mapping"
}

// anchorName returns the name an anchor declares, for the diagnostic. An anchor
// with no name is not something the parser produces, so the empty string is a
// defensive fallback the message still reads acceptably with.
func anchorName(n *ast.AnchorNode) string {
	return nameToken(n.Name)
}

// aliasName returns the name an alias refers to, for the diagnostic.
func aliasName(n *ast.AliasNode) string {
	return nameToken(n.Value)
}

// nameToken returns the name a node spells, read from its own token rather than
// from [ast.Node.String].
//
// String renders a node *with its comments*, and an alias is the one place that
// matters: `b: *g # said twice` renders as `g # said twice`, so a diagnostic built
// from it names an anchor called "g # said twice", and anything that goes looking
// for `*` + that name on the line finds nothing. The token is the name and only
// the name.
func nameToken(n ast.Node) string {
	if n == nil {
		return ""
	}
	tok := n.GetToken()
	if tok == nil {
		return ""
	}

	return tok.Value
}

// strictYAMLRefusalsIn walks every document in a parsed file and returns a
// [Diagnostic] for each anchor, alias, and merge key, for [flow fix] — which
// works on a whole file rather than the single body the compiler has already
// reduced to. Named separately from the node-level collector so the fixer's call
// site reads as what it does.
func strictYAMLRefusalsIn(file *ast.File) []Diagnostic {
	var out []Diagnostic
	for _, doc := range file.Docs {
		for _, f := range strictYAMLRefusals(doc.Body) {
			out = append(out, Diagnostic{Line: f.line, Column: f.column, Message: f.message})
		}
	}
	return out
}

// refuseStrictYAML reports every anchor, alias, and merge key in root through the
// compiler's diagnostic stream and returns whether the document is free of all
// three. See strict.go's file comment for why this runs before any expansion.
func (c *compiler) refuseStrictYAML(root ast.Node) bool {
	findings := strictYAMLRefusals(root)
	for _, f := range findings {
		point := Position{Line: f.line, Column: f.column}
		c.report(Span{Start: point, End: point}, ref{path: "strict"}, "%s", f.message)
	}
	return len(findings) == 0
}
