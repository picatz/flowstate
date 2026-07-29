package flowfile

import (
	"fmt"
	"slices"
	"strings"

	"github.com/goccy/go-yaml/ast"
)

// An edition names the version of the Flowfile grammar a file is written in.
//
// This exists because of a decision recorded in docs/DSL.md: surface syntax gets
// no deprecation window. A spelling that is replaced is *gone*, and `flow fix`
// rewrites files across the boundary. That decision is only safe if a file can
// say which grammar it was written in — otherwise a future change silently
// reinterprets a file rather than refusing it, which is the failure mode a
// deprecation window is usually protecting against.
//
// What it is deliberately not: a compatibility mechanism. Declaring an older
// edition does not make an older grammar compile. There is one grammar in a
// build, and an edition it does not know is refused. Carrying two grammars is
// exactly the cost the no-deprecation decision was made to avoid.
//
// The stable contract is the compiled spec, not the source. An edition is a
// property of a *file*, so a workflow already running is unaffected by any of
// this — the schema has no field for it, and should not grow one.

// CurrentEdition is the grammar this build compiles.
//
// Dated rather than numbered, because an edition answers "when was this written"
// rather than "how many have there been", and because a reader seeing 2026.1 in
// a file knows immediately whether it is old.
const CurrentEdition = "2026.1"

// knownEditions are every edition this build recognises, oldest first.
//
// One entry today. It is a list rather than a constant comparison so that the
// day there are two, the code that must change is this line and not a condition
// spelled out somewhere else.
var knownEditions = []string{CurrentEdition}

// KnownEditions returns the editions this build recognises, oldest first.
func KnownEditions() []string {
	return slices.Clone(knownEditions)
}

// checkEdition reports whether a declared edition is one this build compiles.
//
// Fails closed, and the two ways it can fail want different answers. An edition
// this build has never heard of is a file from the future — a newer `flow` wrote
// it — and the fix is to upgrade, not to edit the file. A known-but-older
// edition would be a file this build could rewrite, and says so.
func checkEdition(declared string) error {
	if declared == CurrentEdition {
		return nil
	}
	if slices.Contains(knownEditions, declared) {
		return fmt.Errorf(
			"edition %q is older than this build compiles (%s); run `flow fix` to rewrite the file",
			declared, CurrentEdition)
	}
	return fmt.Errorf(
		"edition %q is not one this build knows (%s); a newer flow may have written this file, "+
			"so upgrade rather than editing the edition",
		declared, strings.Join(knownEditions, ", "))
}

// editionText reads a declared edition from the node it was written as.
//
// An edition is dated, and a date with one dot is a YAML float: `edition: 2026.1`
// arrives as a number, not a string. Two ways to handle that are wrong. Refusing
// it teaches an author to quote a value for a reason no other key has. Converting
// the float is worse and quietly so — 2026.10 and 2026.1 are the same float, so
// the tenth edition of a year would silently compile as the first.
//
// So the number is never converted. The token's own source text is read instead,
// which is the string the author typed and is exactly what an edition is.
func editionText(n ast.Node) (string, bool) {
	switch node := n.(type) {
	case *ast.StringNode:
		return node.Value, true
	case *ast.FloatNode, *ast.IntegerNode:
		if tok := n.GetToken(); tok != nil {
			return tok.Value, true
		}
		return "", false
	default:
		return "", false
	}
}
