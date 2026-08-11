package flowfile

import (
	"fmt"
	"slices"
	"strconv"
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
// rather than "how many have there been", and because a reader seeing v2026.2 in
// a file knows immediately whether it is old.
//
// # The `v` reverses a recorded decision, and the reason is one line below
//
// The first edition was written `2026.1`, unprefixed. A date with one dot is a YAML
// *float*, so the value arrives as a number and [editionText] exists entirely to read
// the token's source text instead — because converting the float is quietly wrong
// (2026.10 and 2026.1 are the same float, so the tenth edition of a year would compile
// as the first) and refusing it teaches an author to quote a value for a reason no other
// key has.
//
// A `v` makes it a string in every YAML parser, which deletes that whole problem for
// every edition after the first. The workaround stays for reading `2026.1`, and can go
// when that edition does.
//
// # v2026.3: optional traversal is part of the language, and the edition says so
//
// This edition's grammar includes CEL optional types' read-side surface — `.?`
// traversal and `orValue()` — as a documented part of the dialect (issue #412).
// The extension is additive: every v2026.2 file is byte-for-byte a valid v2026.3
// file with the same meaning, so `flow fix` brings a file forward by stamping the
// marker, and rewrites the `has(x.y) && x.y` guarded-read idiom into the spelling
// this edition exists to carry. An engine that predates this edition refuses a
// v2026.3 file with the unknown-edition diagnostic below, which is the whole
// point of the marker: refusal rather than reinterpretation, on the one dialect
// axis the language has.
const CurrentEdition = "v2026.3"

// firstEdition is the unprefixed spelling, kept so a file written in it can be read far
// enough to be rewritten.
//
// It is the only edition this build knows and does not compile. That asymmetry is the
// point of an edition: recognising a grammar is what lets `flow fix` bring a file
// forward, and compiling it would be carrying two grammars — the cost the
// no-deprecation decision was made to avoid.
const firstEdition = "2026.1"

// editionV2026_2 is the edition before optionals were part of the documented
// dialect. Known so `flow fix` can bring a file forward; not compiled, because
// there is one grammar in a build (see the package comment above).
const editionV2026_2 = "v2026.2"

// knownEditions are every edition this build recognises, oldest first.
var knownEditions = []string{firstEdition, editionV2026_2, CurrentEdition}

// KnownEditions returns the editions this build recognises, oldest first.
func KnownEditions() []string {
	return slices.Clone(knownEditions)
}

// checkEdition reports whether a declared edition is one this build compiles.
//
// Fails closed, and the two ways it can fail want different answers. An edition
// this build has never heard of is a file from the future — a newer `flow` wrote
// it — and the fix is to upgrade, not to edit the file. A known-but-older
// edition is a file this build can rewrite, and says so.
func checkEdition(declared string) error {
	if declared == CurrentEdition {
		return nil
	}
	if slices.Contains(knownEditions, declared) {
		return fmt.Errorf(
			"edition %q is older than this build compiles (%s); run `flow fix` to rewrite the file",
			declared, editionName(CurrentEdition))
	}
	return fmt.Errorf(
		"edition %q is not one this build knows; this build knows %s and compiles %s; "+
			"a newer flow may have written this file, so upgrade rather than editing the edition",
		declared, editionList(knownEditions), editionName(CurrentEdition))
}

// editionName renders one edition as an author has to type it.
//
// Quoted, because an edition is a literal string rather than a number, and
// because the set genuinely holds two shapes: `2026.1` is unprefixed and
// everything from `v2026.2` on carries a `v` (see [CurrentEdition] for why the
// prefix arrived). Rendered bare, a list of both reads as one name spelled two
// ways by an inconsistent formatter, when it is in fact two names each spelled
// the only way it can be. Quoting says "type this" about each of them.
//
// One formatter, so a member of the list and the edition a message names on its
// own cannot come out differently: that is the one-value-written-twice rule from
// CLAUDE.md, wearing prose (#385).
func editionName(edition string) string {
	return strconv.Quote(edition)
}

// editionList renders a set of editions, every member through [editionName].
func editionList(editions []string) string {
	names := make([]string, 0, len(editions))
	for _, edition := range editions {
		names = append(names, editionName(edition))
	}
	return strings.Join(names, ", ")
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

// missingEdition is what an author is told when no `edition:` is written.
//
// # Requiring it reverses a recorded decision, and this sweep is why
//
// It used to be optional, and the reasoning was good: a line of ceremony at the top of
// every file to say the only thing it could say, when a file that does not care which
// grammar it is in is the common case.
//
// What that missed is that "absent means current" is not a default — it is a promise to
// *reinterpret*. This edition renamed `iterator:` to `as:` and rooted the http response.
// A file written last month with no marker is not a file that does not care; it is a
// file written in the older grammar, and reading it as this one is precisely the silent
// reinterpretation `edition:` was introduced to prevent. The optional spelling made the
// mechanism unable to do its own job for exactly the files most likely to need it.
//
// So it is required, and `flow fix` writes it — which is what keeps the ceremony from
// being an author's problem. The cost is one line per file; the thing it buys is that no
// future sweep can change what an existing file means without saying so.
func missingEdition() string {
	return fmt.Sprintf(
		"no `edition:` is declared, and one is required: without it a file written in an older "+
			"grammar is silently read as this one. Write `edition: %s` at the top, or run "+
			"`flow fix` to add it", CurrentEdition)
}
