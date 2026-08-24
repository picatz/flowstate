package flowfile_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The defect behind #880 was not in the YAML emitter, which is where a scalar
// beginning `-----` points. It was in the compiler: `normalizeExpr` stored the
// expression as cel-go's unparser writes it, ran exactly one parse-unparse pass,
// and assumed that reached a fixed point.
//
// For most expressions it does. For a negation of a numeric literal it does not,
// because the two halves fold at different times. `-----0` *parses* to a negation
// applied to a folded zero; the unparser writes that as `-0`; parsing `-0` folds
// again, to the constant `0`. So the workflow held the tree for `-0` while
// Marshal wrote `${0}`, and reading the file back produced a tree that was not
// the one written — the second of FuzzMarshalRoundTrip's three properties, and
// the one that says a formatter must not change what a file means.
//
// The class is every expression whose unparsed form parses to something the
// unparser then writes differently again. Parentheses are the other half of it:
// the unparser drops them, and dropping them puts two operators next to each
// other that the parser folds on the following pass. `-(0)`, `- - -0` and
// `!(!(true))` all need three passes, and the depth of the nesting does not
// change that — each pass folds the whole tree rather than one layer, which is
// why the bound in [maxNormalizePasses] is a small constant rather than
// something scaled to the source.
//
// These run as named cases rather than being left to the fuzzer for the reason
// #728's list gives: a fuzzer reaches this shape only when a seed happens to
// generate it, so the same tree fails on some runs and passes on others.
var normalizationFixedPointExprs = []string{
	// The corpus entry, and the shapes either side of it.
	"-----0",
	"-0",
	"--0",
	"---0",
	"- - -0",
	"-(0)",
	"-(-(0))",
	"-(-(-(0)))",
	"-(-(-(-(-(0)))))",
	"(-0)",
	"((((-0))))",
	"-0.0",
	"-----0.0",
	"-0x0",
	"0x0",

	// Logical negation folds the same way, one `!` at a time, and parentheses
	// have the same effect on it.
	"!!true",
	"!!!true",
	"!(true)",
	"!(!(true))",
	"!(!(!(!(true))))",

	// Spellings that differ from their unparsing without any folding at all.
	// These were already a fixed point after one pass; they are here so that a
	// change to the loop that broke them would be visible.
	"1  +  1",
	"(1)",
	"((1))",
	"1 - -0",
	"[-0]",
	"{'a': -0}",
	"-0 + 1",
}

// TestNormalizedExpressionsRoundTrip asserts #880's property over the class: for
// each spelling, compiling the workflow and marshalling it back produces bytes
// that parse to the same workflow, and marshalling twice is a fixed point.
//
// It runs through [requireSourceRoundTrip] deliberately, rather than reaching
// into the compiler, because that function is the three properties the fuzzer
// asserts. A test of `normalizeExpr` alone would pin the fix and not the defect:
// what broke was the agreement between the compiler and Marshal, and only a
// round trip can see an agreement.
func TestNormalizedExpressionsRoundTrip(t *testing.T) {
	t.Parallel()

	for _, source := range normalizationFixedPointExprs {
		t.Run(source, func(t *testing.T) {
			t.Parallel()

			document, ok := probeDocument("value", source)
			require.True(t, ok)

			require.True(t, requireSourceRoundTrip(t, []byte(document)),
				"%q was refused, so the round trip asserted nothing about it; "+
					"an expression this test lists has to be one the compiler accepts",
				source)
		})
	}
}

// TestFormatPreservesNormalizedExpressions asks the same question of the verb an
// author actually runs.
//
// `flow fmt` is [flowfile.Format], which renders [Marshal]'s document and places
// the source's comments back into it — so it writes through the same emitter
// #880 disagreed with, and it writes *over the author's file*. `flow fix` does
// not: it edits source lines in place and never renders a workflow back out, so
// the same bytes cannot reach a file through it.
//
// Two properties, both of them things `flow fmt` promises rather than things
// Marshal does. What comes back means the same as what went in, and running it
// twice changes nothing the second time.
func TestFormatPreservesNormalizedExpressions(t *testing.T) {
	t.Parallel()

	for _, source := range normalizationFixedPointExprs {
		t.Run(source, func(t *testing.T) {
			t.Parallel()

			// With a comment, because that is the path with something to place:
			// a file with no comments formats to Marshal's bytes directly, and
			// the comment placement walk re-reads the rendered document.
			document := fmt.Sprintf(
				"edition: %s\nname: formatted\nsteps:\n  - id: valued\n    # why this value\n    value: %q\n"+
					"  - id: shown\n    log:\n      message: hello\n",
				flowfile.CurrentEdition, source)

			before, _, err := flowfile.Parse([]byte(document))
			require.NoError(t, err, "the test's own document must parse, or it asserts nothing")

			formatted, err := flowfile.Format([]byte(document), before)
			require.NoError(t, err)
			require.Contains(t, string(formatted), "# why this value",
				"the comment was dropped:\n%s", formatted)

			after, _, err := flowfile.Parse(formatted)
			require.NoError(t, err, "`flow fmt` wrote a document that does not parse:\n%s", formatted)
			require.True(t, proto.Equal(before, after),
				"`flow fmt` wrote a file that means something else:\n%s", formatted)

			again, err := flowfile.Format(formatted, after)
			require.NoError(t, err)
			require.Equal(t, string(formatted), string(again),
				"`flow fmt` does not settle: a second run moves the file again")
		})
	}
}

// TestMarshalledIndicatorScalarsRoundTripInEveryTextPosition is #728's list
// asked in every position a *text* scalar can occupy, rather than only in the
// task input where that defect was found.
//
// #728 was a literal in a task input; #880 was an expression in a step's value.
// Both are "a scalar in one position was not asked the question every other
// position is asked", and the answer to that is to ask every position. The
// positions come from [probePositions], the same set the fuzzer substitutes
// into, so a position added there is covered here without anyone remembering to
// add it.
//
// `value:` is left out and covered by [TestNormalizedExpressionsRoundTrip]
// instead. A scalar there is CEL source rather than text, so `# not a comment`
// is refused by the language and asserting anything about the refusals would be
// asserting which strings happen to be CEL.
func TestMarshalledIndicatorScalarsRoundTripInEveryTextPosition(t *testing.T) {
	t.Parallel()

	scalars := append(append([]string{}, yamlIndicatorScalars...), yamlAmbiguousScalars...)

	for _, position := range probePositions {
		if position == "value" {
			continue
		}

		t.Run(position, func(t *testing.T) {
			t.Parallel()

			for _, scalar := range scalars {
				t.Run(fmt.Sprintf("%q", scalar), func(t *testing.T) {
					document, ok := probeDocument(position, scalar)
					require.True(t, ok)

					// Asserted rather than counted, in both directions. A round
					// trip is only conditional on the parser accepting the
					// document, so a table that stopped being accepted anywhere
					// would decline every case and pass green — the failure
					// [mustRoundTripIn] exists to stop. The one refusal the
					// language really makes is named in [refusedByLanguage], and
					// asserting it is still refused keeps that exception from
					// outliving its reason.
					if refusedByLanguage(position, scalar) {
						require.False(t, requireSourceRoundTrip(t, []byte(document)),
							"%q is accepted in %q now, so its exception here is stale",
							scalar, position)
						return
					}
					require.True(t, requireSourceRoundTrip(t, []byte(document)),
						"%q was refused in %q, so the round trip asserted nothing about it",
						scalar, position)
				})
			}
		})
	}
}

// refusedByLanguage names the one case in this table the language refuses
// rather than round-trips, so that the assertion above can be an equality in
// both directions instead of a count.
//
// The empty string is not a legal `log` message: the task requires one, which is
// a rule about what a workflow may say and not about how a scalar is written. It
// is still in the table because the *other* six positions have to carry it —
// a description that is the empty string is ordinary, and an emitter that writes
// it bare produces `description:` with nothing after it, which reads back as
// null rather than as "".
func refusedByLanguage(position, scalar string) bool {
	return position == "log message" && scalar == ""
}

// yamlAmbiguousScalars are strings a YAML emitter has to quote for a reason
// other than a leading indicator: they are text that reads back as some other
// type, or as nothing at all.
//
// [yamlIndicatorScalars] covers the characters that introduce *structure*. These
// are the ones that resolve to a different *scalar* — a plain `null` is the null
// value, a plain `0644` is an integer in one YAML version and a string in
// another, and a plain empty document position is null rather than "". A value
// that comes back as the wrong type parses perfectly, which is what makes this
// half of the class the quieter one.
//
// A literal tab is absent because [probeDocument] cannot write one: it quotes a
// probe by wrapping it in double quotes, and a raw tab inside those is refused
// by the YAML parser reading the test's own input. The escape spelling an author
// would actually use is covered by
// [TestMarshalledEscapedWhitespaceRoundTrips] instead — and the fuzzer skips
// control characters for the same reason, in [isPrintable].
var yamlAmbiguousScalars = []string{
	"",
	" ",
	"  leading space",
	"trailing space  ",
	"null",
	"Null",
	"NULL",
	"~",
	"true",
	"True",
	"false",
	"no",
	"No",
	"yes",
	"on",
	"off",
	"y",
	"n",
	"0",
	"-0",
	"007",
	"0644",
	"0x1f",
	"0o17",
	"1.0",
	"1e3",
	"-1.0e-3",
	".inf",
	"-.inf",
	".nan",
	"1:30",
	"2026-08-22",
	"12:34:56",
	"=",
	"<<",
	"'quoted'",
	"\"quoted\"",
	"a: b",
	"a #comment",
}

// TestMarshalledEscapedWhitespaceRoundTrips covers the whitespace end of the
// class, which the table above cannot reach.
//
// The scalars are written the way an author writes them — as YAML escapes — so
// what is under test is whether Marshal writes a description holding a tab, a
// newline or a trailing space back as something that reads as the same string.
// An emitter that drops the quotes here loses the whitespace silently: the
// document still parses, and the description is simply shorter.
func TestMarshalledEscapedWhitespaceRoundTrips(t *testing.T) {
	t.Parallel()

	for _, escaped := range []string{`\t`, `a\tb`, `\ttab`, ` `, `\n`, `a\nb`, `trailing `, ` leading`, `\r`, `a b`} {
		t.Run(escaped, func(t *testing.T) {
			t.Parallel()

			source := fmt.Sprintf(
				"edition: %s\nname: whitespace\ndescription: \"%s\"\nsteps:\n  - id: shown\n    log:\n      message: \"%s\"\n",
				flowfile.CurrentEdition, escaped, escaped)

			require.True(t, requireSourceRoundTrip(t, []byte(source)),
				"the document was refused, so the round trip asserted nothing:\n%s", source)
		})
	}
}
