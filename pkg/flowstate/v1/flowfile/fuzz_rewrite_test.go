package flowfile_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The two targets in this file fuzz the two rewriters — [flowfile.Fix] and
// [flowfile.Format] — over the properties their own documentation states, rather
// than over "did not panic".
//
// The distinction matters here more than anywhere else in this package. A
// rewriter that panics is a bad afternoon; a rewriter that returns bytes is a
// command that has already overwritten the author's file. CLAUDE.md's account of
// this package says `flow fix` corrupting a valid file is the worst thing this
// repository can do, and records that it has managed it twice — both times
// producing a document that still passed `flow validate`, because a whole-step
// reference with no output name is legal and the file simply computed something
// else. #339 found `flow fmt` dropping a digest pin and depending on walk order,
// #381 found it deleting comments, #382 found it writing a file out half-rewritten.
//
// Every one of those is invisible to a target whose only oracle is a crash, and
// every one of them is a property a fuzzer can check:
//
//   - a fixed point, because both commands are run in a loop by hand and in CI
//     (`flow fix . && git commit`), and a rewriter that never settles turns a
//     no-op run into a diff;
//   - all of the document or none of it, which is #382's contract, stated on
//     [flowfile.FixResult.Complete] and checkable directly;
//   - preservation, which is the corruption class: a document this build accepts
//     must still be one this build accepts after a rewrite, and — for Format,
//     which has a workflow to compare against — must still *mean* the same thing.
//
// # What these do not cover, deliberately
//
// Comment preservation (#381's contract) is not asserted. Deciding which bytes of
// an arbitrary fuzzed document are comments requires the same YAML parse Format
// itself does, and a cheaper approximation — scanning for lines beginning with
// `#` — reports a false failure on any document holding one inside a block scalar
// or a quoted string. A false crasher checked into testdata is worse than an
// uncovered property, because the next person to see it spends their afternoon on
// a document that was never wrong. That contract stays covered by the targeted
// tests in format_test.go, which know which comments they wrote.
//
// Neither target reaches `call:` resolution: [flowfile.Parse] is the bytes-only
// entry point, so a `call:` step is refused for having no location to resolve
// against, exactly as it is for every other caller that holds bytes rather than a
// path. That is a gap in what is fuzzed here and not a gap in the property —
// `call:` files are covered by callpin_test.go and formatpin_test.go.

// rewriteSeeds are the documents both targets start from.
//
// The set is chosen for what the fuzzer can reach *from* each one rather than for
// coverage on its own: mutation explores outward from a seed, so a seed set of
// well-formed current-edition files never reaches a migration, and a seed set of
// migrations never reaches the fixed-point question for a file that has nothing
// to fix. Both halves are here, along with the shapes each rewriter is documented
// as refusing, since a refusal path that is never entered is a contract
// ([flowfile.FixResult.Complete]) that is never checked.
var rewriteSeeds = []string{
	// Nothing to do: the smallest file this language admits, already current. The
	// property that matters for this one is that both rewriters leave it alone.
	`edition: v2026.3
name: hello
steps:
- id: hello
  log:
    message: hello world
`,
	// The same file with no edition at all, which is the migration `flow fix`
	// performs most often — it stamps one.
	`name: hello
steps:
- id: hello
  log:
    message: hello world
`,
	// Both older editions this build recognises and does not compile, so Fix has
	// a grammar to bring forward rather than only a stamp to write.
	`edition: 2026.1
name: legacy
steps:
- id: a
  log:
    message: hi
- id: b
  log:
    message: ${a.result}
`,
	`edition: v2026.2
name: previous
steps:
- id: a
  http:
    url: https://example.com
- id: b
  log:
    message: ${steps.a.body}
`,
	// The retired task keys, one seed each, because each has its own reader
	// (retiredEcho, retiredPrintf, retiredCEL) and its own refusal.
	`edition: 2026.1
name: retired-echo
steps:
- id: a
  echo:
    message: hello
- id: b
  log:
    message: ${a.result}
`,
	`edition: 2026.1
name: retired-printf
steps:
- id: a
  printf:
    format: "%s"
    args: [hi]
- id: b
  log:
    message: ${a.result}
`,
	`edition: 2026.1
name: retired-cel
steps:
- id: a
  cel:
    expr: "1 + 1"
- id: b
  log:
    message: ${string(a.result)}
`,
	// A retired step nothing reads, which is the shape Fix refuses rather than
	// guesses at — and so the shape that exercises "all of the document, or none
	// of it" on a document that also had a real edit to make.
	`edition: 2026.1
name: refused
steps:
- id: a
  echo:
    message: nobody reads this
- id: b
  log:
    message: hi
`,
	// The four names the *grammar* binds bare, all legal alongside a step of the
	// same id. CLAUDE.md records these as the second time `flow fix` corrupted a
	// valid file: each was rewritten into a reference to the step. A seed rather
	// than a note, so the fuzzer mutates around them.
	`edition: 2026.1
name: bound-names
steps:
- id: item
  log:
    message: a step called item
- id: loop
  for_each:
    items: [1, 2]
    steps:
    - id: body
      log:
        message: ${string(item)}
- id: named
  for_each:
    items: [1, 2]
    as: item
    steps:
    - id: inner
      log:
        message: ${string(item)}
- id: n
  vars:
    n: 1
  log:
    message: ${string(n)}
`,
	// Comments in every position Format has to carry one from: above a key,
	// beside a value, at the top of the document, and inside a nested block.
	`# a workflow
edition: v2026.3
name: commented # beside the name
steps:
# above the step
- id: a
  log:
    message: hi # beside the value
`,
	// Flow style, which Fix refuses for having no line structure to rewrite.
	`edition: 2026.1
name: flow-style
steps: [{id: a, log: {message: hi}}]
`,
	// Control flow, so the fuzzer can reach a loop body and a parallel branch —
	// the positions a round trip is most easily lost in.
	`edition: v2026.3
name: control
steps:
- id: loop
  for_each:
    items: ${['a', 'b']}
    as: n
    max_parallel: 2
    steps:
    - id: body
      log:
        message: ${n}
- id: fan
  parallel:
  - steps:
    - id: left
      log:
        message: left
  - steps:
    - id: right
      log:
        message: right
`,
	// Policy, conditions in both spellings, and a wait — the keys that carry
	// durations and expressions Marshal has to render back.
	`edition: v2026.3
name: policy
steps:
- id: a
  if: ${gate.result == 'go'}
  timeout: 30s
  retry:
    attempts: 3
    interval: 1s
    backoff: 2
    max_interval: 10s
  continue_on_error: true
  log:
    message: go
- id: b
  if: a.result != ''
  wait:
    until: ${now > timestamp('2026-01-01T00:00:00Z')}
  log:
    message: waited
`,
}

// FuzzFixIdempotent fuzzes [flowfile.Fix] over the three properties its own
// documentation states, on whatever document a fuzzer builds from a real one.
//
// The properties, and what each one is worth:
//
//  1. **A fixed point.** Running Fix on what Fix wrote reports no further change
//     and returns the same bytes. Fix already loops internally to a fixed point
//     (maxFixRounds, with a refusal for a document two rules are fighting over),
//     so this asserts that the loop's answer is stable across a *fresh* call —
//     which is the shape `flow fix . && git commit` and the pre-commit hook
//     actually run in, and the shape a rule added tomorrow could break without
//     the internal loop noticing.
//
//  2. **All of the document, or none of it** — #382, stated on
//     [flowfile.FixResult.Complete]. An incomplete run must return the bytes it
//     was handed, byte for byte. This is the one property whose failure writes a
//     half-migrated file over an author's work.
//
//  3. **Preservation.** A document this build validates must still validate after
//     a rewrite. This is the corruption class CLAUDE.md records twice, and the
//     reason it is stated as "still validates" rather than "still parses": both
//     historical corruptions produced documents that parsed and that meant
//     something else. It is a necessary condition, not a sufficient one — a
//     rewrite that swaps two step ids passes it — which is why the seed set above
//     carries the bound-name shapes that produced the real defect rather than
//     relying on the property alone to find them.
//
// Not asserted: anything about the *content* of Changes, Refusals or Notes. There
// is no oracle for what an arbitrary fuzzed document should have been told about
// itself, and asserting a count is how a test starts failing for edits that are
// improvements.
func FuzzFixIdempotent(f *testing.F) {
	for _, seed := range rewriteSeeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		data := []byte(input)

		first, err := flowfile.Fix(data)
		if err != nil {
			// Not YAML at all, past the byte bound, or a document two rules
			// could not settle on. An error is an ordinary answer here — what
			// matters is that nothing was written, which is the caller's
			// contract in cmd/flow/fix.go: an error means the file is left
			// alone.
			return
		}

		// Property two, checked before the others because the bytes it is about
		// are the bytes the other two are measured over.
		if !first.Complete() {
			require.True(t, bytes.Equal(first.Source, data),
				"an incomplete fix returned rewritten bytes; a refusal must hand back the document as it arrived\n--- in ---\n%s\n--- out ---\n%s",
				data, first.Source)
		}

		// Property one. A document Fix wrote is one Fix must be able to read: an
		// error on the second pass would mean the first wrote something this
		// build cannot parse, which is the same defect as writing something it
		// cannot validate.
		second, err := flowfile.Fix(first.Source)
		require.NoError(t, err, "fix produced a document fix cannot read back\n--- written ---\n%s", first.Source)
		require.True(t, bytes.Equal(first.Source, second.Source),
			"fix is not a fixed point: a second pass rewrote what the first wrote\n--- first ---\n%s\n--- second ---\n%s",
			first.Source, second.Source)
		require.False(t, second.Changed(),
			"fix reported changes on a document it had already fixed\n--- document ---\n%s", first.Source)

		// Property three. Measured only where there is something to preserve: a
		// document that does not validate before the rewrite has no claim on
		// validating after it, and most fuzzed input is in that state.
		before, err := flowfile.ValidateSource(data)
		if err != nil || len(before) > 0 {
			return
		}
		after, err := flowfile.ValidateSource(first.Source)
		require.NoError(t, err,
			"fix turned a valid document into one that does not parse\n--- before ---\n%s\n--- after ---\n%s",
			data, first.Source)
		require.Empty(t, after,
			"fix turned a valid document into one that does not validate\n--- before ---\n%s\n--- after ---\n%s\n--- diagnostics ---\n%v",
			data, first.Source, after)
	})
}

// FuzzFormatIdempotent fuzzes [flowfile.Format] — what `flow fmt` writes — over
// the three properties a formatter owes the file it overwrites.
//
// Format takes the source and the workflow compiled from it, so unlike Fix it has
// an oracle for meaning: the workflow. That is what makes property two here
// stronger than Fix's "still validates".
//
//  1. **The output compiles.** Bytes `flow fmt` writes over an author's file must
//     be bytes this build reads back.
//
//  2. **It says the same thing.** The workflow compiled from the formatted
//     document is proto-equal to the one compiled from the source. A formatter
//     that renders a value into something that parses as a *different* value —
//     an unescaped `${` in a description, #339's dropped digest pin — passes
//     property one and fails here.
//
//  3. **A fixed point.** Formatting the formatted document returns it unchanged,
//     so `flow fmt --check` in CI does not fail on a file `flow fmt` just wrote.
//
// [FuzzMarshalRoundTrip] asserts the same three properties over [flowfile.Marshal]
// with a fuzzer-chosen scalar in a fixed document. This is the other axis: the
// document is what varies, and the function under test is Format rather than
// Marshal — which is to say, the comment- and pin-carrying path that Marshal does
// not have and that #339, #381 and #640 all lived in. Marshal's output is the
// common case Format returns directly (a document with no comments and no pins);
// everything past that is this target's own ground.
//
// The first thing it found is committed beside it as the corpus entry
// `comment_folded_into_key`: `name: #` with the scalar continuing on the next
// line, which the emitter wrote back as `name #: A0` — the comment folded into
// the key, a document that no longer parses (#860). It ran through this target
// as a refusal for as long as #862's answer stood; it now runs through all three
// properties, because the comment is written after the value instead
// (`name: A0 #`). The named regression tests are, in format_test.go,
// TestFormatWritesAKeyLineCommentAfterTheValueWhereTheKeyHasNoRoom for that
// rendering, TestFormatRefusesTwoCommentsThatWouldShareOneSlot for the position
// still refused, and TestFormatKeepsTheCommentPositionsAroundTheFoldingOne for
// the neighbours — all byte-exact, since a placement that moved one comment too
// far would look identical here.
//
// An error from Format is an ordinary answer and not asserted against: a workflow
// this build compiled but cannot write back — a literal holding `${`, an
// expression written with a macro cel-go cannot unparse, a comment the rewrite
// cannot keep — is a refusal, and `flow fmt` reports it and leaves the file alone.
// The property that matters about a refusal is that nothing is written, which is
// the command's structure rather than this function's return.
func FuzzFormatIdempotent(f *testing.F) {
	for _, seed := range rewriteSeeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, input string) {
		data := []byte(input)

		// Bytes-only, as [flowfile.Parse] is: a document the compiler refuses has
		// no workflow to render and is not this target's subject.
		workflow, _, err := flowfile.Parse(data)
		if err != nil {
			return
		}

		formatted, err := flowfile.Format(data, workflow)
		if err != nil {
			return
		}

		// Property one.
		reparsed, _, err := flowfile.Parse(formatted)
		require.NoError(t, err,
			"the formatted document does not parse\n--- source ---\n%s\n--- formatted ---\n%s", data, formatted)

		// Property two. Proto equality rather than a byte comparison of the two
		// documents, because formatting is *allowed* to move bytes around — that
		// is what it is for — and is not allowed to change what they mean.
		require.True(t, proto.Equal(workflow, reparsed),
			"formatting changed what the document says\n--- source ---\n%s\n--- formatted ---\n%s\n--- before ---\n%v\n--- after ---\n%v",
			data, formatted, workflow, reparsed)

		// Property three.
		again, err := flowfile.Format(formatted, reparsed)
		require.NoError(t, err,
			"a formatted document could not be formatted again\n--- formatted ---\n%s", formatted)
		require.Equal(t, string(formatted), string(again),
			"formatting is not a fixed point\n--- once ---\n%s\n--- twice ---\n%s", formatted, again)
	})
}
