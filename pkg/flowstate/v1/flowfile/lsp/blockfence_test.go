package lsp

import (
	"strings"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A whole expression written as a block scalar — `message: |-` with `${...}`
// under it — is the natural spelling for anything too long for one line, and
// examples/paged-fan-out ships one. The model used to hand those to consumers
// with `fenced` false, which turned off every expression service on them, so
// these tests are about the flag being set and about what may be built on it.
//
// See #306.

// TestCompilerEvaluatesABlockScalarFence is the premise the rest of this file
// rests on, asserted rather than assumed.
//
// The model used to say the opposite in a comment — "the compiler does not treat
// a multi-line scalar as an expression either, so nothing is lost" — and acted on
// it by skipping the fence scan. It was not true. [flowfile.SplitFence] runs on
// the *decoded* text, by which time a block scalar is one folded string like any
// other, so the engine evaluates these and the validator reports a broken one at
// the header's position. The model owes an author the same answer the compiler
// gives, which is why it now asks the same question.
func TestCompilerEvaluatesABlockScalarFence(t *testing.T) {
	t.Parallel()

	valid := []byte(`name: premise-ok
steps:
  - id: a
    vars:
      greeting: ${'hello'}
    log:
      message: |-
        ${greeting}
edition: v2026.2
`)
	diags, err := flowfile.ValidateSource(valid)
	require.NoError(t, err)
	assert.NoError(t, diags.Err(), "a block scalar holding a good expression must compile")

	// The other direction, which is the one that proves it is *evaluated* rather
	// than merely tolerated: broken CEL inside a block scalar fails the compiler.
	// Nothing but parsing the contents as an expression could produce this.
	broken := []byte(`name: premise-broken
steps:
  - id: a
    log:
      message: |-
        ${ "a" + }
edition: v2026.2
`)
	diags, err = flowfile.ValidateSource(broken)
	if err != nil {
		assert.Contains(t, err.Error(), "not a valid expression")
		return
	}
	require.Error(t, diags.Err(), "broken CEL inside a block scalar must be rejected")
	assert.Contains(t, diags.Error(), "not a valid expression")
}

// TestBlockScalarFenceIsCheckedAsCEL is the direction that was silent: a fence
// written as a block scalar is now parsed, so a broken one is reported.
//
// Where the squiggle lands depends on which block scalar it is. A literal one is
// not folded, so every byte of it is still a byte of the document and the error
// goes on the offending token. A folded one has had its breaks turned into
// spaces before the model saw the text, so the line and column CEL reports
// address the parser's copy — there the whole value is the finest honest answer.
func TestBlockScalarFenceIsCheckedAsCEL(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	t.Run("a literal block scalar with broken CEL is reported once", func(t *testing.T) {
		const src = `name: block-broken
steps:
  - id: a
    log:
      message: |-
        ${ 1 + + 2 }
edition: v2026.2
`
		got := c.open("file:///block-broken.yaml", src).Diagnostics
		require.Len(t, got, 1, "expected exactly one diagnostic, got %v", messages(got))
		assert.Equal(t, codeCELSyntax, got[0].Code)
		assert.Equal(t, lsp.Error, got[0].Severity)
		assert.Contains(t, got[0].Message, "Syntax error")
		// The break is in the middle of the expression rather than at its end,
		// and that is load-bearing. CEL reports an unfinished expression at the
		// last character, where the offset equals the end of the source and
		// reportCELErrors falls back to the whole range anyway — so a fixture
		// ending in `${ "a" + }` passes whether or not the mapping works at all.
		// An error CEL can place mid-source is what makes the difference visible.
		assert.Equal(t, spanOfText(t, src, "+ 2 }", 0, 1), got[0].Range,
			"the second `+` is the token CEL objects to, and it is what must be underlined")
	})

	t.Run("a literal block scalar is placed on the line the error is on", func(t *testing.T) {
		// The second line, which is the one a line-by-line mapping can get wrong
		// in every direction: one line too far, one column short by the width of
		// the `${` that only line one carries, or the whole thing collapsed onto
		// the header.
		const src = `name: block-broken-second-line
steps:
  - id: a
    log:
      message: |-
        ${ 1 +
           2 + + 3 }
edition: v2026.2
`
		got := c.open("file:///block-broken-second.yaml", src).Diagnostics
		require.Len(t, got, 1, "expected exactly one diagnostic, got %v", messages(got))
		assert.Equal(t, codeCELSyntax, got[0].Code)
		assert.Equal(t, spanOfText(t, src, "+ 3 }", 0, 1), got[0].Range)
		assert.Equal(t, 6, got[0].Range.Start.Line, "the error is on the expression's second line")
	})

	t.Run("a folded block scalar with broken CEL is reported once", func(t *testing.T) {
		// Folding is the half that stays coarse — the newline between the two
		// lines has become a space by the time the model sees the text, so the
		// second line's columns mean nothing in the document. One diagnostic over
		// the whole value is the answer, not two placed by arithmetic. This is
		// #310's behavior, pinned: giving literal scalars real positions must not
		// tempt the folded ones into borrowing them.
		const src = `name: fold-broken
steps:
  - id: a
    log:
      message: >-
        ${ 1 + +
           2 }
edition: v2026.2
`
		got := c.open("file:///fold-broken.yaml", src).Diagnostics
		require.Len(t, got, 1, "expected exactly one diagnostic, got %v", messages(got))
		assert.Equal(t, codeCELSyntax, got[0].Code)
		assert.Equal(t, ">-\n        ${ 1 + +\n           2 }", textInRange(src, got[0].Range))
	})

	t.Run("a valid block scalar fence is clean", func(t *testing.T) {
		const src = `name: block-ok
steps:
  - id: a
    vars:
      greeting: ${'hello'}
    log:
      message: |-
        ${greeting}
edition: v2026.2
`
		assert.Empty(t, messages(c.open("file:///block-ok.yaml", src).Diagnostics))
	})
}

// TestDeferredBlockScalarFenceStaysQuiet holds the false-positive direction shut.
//
// An input the task evaluates itself — http's `outputs:` — is expression source
// with no fence, and diagnostics parses those as CEL directly. A whole fence
// written as a block scalar used to fall into that branch unmarked and come back
// with two confident syntax errors about the `${` and the `}` on a file
// `flow validate` accepts. #305 closed it by declining any raw text carrying
// `${`; this closes it properly, by recognizing the fence and checking what is
// inside it.
//
// The fixture is examples/paged-fan-out's `outputs:` reduced to the shape that
// matters. That example remains the live proof — TestExamplesAreClean runs it.
func TestDeferredBlockScalarFenceStaysQuiet(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const src = `name: deferred-block
steps:
  - id: fetch
    http:
      method: GET
      url: https://example.com/records
      parse_json: true
      outputs: >-
        ${ {"records": response.json.records,
            "next_cursor": response.json.next_cursor} }
edition: v2026.2
`
	assert.Empty(t, messages(c.open("file:///deferred-block.yaml", src).Diagnostics),
		"a deferred input written as a block-scalar fence must not be squiggled")
}

// TestFoldedBlockScalarFenceHasNoInnerPositions is the negative direction on
// positions, and the assertion is nil.
//
// Marking a block scalar fenced says what it is, not where anything inside it is.
// Hover and go-to-definition both need to find the cursor within the expression
// source, and a *folded* scalar cannot answer that: the breaks became spaces
// before the model saw the text, so an offset into it resolves against the
// parser's copy and names whatever character happens to sit there in the
// document, somewhere on another line. So both decline.
//
// This is where #306 stops. A literal scalar has since gained real inner
// positions — see [TestLiteralBlockScalarFenceHasInnerPositions] — and this test
// is what keeps that from being taken as permission to compute one here. The
// silence is chosen, and it is chosen for a reason no later work removes: the
// information is gone by the time the model has the text.
func TestFoldedBlockScalarFenceHasNoInnerPositions(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const src = `name: block-hover
steps:
  - id: fetch
    http:
      method: GET
      url: https://example.com/records
      parse_json: true
      outputs: '${ {"records": response.json.records} }'
  - id: folded
    log:
      message: >-
        ${ steps.fetch.records }
  - id: inline
    log:
      message: ${ steps.fetch.records }
edition: v2026.2
`
	const uri = "file:///block-hover.yaml"
	require.Empty(t, messages(c.open(uri, src).Diagnostics), "the fixture must be a working file")

	// The positive control first, so the silence below cannot be explained by the
	// fixture being wrong: written on one line, the very same reference answers.
	inline := positionOf(t, src, "steps.fetch.records }\nedition", len("steps."))
	require.NotNil(t, c.hover(uri, inline.Line, inline.Character),
		"the inline spelling must still resolve, or this test proves nothing")
	require.Len(t, c.definition(uri, inline.Line, inline.Character), 1)

	// The same reference inside a folded block scalar, at the same offset into
	// the same word.
	folded := positionOf(t, src, "steps.fetch.records }\n  - id: inline", len("steps."))
	assert.Nil(t, c.hover(uri, folded.Line, folded.Character),
		"hover inside a folded block-scalar expression must answer nothing rather than a position it cannot compute")
	assert.Empty(t, c.definition(uri, folded.Line, folded.Character),
		"go-to-definition inside a folded block-scalar expression must not jump")
}

// TestLiteralBlockScalarFenceHasInnerPositions is the half of #306 that #310
// left open, and it is asserted the only way worth asserting: against the exact
// range the answer must carry, computed from the fixture.
//
// "Hover answered something" is not the claim. A mapping that is one line out, or
// short by the width of the `${` the first line carries and no other, answers
// something for every cursor in the block — it just describes the wrong name and
// underlines the wrong text, which is the failure this whole area is careful
// about. Only a byte-exact range can tell the two apart.
//
// The reference under test is on the expression's *second* line, deliberately.
// A mapping that ignores lines entirely still gets the first one right.
func TestLiteralBlockScalarFenceHasInnerPositions(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const src = `name: literal-positions
steps:
  - id: fetch
    http:
      method: GET
      url: https://example.com/records
      parse_json: true
      outputs: '${ {"records": response.json.records} }'
  - id: use
    log:
      message: |-
        ${ "count=" +
           string(size(steps.fetch.records)) }
edition: v2026.2
`
	const uri = "file:///literal-positions.yaml"
	require.Empty(t, messages(c.open(uri, src).Diagnostics), "the fixture must be a working file")

	// The cursor lands part-way into the reference, where an off-by-one in either
	// direction still resolves — the range is what catches it.
	at := positionOf(t, src, "steps.fetch.records)) }", len("steps.fe"))
	require.Equal(t, 12, at.Line, "the reference must be on the expression's second line")

	h := c.hover(uri, at.Line, at.Character)
	require.NotNil(t, h, "hover inside a literal block scalar must resolve")
	require.NotNil(t, h.Range)
	assert.Equal(t, spanOfText(t, src, "steps.fetch.records))", 0, len("steps.fetch.records")), *h.Range,
		"the hover must underline the reference itself, not the line and not the value")

	locations := c.definition(uri, at.Line, at.Character)
	require.Len(t, locations, 1, "go-to-definition inside a literal block scalar must jump")
	assert.Equal(t, lsp.DocumentURI(uri), locations[0].URI)
	assert.Equal(t, spanOfText(t, src, "id: fetch", len("id: "), len("fetch")), locations[0].Range,
		"the jump must land on the step's id, exactly")
}

// TestLiteralBlockScalarFirstLineColumn isolates the one shift the rest of the
// mapping cannot see.
//
// An expression's source begins *after* the `${`, so its line 1 starts two bytes
// into the value's line 1 while every later line starts where the value's does.
// Drop that and the second-line tests above still fail, but so does everything —
// which is exactly why the case wants a test that can only be about line one.
func TestLiteralBlockScalarFirstLineColumn(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const src = `name: literal-first-line
steps:
  - id: fetch
    http:
      method: GET
      url: https://example.com/records
      parse_json: true
      outputs: '${ {"records": response.json.records} }'
  - id: use
    log:
      message: |-
        ${ size(steps.fetch.records) > 0
           ? "some" : "none" }
edition: v2026.2
`
	const uri = "file:///literal-first-line.yaml"
	require.Empty(t, messages(c.open(uri, src).Diagnostics), "the fixture must be a working file")

	at := positionOf(t, src, "steps.fetch.records) >", len("steps.fe"))
	require.Equal(t, 11, at.Line, "the reference must be on the expression's first line")

	h := c.hover(uri, at.Line, at.Character)
	require.NotNil(t, h, "hover on the first line of a literal block scalar must resolve")
	require.NotNil(t, h.Range)
	assert.Equal(t, spanOfText(t, src, "steps.fetch.records) >", 0, len("steps.fetch.records")), *h.Range)
}

// TestLiteralBlockScalarIndentationVariants covers the two lines a literal
// scalar's decoder treats unlike its neighbours.
//
// A line indented deeper than the block keeps the extra spaces as *content* —
// they are part of the decoded text, so the mapping has to strip only what the
// block header claims and no more. A blank line has no indentation to strip at
// all, and a mapping that subtracts a fixed width from every line puts the rest
// of the expression off by that width from there on.
//
// Both are asserted through a position on the line *after* them, because that is
// where getting either wrong actually shows.
func TestLiteralBlockScalarIndentationVariants(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	t.Run("a line indented deeper than the block", func(t *testing.T) {
		const src = `name: literal-deep-indent
steps:
  - id: fetch
    http:
      method: GET
      url: https://example.com/records
      parse_json: true
      outputs: '${ {"records": response.json.records} }'
  - id: use
    log:
      message: |-
        ${ "count=" +
               string(size(steps.fetch.records)) }
edition: v2026.2
`
		const uri = "file:///literal-deep-indent.yaml"
		require.Empty(t, messages(c.open(uri, src).Diagnostics), "the fixture must be a working file")

		at := positionOf(t, src, "steps.fetch.records)) }", len("steps.fe"))
		h := c.hover(uri, at.Line, at.Character)
		require.NotNil(t, h, "the extra indentation is content, and the reference after it still resolves")
		require.NotNil(t, h.Range)
		assert.Equal(t, spanOfText(t, src, "steps.fetch.records))", 0, len("steps.fetch.records")), *h.Range)
	})

	t.Run("a blank line inside the expression", func(t *testing.T) {
		const src = `name: literal-blank-line
steps:
  - id: fetch
    http:
      method: GET
      url: https://example.com/records
      parse_json: true
      outputs: '${ {"records": response.json.records} }'
  - id: use
    log:
      message: |-
        ${ "count=" +

           string(size(steps.fetch.records)) }
edition: v2026.2
`
		const uri = "file:///literal-blank-line.yaml"
		require.Empty(t, messages(c.open(uri, src).Diagnostics), "the fixture must be a working file")

		at := positionOf(t, src, "steps.fetch.records)) }", len("steps.fe"))
		require.Equal(t, 13, at.Line, "the reference sits below the blank line")
		h := c.hover(uri, at.Line, at.Character)
		require.NotNil(t, h, "a blank line must not shift the lines under it")
		require.NotNil(t, h.Range)
		assert.Equal(t, spanOfText(t, src, "steps.fetch.records))", 0, len("steps.fetch.records")), *h.Range)
	})
}

// TestLiteralBlockScalarCursorOutsideTheExpression holds the edges of the
// mapping shut.
//
// The value's range covers whole lines — the header, and every content line from
// its first column — because a diagnostic falling back to it wants the generous
// answer. A cursor is a different question, and the two places inside that range
// that are not expression source must answer nothing: the `|-` itself, and the
// indentation the decoder stripped off a content line.
func TestLiteralBlockScalarCursorOutsideTheExpression(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	const src = `name: literal-edges
steps:
  - id: fetch
    http:
      method: GET
      url: https://example.com/records
      parse_json: true
      outputs: '${ {"records": response.json.records} }'
  - id: use
    log:
      message: |-
        ${ "count=" +
           string(size(steps.fetch.records)) }
edition: v2026.2
`
	const uri = "file:///literal-edges.yaml"
	require.Empty(t, messages(c.open(uri, src).Diagnostics), "the fixture must be a working file")

	header := positionOf(t, src, "|-\n", 1)
	assert.Nil(t, c.hover(uri, header.Line, header.Character),
		"the block header is not expression source and has nothing to describe")

	indent := positionOf(t, src, "   string(size(", 1)
	assert.Nil(t, c.hover(uri, indent.Line, indent.Character),
		"indentation the decoder stripped is not part of the expression")
}

// spanOfText returns the range covering length bytes at offset into the sole
// occurrence of needle in src.
//
// Uniqueness is required rather than assumed: a needle matching twice would
// silently compare against the wrong one of them, which is the same class of
// mistake as the positions these tests exist to check.
func spanOfText(t *testing.T, src, needle string, offset, length int) lsp.Range {
	t.Helper()
	at := strings.Index(src, needle)
	require.GreaterOrEqual(t, at, 0, "test source does not contain %q", needle)
	require.Equal(t, at, strings.LastIndex(src, needle), "test source contains %q more than once", needle)
	ix := newLineIndex(src)
	return ix.rangeOfOffsets(at+offset, at+offset+length)
}
