package lsp

import (
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
// The position is the whole value rather than the offending token, and that is
// deliberate. A block scalar reaches the model folded, so the line and column CEL
// reports address the parser's copy of the text and not the document — see
// [value.inline]. Underlining the value the author wrote is coarse and true;
// resolving the folded column would be precise and wrong.
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
		// last character, where `srcOffset + offsetInExpr(...)` equals the end of
		// the source and reportCELErrors falls back to the whole range anyway —
		// so a fixture ending in `${ "a" + }` passes whether or not the model
		// knows a block scalar has no inner positions. An error CEL can place
		// mid-source is what makes the difference visible.
		//
		// The range is the header line through the last content line, and no
		// further: the `edition:` key below it must stay out of the squiggle.
		assert.Equal(t, "|-\n        ${ 1 + + 2 }", textInRange(src, got[0].Range))
	})

	t.Run("a folded block scalar with broken CEL is reported once", func(t *testing.T) {
		// Folding is the harder half — the newline between the two lines has
		// become a space by the time the model sees the text, so the second
		// line's columns mean nothing in the document. One diagnostic over the
		// whole value is the answer, not two placed by arithmetic.
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

// TestBlockScalarFenceHasNoInnerPositions is the negative direction on positions,
// and the assertion is nil.
//
// Marking a block scalar fenced says what it is, not where anything inside it is.
// Hover and go-to-definition both need to find the cursor within the expression
// source, which folded text cannot answer: the offset would resolve against the
// parser's copy and name whatever character happens to sit there in the document,
// somewhere on another line. So both decline.
//
// This is the remaining half of #306 — expression services inside a block scalar
// need a position mapping the model does not have yet — and the test exists to
// record that the silence is chosen. If a later change gives block scalars real
// inner positions, this test should fail and be replaced by one asserting the
// right answer, never deleted to make room for a wrong one.
func TestBlockScalarFenceHasNoInnerPositions(t *testing.T) {
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

	// The same reference inside a block scalar, at the same offset into the same
	// word.
	folded := positionOf(t, src, "steps.fetch.records }\n  - id: inline", len("steps."))
	assert.Nil(t, c.hover(uri, folded.Line, folded.Character),
		"hover inside a block-scalar expression must answer nothing rather than a position it cannot compute")
	assert.Empty(t, c.definition(uri, folded.Line, folded.Character),
		"go-to-definition inside a block-scalar expression must not jump")
}
