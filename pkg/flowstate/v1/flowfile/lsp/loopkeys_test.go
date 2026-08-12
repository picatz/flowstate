package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A `loop:` block's `init:`, `until:` and `update:` are expressions, and for as
// long as issue #306 stood open the model deliberately left them out of the
// expression walk: every consumer assumed the *step's* scope, and those three
// are evaluated in scopes no other position has. These tests are the walk with
// the scopes modeled — the engine's rules, mirrored from
// pkg/flowstate/v1/loop.go and flowfile/validate.go:
//
//   - `init:` sees the enclosing scope only. It is defining the carried state,
//     so the name is not bound in it, and no body step has run.
//   - `until:` and `update:` run after the body each iteration: they see the
//     enclosing scope, the carried state bare (only when `as:` names one), and
//     the body's top-level steps.
//
// Per the house rule, the negative directions are the tests that matter: a
// binding described where the engine refuses it is the confidently-wrong
// answer the old comment in parse.go warned about.

// loopKeysFile is a legal Flowfile whose loop carries state and whose `until:`
// and `update:` read both the state and a body step — the whole after-body
// scope, exercised from a file `flow validate` accepts.
const loopKeysFile = `edition: v2026.3
name: loop-keys
steps:
  - id: paginate
    loop:
      as: cursor
      init: ${'start'}
      until: ${steps.page.body == 'done' && cursor != ''}
      update: ${steps.page.body}
      max_iterations: 5
      steps:
        - id: page
          http:
            method: GET
            url: ${'https://example.invalid/' + cursor}
  - id: after
    log:
      message: ${steps.paginate.state}
`

// TestTheLoopKeysFileIsLegal is the premise: every claim below about what the
// editor says over this file is only worth anything if the engine accepts it.
func TestTheLoopKeysFileIsLegal(t *testing.T) {
	t.Parallel()

	diags, err := flowfile.ValidateSource([]byte(loopKeysFile))
	require.NoError(t, err)
	assert.Empty(t, diags, "the fixture is meant to be a file the engine accepts")
}

// TestBrokenCELInLoopKeysIsReportedAtItsPosition is the squiggle: a syntax
// error inside each of the three keys produces exactly one diagnostic, at the
// character at fault rather than on the step or at the top of the file.
//
// The broken token is a stray identifier after a complete expression — the one
// shape of CEL syntax error that is a single error at a single position —
// sitting mid-expression and mid-file, so a diagnostic landing there cannot be
// the error-position fallback that masked bad offsets in #310.
func TestBrokenCELInLoopKeysIsReportedAtItsPosition(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	for _, tc := range []struct {
		key   string
		value string
	}{
		{"init", "${'a' extra}"},
		{"until", "${cursor extra}"},
		{"update", "${cursor extra}"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			src := strings.Replace(loopKeysFile,
				tc.key+": "+lineValueOf(t, loopKeysFile, tc.key),
				tc.key+": "+tc.value, 1)
			require.NotEqual(t, loopKeysFile, src, "the fixture no longer holds the key this case rewrites")

			uri := "file:///loop-keys-broken-" + tc.key + ".yaml"
			got := c.open(uri, src).Diagnostics
			require.Len(t, got, 1,
				"one broken expression is one mistake; %d diagnostics were reported: %v", len(got), got)

			d := got[0]
			assert.Equal(t, "cel-syntax", d.Code)
			at := positionOf(t, src, "extra}", 0)
			assert.Equal(t, at, d.Range.Start,
				"the squiggle does not start at the token CEL refused")
			assert.Equal(t, "extra", textInRange(src, d.Range),
				"the squiggle does not cover exactly the offending token")
			require.Positive(t, d.Range.Start.Line, "the fixture puts the broken key mid-file; line 1 means the position fell back")
		})
	}
}

// TestHoverInLoopUntilAndUpdateSeesTheAfterBodyScope is the positive half of
// the scope: the carried state answers with the carried-value wording, and a
// body step reference answers with the step's output.
func TestHoverInLoopUntilAndUpdateSeesTheAfterBodyScope(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///loop-keys.yaml"
	c.open(uri, loopKeysFile)

	// The bare `cursor` inside `until:`, two units past its start.
	at := positionOf(t, loopKeysFile, "cursor != ''", 2)
	h := c.hover(uri, at.Line, at.Character)
	require.NotNil(t, h, "hover says nothing about the carried state inside until:")
	assert.Contains(t, hoverText(h), "the value the `paginate` loop carries between iterations",
		"the carried state is not described as the value a loop: carries")

	// The body step reference inside `update:`.
	at = positionOf(t, loopKeysFile, "update: ${steps.page.body}", len("update: ${steps.")+1)
	h = c.hover(uri, at.Line, at.Character)
	require.NotNil(t, h, "hover says nothing about a body step reference inside update:")
	assert.Contains(t, hoverText(h), "Output of step `page`",
		"the reference is not described as the body step's own output")
}

// TestDefinitionFromLoopUntilLandsOnTheBodyStep follows `steps.page` written in
// `until:` back to the body step that produces it.
func TestDefinitionFromLoopUntilLandsOnTheBodyStep(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///loop-keys-def.yaml"
	c.open(uri, loopKeysFile)

	at := positionOf(t, loopKeysFile, "until: ${steps.page.body", len("until: ${steps.")+1)
	locs := c.definition(uri, at.Line, at.Character)
	require.Len(t, locs, 1, "a body step reference in until: does not resolve to a definition")
	assert.Equal(t, "page", textInRange(loopKeysFile, locs[0].Range),
		"the definition is not the body step's own id")
	line := positionOf(t, loopKeysFile, "- id: page", 0).Line
	assert.Equal(t, line, locs[0].Range.Start.Line, "the definition does not land on the body step's declaration")
}

// TestCompletionInLoopUntilOffersTheAfterBodyScope covers the third consumer:
// the menu inside `until:` holds the carried state bare and the body step under
// the root.
func TestCompletionInLoopUntilOffersTheAfterBodyScope(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///loop-keys-complete.yaml"
	c.open(uri, loopKeysFile)

	// Bare position: just inside the fence of until:'s value.
	at := positionOf(t, loopKeysFile, "until: ${steps", len("until: ${"))
	items := c.complete(uri, at.Line, at.Character).Items
	var carried bool
	for _, item := range items {
		if item.Label == "cursor" && item.Detail == "carried value" {
			carried = true
		}
	}
	assert.True(t, carried, "the carried state is not offered bare inside until:; got %v", labels(items))

	// After the root: the body's top-level step id.
	at = positionOf(t, loopKeysFile, "until: ${steps.page", len("until: ${steps."))
	assert.Contains(t, labels(c.complete(uri, at.Line, at.Character).Items), "page",
		"the body's top-level step is not offered under the root inside until:")
}

// loopKeysNegativeFile puts references where the engine refuses them: the
// carried state and a body step read from `init:`, and a body step read from a
// step after the loop. It does not validate — that is the point — but it
// parses, which is the level hover and definition answer at.
const loopKeysNegativeFile = `edition: v2026.3
name: loop-keys-negative
steps:
  - id: paginate
    loop:
      as: cursor
      init: ${cursor + steps.page.body}
      until: ${steps.page.body == 'done'}
      update: ${steps.page.body}
      max_iterations: 5
      steps:
        - id: page
          http:
            method: GET
            url: https://example.invalid/x
  - id: after
    log:
      message: ${steps.page.body}
`

// TestLoopInitDoesNotSeeTheStateOrTheBody is negative direction one, and the
// confidently-wrong answer it keeps out is the one the old comment in parse.go
// warned about: `init:` is *defining* the carried state, so describing the name
// there as the carried value would document a binding the engine refuses — and
// no body step has run when `init:` is evaluated.
func TestLoopInitDoesNotSeeTheStateOrTheBody(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///loop-keys-negative.yaml"
	c.open(uri, loopKeysNegativeFile)

	// The carried name inside init: gets no carried-value answer.
	at := positionOf(t, loopKeysNegativeFile, "init: ${cursor", len("init: ${")+1)
	if h := c.hover(uri, at.Line, at.Character); h != nil {
		assert.NotContains(t, hoverText(h), "carries between iterations",
			"init: described the state it is defining as though it already existed")
	}

	// A body step reference inside init: resolves to nothing, in both surfaces.
	at = positionOf(t, loopKeysNegativeFile, "init: ${cursor + steps.page", len("init: ${cursor + steps.")+1)
	assert.Nil(t, c.hover(uri, at.Line, at.Character),
		"init: described a body step that has not run when init: is evaluated")
	assert.Empty(t, c.definition(uri, at.Line, at.Character),
		"init: navigated to a body step that has not run when init: is evaluated")
}

// TestAStepAfterTheLoopStillCannotSeeBodySteps is the control: widening
// visibility for the loop's own keys must not widen it for anything else. Body
// outputs do not escape the loop, and this direction already held before #306's
// residual was fixed — it is here so a regression has a test to fail.
func TestAStepAfterTheLoopStillCannotSeeBodySteps(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///loop-keys-after.yaml"
	c.open(uri, loopKeysNegativeFile)

	at := positionOf(t, loopKeysNegativeFile, "message: ${steps.page.body}", len("message: ${steps.")+1)
	assert.Nil(t, c.hover(uri, at.Line, at.Character),
		"a step after the loop described a body step whose outputs do not escape it")
	assert.Empty(t, c.definition(uri, at.Line, at.Character),
		"a step after the loop navigated to a body step whose outputs do not escape it")
}

// loopNoAsFile is a stateless loop. Its `until:` reads the body step — which is
// legal — and the name `item`, which is not: a `loop:` without `as:` binds
// nothing at all, unlike a `for_each`, whose missing `as:` falls back to
// `item`. The file does not validate, deliberately; parsing is the level these
// surfaces answer at.
const loopNoAsFile = `edition: v2026.3
name: loop-no-as
steps:
  - id: poll
    loop:
      until: ${steps.probe.body == 'ready' && item == ''}
      max_iterations: 5
      steps:
        - id: probe
          http:
            method: GET
            url: https://example.invalid/health
`

// TestALoopWithoutAsBindsNoStateInUntil is negative direction two: the
// after-body scope of a stateless loop is the enclosing scope plus the body's
// steps and *nothing else*. Sharing `for_each`'s `item` fallback here would
// describe and offer a name the validator rejects.
func TestALoopWithoutAsBindsNoStateInUntil(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///loop-no-as.yaml"
	c.open(uri, loopNoAsFile)

	// `item` in until: is not a binding: no hover, no definition.
	at := positionOf(t, loopNoAsFile, "item == ''", 1)
	assert.Nil(t, c.hover(uri, at.Line, at.Character),
		"until: described a state binding on a loop that carries nothing")
	assert.Empty(t, c.definition(uri, at.Line, at.Character),
		"until: navigated a state binding on a loop that carries nothing")

	// Completion inside until: offers no carried value either — while the body
	// step stays reachable, which is what "nothing extra beyond body steps" means.
	at = positionOf(t, loopNoAsFile, "until: ${steps", len("until: ${"))
	items := c.complete(uri, at.Line, at.Character).Items
	for _, item := range items {
		assert.NotEqual(t, "carried value", item.Detail,
			"completion offered a carried value (%q) on a loop that carries nothing", item.Label)
	}
	assert.NotContains(t, labels(items), v1.DefaultIterator,
		"completion offered for_each's fallback iterator on a loop, which binds nothing without as:")

	at = positionOf(t, loopNoAsFile, "until: ${steps.probe", len("until: ${steps."))
	assert.Contains(t, labels(c.complete(uri, at.Line, at.Character).Items), "probe",
		"the body step must stay reachable from a stateless loop's until:")
}

// loopCollisionFile names its carried state `join`, which is also a profile
// function — the exact collision the old comment in parse.go warned would be
// answered confidently and wrongly if the loop's keys were walked with the
// step's own scope. A later step calls the real `join`.
const loopCollisionFile = `edition: v2026.3
name: loop-collision
steps:
  - id: gather
    loop:
      as: join
      init: ${'go'}
      until: ${join == 'done'}
      update: ${join + 'x'}
      max_iterations: 5
      steps:
        - id: emit
          log:
            message: ${join}
  - id: report
    log:
      message: ${['a', 'b'].join('-')}
`

// TestTheLoopCollisionFileIsLegal is the premise again: the collision is only
// worth testing if the engine accepts a state named like a function.
func TestTheLoopCollisionFileIsLegal(t *testing.T) {
	t.Parallel()

	diags, err := flowfile.ValidateSource([]byte(loopCollisionFile))
	require.NoError(t, err)
	assert.Empty(t, diags, "the fixture is meant to be a file the engine accepts")
}

// TestACarriedNameShadowsAFunctionOnlyWhereItIsBound is both directions of the
// collision guard. Inside `until:`/`update:` the binding is what the author
// wrote and the function of the same spelling is a coincidence, so the binding
// wins; outside the loop the binding does not exist, so the function answers.
func TestACarriedNameShadowsAFunctionOnlyWhereItIsBound(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()

	uri := "file:///loop-collision.yaml"
	c.open(uri, loopCollisionFile)

	for _, needle := range []string{"until: ${join", "update: ${join"} {
		at := positionOf(t, loopCollisionFile, needle, len(needle)-2)
		h := c.hover(uri, at.Line, at.Character)
		require.NotNil(t, h, "hover says nothing about the carried state at %q", needle)
		text := hoverText(h)
		assert.Contains(t, text, "the value the `gather` loop carries between iterations",
			"at %q the binding must win over the function of the same spelling", needle)
		assert.NotContains(t, text, "library",
			"at %q the carried state was described as the profile function", needle)
	}

	// Outside the loop the same spelling is the function, and nothing else.
	at := positionOf(t, loopCollisionFile, ".join('-')", 2)
	h := c.hover(uri, at.Line, at.Character)
	require.NotNil(t, h, "hover says nothing about the real join function outside the loop")
	text := hoverText(h)
	assert.Contains(t, text, "library",
		"outside the loop, `join` is the profile function and must be described as one")
	assert.NotContains(t, text, "carries between iterations",
		"outside the loop there is no carried state for `join` to be")
}

// lineValueOf returns the value written after `key: ` on the line declaring
// key, so a test can rewrite one key of a shared fixture without keeping a
// second copy of it.
func lineValueOf(t *testing.T, src, key string) string {
	t.Helper()
	for _, line := range strings.Split(src, "\n") {
		trimmed := strings.TrimSpace(line)
		if rest, ok := strings.CutPrefix(trimmed, key+": "); ok {
			return rest
		}
	}
	t.Fatalf("fixture has no %s: line", key)
	return ""
}
