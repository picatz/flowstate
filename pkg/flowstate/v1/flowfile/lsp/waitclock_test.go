package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The defect these tests pin (#319): the validator binds `now` in all three of
// a wait's expressions (`wait_until:`, an expression-valued `sleep:`, and a
// signal's `timeout:`) plus the signal's `outputs:` shaping, whose scope
// validateWait builds from the waiting one. The editor's two readers of that
// rule, parsedStep.bindsNow and completion's bindsClock line scan, answered for
// `wait_until:` (and shaping, in the model's case) only, so completion withheld
// a name the validator accepts and hover had nothing to say about it. nowDoc
// compounded it by claiming in prose that `wait_until:` is the only position.
//
// `wait_until:` itself is not re-tested here:
// TestNowIsExplainedTheSameWayTheValidatorRefusesIt (dsl_test.go) covers
// completion and hover there, and TestWaitUntilIsFirstClass covers hover on the
// clock in a wait.

// waitClockFile uses `now` in the two positions #319 found withheld (an
// expression-valued `sleep:` and a signal's `timeout:`) and in the signal's
// `outputs:` shaping, which validateWait scopes from the same waiting scope.
const waitClockFile = `edition: v2026.3
name: wait-clock
steps:
  - id: pause
    sleep: ${(now + seconds(30)) - now}
  - id: approval
    wait_for_signal:
      name: go
      timeout: ${(now + minutes(5)) - now}
      outputs:
        cutoff: "${now + days(1)}"
  - id: done
    log:
      message: waited
`

// TestWaitClockFileIsLegal is the premise: every claim below about what the
// editor should offer is only worth anything if the validator accepts the file
// that uses it.
func TestWaitClockFileIsLegal(t *testing.T) {
	t.Parallel()

	diags, err := flowfile.ValidateSource([]byte(waitClockFile))
	require.NoError(t, err)
	assert.Empty(t, diags, "the fixture is meant to be a file the engine accepts")
}

// TestCompletionOffersTheClockInEveryWaitExpression asks at each of the three
// positions the validator binds `now` beyond `wait_until:`.
func TestCompletionOffersTheClockInEveryWaitExpression(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///wait-clock.yaml"
	require.Empty(t, messages(c.open(uri, waitClockFile).Diagnostics),
		"premise: the fixture opens clean")

	for name, needle := range map[string]string{
		"an expression-valued sleep": "${(now + seconds(30)) - now}",
		"a signal's timeout":         "${(now + minutes(5)) - now}",
		"a signal's outputs shaping": "${now + days(1)}",
	} {
		t.Run(name, func(t *testing.T) {
			// Just past the fence, where a bare name is being typed.
			at := positionOf(t, waitClockFile, needle, len("${"))
			item := findItem(c.complete(uri, at.Line, at.Character).Items, v1.NowIdentifier)
			require.NotNil(t, item, "the clock is bound here by the validator and must be offered")
			assert.Equal(t, plainText(nowDoc()), item.Documentation,
				"completion and hover must give one account of the name")
		})
	}
}

// TestHoverDescribesTheClockInEveryWaitExpression is the same three positions,
// on the name once written.
func TestHoverDescribesTheClockInEveryWaitExpression(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.initialize()
	const uri = "file:///wait-clock-hover.yaml"
	require.Empty(t, messages(c.open(uri, waitClockFile).Diagnostics),
		"premise: the fixture opens clean")

	for name, needle := range map[string]string{
		"an expression-valued sleep": "now + seconds",
		"a signal's timeout":         "now + minutes",
		"a signal's outputs shaping": "now + days",
	} {
		t.Run(name, func(t *testing.T) {
			at := positionOf(t, waitClockFile, needle, 1) // inside `now`
			got := c.hover(uri, at.Line, at.Character)
			require.NotNil(t, got, "hover says nothing about a name the validator binds here")
			assert.Contains(t, hoverText(got), "the moment the wait is evaluated")
		})
	}
}

// TestCompletionStillOffersTheClockWhileTheDocumentDoesNotParse pins the line
// scan fallback: completion is asked for mid-edit, exactly when there is no
// parsed model to ask, so bindsClock has to answer the widened rule on its own.
// The later step's unterminated flow sequence keeps the whole document from
// parsing as YAML.
func TestCompletionStillOffersTheClockWhileTheDocumentDoesNotParse(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: mid-edit
steps:
  - id: pause
    sleep: ${
  - id: approval
    wait_for_signal:
      name: go
      timeout: ${
  - id: broken
    log: [unterminated
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///wait-clock-midedit.yaml"
	require.NotEmpty(t, messages(c.open(uri, src).Diagnostics),
		"premise: the document is mid-edit and must not open clean")

	for name, needle := range map[string]string{
		"an expression-valued sleep": "sleep: ${",
		"a signal's timeout":         "timeout: ${",
	} {
		t.Run(name, func(t *testing.T) {
			at := positionOf(t, src, needle, len(needle))
			item := findItem(c.complete(uri, at.Line, at.Character).Items, v1.NowIdentifier)
			assert.NotNil(t, item, "the line scan must offer the clock while the document does not parse")
		})
	}
}

// TestTheClockStopsAtTheWait is the negative direction, kept tested because it
// is the half that was already right: the validator refuses `now` where no
// clock exists, and offering or describing it there would walk an author into
// the diagnostic.
//
// The step-level `timeout:` is the nearest miss (the same word as a signal's,
// one level up, bounding one attempt rather than a wait), so it gets its own
// case alongside the task input.
func TestTheClockStopsAtTheWait(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: no-clock-here
steps:
  - id: fetch
    http:
      url: ${now}
  - id: after
    timeout: ${
    log:
      message: done
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///no-clock-here.yaml"
	c.open(uri, src)

	t.Run("a task input does not offer the clock", func(t *testing.T) {
		at := positionOf(t, src, "url: ${now}", len("url: ${"))
		assert.Nil(t, findItem(c.complete(uri, at.Line, at.Character).Items, v1.NowIdentifier),
			"completion offered the clock in a task input, where the validator refuses it")
	})

	t.Run("hover stays silent on the clock in a task input", func(t *testing.T) {
		// One character in, on the `n` of a name that is not bound here. The
		// diagnostic on the file says why; hover describing the clock would
		// contradict the squiggle.
		at := positionOf(t, src, "url: ${now}", len("url: ${")+1)
		assert.Nil(t, c.hover(uri, at.Line, at.Character),
			"hover described the clock in a task input, where the validator refuses it")
	})

	t.Run("the step's own timeout does not offer the clock", func(t *testing.T) {
		at := positionOf(t, src, "timeout: ${", len("timeout: ${"))
		assert.Nil(t, findItem(c.complete(uri, at.Line, at.Character).Items, v1.NowIdentifier),
			"the step-level timeout bounds one attempt and has no clock; only a signal's does")
	})
}

// TestNowDocNamesEveryPositionTheValidatorBinds keeps the prose from repeating
// #319: the sentence names all four positions the validator binds `now` in
// (validateWait's own three, plus the shaping scope it builds from `waiting` at
// validate.go:1889-1897), and the old wait_until-only claim stays out.
func TestNowDocNamesEveryPositionTheValidatorBinds(t *testing.T) {
	t.Parallel()

	doc := nowDoc()
	for _, position := range []string{
		"`" + waitUntilKey + ":`",
		"`" + sleepKey + ":`",
		"a signal's `" + signalTimeoutKey + ":`",
		"a signal's `" + taskShapingKey + ":` shaping",
	} {
		assert.Contains(t, doc, position,
			"nowDoc no longer names a position the validator binds the clock in")
	}
	assert.False(t, strings.Contains(doc, "Bound inside `wait_until:` and nowhere else"),
		"nowDoc repeats the wait_until-only claim #319 corrected")
}

// waitClockNestedShapingFile nests a value under a signal's `outputs:` shaping
// (block form: `outputs: { audit: { observed_at: ... } }`) rather than binding
// it directly. validateWait builds the shaping scope once, from the waiting
// one, and walks every entry the shaping mapping holds regardless of depth
// (validate.go:1889-1897), so the clock is bound just as much two levels down
// as it is at the top of `outputs:`.
const waitClockNestedShapingFile = `edition: v2026.3
name: wait-clock-nested
steps:
  - id: approval
    wait_for_signal:
      name: go
      outputs:
        audit:
          observed_at: ${now}
  - id: done
    log:
      message: waited
`

// TestWaitClockNestedShapingFileIsLegal is the premise for the nested-shaping
// tests below.
func TestWaitClockNestedShapingFileIsLegal(t *testing.T) {
	t.Parallel()

	diags, err := flowfile.ValidateSource([]byte(waitClockNestedShapingFile))
	require.NoError(t, err)
	assert.Empty(t, diags, "the fixture is meant to be a file the engine accepts")
}

// TestCompletionOffersTheClockInNestedShaping is spelling (a) of #330's second
// finding: a shaping value nested inside `outputs:` (not directly beneath it)
// still sits inside the wait's scope, and completion has to walk the whole
// keyPath rather than stop at an exact `[wait_for_signal, outputs]` suffix to
// see that. It is asked twice: once on a document that parses, where the
// scope also comes from bindsClock's keyPath (completion never asks the model
// for this decision, see [bindsClock]'s doc), and once on a document that
// does not, exercising the same fallback
// TestCompletionStillOffersTheClockWhileTheDocumentDoesNotParse pins.
func TestCompletionOffersTheClockInNestedShaping(t *testing.T) {
	t.Parallel()

	t.Run("a document that parses", func(t *testing.T) {
		t.Parallel()

		c := newClient(t)
		c.initialize()
		const uri = "file:///wait-clock-nested.yaml"
		require.Empty(t, messages(c.open(uri, waitClockNestedShapingFile).Diagnostics),
			"premise: the fixture opens clean")

		at := positionOf(t, waitClockNestedShapingFile, "${now}", len("${"))
		item := findItem(c.complete(uri, at.Line, at.Character).Items, v1.NowIdentifier)
		require.NotNil(t, item, "the clock is bound in a nested shaping value and must be offered")
		assert.Equal(t, plainText(nowDoc()), item.Documentation,
			"completion and hover must give one account of the name")
	})

	t.Run("a document that does not parse", func(t *testing.T) {
		t.Parallel()

		const src = `edition: v2026.3
name: wait-clock-nested-midedit
steps:
  - id: approval
    wait_for_signal:
      name: go
      outputs:
        audit:
          observed_at: ${
  - id: broken
    log: [unterminated
`
		c := newClient(t)
		c.initialize()
		const uri = "file:///wait-clock-nested-midedit.yaml"
		require.NotEmpty(t, messages(c.open(uri, src).Diagnostics),
			"premise: the document is mid-edit and must not open clean")

		at := positionOf(t, src, "observed_at: ${", len("observed_at: ${"))
		item := findItem(c.complete(uri, at.Line, at.Character).Items, v1.NowIdentifier)
		assert.NotNil(t, item, "the line scan must offer the clock in a nested shaping value while the document does not parse")
	})
}

// TestNestedOutputsOutsideAWaitDoNotBindTheClock is the negative direction for
// spelling (a): a nested map under an `outputs:` that is not a signal's
// shaping must not start matching just because the widened check now looks
// past the top level. A task input literally named `outputs` (the http task's
// own shaping key, [taskShapingKey]) with a nested map is the near miss, and
// the step-level `timeout:` from TestTheClockStopsAtTheWait is retested
// alongside it as the other near miss the widening must not touch.
func TestNestedOutputsOutsideAWaitDoNotBindTheClock(t *testing.T) {
	t.Parallel()

	const src = `edition: v2026.3
name: no-clock-in-nested-outputs
steps:
  - id: fetch
    http:
      url: https://example.com
      outputs:
        computed:
          when: ${
  - id: after
    timeout: ${
    log:
      message: done
`
	c := newClient(t)
	c.initialize()
	const uri = "file:///no-clock-in-nested-outputs.yaml"
	c.open(uri, src)

	t.Run("a task input named outputs with a nested map does not offer the clock", func(t *testing.T) {
		at := positionOf(t, src, "when: ${", len("when: ${"))
		assert.Nil(t, findItem(c.complete(uri, at.Line, at.Character).Items, v1.NowIdentifier),
			"completion offered the clock inside a task's own outputs shaping, which is not a wait")
	})

	t.Run("the step's own timeout still does not offer the clock", func(t *testing.T) {
		at := positionOf(t, src, "timeout: ${", len("timeout: ${"))
		assert.Nil(t, findItem(c.complete(uri, at.Line, at.Character).Items, v1.NowIdentifier),
			"the step-level timeout bounds one attempt and has no clock; only a signal's does")
	})
}

// waitClockBlockScalarFile writes `sleep:`'s expression as a block scalar, so
// the cursor's own line is a continuation line rather than the `sleep:` line
// itself. TestBlockScalarContinuationClock uses it to answer, rather than
// assume, what keyPath does with a continuation line: see that test's comment.
const waitClockBlockScalarFile = `edition: v2026.3
name: wait-clock-block-scalar
steps:
  - id: pause
    sleep: |-
      ${now + seconds(30)}
  - id: done
    log:
      message: waited
`

// TestBlockScalarContinuationClock is spelling (b) of #330's second finding: a
// block-scalar or multi-line value under sleep:/timeout: puts the cursor on a
// continuation line the current line has no `key:` on, so keyAndPosition
// returns "" for the key ([keyAndPosition]) and bindsClock's `steps`/
// `wait_for_signal` cases, which decide by comparing that key against
// waitUntilKey/sleepKey/signalTimeoutKey, cannot recognize the position no
// matter what path says.
//
// keyPath itself does keep walking past a continuation line to the enclosing
// keys (outline.go's keyPath skips any line scanKeyLine does not match and keeps
// looking for a shallower-indented one), so path here is ["steps", "sleep"]
// as if the query were a value directly under `sleep:`, but bindsClock's
// `steps` case tests the *key*, not the path's last element, so a continuation
// line under `sleep:` is left unrecognized regardless. This is unlike the
// nested-shaping fix above, which never depended on `key` at all.
//
// This is the gap the task called out and said not to fix by reworking the
// line scanner's indentation model: fixing it would mean bindsClock (or its
// caller) learning to fall back to path's last element when key is "", and
// deciding whether that is sound for every one of bindsClock's cases, not
// only this one. Filed as a follow-up; this test documents the gap rather
// than asserting a fix.
func TestBlockScalarContinuationClock(t *testing.T) {
	t.Skip("known gap, see issue TBD: bindsClock only reads the current line's own key, so a block-scalar continuation line under sleep:/timeout: has no key for it to compare and the clock is not offered there")

	c := newClient(t)
	c.initialize()
	const uri = "file:///wait-clock-block-scalar.yaml"
	require.Empty(t, messages(c.open(uri, waitClockBlockScalarFile).Diagnostics),
		"premise: the fixture opens clean")

	at := positionOf(t, waitClockBlockScalarFile, "${now + seconds(30)}", len("${"))
	item := findItem(c.complete(uri, at.Line, at.Character).Items, v1.NowIdentifier)
	assert.NotNil(t, item, "the clock is bound in a sleep: block scalar's continuation line and must be offered")
}
