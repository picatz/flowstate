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
const waitClockFile = `edition: v2026.2
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

	const src = `edition: v2026.2
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

	const src = `edition: v2026.2
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
// #319: the sentence names all three of a wait's expressions, and the old
// wait_until-only claim stays out.
func TestNowDocNamesEveryPositionTheValidatorBinds(t *testing.T) {
	t.Parallel()

	doc := nowDoc()
	for _, position := range []string{
		"`" + waitUntilKey + ":`",
		"`" + sleepKey + ":`",
		"a signal's `" + signalTimeoutKey + ":`",
	} {
		assert.Contains(t, doc, position,
			"nowDoc no longer names a position the validator binds the clock in")
	}
	assert.False(t, strings.Contains(doc, "Bound inside `wait_until:` and nowhere else"),
		"nowDoc repeats the wait_until-only claim #319 corrected")
}
