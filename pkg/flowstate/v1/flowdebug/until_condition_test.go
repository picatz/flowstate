package flowdebug_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// `until <step-id> if <expr>` is the one-shot spelling of what `break x if e`
// plus `continue` already composes: run to this step, but only where the
// condition holds. #1111's design deferred it until `break`'s shape survived
// review, on the condition that it share the parse and the evaluation — so
// these tests are about the sharing as much as the feature: every refusal,
// notice and evaluation below must be the breakpoint's own, reached through
// `until`.

// TestAConditionalUntilStopsAtTheIterationItNames is the traversal claim,
// the same one the conditional breakpoint makes: twenty iterations, one stop,
// at the one the condition names — without arming anything that outlives it.
func TestAConditionalUntilStopsAtTheIterationItNames(t *testing.T) {
	t.Parallel()

	// One `continue`: the until stop is the only stop, and leaving it lets
	// the loop finish.
	out, ran := loopingRun(t, 20, "until body if n == 7\ncontinue\n")

	assert.Len(t, ran, 20, "every iteration still runs; the condition decides stopping, not running")
	assert.Equal(t, 1, strings.Count(out, "break at body"),
		"stopped once, at the iteration the condition named — not on all twenty")
}

// TestAConditionalUntilThatNeverHoldsRunsToTheEnd is the negative direction:
// an unmet condition means the run completes with no stop at the named step,
// exactly as `continue` past an unmet breakpoint does.
func TestAConditionalUntilThatNeverHoldsRunsToTheEnd(t *testing.T) {
	t.Parallel()

	out, ran := loopingRun(t, 5, "until body if n == 99\n")

	assert.Len(t, ran, 5)
	assert.NotContains(t, out, "break at body",
		"a condition no iteration satisfies is an `until` that never fires")
}

// TestAConditionalUntilIsOneShot pins the difference between `until … if` and
// a conditional breakpoint: the condition goes with the resume that spent it.
// Three of the six iterations satisfy `n >= 3`, and the run stops at exactly
// one of them — the first — because the `continue` typed there cleared the
// condition along with the mode.
func TestAConditionalUntilIsOneShot(t *testing.T) {
	t.Parallel()

	out, ran := loopingRun(t, 6, "until body if n >= 3\ncontinue\n")

	assert.Len(t, ran, 6)
	assert.Equal(t, 1, strings.Count(out, "break at body"),
		"an `until` condition is one-shot; a later iteration satisfying it is not a stop nobody asked for")
}

// TestAnUntilConditionThatErrorsDoesNotHoldTheRun gives `until` the same
// declined-arrival treatment the breakpoint settled on in #1116: not stopping,
// but never silently — once, in the verb's own name.
func TestAnUntilConditionThatErrorsDoesNotHoldTheRun(t *testing.T) {
	t.Parallel()

	out, ran := loopingRun(t, 3, "until body if n.missing\n")

	assert.Len(t, ran, 3)
	assert.NotContains(t, out, "break at body",
		"an unanswerable condition does not hold the run")
	assert.Contains(t, out, "until body: the condition could not be evaluated here",
		"and the notice names `until` as the asker, not a breakpoint nobody set")
	assert.Equal(t, 1, strings.Count(out, "could not be evaluated here"),
		"once per verb, not once per iteration")
}

// TestAnUntilWithAMalformedConditionIsRefusedWhenItIsTyped: compiled when
// `until` accepts it, through the same compiler as `break`, so a typo is a
// refusal at the prompt rather than a run that quietly goes to the end.
func TestAnUntilWithAMalformedConditionIsRefusedWhenItIsTyped(t *testing.T) {
	t.Parallel()

	out, ran := loopingRun(t, 3, "until body if n ===\ncontinue\n")

	assert.Len(t, ran, 3, "the refusal keeps the session parked; the `continue` after it finishes the run")
	assert.Contains(t, out, "until body: parse condition",
		"refused in the verb's own name when it was typed")
	assert.NotContains(t, out, "break at body", "nothing was armed by a refused command")
}

// TestAnUntilWithAnEmptyConditionIsRefusedInItsOwnGrammar: `until body if `
// must not degrade into an unconditional `until` — the same silent-generosity
// failure splitCondition was built to refuse for `break` — and the correction
// must spell `until`, not `break`.
func TestAnUntilWithAnEmptyConditionIsRefusedInItsOwnGrammar(t *testing.T) {
	t.Parallel()

	out, ran := loopingRun(t, 3, "until body if   \ncontinue\n")

	assert.Len(t, ran, 3)
	assert.Contains(t, out, "until <step-id> [if <expr>]",
		"the usage the author reads is the verb they typed")
	assert.NotContains(t, out, "break <step-id>",
		"a correction quoting another verb's grammar is a wrong answer that looks helpful")
}

// TestAnUnconditionalUntilAfterAConditionalOneIsUnconditional pins the
// clearing: the condition travels with its own resume and no further. A
// leaked condition would gate the plain `until body` typed after it — a stop
// the author asked for that never comes, in a run that ends looking finished.
func TestAnUnconditionalUntilAfterAConditionalOneIsUnconditional(t *testing.T) {
	t.Parallel()

	out, ran := loopingRun(t, 6, "until body if n == 2\nuntil body\ncontinue\n")

	assert.Len(t, ran, 6)
	assert.Equal(t, 2, strings.Count(out, "break at body"),
		"the conditional stop at n == 2, then the plain one at the very next arrival — a leaked condition would eat the second")
}

// TestEachAcceptedUntilGetsItsOwnDeclinedNotice: the once-only memory behind
// the declined-arrival notice is per accepted command, not per step forever.
// A second `until body if <broken>` after a declined first one must warn
// again — a fresh condition skipped in silence behind a prompt that said it
// was set is the exact failure the notice exists to prevent (Copilot, #1274).
func TestEachAcceptedUntilGetsItsOwnDeclinedNotice(t *testing.T) {
	t.Parallel()

	// A gate step beside the body gives the prompt back on every iteration,
	// which is what lets a second `until` be typed mid-run at all.
	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In: strings.NewReader(
			"break gate\ncontinue\nuntil body if first.missing\nuntil body if second.missing\ncontinue\n"),
		Out: &console,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	seen := &ranSteps{}
	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, seen))
	ctx = v1.NewContextWithDebugger(ctx, session)

	looping := &v1.Workflow{Name: "looping", Steps: []*v1.Node{{
		Id: "each",
		Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items:    v1.NewLiteralList(0, 1, 2),
			Iterator: "n",
			Body:     []*v1.Node{markStep("body"), markStep("gate")},
		}},
	}}}

	_, err = v1.Run(ctx, looping)
	require.NoError(t, err)

	out := console.String()
	assert.Equal(t, 2, strings.Count(out, "until body: the condition could not be evaluated here"),
		"two accepted conditions, two notices — the first must not spend the second's")
}

// TestAConditionalUntilIsRecordedAsTyped: the replay script is the session's
// decisions written down, and an `until` that stopped at iteration 5,000
// must reproduce that stop — condition and all — when the script is fed back.
func TestAConditionalUntilIsRecordedAsTyped(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	session, err := flowdebug.New(flowdebug.Options{
		In:  strings.NewReader("until build if steps.setup.ok == 'ok'\n"),
		Out: &out,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	node := &v1.Node{Id: "held", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}
	require.NoError(t, session.BeforeStep(t.Context(), node, v1.NewScope(v1.CurrentProfile, nil)))

	assert.Equal(t, []string{"until build if steps.setup.ok == 'ok'"}, session.Script(),
		"accepted, compiled, and recorded with its condition, so a replay stops where this session did")
}
