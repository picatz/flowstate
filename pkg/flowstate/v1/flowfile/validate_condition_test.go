package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// An `if:` is a property of a *node*, so every kind of step may carry one — and
// until #869 only a task step's was reference-checked. The identical misspelling
// was a positioned diagnostic on a task and silence on a `wait_for_signal:`, a
// `sleep:`, a loop, a parallel block, a `value:`, a `switch:` or a `call:`.
//
// The tests below are the two directions of that, kind by kind: a name that
// resolves nowhere must be reported wherever it is written, and a name the
// grammar binds must stay legal wherever the engine binds it. The second half is
// the one that decides whether the first is safe to ship — a condition check that
// does not know what a loop binds reports every correct body condition as an
// unknown step, which is the "false diagnostics are worse than missing ones" trade
// CLAUDE.md names.

// conditionKinds is one step of every node kind that can carry an `if:`, written
// as the YAML that follows the `if:` line. Enumerated from the `kind` oneof of
// [v1.Node] (proto/flowstate/v1/workflow.proto) — task, for_each, parallel, wait,
// call, loop, value and switch — with the wait's three spellings written out,
// since `sleep:`, `wait_until:` and `wait_for_signal:` are one kind in the schema
// and three different-looking steps in a file.
//
// `call:` is absent because it needs a callee on disk; it has its own test below.
var conditionKinds = map[string]string{
	"task": `    log:
      message: hi`,

	"sleep": `    sleep: 10s`,

	"wait_until": `    wait_until: ${now + duration("1s")}`,

	"wait_for_signal": `    wait_for_signal:
      name: approved
      timeout: 24h`,

	"for_each": `    for_each:
      items: ${[1, 2]}
      as: n
      steps:
        - id: inner
          log:
            message: ${string(n)}`,

	"loop": `    loop:
      as: total
      init: ${0}
      update: ${total + 1}
      until: ${total > 2}
      max_iterations: 5
      steps:
        - id: body
          log:
            message: hi`,

	"parallel": `    parallel:
      - steps:
          - id: left
            log:
              message: L
      - steps:
          - id: right
            log:
              message: R`,

	"value": `    value: ${1 + 1}`,

	"switch": `    switch:
      value: ${string(1)}
      cases:
        - case: "1"
          steps: []
      default:
        steps: []`,
}

// TestValidateChecksTheConditionOfEveryNodeKind is the issue's own case, run over
// every kind that can carry an `if:` rather than over the two the report happened
// to name (#869).
//
// The mistake is a misspelled workflow var, because that is the one where silence
// is worst: `${vars.token_budgets > 1}` is a run-time failure, and a typo that
// happens to resolve to a *different* real name is not even that — the step takes
// the branch its author did not intend, forever, with nothing said at authoring
// time.
func TestValidateChecksTheConditionOfEveryNodeKind(t *testing.T) {
	t.Parallel()

	for kind, body := range conditionKinds {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(`edition: v2026.3
name: w
vars:
  token_budget: 5
steps:
  - id: gate
    if: ${vars.token_budgets > 1}
` + body + `
`))
			require.NoError(t, err)
			require.NotEmpty(t, ds, "a misspelled var in a %s step's if: was accepted", kind)

			text := ds.Error()
			require.Contains(t, text, `references unknown var "token_budgets"`)
			require.Contains(t, text, `did you mean "token_budget"?`,
				"the near miss the task path names must be named for every kind")
		})
	}
}

// TestValidateReportsAConditionAtItsOwnPosition is the other half of a
// diagnostic: a message with no line and column is a message an editor cannot
// place, and the whole complaint in #869 was that the wait path said nothing at
// all where the task path said `mini.yaml:7:9`.
func TestValidateReportsAConditionAtItsOwnPosition(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSourceAt([]byte(`edition: v2026.3
name: mini3
vars:
  token_budget: 5
steps:
  - id: review
    if: ${vars.token_budgets > 1}
    wait_for_signal:
      name: turn-approved
      timeout: 24h
`), "mini3.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, ds)
	require.Contains(t, ds.Error(), "7:9: ",
		"the diagnostic must land on the if: line, the way the task path's does")
}

// TestValidateChecksAConditionInsideABlock covers the second walk. The top-level
// walk and [validateNested] each dispatch on node kind, and a rule present in one
// and missing from the other is how a scope rule comes to have two spellings —
// the failure the package's own comments record for switch-case ids.
func TestValidateChecksAConditionInsideABlock(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSource([]byte(`edition: v2026.3
name: w
vars:
  token_budget: 5
steps:
  - id: each
    for_each:
      items: ${[1, 2]}
      as: n
      steps:
        - id: parked
          if: ${vars.token_budgets > 1}
          sleep: 1s
`))
	require.NoError(t, err)
	require.NotEmpty(t, ds, "a misspelled var in a nested wait's if: was accepted")
	require.Contains(t, ds.Error(), `references unknown var "token_budgets"`)
}

// TestValidateChecksTheConditionOfACall is the one kind the table above cannot
// reach, because a call names a file.
func TestValidateChecksTheConditionOfACall(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
vars:
  token_budget: 5
steps:
  - id: provision
    if: ${vars.token_budgets > 1}
    call: ./callee.yaml
    with:
      tenant: acme
`)

	ds, err := flowfile.ValidateSourceFile(caller)
	require.NoError(t, err)
	require.NotEmpty(t, ds, "a misspelled var in a call's if: was accepted")
	require.Contains(t, ds.Error(), `references unknown var "token_budgets"`)
}

// TestValidateAcceptsWhatTheGrammarBindsInACondition is the direction that makes
// the check above safe to ship, and the one the rewriter section of CLAUDE.md was
// written about: a loop's `as:`, and the `item` a loop binds when it writes no
// `as:`, are bound for the body — so a body step's `if:` naming one is correct,
// and reporting it would be a false diagnostic on a working file.
//
// Taken from where the engine evaluates the condition: `runNodes` (eval.go)
// evaluates a body node's `if:` inside the body, with the iterator already bound.
func TestValidateAcceptsWhatTheGrammarBindsInACondition(t *testing.T) {
	t.Parallel()

	for name, source := range map[string]string{
		"loop as": `edition: v2026.3
name: w
steps:
  - id: each
    for_each:
      items: ${[1, 2]}
      as: n
      steps:
        - id: parked
          if: ${n > 1}
          sleep: 1s
`,
		"default item": `edition: v2026.3
name: w
steps:
  - id: each
    for_each:
      items: ${[1, 2]}
      steps:
        - id: parked
          if: ${item > 1}
          sleep: 1s
`,
		"carried loop state": `edition: v2026.3
name: w
steps:
  - id: fold
    loop:
      as: total
      init: ${0}
      update: ${total + 1}
      until: ${total > 2}
      max_iterations: 5
      steps:
        - id: parked
          if: ${total > 1}
          sleep: 1s
`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(source))
			require.NoError(t, err)
			require.Empty(t, ds, "a name the grammar binds for the body was reported: %s", ds.Error())
		})
	}
}

// TestValidateRefusesNowInAWaitsCondition is the name that reads like it should be
// legal here and is not, which is why it gets a sentence of its own rather than a
// line in a table.
//
// `now` is bound by the engine *inside* a wait's own expressions
// ([v1.NowIdentifier], bound by evalWaitExpr). A condition is evaluated a level
// above that — `runNodes` calls [v1.EvalConditionInScope] before entering the
// node, and the durable driver does the same in execute.go — so the wait has not
// started and there is no moment to bind. Extending the check to waits without
// this test is exactly how a scope taken from where a thing is *written* rather
// than from where it is *evaluated* would have shipped.
func TestValidateRefusesNowInAWaitsCondition(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSource([]byte(`edition: v2026.3
name: w
steps:
  - id: review
    if: ${now.getHours() > 1}
    wait_for_signal:
      name: approved
      timeout: 24h
`))
	require.NoError(t, err)
	require.NotEmpty(t, ds, "`now` in a wait's if: is not bound there and was accepted")

	text := ds.Error()
	require.Contains(t, text, "`now` is only available inside a wait")
	require.Contains(t, text, "evaluated before the step is entered",
		"the diagnostic must say why a wait's own if: still has no clock")
	require.NotContains(t, text, "resolved inside an activity",
		"the task-input reason does not apply to a condition, which is workflow code")
}

// TestValidateAcceptsNowInsideTheWaitItself is that refusal's other direction: the
// fields the engine really does bind `now` for keep working, which is what makes
// the refusal above a statement about scope rather than about the name.
func TestValidateAcceptsNowInsideTheWaitItself(t *testing.T) {
	t.Parallel()

	ds, err := flowfile.ValidateSource([]byte(`edition: v2026.3
name: w
steps:
  - id: parked
    wait_until: ${now + duration("1h")}
`))
	require.NoError(t, err)
	require.Empty(t, ds, "`now` inside a wait's own expression was reported: %s", ds.Error())
}

// TestFixAndValidateAgreeAboutAWaitsCondition is the join of the two surfaces
// that now both read a wait's `if:`, and the reason this test exists at all is
// that they can only disagree in one direction each, both of them silent.
//
// `flow fix` decides which bare names in a condition are step references to root;
// [validateCondition] decides which are unresolved. A name an enclosing loop binds
// is neither — so a rewriter that roots it corrupts a working file (the two
// corruptions CLAUDE.md records), and a validator that reports it refuses one.
// Here a step, a loop's item, and a wait's own `if:` all share the spelling `host`,
// which is legal on purpose, and both surfaces have to reach the same answer:
// inside the body it is the item, and the file is left alone and accepted.
func TestFixAndValidateAgreeAboutAWaitsCondition(t *testing.T) {
	t.Parallel()

	source := `edition: v2026.3
name: agree
steps:
  - id: host
    log:
      message: a step whose id is host
  - id: each
    for_each:
      items: "${['alpha', 'be']}"
      as: host
      steps:
        - id: parked
          if: "${size(host) > 2}"
          sleep: 1s
`

	result, err := flowfile.Fix([]byte(source))
	require.NoError(t, err)
	require.Empty(t, result.Refusals)
	require.Equal(t, source, string(result.Source),
		"the loop's item was rewritten inside a wait's if:, so the file still validates "+
			"and asks a different question")

	ds, err := flowfile.ValidateSource(result.Source)
	require.NoError(t, err)
	require.Empty(t, ds, "the loop's item was reported in a wait's if:: %s", ds.Error())
}

// TestValidateRefusesAStepsOwnVarInItsCondition is the third scope rule, and the
// one a reader is most likely to get backwards: a step's `vars:` are bound
// throughout the step, but its `if:` decides whether the step runs at all, so
// `runNodes` evaluates the condition before binding them. This was already true on
// the task path (the walk passes `scope`, not `inner`); extending the check to
// every kind must not quietly widen it.
func TestValidateRefusesAStepsOwnVarInItsCondition(t *testing.T) {
	t.Parallel()

	for kind, body := range map[string]string{
		"task":  "    log:\n      message: hi",
		"sleep": "    sleep: 1s",
	} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(`edition: v2026.3
name: w
steps:
  - id: gate
    vars:
      budget: ${5}
    if: ${budget > 1}
` + body + `
`))
			require.NoError(t, err)
			require.NotEmpty(t, ds,
				"a step's own var is not bound in its own if: and was accepted on a %s step", kind)
			require.True(t, strings.Contains(ds.Error(), "budget"),
				"the diagnostic must name the var: %s", ds.Error())
		})
	}
}
