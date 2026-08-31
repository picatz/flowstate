package flowfile_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Expressions were parsed and never checked, so `flow validate` said `ok` to a
// call to a function that does not exist.
//
// The half these tests care most about is the quiet one. A type check that reports
// something true about an expression nobody can fix is worse than no type check —
// "false diagnostics are worse than missing ones" is the rule, and this is a check
// with an unusually large surface to be wrong on, since it runs over every
// expression in every position.

// diagnosticsFor validates a Flowfile and returns what it said.
func diagnosticsFor(t *testing.T, src string) []string {
	t.Helper()

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err, "the file does not compile, so this says nothing about validation")

	return diagnosticStrings(ds)
}

// diagnosticsForAt is [diagnosticsFor] for a file that may contain a `call:`
// step, resolved relative to path's directory.
func diagnosticsForAt(t *testing.T, src string, path string) []string {
	t.Helper()

	ds, err := flowfile.ValidateSourceAt([]byte(src), path)
	require.NoError(t, err, "the file does not compile, so this says nothing about validation")

	return diagnosticStrings(ds)
}

func diagnosticStrings(ds flowfile.Diagnostics) []string {
	out := make([]string, 0, len(ds))
	for _, d := range ds {
		out = append(out, d.Error())
	}

	return out
}

// sayingInStep wraps an expression in a file that does nothing else.
func sayingInStep(expression string) string {
	return strings.Join([]string{
		"edition: v2026.3",
		"name: check",
		"steps:",
		"  - id: say",
		"    log:",
		`      message: "${` + expression + `}"`,
		"",
	}, "\n")
}

// afterAFetch puts an expression in a step that follows one producing a response,
// so a reference to `steps.web` resolves.
//
// Needed because the reference walk runs alongside this check and is right to refuse
// a step nobody declared — the first version of these cases forgot the step and got
// a real diagnostic about a real mistake, which is the reference walk working.
func afterAFetch(expression string) string {
	return strings.Join([]string{
		"edition: v2026.3",
		"name: check",
		"steps:",
		"  - id: web",
		"    http:",
		"      method: GET",
		"      url: https://example.com",
		"  - id: say",
		"    log:",
		`      message: "${` + expression + `}"`,
		"",
	}, "\n")
}

// TestAnExpressionThatCannotEvaluateIsReported is the direct claim.
//
// Every one of these fails at run time, every time, with nothing about the file
// changing in between — which is the definition of something knowable from the
// document.
func TestAnExpressionThatCannotEvaluateIsReported(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		expr string
		says string
	}{
		{
			// The one that shipped, in `examples/http-query-and-json`, and ran
			// broken for as long as it existed.
			name: "string of a map",
			expr: `string({'a': 1})`,
			says: "no matching overload for 'string'",
		},
		{
			name: "string of a list",
			expr: `string(['a'])`,
			says: "no matching overload for 'string'",
		},
		{
			name: "an operator with no overload",
			expr: `string(1 + 'a')`,
			says: "no matching overload for '_+_'",
		},
		{
			name: "a function nobody declared",
			expr: `nosuchfunc(1)`,
			says: `no function called "nosuchfunc"`,
		},
		{
			name: "the wrong argument type",
			expr: `string(size(1))`,
			says: "no matching overload for 'size'",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reported := diagnosticsFor(t, sayingInStep(test.expr))
			require.NotEmpty(t, reported, "an expression that cannot evaluate was accepted")
			assert.Contains(t, strings.Join(reported, "\n"), test.says)
		})
	}
}

// TestNothingIsReportedAboutAnExpressionThatIsFine is the direction that decides
// whether this check can exist at all.
//
// Each of these is a working expression that some plausible implementation of a type
// check would refuse. The list is the record of what "declare every name as dyn"
// buys, and three entries are cases an earlier version genuinely got wrong.
func TestNothingIsReportedAboutAnExpressionThatIsFine(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
	}{
		{
			// The common case, and the reason scope is not modelled: a response is
			// not in the file, so nothing here can judge what is in it.
			name: "a reference to a step's output",
			src:  afterAFetch(`string(steps.web.status_code)`),
		},
		{
			name: "arithmetic on something unknowable",
			src:  afterAFetch(`string(steps.web.status_code + 1)`),
		},
		{
			name: "a namespaced function",
			src:  sayingInStep(`regex.replace('a', 'b', 'c')`),
		},
		{
			name: "a namespaced function from a differently-named library",
			src:  sayingInStep(`string(base64.encode(b'x'))`),
		},
		{
			// A macro. Its bound variable is not a name anything declares, and the
			// only thing that makes that visible is the expansion.
			name: "a macro that binds a variable",
			src:  sayingInStep(`[3, 1, 2].sortBy(v, -v)[0] == 3 ? 'yes' : 'no'`),
		},
		{
			name: "a macro whose qualifier is a namespace",
			src:  sayingInStep(`string(math.greatest(1, 2))`),
		},
		{
			// A fold over a *typed* literal, which is the corner sum's expansion
			// wraps in dyn() for: the empty-list branch is int 0, and without the
			// wrapper the checker would refuse a working double sum for mixing
			// int and double across `?:`.
			name: "a sum over doubles",
			src:  sayingInStep(`string([1.5, 2.5].sum())`),
		},
		{
			name: "a sum chained after map",
			src:  afterAFetch(`string(steps.web.json.items.map(o, o.amount).sum())`),
		},
		{
			// The case that caught an earlier version of this check. `json` is a
			// function namespace *and* an ordinary word, and a check that skipped
			// the name everywhere refused a step that had simply declared one.
			name: "a var named after a function namespace",
			src: strings.Join([]string{
				"edition: v2026.3",
				"name: check",
				"steps:",
				"  - id: shout",
				"    vars:",
				"      json: loud",
				"    log:",
				"      message: ${json}",
				"",
			}, "\n"),
		},
		{
			// A loop's iterator is bound by the loop, and this check has no idea
			// loops exist.
			name: "a loop's iterator",
			src: strings.Join([]string{
				"edition: v2026.3",
				"name: check",
				"steps:",
				"  - id: each",
				"    for_each:",
				`      items: ${['a', 'b']}`,
				"      as: item",
				"      steps:",
				"        - id: inner",
				"          log:",
				"            message: ${item}",
				"",
			}, "\n"),
		},
		{
			// An input the task evaluates itself, against a scope the validator
			// cannot see. The reference walk correctly declines to judge these;
			// this one can, because `response` is declared like any other name.
			name: "a deferred input naming the response",
			src: strings.Join([]string{
				"edition: v2026.3",
				"name: check",
				"steps:",
				"  - id: web",
				"    http:",
				"      method: GET",
				"      url: https://example.com",
				"      expect: ${response.status_code == 200}",
				"",
			}, "\n"),
		},
		{
			name: "the moment inside a wait",
			src: strings.Join([]string{
				"edition: v2026.3",
				"name: check",
				"steps:",
				"  - id: pause",
				"    wait_until: ${now + days(1)}",
				"",
			}, "\n"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Empty(t, diagnosticsFor(t, test.src),
				"a working expression was reported, which is the failure this check cannot afford")
		})
	}
}

// TestEveryExpressionPositionIsChecked is the traversal, rather than the step.
//
// The check walks the workflow itself rather than hanging off the per-position
// reference checks, precisely so a position cannot be missed — which is only worth
// anything if something asserts every position is reached. Each case puts the same
// unevaluatable expression somewhere the language allows one.
func TestEveryExpressionPositionIsChecked(t *testing.T) {
	t.Parallel()

	// Broken the same way everywhere, so a case that passes is a position that is
	// not checked rather than an expression that happens to be fine there.
	const broken = `nosuchfunc(1)`

	for _, test := range []struct {
		name string
		src  string
	}{
		{
			name: "a workflow's vars",
			src: "edition: v2026.3\nname: check\nvars:\n  bad: ${" + broken + "}\n" +
				"steps:\n  - id: say\n    log:\n      message: hi\n",
		},
		{
			name: "a step's condition",
			src: "edition: v2026.3\nname: check\nsteps:\n  - id: say\n" +
				"    if: ${" + broken + "}\n    log:\n      message: hi\n",
		},
		{
			name: "a step's own vars",
			src: "edition: v2026.3\nname: check\nsteps:\n  - id: say\n" +
				"    vars:\n      bad: ${" + broken + "}\n    log:\n      message: hi\n",
		},
		{
			name: "a task input",
			src:  sayingInStep(broken),
		},
		{
			name: "an input the task evaluates itself",
			src: "edition: v2026.3\nname: check\nsteps:\n  - id: web\n    http:\n" +
				"      method: GET\n      url: https://example.com\n      expect: ${" + broken + "}\n",
		},
		{
			name: "a loop's items",
			src: "edition: v2026.3\nname: check\nsteps:\n  - id: each\n    for_each:\n" +
				"      items: ${" + broken + "}\n      as: item\n      steps:\n" +
				"        - id: inner\n          log:\n            message: ${item}\n",
		},
		{
			name: "a step inside a loop body",
			src: "edition: v2026.3\nname: check\nsteps:\n  - id: each\n    for_each:\n" +
				"      items: ${['a']}\n      as: item\n      steps:\n" +
				"        - id: inner\n          log:\n            message: ${" + broken + "}\n",
		},
		{
			name: "a step inside a parallel branch",
			src: "edition: v2026.3\nname: check\nsteps:\n  - id: both\n    parallel:\n" +
				"      - steps:\n          - id: inner\n            log:\n" +
				"                message: ${" + broken + "}\n",
		},
		{
			name: "the moment a wait waits for",
			src: "edition: v2026.3\nname: check\nsteps:\n  - id: pause\n" +
				"    wait_until: ${" + broken + "}\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert.Contains(t, strings.Join(diagnosticsFor(t, test.src), "\n"), "nosuchfunc",
				"an expression in this position is never type-checked")
		})
	}
}

// TestTypeCheckingIsQuietOnTheCorpus is the measurement this check was built
// against, kept as a test.
//
// Sixty expressions across nineteen examples, and the number that may be reported is
// zero. It is worth having beside the case-by-case list above because the examples
// are written by hand for readability rather than assembled to exercise a checker,
// so they contain shapes nobody thought to put in a table.
func TestTypeCheckingIsQuietOnTheCorpus(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "..", "examples", "*", "workflow.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no examples found; the glob is wrong")

	for _, path := range paths {
		name := filepath.Base(filepath.Dir(path))

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			data, err := os.ReadFile(path)
			require.NoError(t, err)

			assert.Empty(t, diagnosticsForAt(t, string(data), path),
				"a shipped example was reported, and it runs in CI, so it is not the example that is wrong")
		})
	}
}

// TestTheDiagnosticIsWrittenForAnAuthor covers the two rewritings, and the fact that
// there are only two.
//
// cel-go's sentences are accurate and are mostly kept: a missing overload names the
// function and the types it was given, which is what somebody needs. What is removed
// is what an author cannot act on, and what is added is the one next step that is not
// discoverable from the message.
func TestTheDiagnosticIsWrittenForAnAuthor(t *testing.T) {
	t.Parallel()

	t.Run("cel-go's container is not an author's problem", func(t *testing.T) {
		t.Parallel()

		reported := strings.Join(diagnosticsFor(t, sayingInStep(`nosuchfunc(1)`)), "\n")

		assert.NotContains(t, reported, "in container",
			"a diagnostic named cel-go's container, which this build never sets and nobody can change")
		assert.Contains(t, reported, "flow tasks",
			"a call to a function that does not exist was reported with no way to find one that does")
	})

	t.Run("string of a structure says what does work", func(t *testing.T) {
		t.Parallel()

		reported := strings.Join(diagnosticsFor(t, sayingInStep(`string({'a': 1})`)), "\n")

		// The advice is here because the answer is not guessable: `string()` is the
		// obvious spelling, it is wrong, and nothing in a message about overloads
		// points at the encoder.
		assert.Contains(t, reported, "json.encode(value)",
			"the one mistake here with a known answer was reported without it")

		// And not the other renderer. `"%s".format([value])` also turns a map into
		// a string, and into CEL's debug form — `{a: 1, b: [x, y]}`, unquoted, which
		// no sink can parse back. Advice that produces something unusable is worse
		// than none, because it looks like it worked.
		assert.NotContains(t, reported, "format",
			"the diagnostic recommended a renderer whose output nothing can read back")
	})

	t.Run("an ordinary overload error is left as cel-go wrote it", func(t *testing.T) {
		t.Parallel()

		reported := strings.Join(diagnosticsFor(t, sayingInStep(`string(size(1))`)), "\n")

		assert.Contains(t, reported, "no matching overload for 'size' applied to '(int)'",
			"an accurate message was rewritten into a vaguer one")
		assert.NotContains(t, reported, "json.encode",
			"advice for one case was offered for another, which is how advice stops being read")
	})
}
