package flowfile_test

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A macro is expanded by the parser, so a parser told about fewer libraries than
// the language has does not expand the ones it was not told about — it leaves them
// as ordinary calls, on identifiers nothing binds.
//
// Flowfile expressions were parsed against a bare environment for exactly as long as
// this file did not exist, so eighteen of the profile's macros could be written,
// validated, and never evaluated. The two ways that showed up looked unrelated,
// which is most of why it lasted:
//
//   - `${math.greatest(1, 2)}` validated clean and died at run time saying
//     `no such attribute(s): math`.
//   - `${[3,1,2].sortBy(v, -v)}` was refused by `flow validate` — `references
//     unknown name "v"` — naming the macro's own bound variable as though the
//     author had written a step called `v`.
//
// The standard macros were unaffected, because cel-go declares those in its default
// environment rather than in a library. So `filter` worked and `transformList` did
// not, which reads like a fact about those two functions rather than about where
// either of them is declared.

// macroCase is a Flowfile expression exercising one macro, and what it must produce.
type macroCase struct {
	// expr is written as an author writes it, and run through the compiler rather
	// than built as a tree. A test that constructs the expanded form directly
	// proves the evaluator works and says nothing about whether the expansion ever
	// happened, which is the entire bug.
	expr string

	// want is the value, rendered with `string()`.
	want string

	// why, when set, says this macro cannot be reached from a Flowfile at all, and
	// the case is a statement rather than a test. Named rather than omitted: a
	// silently absent macro is indistinguishable from one nobody thought about.
	why string
}

// macroCases covers every macro the profile makes available.
//
// Keyed by the name and argument count cel-go identifies a macro by, because that
// is the key [TestEveryMacroInTheProfileIsExercised] derives its expectations from.
// Two spellings of one behaviour — `existsOne` and `exists_one` — are two entries,
// since either could break without the other noticing.
var macroCases = map[string]macroCase{
	// The standard macros. They worked before this and are here anyway: they are
	// the control that says the harness can tell a working macro from a broken one,
	// and `filter` is the one whose round trip this change repairs.
	"has/1":        {expr: `has({'a':1}.a)`, want: "true"},
	"all/2":        {expr: `[1,2].all(v, v > 0)`, want: "true"},
	"exists/2":     {expr: `[1,2].exists(v, v > 1)`, want: "true"},
	"exists_one/2": {expr: `[1,2].exists_one(v, v > 1)`, want: "true"},
	"map/2":        {expr: `[1,2].map(v, v * 2)[1]`, want: "4"},
	"map/3":        {expr: `[1,2].map(v, v > 1, v * 2)[0]`, want: "4"},
	"filter/2":     {expr: `[1,2].filter(v, v > 1)[0]`, want: "2"},

	// `bindings`.
	"bind/3": {expr: `cel.bind(x, 2, x + 1)`, want: "3"},

	// `comprehensions`, which was entirely unreachable.
	"all/3":               {expr: `[1,2].all(i, v, v > 0)`, want: "true"},
	"exists/3":            {expr: `[1,2].exists(i, v, v > 1)`, want: "true"},
	"existsOne/3":         {expr: `[1,2].existsOne(i, v, v > 1)`, want: "true"},
	"exists_one/3":        {expr: `[1,2].exists_one(i, v, v > 1)`, want: "true"},
	"transformList/3":     {expr: `[1,2].transformList(i, v, v * 2)[1]`, want: "4"},
	"transformList/4":     {expr: `[1,2].transformList(i, v, v > 1, v * 2)[0]`, want: "4"},
	"transformMap/3":      {expr: `{'a':1}.transformMap(k, v, v * 10)['a']`, want: "10"},
	"transformMap/4":      {expr: `{'a':1,'b':2}.transformMap(k, v, v > 1, v * 10)['b']`, want: "20"},
	"transformMapEntry/3": {expr: `{'a':1}.transformMapEntry(k, v, {k: v * 2})['a']`, want: "2"},
	"transformMapEntry/4": {expr: `{'a':1,'b':2}.transformMapEntry(k, v, v > 1, {k: v * 2})['b']`, want: "4"},

	// `lists`.
	"sortBy/2": {expr: `[3,1,2].sortBy(v, -v)[0]`, want: "3"},
	"sum/0":    {expr: `[1,2,3].sum()`, want: "6"},

	// `math`. Its other functions are ordinary declarations and always worked; these
	// two are macros because they are variadic.
	"least/0":    {expr: `math.least(3, 4)`, want: "3"},
	"greatest/0": {expr: `math.greatest(1, 2)`, want: "2"},

	// `optional`.
	"optMap/2":     {expr: `optional.of(2).optMap(v, v * 3).value()`, want: "6"},
	"optFlatMap/2": {expr: `optional.of(2).optFlatMap(v, optional.of(v * 3)).value()`, want: "6"},

	// `protos`. Both take a protobuf extension field as their second argument, which
	// is a name in a descriptor rather than a value an expression can write, so there
	// is no Flowfile that reaches either. They are fixed by the same change — the
	// parser expands them now — and are stated here rather than left to look like an
	// oversight.
	"getExt/2": {why: "takes a protobuf extension field, which no Flowfile expression can name"},
	"hasExt/2": {why: "takes a protobuf extension field, which no Flowfile expression can name"},
}

// TestEveryProfileMacroEvaluatesFromAFlowfile is the direct claim.
//
// Through the compiler and the local driver, because those are the two halves that
// disagreed: the compiler stored something the driver could not evaluate, and each
// was individually working.
func TestEveryProfileMacroEvaluatesFromAFlowfile(t *testing.T) {
	t.Parallel()

	for _, name := range slices.Sorted(maps.Keys(macroCases)) {
		test := macroCases[name]
		if test.why != "" {
			continue
		}

		t.Run(subtestName(name), func(t *testing.T) {
			t.Parallel()

			wf, err := flowfile.Unmarshal([]byte(flowfileAsserting(test.expr, test.want)))
			require.NoError(t, err, "%s does not compile", name)

			require.Empty(t, flowfile.Validate(wf).Err(),
				"%s was refused by the validator, which is how an unexpanded macro's "+
					"bound variable reads to the reference walk", name)

			outputs, err := v1.Run(t.Context(), wf)
			require.NoError(t, err, "%s compiles and validates but does not evaluate", name)

			assert.Contains(t, outputs.GetStepValues(), "say",
				"%s evaluated, but not to %q", name, test.want)
		})
	}
}

// TestEveryMacroInTheProfileIsExercised is the completeness half, and the reason the
// table above is keyed the way it is.
//
// Derived from the environment rather than from a list somebody maintains: adding a
// library to a profile adds its macros silently, and every one of them arrives with
// the same defect this file exists for if the parser is ever narrowed again. A macro
// with no case here fails, naming itself.
func TestEveryMacroInTheProfileIsExercised(t *testing.T) {
	t.Parallel()

	libs, err := v1.ProfileLibraries(v1.CurrentProfile)
	require.NoError(t, err)

	env, err := v1.DefaultEvaluator().Env(libs...)
	require.NoError(t, err)

	declared := map[string]bool{}
	for _, macro := range env.Macros() {
		declared[fmt.Sprintf("%s/%d", macro.Function(), macro.ArgCount())] = true
	}
	require.NotEmpty(t, declared, "no macros found, so this test proves nothing")

	for name := range declared {
		assert.Contains(t, macroCases, name,
			"the profile declares macro %s and nothing here uses it; add a case, or a `why` "+
				"saying no Flowfile can reach it", name)
	}

	// And the other direction, so a macro that leaves a library does not leave a
	// case behind claiming coverage of something that no longer exists.
	for name := range macroCases {
		assert.Contains(t, declared, name,
			"%s is exercised here but the profile no longer declares it", name)
	}
}

// TestAMacroSurvivesBeingWrittenBackToSource covers `flow fix`, which rewrites a
// file and therefore has to be able to render every expression in it.
//
// This is the half that had to be fixed *with* the parser rather than after it.
// Expanding a macro without recording that it was one leaves the unparser looking at
// the expansion: `math.greatest(1, 2)` came back as `math.@max(1, 2)`, which is not
// a spelling anybody can write, so `flow fix` would have rewritten a working file
// into one that no longer compiles.
//
// It also repairs something that predates the macro bug. A standard macro was always
// expanded, so `flow fix` on a file containing `${[1,2].filter(v, v > 1)}` failed
// outright — "expression cannot be written back as source". Macro call tracking is
// what makes both writable, which is why it is not a separable change.
func TestAMacroSurvivesBeingWrittenBackToSource(t *testing.T) {
	t.Parallel()

	for _, name := range slices.Sorted(maps.Keys(macroCases)) {
		test := macroCases[name]
		if test.why != "" {
			continue
		}

		t.Run(subtestName(name), func(t *testing.T) {
			t.Parallel()

			wf, err := flowfile.Unmarshal([]byte(flowfileAsserting(test.expr, test.want)))
			require.NoError(t, err)

			written, err := flowfile.Marshal(wf)
			require.NoError(t, err, "%s cannot be written back, so `flow fix` would refuse the file", name)

			// Round-tripped rather than string-compared: the unparser normalises
			// spacing, so asserting the exact source would be a test of its
			// formatting. What matters is that reading it back gives a file that
			// still evaluates to the same thing.
			again, err := flowfile.Unmarshal(written)
			require.NoError(t, err, "%s was written back as something that no longer compiles:\n%s", name, written)

			outputs, err := v1.Run(t.Context(), again)
			require.NoError(t, err, "%s stopped evaluating after a round trip:\n%s", name, written)

			assert.Contains(t, outputs.GetStepValues(), "say",
				"%s changed meaning across a round trip:\n%s", name, written)
		})
	}
}

// TestFixLeavesAMacrosBoundVariableAlone is the case that made this a corruption bug
// rather than a missing feature.
//
// `flow fix` roots a bare step reference under `steps.`, and it decides what is a
// *free* identifier by parsing the expression. A macro's bound variable is not free —
// `[3,1,2].sortBy(name, name)` binds `name` — but only a parser that knows the macro
// can tell, because knowing means expanding it into a comprehension whose variable
// the rewriter's walk never reaches.
//
// The rewriter parsed against a bare environment, so it knew cel-go's standard macros
// and none of the profile's. With a step called `name` beside it, `sortBy(name, name)`
// was rewritten to `sortBy(steps.name, steps.name)` — which is not a macro invocation
// at all, so a command whose entire promise is that it is safe to run turned a valid
// file into one that does not compile.
//
// `filter` is here as the control. It was always safe, because it is a standard macro,
// and that difference is the whole shape of the bug: the two behaved differently for
// no reason visible in either expression.
//
// Found by review on the change that made profile macros work — which is what made it
// reachable, since until then nobody could write one that ran.
func TestFixLeavesAMacrosBoundVariableAlone(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		expr string
	}{
		{name: "a profile macro", expr: `['zeta', 'alpha'].sortBy(name, name)[0]`},
		{name: "a standard macro", expr: `['zeta', 'alpha'].filter(name, name > 'a')[0]`},
		{name: "a two-variable comprehension", expr: `['zeta'].transformList(i, name, name)[0]`},
		{name: "a binding", expr: `cel.bind(name, 'x', name)`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// A step whose id is exactly the name the macro binds. That collision is
			// legal — a step is `steps.<id>` and a bound name is bare, so the two do
			// not share a namespace — and it is what the rewriter has to get right.
			src := strings.Join([]string{
				"edition: v2026.3",
				"name: collide",
				"steps:",
				"  - id: name",
				"    log:",
				"      message: placeholder",
				"  - id: pick",
				"    log:",
				`      message: "${` + test.expr + `}"`,
				"",
			}, "\n")

			fixed, err := flowfile.Fix([]byte(src))
			require.NoError(t, err)

			assert.Equal(t, src, string(fixed.Source),
				"`flow fix` rewrote a macro's bound variable into a step reference")

			// And the result is still a file, which is the claim the command makes.
			// Asserted separately because a rewrite that produced something merely
			// different from the source would fail the check above without saying
			// whether it had broken anything.
			wf, err := flowfile.Unmarshal(fixed.Source)
			require.NoError(t, err, "what `flow fix` wrote no longer compiles:\n%s", fixed.Source)
			require.Empty(t, flowfile.Validate(wf).Err(),
				"what `flow fix` wrote no longer validates:\n%s", fixed.Source)
		})
	}
}

// flowfileAsserting wraps an expression in the smallest file whose *outcome* says
// what the expression evaluated to.
//
// The value is compared inside the file rather than read out of the run, because
// nothing a local run produces carries one: `log:` returns empty outputs by design —
// "this step ran and produced nothing" — and no other task evaluates without a
// network. A condition is the one place a value reaches an observable outcome, so
// the step runs when the expression is right and is skipped when it is wrong.
//
// The two failures stay distinguishable, which is what makes this worth doing rather
// than merely convenient: an expression that cannot evaluate fails the run, and one
// that evaluates to the wrong thing leaves the step absent from the outputs.
func flowfileAsserting(expression, want string) string {
	return strings.Join([]string{
		"edition: v2026.3",
		"name: macros",
		"steps:",
		"  - id: say",
		`    if: "${string(` + expression + `) == ` + quoteForCEL(want) + `}"`,
		"    log:",
		"      message: matched",
		"",
	}, "\n")
}

// quoteForCEL renders a want value as a CEL string literal, in single quotes so the
// surrounding YAML double-quoted scalar does not have to escape it.
func quoteForCEL(want string) string {
	return "'" + strings.ReplaceAll(want, "'", "\\'") + "'"
}

// subtestName makes a macro's name usable as one.
//
// `all/2` is a name with a slash in it, and Go reads a slash in a subtest name as a
// level of nesting — so `-run` cannot address it and the output reads as though
// there were a parent test called `all`.
func subtestName(macro string) string {
	return strings.ReplaceAll(macro, "/", "-")
}
