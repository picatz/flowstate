package flowtest_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// Computed `vars:` (#1072 slice 4): a var value that is a whole-value `${...}`
// fence is an expression over the block's other vars, evaluated once when the
// file loads, in dependency order. Everything else about a var is unchanged,
// which is a claim with its own test at the bottom of this file.

// echoWorkflow is a workflow that reports what it was given, which is what
// every fixture-position claim below actually needs.
const echoWorkflow = `
edition: v2026.3
name: echo
inputs:
  order:
    type: struct
steps:
  - id: keep
    value: ${inputs.order.id}
outputs:
  id:
    value: ${steps.keep.value}
`

// TestAComputedVarComposesItsSiblings is the feature: a base fixture stated
// once and a variant built from it, both reaching a case's `inputs:` and a
// check. Without composition the two orders are two hand-maintained copies of
// one shape, which is what the corpus does today.
func TestAComputedVarComposesItsSiblings(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), echoWorkflow)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
vars:
  region: eu-west-1
  base: "${ {'id': 'ord_1', 'region': vars.region} }"
  rush: "${ {'id': vars.base.id + '_rush', 'region': vars.base.region} }"
tests:
  - name: the variant is built from the base
    workflow: ./workflow.yaml
    inputs:
      order: "${vars.rush}"
    expect:
      ran: [keep]
      outputs:
        id: ord_1_rush
      check:
        - vars.rush.region == vars.region
        - vars.base.id == 'ord_1'
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	require.Len(t, report.GetCases(), 1)
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
}

// TestComputedVarsEvaluateInDependencyOrderNotNameOrder is the ordering claim,
// written where the two orders disagree: `a` reads `z`, and sorted by name `a`
// comes first. A loader evaluating in name order hands `a` the *fence text* of
// `z` — `${'zed'}!` rather than `zed!` — which is a wrong answer rather than a
// failure, so nothing but this comparison can see it.
func TestComputedVarsEvaluateInDependencyOrderNotNameOrder(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), echoWorkflow)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
vars:
  a: "${vars.z + '!'}"
  z: "${'zed'}"
  order: "${ {'id': vars.a} }"
tests:
  - name: the dependency is evaluated first
    workflow: ./workflow.yaml
    inputs:
      order: "${vars.order}"
    expect:
      ran: [keep]
      outputs:
        id: "zed!"
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
}

// TestALongVarChainEvaluatesEachVarOnce is the bound the design turns on: a
// var is evaluated once, never once per reference.
//
// Every link reads the one before it twice, so a loader expanding per
// reference rather than per var would evaluate the first link 2^198 times. A
// chain of [flowtest.MaxVarsPerFile] — the widest a file may declare — either
// loads in milliseconds or does not finish in this universe, and nothing in
// between.
func TestALongVarChainEvaluatesEachVarOnce(t *testing.T) {
	t.Parallel()

	var block strings.Builder
	block.WriteString("vars:\n  v0: \"${'x' == 'x'}\"\n")
	for i := 1; i < flowtest.MaxVarsPerFile; i++ {
		// A boolean rather than a string, so the *value* stays one word: a
		// chain doubling its own text would meet the cost bound at link
		// sixteen, which is that bound working and not this claim.
		fmt.Fprintf(&block, "  v%d: \"${vars.v%d && vars.v%d}\"\n", i, i-1, i-1)
	}

	file, err := flowtest.Load(writeInline(t, t.TempDir(), block.String()+`
tests:
  - name: the chain resolves
    workflow: ./workflow.yaml
`))
	require.NoError(t, err)
	require.Equal(t, true, file.Vars[fmt.Sprintf("v%d", flowtest.MaxVarsPerFile-1)])
	require.Len(t, file.Vars, flowtest.MaxVarsPerFile)
}

// TestAVarCycleIsRefusedNamingThePath: the diagnostic is the path an author
// walks, not "there is a cycle somewhere in your vars".
func TestAVarCycleIsRefusedNamingThePath(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		block string
		path  string
	}{
		{
			name:  "two vars",
			block: "vars:\n  a: \"${vars.b}\"\n  b: \"${vars.a}\"\n",
			path:  "vars.a → vars.b → vars.a",
		},
		{
			name:  "a var reading itself",
			block: "vars:\n  a: \"${vars.a + '!'}\"\n",
			path:  "vars.a → vars.a",
		},
		{
			name:  "three vars",
			block: "vars:\n  a: \"${vars.b}\"\n  b: \"${vars.c}\"\n  c: \"${vars.a}\"\n",
			path:  "vars.a → vars.b → vars.c → vars.a",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := flowtest.Load(writeInline(t, t.TempDir(), tc.block+`
tests:
  - name: never loads
    workflow: ./workflow.yaml
`))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "is computed from itself")
			assert.Contains(t, err.Error(), tc.path)
		})
	}
}

// TestACycleSearchStopsAtWhatAReportCanHold: the count of cycles is the
// document's, and it multiplies against the length of each rendered path, so
// the search stops rather than the report.
//
// The bound is asserted in both directions — reached, and not exceeded — which
// is the habit CLAUDE.md asks for: a search that gave up after one cycle would
// also satisfy "fewer than a hundred", and it would be hiding the diagnostic an
// author needs.
func TestACycleSearchStopsAtWhatAReportCanHold(t *testing.T) {
	t.Parallel()

	const vars = 30

	var block strings.Builder
	block.WriteString("vars:\n")
	for i := range vars {
		fmt.Fprintf(&block, "  v%d: \"${", i)
		for j := range vars {
			if j > 0 {
				block.WriteString(" + ")
			}
			fmt.Fprintf(&block, "vars.v%d", j)
		}
		block.WriteString("}\"\n")
	}

	_, err := flowtest.Load(writeInline(t, t.TempDir(), block.String()+`
tests:
  - name: never loads
    workflow: ./workflow.yaml
`))
	require.Error(t, err)

	problems, refused := errorAsDiagnostics(t, err)
	require.True(t, refused, "a refused suite reports diagnostics")
	assert.NotEmpty(t, problems.Problems, "the cycle an author has to fix is still named")
	assert.Contains(t, problems.Problems[0].Message, "is computed from itself")
	assert.LessOrEqual(t, problems.Total, flowtest.MaxLoadProblems,
		"every back edge in a %d-var block reading itself is %d cycles, and formatting them all "+
			"costs far more than the twenty a report can show", vars, vars*vars)
}

// errorAsDiagnostics is the typed door [flowtest.Diagnostics] documents, so a
// test asserting about the *count* of problems reads the count rather than
// counting newlines in a rendering.
func errorAsDiagnostics(t *testing.T, err error) (*flowtest.Diagnostics, bool) {
	t.Helper()

	return errors.AsType[*flowtest.Diagnostics](err)
}

// TestAVarReadingARefusedVarIsNotAlsoRefused: one mistake earns one
// diagnostic. `c` reads a var that is on a cycle, so it cannot be evaluated —
// and saying so would report the cycle once per var that happens to read it,
// which is the cascade [problems] already refuses for a value whose kind is
// wrong.
//
// The two readers select a field on purpose. A var that failed still holds the
// *text* of its fence, so a loader that evaluated `c` anyway would find a
// string where a map was meant and report a second, invented mistake — which
// is exactly what the skip exists to prevent, and the only shape in which it
// is visible from outside.
func TestAVarReadingARefusedVarIsNotAlsoRefused(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  a: "${vars.b}"
  b: "${vars.a}"
  c: "${vars.a.region}"
  d: "${vars.c.deeper}"
tests:
  - name: never loads
    workflow: ./workflow.yaml
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "vars.a is computed from itself")
	assert.NotContains(t, err.Error(), "vars.c",
		"a var reading one on a cycle is not a second mistake: %s", err)
	assert.NotContains(t, err.Error(), "vars.d",
		"nor is a var reading that one: %s", err)
}

// TestACrossFileVarCycleNamesEachHopsFile is #1072's repair 6: a directory's
// `testdefaults.yaml` vars merge into the suite's before anything validates,
// so a cycle can exist in neither document on its own. Naming only the suite
// would send a reader to a file that does not contain half of what the
// diagnostic is about.
func TestACrossFileVarCycleNamesEachHopsFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "testdefaults.yaml"), `
vars:
  fromdir: "${vars.fromsuite}"
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
vars:
  fromsuite: "${vars.fromdir}"
tests:
  - name: never loads
    workflow: ./workflow.yaml
`)

	_, err := flowtest.Load(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is computed from itself")
	require.Contains(t, err.Error(), "vars.fromdir (")
	require.Contains(t, err.Error(), flowtest.DirDefaultsName,
		"the hop the sibling file wrote must name that file: %s", err)
	require.NotContains(t, err.Error(), "vars.fromsuite ("+flowtest.DirDefaultsName,
		"the suite's own hop must not be attributed to the sibling: %s", err)
}

// TestAComputedVarReadsItsSiblingsAndNothingElse: each root a var may not read
// is refused in its own words, because they are different mistakes — `steps`
// is a question of when, `inputs` a question of whose.
func TestAComputedVarReadsItsSiblingsAndNothingElse(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		value    string
		contains string
	}{
		{"steps", "${steps.keep.value}", "no step has produced anything yet"},
		{"inputs", "${inputs.order}", "a case's `inputs:` are its own"},
		{"run", "${run.failed}", "describes a case that has finished"},
		{"trigger", "${trigger.kind}", "a delivery belongs to the case that replays it"},
		{"the whole block", "${vars}", "reads the whole `vars` block"},
		{"the block, indexed", "${vars['other']}", "reads the whole `vars` block"},
		{"an undeclared sibling", "${vars.nope}", "names no \"nope\""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  other: plain
  reader: "`+tc.value+`"
tests:
  - name: never loads
    workflow: ./workflow.yaml
`))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "vars.reader")
			assert.Contains(t, err.Error(), tc.contains)
		})
	}
}

// TestAComprehensionsOwnBindingsAreNotRefused is the other side of that walk,
// and the reason it tracks what the grammar binds (CLAUDE.md, "a rewriter has
// to know what the grammar binds"): `x` in a `map` is the macro's, not a name
// the file got wrong, and `string` is CEL's own type identifier.
//
// The last two vars are the case the tracking exists for: a comprehension may
// legally bind a name the walk above refuses as a root, and inside that loop
// the name is the macro's. A walk that judged names without knowing what the
// grammar binds would refuse both — a false diagnostic about an expression
// that evaluates perfectly well.
func TestAComprehensionsOwnBindingsAreNotRefused(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), echoWorkflow)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
vars:
  ids: "${ ['a', 'b'].map(x, 'ord_' + x) }"
  typed: "${ type(vars.ids[0]) == string }"
  order: "${ {'id': vars.ids[1]} }"
  shadowed: "${ ['ok'].map(steps, steps)[0] }"
  shadowedRoot: "${ ['ok'].map(vars, vars)[0] }"
tests:
  - name: the macro's own names are fine
    workflow: ./workflow.yaml
    inputs:
      order: "${vars.order}"
    expect:
      ran: [keep]
      outputs:
        id: ord_b
      check:
        - vars.typed
        - vars.shadowed == 'ok'
        - vars.shadowedRoot == 'ok'
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
}

// TestAProfileGatedFunctionInAVarNamesTheFunction is #1072's repair 3. A file's
// vars are not bound to a workflow — two cases in one suite may name different
// ones — so they evaluate in the profile-independent environment, and a call
// that needs a profile library is a load-time refusal naming it. cel-go's own
// answer at run time is `no such overload`, which for a member call names
// nothing at all.
func TestAProfileGatedFunctionInAVarNamesTheFunction(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		value    string
		contains string
	}{
		{"a gated global", "${json_parse('{}')}", "calls json_parse()"},
		{"a gated method", "${'a,b'.split(',')}", "calls split()"},
		{"a gated namespace", "${base64.encode(b'x')}", "calls base64.encode()"},
		{"a name nothing has", "${nosuchfunction(1)}", "calls unknown function nosuchfunction()"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  computed: "`+tc.value+`"
tests:
  - name: never loads
    workflow: ./workflow.yaml
`))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "vars.computed")
			assert.Contains(t, err.Error(), tc.contains)
		})
	}
}

// TestAComputedVarIsBoundedByCost, in both directions: the bound has to be
// reached to be a bound, and a fixture-sized expression has to fit under it or
// the bound is a refusal of the feature.
func TestAComputedVarIsBoundedByCost(t *testing.T) {
	t.Parallel()

	twenty := "[" + strings.Repeat("0,", 19) + "0]"

	t.Run("a fixture fits", func(t *testing.T) {
		t.Parallel()

		file, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  spread: "${ `+twenty+`.map(i, 'value-' + string(i)) }"
tests:
  - name: loads
    workflow: ./workflow.yaml
`))
		require.NoError(t, err)
		require.Len(t, file.Vars["spread"], 20)
	})

	t.Run("a program does not", func(t *testing.T) {
		t.Parallel()

		_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  program: "${ `+twenty+`.map(a, `+twenty+`.map(b, `+twenty+`.map(c, c))) }"
tests:
  - name: never loads
    workflow: ./workflow.yaml
`))
		require.Error(t, err)
		require.Contains(t, err.Error(), "vars.program")
		require.Contains(t, err.Error(), "cost limit exceeded")
	})
}

// TestAVarComputedFromASecretIsWithheldWhereverItPrints is #1072's repair 4,
// and the containment shapes CLAUDE.md asks for: the value is withheld in a
// check's witness, in the case's transcript, and in a diagnostic — through the
// one redaction set every surface of a case shares.
//
// The positive direction first, so the absence assertions below have something
// to be about: the case fails on purpose, and its witnesses are rendered.
func TestAVarComputedFromASecretIsWithheldWhereverItPrints(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: bearer-request
inputs:
  headers:
    type: struct
steps:
  - id: call
    http:
      url: https://api.example.com/status
      bearer: ${secret('env:TOKEN')}
  - id: echo
    value: ${inputs.headers.Authorization}
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	// The structure lives at the position that *uses* it, with a computed
	// string var at its leaf — the spelling the container refusal points an
	// author at, and legal at any depth because resolveVarsInValue substitutes
	// a whole-value `${vars.x}` wherever it appears in a fixture tree.
	writeFile(t, path, `
vars:
  token: s3cr3t-value
  header: "${'Bearer ' + vars.token}"
  region: eu-west-1
tests:
  - name: the derived values never print
    workflow: ./workflow.yaml
    inputs:
      headers:
        Authorization: "${vars.header}"
        Accept: application/json
    secrets:
      env:TOKEN: "${vars.token}"
    stubs:
      - task: http
        returns:
          status_code: 200
    expect:
      ran: [call, echo]
      check:
        - that: vars.header == 'nope'
          because: this claim is false on purpose, so its witnesses render
        - that: steps.echo.value == 'nope'
          because: and the leaf, once the fixture has carried it into the run
        - that: inputs.headers.Accept == 'nope'
          because: the untainted leaf of the same structure still shows itself
        - that: vars.region == 'nope'
          because: and so does a var on no path to a secret at all
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.False(t, c.GetPassed(), "the claims are false on purpose")
	require.NotEmpty(t, c.GetFailures())

	rendered := fmt.Sprintf("%v %+v %#v %s", c.GetFailures(), c.GetFailures(), c.GetFailures(), c.GetFailures())
	assert.Contains(t, rendered, "vars.header = [redacted]",
		"a var computed from a secret must be withheld in a witness, not merely cleared of the secret")
	assert.Contains(t, rendered, `steps.echo.value = \"[redacted]\"`,
		"and withheld whole once a fixture has carried it into the run, where no `vars.` name roots it")
	assert.NotContains(t, rendered, "s3cr3t-value")
	assert.NotContains(t, rendered, "Bearer s3cr3t")
	assert.NotContains(t, rendered, `\"Bearer [redacted]\"`,
		"partial clearing still says a header derived from a secret is a header")

	// The controls, without which every assertion above would also pass on a
	// loader that withheld everything. Both matter: withholding is per var, so
	// the untainted leaf of the very structure carrying the tainted one is
	// still shown, and so is a var on no path to a secret at all.
	assert.Contains(t, rendered, `inputs.headers.Accept = \"application/json\"`)
	assert.Contains(t, rendered, `vars.region = \"eu-west-1\"`)
}

// TestAWithheldVarsMaterialIsWithheldWhereverItTravelled is the other half of
// repair 4, and the half a `vars.`-rooted rule cannot reach: a withheld var
// substituted into a case's `inputs:` is run data by the time anything prints
// it, so what protects it there is the *material* joining the case's redaction
// set rather than the name.
func TestAWithheldVarsMaterialIsWithheldWhereverItTravelled(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: forwarder
inputs:
  header:
    type: string
steps:
  - id: keep
    value: ${inputs.header}
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
vars:
  token: s3cr3t-value
  header: "${'Bearer ' + vars.token}"
tests:
  - name: the material never prints, whatever position it reached
    workflow: ./workflow.yaml
    inputs:
      header: "${vars.header}"
    secrets:
      env:TOKEN: "${vars.token}"
    expect:
      ran: [keep]
      check:
        - that: steps.keep.value == 'nope'
          because: false on purpose, so the witness for a non-vars path renders
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.False(t, c.GetPassed(), "the claim is false on purpose")

	rendered := fmt.Sprintf("%v %+v %#v %s", c.GetFailures(), c.GetFailures(), c.GetFailures(), c.GetFailures())
	assert.Contains(t, rendered, `steps.keep.value = \"[redacted]\"`,
		"a withheld var's material is withheld whole, not cleared of the secret inside it")
	assert.NotContains(t, rendered, "s3cr3t-value")
	assert.NotContains(t, rendered, "Bearer [redacted]",
		"`Bearer [redacted]` is what the substring backstop alone produces, and it says "+
			"that a header derived from a secret is a header — which is the shape #1072 "+
			"repair 4 withholds")
}

// TestASourceOfASecretIsWithheldToo is Codex's scenario on #1197, exactly: a
// computed var is what `secrets:` names, so the redaction set holds the whole
// `Bearer …` string and nothing matches the token on its own. A check
// witnessing `vars.token` printed it.
//
// The taint therefore runs backward through the dependencies of a
// secret-holding var as well as forward through its readers — the source
// material of a secret is secret. This test fails on f4dcbca6.
func TestASourceOfASecretIsWithheldToo(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: bearer-request
steps:
  - id: call
    http:
      url: https://api.example.com/status
      bearer: ${secret('env:TOKEN')}
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
vars:
  token: s3cr3t-value
  derived: "${'Bearer ' + vars.token}"
  region: eu-west-1
tests:
  - name: the source never prints
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: "${vars.derived}"
    stubs:
      - task: http
        returns:
          status_code: 200
    expect:
      ran: [call]
      check:
        - that: vars.token == 'nope'
          because: false on purpose, so the witness for the source renders
        - that: vars.region == 'nope'
          because: and so does the witness for a var on no path to a secret
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.False(t, c.GetPassed(), "the claims are false on purpose")

	rendered := fmt.Sprintf("%v %+v %#v %s", c.GetFailures(), c.GetFailures(), c.GetFailures(), c.GetFailures())
	assert.Contains(t, rendered, "vars.token = [redacted]",
		"the token is what `Bearer <token>` was built from, and the set holds only the whole string")
	assert.NotContains(t, rendered, "s3cr3t-value")

	// The positive control, without which the assertion above would also pass
	// on a loader that withheld every var it has: a var on no path to a secret
	// still prints, or checks stop being able to say what they saw.
	assert.Contains(t, rendered, `vars.region = \"eu-west-1\"`)
}

// TestASecretDerivedValueRedactionCannotWithholdIsRefused is Codex's second
// P1: a tainted var holding a non-string adds nothing to the redaction set, so
// once a fixture substitutes it the transcript line is rooted at `steps.*`
// rather than at the withheld `vars.*` name and the number prints. It is also
// a length oracle in its own right.
//
// So it may not exist. The refusal is scoped to the tainted set exactly, which
// is what the second subtest is for: the identical expression over a var on no
// path to a secret is an ordinary fixture. Both fail on f4dcbca6 — the first
// loads there, and the second is the control that keeps this from being a ban
// on `size()`.
func TestASecretDerivedValueRedactionCannotWithholdIsRefused(t *testing.T) {
	t.Parallel()

	t.Run("tainted", func(t *testing.T) {
		t.Parallel()

		_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  token: s3cr3t-value
  fingerprint: "${size(vars.token)}"
tests:
  - name: never loads
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: "${vars.token}"
`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "vars.fingerprint is computed from a secret and holds an integer")
		assert.Contains(t, err.Error(), "only a non-empty string can be withheld")
		assert.Contains(t, err.Error(), `vars.fingerprint → vars.token, which tests[0].secrets["env:TOKEN"] references`,
			"the refusal names the chain, or its claim is one an author cannot check")
		assert.NotContains(t, err.Error(), "s3cr3t-value")
	})

	t.Run("untainted", func(t *testing.T) {
		t.Parallel()

		file, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  hostlist: "${['a', 'b', 'c']}"
  count: "${size(vars.hostlist)}"
tests:
  - name: loads
    workflow: ./workflow.yaml
`))
		require.NoError(t, err, "a size() over a var on no path to a secret is an ordinary fixture")
		require.Equal(t, int64(3), file.Vars["count"])
	})
}

// TestATaintedContainerIsRefusedForItsShape is Codex's third P1 on #1197 and
// the owner's ruling on it, which completes the family: a string leaks by
// value and is withheld, a scalar leaks by value with nothing to match and is
// refused, and a container leaks by *shape*.
//
// The ternary is the scenario as filed. Redaction clears every string in the
// map and leaves whether the map is empty, which is an equality oracle about
// the secret — so a tainted container may not exist, whatever its leaves turn
// out to be. Both subtests fail on 7e600f67, where the classifier walked to
// the first non-string leaf and called a map of strings protectable.
func TestATaintedContainerIsRefusedForItsShape(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		value string
		kind  string
	}{
		{
			name:  "a shape a secret chooses",
			value: "${vars.token == 'guess' ? {} : {'x': 'y'}}",
			kind:  "a map",
		},
		{
			// The delta from the previous classifier, and the reason the rule
			// is about containers rather than about leaves: every leaf here is
			// a string, and redaction still leaves the shape standing.
			name:  "a container whose every leaf is a string",
			value: "${ {'Authorization': 'Bearer ' + vars.token} }",
			kind:  "a map",
		},
		{
			name:  "a list",
			value: "${ [vars.token] }",
			kind:  "a list",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  token: s3cr3t-value
  shaped: "`+tc.value+`"
tests:
  - name: never loads
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: "${vars.token}"
`))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "vars.shaped is computed from a secret and holds "+tc.kind)
			assert.Contains(t, err.Error(), "a container's shape, an empty string's very emptiness")
			assert.Contains(t, err.Error(),
				`vars.shaped → vars.token, which tests[0].secrets["env:TOKEN"] references`)
			assert.Contains(t, err.Error(), "express any structure where it is used",
				"the refusal costs one respelling, so it names it")
			assert.NotContains(t, err.Error(), "s3cr3t-value")
		})
	}

	// The control: the identical shapes over vars on no path to any secret are
	// ordinary fixtures, which is what keeps a rule this blunt off files that
	// have nothing to do with secrets.
	t.Run("untainted containers are ordinary fixtures", func(t *testing.T) {
		t.Parallel()

		file, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  region: eu-west-1
  order: "${ {'id': 'ord_1', 'region': vars.region} }"
  hosts: "${ ['a', 'b'] }"
tests:
  - name: loads
    workflow: ./workflow.yaml
`))
		require.NoError(t, err)
		require.Equal(t, map[string]any{"id": "ord_1", "region": "eu-west-1"}, file.Vars["order"])
		require.Equal(t, []any{"a", "b"}, file.Vars["hosts"])
	})
}

// TestATaintedEmptyStringIsRefused is Codex's fifth P1 and the ruling that
// completes the family: emptiness is shape, and shape was already refused.
//
// The set cannot hold `""` — it occurs at every position of every string, so
// [collectVarStrings] declines it and redacting it would destroy the text while
// protecting nothing — and a value the set cannot hold is a value that prints.
// `${t == 'guess' ? ” : 'x'}` therefore renders `""` in one branch and
// `[redacted]` in the other, which is an equality oracle read straight off a
// report. Fails on af336db4.
//
// The conditional below is written so the *empty* branch is the one taken,
// because the refusal reads the evaluated value: a file whose ternary lands on
// the non-empty side produces an ordinary withheld string and is not refused.
// That asymmetry is the second-order residual the package doc names — the
// refusal firing at all tells the file's own author which branch ran — and it
// is strictly less than the `""` it stops from being printed.
func TestATaintedEmptyStringIsRefused(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  token: s3cr3t-value
  probe: "${vars.token == 's3cr3t-value' ? '' : 'x'}"
tests:
  - name: never loads
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: "${vars.token}"
`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vars.probe is computed from a secret and holds the empty string")
	assert.Contains(t, err.Error(), "an empty string's very emptiness")
	assert.Contains(t, err.Error(),
		`vars.probe → vars.token, which tests[0].secrets["env:TOKEN"] references`)
}

// TestATaintedNonEmptyStringIsWithheldNotRefused is the boundary's edge, and
// the control the ruling above needs: the refusal reaches emptiness and stops
// there. A tainted string with content in it is still the *withheld* case, or
// the whole first row of the contract has quietly become a refusal.
func TestATaintedNonEmptyStringIsWithheldNotRefused(t *testing.T) {
	t.Parallel()

	file, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  token: s3cr3t-value
  header: "${'Bearer ' + vars.token}"
  space: "${' '}"
tests:
  - name: loads
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: "${vars.token}"
`))
	require.NoError(t, err, "a tainted string with content is withheld, never refused")
	assert.Equal(t, "Bearer s3cr3t-value", file.Vars["header"])
	assert.Equal(t, " ", file.Vars["space"],
		"a single space is content: the rule is emptiness, not blankness")
}

// TestATaintedStructureRespellsAtTheFixture is the other half of that
// refusal's promise: the diagnostic tells an author to express the structure
// where it is used and keep the derived value a string, and that spelling has
// to actually work — at depth, inside a list, and through `defaults.inputs:`.
//
// A refusal whose advice does not compile is worse than no advice.
func TestATaintedStructureRespellsAtTheFixture(t *testing.T) {
	t.Parallel()

	file, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  token: s3cr3t-value
  header: "${'Bearer ' + vars.token}"
defaults:
  workflow: ./workflow.yaml
  inputs:
    headers:
      - name: Authorization
        value: "${vars.header}"
tests:
  - name: loads
    inputs:
      envelope:
        auth:
          scheme: bearer
          credential: "${vars.header}"
    secrets:
      env:TOKEN: "${vars.token}"
`))
	require.NoError(t, err, "the respelling the refusal names must load")

	require.Equal(t,
		map[string]any{"headers": []any{map[string]any{"name": "Authorization", "value": "Bearer s3cr3t-value"}}},
		file.Defaults.Inputs, "a `${vars.x}` leaf resolves inside a list under defaults")

	// The case's own inputs, which by now also carry the block's — `Load`
	// merges `defaults:` into every case — so both halves of the respelling
	// are asserted here on the effective fixture the run will see.
	require.Equal(t, map[string]any{
		"envelope": map[string]any{
			"auth": map[string]any{"scheme": "bearer", "credential": "Bearer s3cr3t-value"},
		},
		"headers": []any{map[string]any{"name": "Authorization", "value": "Bearer s3cr3t-value"}},
	}, file.Tests[0].Inputs, "and at depth in a case's own inputs")
}

// TestARefusedValueNeverReachesALaterEvaluation is Codex's fourth P1 on #1197,
// which is an ordering defect rather than a change to what is refused: every
// var used to evaluate before the refusal swept the block, so an unprotectable
// value existed long enough to be quoted by a *downstream* CEL error —
// `index out of bounds: 12` naming a secret's length.
//
// "Refused into existence" has to mean it. A var is now judged the moment it
// evaluates and before it is stored, so it never enters the activation and no
// later expression can read it. Fails on 3a8c5485, where the digits appear.
func TestARefusedValueNeverReachesALaterEvaluation(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  token: s3cr3t-value
  length: "${size(vars.token)}"
  bad: "${[0][vars.length]}"
tests:
  - name: never loads
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: "${vars.token}"
`))
	require.Error(t, err)

	problems, refused := errorAsDiagnostics(t, err)
	require.True(t, refused)

	// Every problem the report carries, message and field only. Deliberately
	// not `err.Error()`: that prefixes each line with the file's path, and a
	// `t.TempDir()` path is full of random digits — the assertion below is
	// about digits, and CLAUDE.md records the run-in-three flake that costs
	// ("NotContains(stderr, \"42\")", #1145). The path is covered separately
	// for the substrings that cannot occur in one.
	messages := problemMessages(problems)

	assert.Contains(t, messages, "vars.length is computed from a secret and holds an integer")
	assert.NotContains(t, messages, strconv.Itoa(len("s3cr3t-value")),
		"the length of the secret must appear in no diagnostic: %s", messages)
	assert.NotContains(t, messages, "s3cr3t-value")
	assert.NotContains(t, err.Error(), "index out of bounds",
		"the dependent must not evaluate at all, so its error cannot exist to quote anything")
	assert.NotContains(t, err.Error(), "vars.bad",
		"a dependent of a refused var adds no cascade diagnostic; the root refusal stands for the chain")
}

// problemMessages joins what a refusal says, without the file path each line is
// rendered with — so an assertion about digits is about the diagnostic rather
// than about the random ones in a `t.TempDir()`.
func problemMessages(problems *flowtest.Diagnostics) string {
	var b strings.Builder
	for _, d := range problems.Problems {
		b.WriteString(d.Message + " " + d.Field + "\n")
	}

	return b.String()
}

// TestARefusedLiteralNeverReachesALaterEvaluation is the same ordering claim
// on the other code path. A literal is never "evaluated", so it is in the
// activation from the first expression onward — which means judging it in the
// loop would be too late and it is judged before the loop begins.
//
// Reachable only through the backward closure: `port` is a literal integer
// that contributes to a value `secrets:` names, so it is secret material.
func TestARefusedLiteralNeverReachesALaterEvaluation(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  port: 8080
  password: hunter2
  dsn: "${'postgres://u:' + vars.password + '@h:' + string(vars.port)}"
  bad: "${[0][vars.port]}"
tests:
  - name: never loads
    workflow: ./workflow.yaml
    secrets:
      env:DSN: "${vars.dsn}"
`))
	require.Error(t, err)

	problems, refused := errorAsDiagnostics(t, err)
	require.True(t, refused)
	messages := problemMessages(problems)

	assert.Contains(t, messages, "vars.port is computed from a secret and holds an integer")
	assert.Contains(t, messages,
		`vars.port → vars.dsn, which tests[0].secrets["env:DSN"] references`)
	// Messages rather than the rendering, for the `t.TempDir()` digit reason
	// the test above records.
	assert.NotContains(t, messages, "8080",
		"a literal source of a secret must not reach a later evaluation's error either: %s", messages)
	assert.NotContains(t, messages, "hunter2")
	assert.NotContains(t, err.Error(), "index out of bounds")
}

// TestACheckErrorQuotingAWithheldValueIsWithheld is Codex's sixth P1: a check
// that *errors* rather than answering false was the third rendering in
// check.go, and the one going through neither redaction — it formatted cel-go's
// error straight in while the witnesses beside it were guarded.
//
// The claim below fails because a map has no such key, and cel-go's error
// carries the key: the withheld header's full value. Fails on af336db4.
func TestACheckErrorQuotingAWithheldValueIsWithheld(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: bearer-request
steps:
  - id: call
    http:
      url: https://api.example.com/status
      bearer: ${secret('env:TOKEN')}
outputs: {}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
vars:
  token: s3cr3t-value
  header: "${'Bearer ' + vars.token}"
  region: eu-west-1
tests:
  - name: an erroring claim says nothing it may not
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: "${vars.token}"
    stubs:
      - task: http
        returns:
          status_code: 200
    expect:
      ran: [call]
      check:
        - that: "{'known': 1}[vars.header] == 1"
          because: errors on the missing key, whose text carries the key itself
        - that: "[0][size(vars.header)] == 1"
          because: errors with a length computed inside the claim, which no set ever saw
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.False(t, c.GetPassed(), "the claims error on purpose")
	require.Len(t, c.GetFailures(), 2)

	rendered := fmt.Sprintf("%v %+v %#v %s", c.GetFailures(), c.GetFailures(), c.GetFailures(), c.GetFailures())
	assert.Contains(t, rendered, "check errored",
		"the failure must still be reported, or this hides a real problem")
	assert.Contains(t, rendered, "withheld: this claim reads vars.header",
		"the var is named so an author knows which claim to rewrite; a name is not a value")
	assert.NotContains(t, rendered, "s3cr3t-value")
	assert.NotContains(t, rendered, "Bearer s3cr3t")

	// The second claim is why the withheld-var rule exists beside the set. Its
	// error reports on a length computed *inside* the claim, which the set never
	// saw and could not match — the fourth finding's shape at this surface — so
	// the digits must be absent because the claim was refused a rendering, not
	// because a substring happened to be recognised.
	assert.NotContains(t, problemDigits(t, c.GetFailures()), strconv.Itoa(len("Bearer s3cr3t-value")))
}

// problemDigits joins failure messages for an assertion about digits, keeping
// the author's own claim text out of it: the claims above quote `1` and `0`,
// and the question is what the *evaluator's* answer said.
func problemDigits(t *testing.T, failures []*v1.Diagnostic) string {
	t.Helper()

	var b strings.Builder
	for _, f := range failures {
		_, after, found := strings.Cut(f.GetMessage(), "\n")
		if found {
			b.WriteString(after + "\n")
		}
	}

	return b.String()
}

// TestACheckErrorOverUntaintedValuesKeepsItsDetail is that fix's control: the
// same shape of failure over vars on no path to a secret keeps cel-go's own
// message, because a diagnostic that withholds what it need not is a diagnostic
// that stopped being useful.
func TestACheckErrorOverUntaintedValuesKeepsItsDetail(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), echoWorkflow)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
vars:
  region: eu-west-1
  order: "${ {'id': 'ord_1'} }"
tests:
  - name: an erroring claim over ordinary values
    workflow: ./workflow.yaml
    inputs:
      order: "${vars.order}"
    expect:
      ran: [keep]
      check:
        - that: "{'known': 1}[vars.region] == 1"
          because: errors on the missing key, and nothing here is secret
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.False(t, c.GetPassed(), "the claim errors on purpose")

	rendered := fmt.Sprintf("%v", c.GetFailures())
	assert.Contains(t, rendered, "check errored")
	assert.Contains(t, rendered, "eu-west-1",
		"an untainted value's evaluator error keeps the detail that makes it actionable")
	assert.NotContains(t, rendered, "withheld")
}

// TestAVarErrorOverATransformedTaintIsWithheldWhole is Codex's seventh P1, and
// the same shape as its sixth at the sibling surface: a rendering that clears
// *known strings* cannot reach a fact computed inside the failing expression.
//
// `${[0][size(vars.token)]}` transforms its tainted dependency and fails in one
// step, so cel-go's error carries the token's length rather than the token —
// nothing the set ever held — and the eager unprotectable-value refusal never
// runs, because the var produced no value at all. The load-time error is
// therefore withheld whole whenever the expression reads a tainted var, which
// is what `checkErrorText` already does one file over. Fails on 90241327.
func TestAVarErrorOverATransformedTaintIsWithheldWhole(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  token: s3cr3t-value
  bad: "${[0][size(vars.token)]}"
tests:
  - name: never loads
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: "${vars.token}"
`))
	require.Error(t, err)

	problems, refused := errorAsDiagnostics(t, err)
	require.True(t, refused)
	messages := problemMessages(problems)

	assert.Contains(t, messages, "vars.bad: evaluating ${[0][size(vars.token)]}",
		"the expression the author wrote is still quoted; it is theirs")
	assert.Contains(t, messages, "[withheld: this expression reads vars.token, which this file withholds]",
		"and the dependency that cost it its detail is named, because a name is not a value")
	assert.NotContains(t, messages, "index out of bounds")
	// The token's length, which cel-go's error carried and no redaction set
	// could have matched. Messages rather than the rendering, for the
	// `t.TempDir()` digit reason the neighbouring tests record.
	assert.NotContains(t, messages, strconv.Itoa(len("s3cr3t-value")))
	assert.NotContains(t, messages, "s3cr3t-value")
}

// TestAnUntaintedValuesEvaluationErrorIsStillQuoted is the positive control for
// that fix, and the reason it is scoped to the tainted set: the eager refusal
// must not swallow legitimate error detail. The identical misuse over vars on
// no path to any secret still produces cel-go's own message, digits and all.
func TestAnUntaintedValuesEvaluationErrorIsStillQuoted(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  hosts: "${['a', 'b', 'c']}"
  length: "${size(vars.hosts)}"
  bad: "${[0][vars.length]}"
tests:
  - name: never loads
    workflow: ./workflow.yaml
`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vars.bad")
	assert.Contains(t, err.Error(), "index out of bounds: 3",
		"an untainted value's evaluation error keeps the detail that makes it useful")
	assert.NotContains(t, err.Error(), "computed from a secret")
}

// TestARefusedVarStillLetsAnIndependentProblemBeReported: the cascade rule
// silences the *chain*, never the file. A second mistake somewhere else is a
// second mistake, and an author fixing a suite one run at a time is what the
// collecting loader exists to prevent.
func TestARefusedVarStillLetsAnIndependentProblemBeReported(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  token: s3cr3t-value
  length: "${size(vars.token)}"
  bad: "${[0][vars.length]}"
  unrelated: "${steps.nope.value}"
coverage:
  allow_unreached:
    orphan: ""
tests:
  - name: never loads
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: "${vars.token}"
`))
	require.Error(t, err)

	assert.Contains(t, err.Error(), "vars.length is computed from a secret",
		"the root refusal")
	assert.Contains(t, err.Error(), "vars.unrelated reads `steps`",
		"a var with its own mistake, on no path to the refused one, is still judged")
	assert.Contains(t, err.Error(), "coverage.allow_unreached[\"orphan\"] has no reason",
		"and so is a problem in another stanza entirely")
	assert.NotContains(t, err.Error(), "vars.bad",
		"only the dependents of the refused var are silent")
}

// TestATransitivelyTaintedNonStringIsRefused is where the two decisions meet:
// the backward closure is what makes `fingerprint` tainted at all — it reads a
// var that is only reachable by walking *into* the secret's sources — and the
// non-string rule is what refuses it. Neither alone reaches this file.
func TestATransitivelyTaintedNonStringIsRefused(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  token: s3cr3t-value
  derived: "${'Bearer ' + vars.token}"
  fingerprint: "${size(vars.token)}"
tests:
  - name: never loads
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: "${vars.derived}"
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "vars.fingerprint is computed from a secret and holds an integer")
	require.Contains(t, err.Error(),
		`vars.fingerprint → vars.token → vars.derived, which tests[0].secrets["env:TOKEN"] references`,
		"the chain runs out through the source and back to the entry that names it")
}

// TestAVarsRefusalQuotesTheExpressionNotTheValue: a load-time refusal is the
// one path where a value could reach a message without passing a redaction
// set, because there is no case yet and so no set. It quotes what the author
// wrote, and scrubs what CEL put in its own error.
func TestAVarsRefusalQuotesTheExpressionNotTheValue(t *testing.T) {
	t.Parallel()

	_, err := flowtest.Load(writeInline(t, t.TempDir(), `
vars:
  token: s3cr3t-value
  probe: "${ {'known': 1}[vars.token] }"
tests:
  - name: never loads
    workflow: ./workflow.yaml
    secrets:
      env:TOKEN: "${vars.token}"
`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "vars.probe")
	require.Contains(t, err.Error(), "{'known': 1}[vars.token]",
		"the refusal quotes the expression the author wrote")
	require.NotContains(t, err.Error(), "s3cr3t-value",
		"cel-go's `no such key` carries the operand, which here is a secret's plaintext")
}

// TestAComputedVarMayShareAWorkflowVarsName is #1072's repair 2, one level up
// from TestAStubsVarsAreTheWorkflowsNotTheFiles: the collision refusal is
// narrowed to names reachable in the check scope, and no workflow ambient var
// is reachable there today. A file computing `greeting` while the workflow
// declares one is two names in two scopes, and both keep their meanings.
func TestAComputedVarMayShareAWorkflowVarsName(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "workflow.yaml"), `
edition: v2026.3
name: spoken
vars:
  greeting: hello
steps:
  - id: say
    log:
      message: ${vars.greeting}
outputs:
  said:
    value: ${steps.say.echoed}
`)
	path := filepath.Join(dir, "workflow.test.yaml")
	writeFile(t, path, `
vars:
  part: good
  greeting: "${vars.part + 'bye'}"
tests:
  - name: two scopes, two meanings
    workflow: ./workflow.yaml
    stubs:
      - task: log
        where: vars.greeting == 'hello'
        returns:
          echoed: "${vars.greeting}"
    expect:
      ran: [say]
      outputs:
        said: hello
      check:
        - vars.greeting == 'goodbye'
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.True(t, c.GetPassed(), "%v / %v", c.GetError(), c.GetFailures())
}

// TestALiteralVarsFileLoadsAsItsInlinedTwin is #1072's repair 7: "strictly
// additive" asserted rather than assumed.
//
// A suite whose vars are all literals must load to exactly the document it
// would be with every reference written out by hand — the same inputs, at the
// same levels, after the same merges. That fails if a literal is ever
// evaluated (`1 + 1` would stop being the string `1 + 1`), and it fails if the
// ordering moves, because substitution has to happen before the table expands
// and before `defaults:` merges for a row to see the value at all.
func TestALiteralVarsFileLoadsAsItsInlinedTwin(t *testing.T) {
	t.Parallel()

	const withVars = `
vars:
  who: team-a
  issuer: https://issuer.example.com
  arith: 1 + 1
  path: steps.greet.result
  order:
    id: ord_1
    region: eu-west-1
defaults:
  workflow: ./workflow.yaml
  inputs:
    who: "${vars.who}"
    order: "${vars.order}"
tests:
  - name: a table
    expect:
      outputs:
        note: "${vars.arith}"
    cases:
      - name: inherits
      - name: overrides
        inputs:
          who: "${vars.who}"
          note: "${vars.path}"
        secrets:
          env:TOKEN: "${vars.who}"
        starter:
          subject: "${vars.who}"
          issuer: "${vars.issuer}"
        signals:
          - name: go
            payload:
              order: "${vars.order}"
`

	const inlined = `
defaults:
  workflow: ./workflow.yaml
  inputs:
    who: team-a
    order:
      id: ord_1
      region: eu-west-1
tests:
  - name: a table
    expect:
      outputs:
        note: 1 + 1
    cases:
      - name: inherits
      - name: overrides
        inputs:
          who: team-a
          note: steps.greet.result
        secrets:
          env:TOKEN: team-a
        starter:
          subject: team-a
          issuer: https://issuer.example.com
        signals:
          - name: go
            payload:
              order:
                id: ord_1
                region: eu-west-1
`

	dir := t.TempDir()
	referenced, err := flowtest.Load(writeInline(t, dir, withVars))
	require.NoError(t, err)
	written, err := flowtest.Load(writeInline(t, t.TempDir(), inlined))
	require.NoError(t, err)

	require.Equal(t, written.Defaults, referenced.Defaults)
	require.Equal(t, written.Tests, referenced.Tests,
		"a literal-vars suite must load to the document it would be with every reference written out")
}

// TestAVarValueThatOnlyLooksLikeAFenceIsALiteral is the other half of that
// claim, over the shapes nearest to a fence. None of these is one, so each
// stays the text the file wrote — before this change because nothing was
// evaluated at all, after it because the fence rule is a whole-value rule.
func TestAVarValueThatOnlyLooksLikeAFenceIsALiteral(t *testing.T) {
	t.Parallel()

	for _, value := range []string{
		"$ {vars.who}",
		"{vars.who}",
		"$vars.who",
		"steps.greet.result",
		"1 + 1",
		"true",
	} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			file, err := flowtest.Load(writeInline(t, t.TempDir(), "vars:\n  who: "+quoteYAML(value)+`
tests:
  - name: loads
    workflow: ./workflow.yaml
`))
			require.NoError(t, err)
			require.Equal(t, value, file.Vars["who"])
		})
	}
}

// TestAVarValueMixingTextWithAFenceIsStillRefused is the same claim from the
// other side: a value that carries a fence without *being* one was refused
// before computed vars and is refused after, because half a fence is neither
// a literal nor something anything evaluates.
func TestAVarValueMixingTextWithAFenceIsStillRefused(t *testing.T) {
	t.Parallel()

	for _, value := range []string{
		"${vars.who} and more",
		"before ${vars.who}",
		`\${vars.who}`,
	} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			_, err := flowtest.Load(writeInline(t, t.TempDir(), "vars:\n  who: "+quoteYAML(value)+`
tests:
  - name: never loads
    workflow: ./workflow.yaml
`))
			require.Error(t, err)
			require.Contains(t, err.Error(), "vars.who holds the expression")
		})
	}
}

// quoteYAML renders one string as a YAML double-quoted scalar, so a value
// chosen for looking like a fence cannot be reinterpreted by the parser on its
// way into the fixture.
func quoteYAML(s string) string {
	return `"` + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`) + `"`
}
