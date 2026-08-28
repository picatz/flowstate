package flowtest_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
  fingerprint: "${size(vars.token)}"
tests:
  - name: the derived values never print
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
        - that: vars.header == 'nope'
          because: this claim is false on purpose, so its witnesses render
        - vars.fingerprint == 0
`)

	report := flowtest.RunFile(path)
	require.Empty(t, report.GetRefused())
	c := report.GetCases()[0]
	require.False(t, c.GetPassed(), "the claims are false on purpose")
	require.NotEmpty(t, c.GetFailures())

	rendered := fmt.Sprintf("%v %+v %#v %s", c.GetFailures(), c.GetFailures(), c.GetFailures(), c.GetFailures())
	assert.Contains(t, rendered, "vars.header = [redacted]",
		"a var computed from a secret must be withheld in a witness, not merely cleared of the secret")
	assert.Contains(t, rendered, "vars.fingerprint = [redacted]",
		"a number derived from a secret shares no string with it, so only the taint can withhold it")
	assert.NotContains(t, rendered, "s3cr3t-value")
	assert.NotContains(t, rendered, "Bearer s3cr3t")
	// The size of the plaintext, which `fingerprint` holds. The claim text
	// beside it is the author's own and prints in full, so the assertion is
	// about the rendered *value*, checked above.
	assert.NotContains(t, rendered, "= 12")
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
