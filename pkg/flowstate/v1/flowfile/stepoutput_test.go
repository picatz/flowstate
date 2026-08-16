package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// A reference names a step *and* one of its outputs, and until `log:` arrived only the
// first half was checked. `${steps.web.nonsense}` validated cleanly and then resolved
// to nothing at run time — the reference silently produced no value, so the step using
// it did something other than what the file said, which is the worst of the available
// failures.
//
// The check is deliberately silent wherever the set of outputs is not knowable in full.
// A false diagnostic about a working file is worse than a missing one, so the cases
// below are as much about what is *not* reported as about what is.

// TestAnUnknownStepOutputIsReported covers the half that is knowable.
func TestAnUnknownStepOutputIsReported(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
		want string
	}{
		{
			name: "a name the task does not produce",
			src: `
edition: v2026.3
name: t
steps:
  - id: a
    http:
      url: https://example.com
  - id: b
    log:
      message: ${steps.a.nonsense}
`,
			want: `step "a" has no output "nonsense"; it produces status_code`,
		},
		{
			name: "a near miss gets a suggestion rather than a list",
			src: `
edition: v2026.3
name: t
steps:
  - id: a
    http:
      url: https://example.com
  - id: b
    log:
      message: ${steps.a.bdy}
`,
			want: `did you mean "body"?`,
		},
		{
			// The reason this check exists. A task with no outputs makes *every*
			// reference to it wrong, so the message explains the design rather than
			// listing an empty set — "it produces: " teaches nothing.
			name: "a task that produces nothing says why",
			src: `
edition: v2026.3
name: t
steps:
  - id: say
    log:
      message: hi
  - id: b
    log:
      message: ${steps.say.result}
`,
			want: "the log task produces no outputs, because a log step is an effect rather than a value",
		},
		{
			// One level down is the language's; deeper is CEL selecting into a value,
			// which this cannot and should not check.
			name: "selecting into a real output is fine",
			src: `
edition: v2026.3
name: t
steps:
  - id: a
    http:
      url: https://example.com
  - id: b
    log:
      message: ${steps.a.body.something.deeper}
`,
		},
		{
			// The whole outputs mapping, which any step has — including one with
			// nothing in it.
			name: "the mapping itself is fine",
			src: `
edition: v2026.3
name: t
steps:
  - id: say
    log:
      message: hi
  - id: b
    log:
      message: ${string(steps.say)}
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reported := diagnose(t, test.src)
			if test.want == "" {
				require.Empty(t, reported, "a legal reference was reported as an unknown output")

				return
			}
			require.Contains(t, reported, test.want)
		})
	}
}

// TestAStepNamingItsOwnOutputsIsNotSecondGuessed is the negative direction, and the one
// that would make this check a liability rather than a feature.
//
// The `http` task's `outputs:` input *replaces* its declared outputs with names the
// author chose. Reporting those against the descriptor would refuse a workflow the
// engine runs perfectly — this rule's own failure mode, pointed at the file it was
// written to help.
//
// Keyed on the input being present rather than on the task's name, so a plugin adopting
// the same shape inherits the exemption instead of being reported against a set it
// replaced.
func TestAStepNamingItsOwnOutputsIsNotSecondGuessed(t *testing.T) {
	t.Parallel()

	src := `
edition: v2026.3
name: t
steps:
  - id: fetch
    http:
      url: https://example.com
      outputs:
        anything: ${status_code}
  - id: use
    log:
      message: ${steps.fetch.anything}
`

	require.Empty(t, diagnose(t, src),
		"a step reading an output the fetch step named for itself was reported as unknown")
}

// TestABlockStepsOutputsAreNotSecondGuessed keeps the check inside what it knows: a
// correct reference to a `for_each`'s `results` and an unshaped `wait_for_signal`'s
// `payload` are both left alone.
//
// `for_each` is checked now (see [TestACertainKindOutputIsReported] below) because its
// one output, `results`, is fixed by the grammar the moment the step is written — no
// registry, sender, or authored expression stands between the file and the answer. An
// unshaped `wait_for_signal` is not: `payload` is whatever a sender sent, so this test
// is what is left of the original claim once that half moved out. See
// [TestACertainKindsAreExactlyWhatOutputNamesAnswersWithCertainty] for the line between
// the two.
func TestABlockStepsOutputsAreNotSecondGuessed(t *testing.T) {
	t.Parallel()

	src := `
edition: v2026.3
name: t
steps:
  - id: each
    for_each:
      items: ${["a"]}
      as: name
      steps:
        - id: inner
          log:
            message: ${name}
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
  - id: use
    log:
      message: ${string(steps.each.results) + string(steps.gate.payload)}
`

	require.Empty(t, diagnose(t, src),
		"a block step's outputs were checked against a task's descriptor, which it does not have")
}

// TestACertainKindOutputIsReported is the positive direction for the four kinds whose
// [v1.OutputNames] answer is certain by construction — a switch, a for_each, a call,
// and a parallel — plus a loop's general (non-`as:`) set, which shares the same
// certainty. Each of these is a real error `unknownStepOutput` missed before this
// change: the name is knowable in full from the file alone, and the reference is
// outside it.
func TestACertainKindOutputIsReported(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
		want string
	}{
		{
			name: "a switch's outputs are exactly value and case",
			src: `
edition: v2026.3
name: t
steps:
  - id: sw
    switch:
      value: ${1}
      cases:
        - case: 1
          steps:
            - id: hit
              log:
                message: hi
  - id: use
    log:
      message: ${string(steps.sw.velue)}
`,
			want: `step "sw" has no output "velue"; did you mean "value"?`,
		},
		{
			name: "a for_each's only output is results",
			src: `
edition: v2026.3
name: t
steps:
  - id: each
    for_each:
      items: ${["a"]}
      as: name
      steps:
        - id: inner
          log:
            message: ${name}
  - id: use
    log:
      message: ${string(steps.each.count)}
`,
			want: `step "each" has no output "count"; a for_each step produces results`,
		},
		{
			name: "a parallel's own id exposes nothing at all",
			src: `
edition: v2026.3
name: t
steps:
  - id: both
    parallel:
      - steps:
          - id: left
            log:
              message: left
      - steps:
          - id: right
            log:
              message: right
  - id: use
    log:
      message: ${string(steps.both.results)}
`,
			want: `step "both" has no output "results"; a parallel step's own id exposes nothing`,
		},
		{
			name: "a loop's as: name read from outside still gets its own message",
			src: `
edition: v2026.3
name: t
steps:
  - id: countup
    loop:
      as: acc
      init:
        n: 0
      until: ${acc.n >= 1}
      update:
        n: ${acc.n + 1}
      steps:
        - id: tick
          log:
            message: tick
  - id: use
    log:
      message: ${string(steps.countup.acc)}
`,
			want: `step "countup" has no output "acc"; ` + "`acc`" + ` is the name the loop binds *inside* itself (its ` + "`as:`" + `)`,
		},
		{
			name: "a loop's general set is checked too, beyond the as: collision",
			src: `
edition: v2026.3
name: t
steps:
  - id: countup
    loop:
      as: acc
      init:
        n: 0
      until: ${acc.n >= 1}
      update:
        n: ${acc.n + 1}
      steps:
        - id: tick
          log:
            message: tick
  - id: use
    log:
      message: ${string(steps.countup.count)}
`,
			want: `step "countup" has no output "count"; a loop step produces results, state`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Contains(t, diagnose(t, test.src), test.want)
		})
	}
}

// TestACertainKindOutputIsNotSecondGuessed is the negative direction for the same five
// cases, and the one that matters most per CLAUDE.md's "Test that A cannot reach B": a
// correct file using each of these shapes must produce zero diagnostics, not merely
// avoid the specific wrong name the positive test above checks.
func TestACertainKindOutputIsNotSecondGuessed(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
	}{
		{
			name: "a switch's value and case",
			src: `
edition: v2026.3
name: t
steps:
  - id: sw
    switch:
      value: ${1}
      cases:
        - case: 1
          steps:
            - id: hit
              log:
                message: hi
  - id: use
    log:
      message: ${string(steps.sw.value) + string(steps.sw.case)}
`,
		},
		{
			name: "a for_each's results",
			src: `
edition: v2026.3
name: t
steps:
  - id: each
    for_each:
      items: ${["a"]}
      as: name
      steps:
        - id: inner
          log:
            message: ${name}
  - id: use
    log:
      message: ${string(steps.each.results)}
`,
		},
		{
			name: "a parallel's whole mapping, which is legal even though nothing is named",
			src: `
edition: v2026.3
name: t
steps:
  - id: both
    parallel:
      - steps:
          - id: left
            log:
              message: left
      - steps:
          - id: right
            log:
              message: right
  - id: use
    log:
      message: ${string(steps.both)}
`,
		},
		{
			name: "a loop's results and its state, both",
			src: `
edition: v2026.3
name: t
steps:
  - id: countup
    loop:
      as: acc
      init:
        n: 0
      until: ${acc.n >= 1}
      update:
        n: ${acc.n + 1}
      steps:
        - id: tick
          log:
            message: tick
  - id: use
    log:
      message: ${string(steps.countup.results) + string(steps.countup.state)}
`,
		},
		{
			name: "a loop with no state: has only results, and results is fine",
			src: `
edition: v2026.3
name: t
steps:
  - id: retry
    loop:
      until: ${true}
      steps:
        - id: attempt
          log:
            message: hi
  - id: use
    log:
      message: ${string(steps.retry.results)}
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			require.Empty(t, diagnose(t, test.src),
				"a correct reference to a certain kind's real output was reported as unknown")
		})
	}
}

// TestACallStepOutputIsChecked exercises the one certain kind the table-driven tests
// above cannot: `call:` resolves a callee relative to the caller's own file, which
// [diagnose]'s bare-source helper cannot do, so this writes both files to disk the
// way [TestCallCompiles] does. Both directions in one test: a name outside the
// callee's declared outputs is reported, the declared name itself is not, and a
// callee declaring none makes every non-empty reference wrong — the "call" row of
// [certainStepOutput]'s emptyMessage.
func TestACallStepOutputIsChecked(t *testing.T) {
	t.Parallel()

	t.Run("a name outside the callee's declared outputs", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		writeFile(t, dir, "callee.yaml", simpleCalleeSource)
		caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      tenant: acme
  - id: use
    log:
      message: ${steps.provision.gretng}
`)

		ds := mustValidate(t, caller)
		require.Contains(t, ds.Error(), `step "provision" has no output "gretng"; did you mean "greeting"?`,
			"a name outside the callee's one declared output was accepted")
	})

	t.Run("the declared name itself is fine", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		writeFile(t, dir, "callee.yaml", simpleCalleeSource)
		caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      tenant: acme
  - id: use
    log:
      message: ${steps.provision.greeting}
`)

		require.Empty(t, mustValidate(t, caller),
			"a call step's reference to its callee's one real declared output was reported as unknown")
	})

	t.Run("a callee declaring no outputs makes every reference wrong", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		writeFile(t, dir, "callee.yaml", `edition: v2026.3
name: callee
inputs:
  tenant:
    type: string
    required: true
steps:
  - id: a
    log:
      message: ${'hi ' + inputs.tenant}
`)
		caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      tenant: acme
  - id: use
    log:
      message: ${steps.provision.anything}
`)

		ds := mustValidate(t, caller)
		require.Contains(t, ds.Error(), `step "provision" has no output "anything"; the called workflow declares no outputs`,
			"a reference into a callee with zero declared outputs was accepted")
	})
}

// TestACertainKindsAreExactlyWhatOutputNamesAnswersWithCertainty pins the boundary
// itself, not just its effects — so a later change to [v1.OutputNames] (a kind that
// stops answering with every entry named, one that starts) fails a readable test here
// rather than silently moving the line `unknownStepOutput`'s certain path checks
// against.
//
// It does not hand-assert the boundary; it computes it the same way [certainNames]
// does — every [v1.NamedOutput.Name] on the answer is non-empty — for the five kinds
// `unknownStepOutput` reads it for (switch, for_each, call, parallel, loop), and
// requires each to still be fully certain. A kind [v1.OutputNames] stops answering
// with full certainty for fails this test by name instead of quietly reintroducing a
// false diagnostic.
func TestACertainKindsAreExactlyWhatOutputNamesAnswersWithCertainty(t *testing.T) {
	t.Parallel()

	for kind, node := range certainKindNodes(t) {
		t.Run(kind, func(t *testing.T) {
			names, ok := v1.OutputNames(node, nil)

			// A parallel is the one kind here where OutputNames answers ok=false: a
			// certain, structural "nothing is ever reachable under this step's own
			// id" — not the uncertain state ok=true-with-an-empty-Name-entry would
			// be. [certainNames] treats it identically to every other empty result,
			// which is exactly what makes it safe to fold into the same loop.
			if !ok {
				require.Empty(t, names, "%q answered ok=false but still listed names; that combination is not one certainNames expects", kind)
				return
			}

			for _, n := range names {
				require.NotEmpty(t, n.Name,
					"%q is read by unknownStepOutput's certain path, but OutputNames answered one of its "+
						"entries with no Name — that is the uncertain signal, and reading it here would let "+
						"an unresolvable reference be reported as unknown", kind)
			}
		})
	}

	// The one kind this test does *not* claim: an unregistered task's answer has
	// exactly the uncertain shape the loop above refuses — proof the check above is
	// discriminating, not vacuously true of every node OutputNames can describe.
	unregistered, _ := v1.OutputNames(&v1.Node{Kind: &v1.Node_Task{Task: &v1.Task{Name: "nonexistent.task"}}}, nil)
	for _, n := range unregistered {
		require.Empty(t, n.Name, "an unregistered task's descriptor is not knowable; OutputNames should say so with an empty Name")
	}

	// And the one kind where certainty and `unknownStepOutput`'s behavior part ways
	// on purpose: an unshaped `wait_for_signal:` answers with every name just as
	// certain as the five above (`timed_out`, `payload`, `sender`, fixed by the
	// grammar) — yet `unknownStepOutput` stays silent there anyway, because two
	// standing tests (`TestWaitOutputsAreReferenceable`, `TestValidateAcceptsWaits`)
	// pin a reference outside that set as legal. This is the one exception the
	// certain-kinds boundary above does not cover, and it is deliberate — see the
	// comment on the wait block in `unknownStepOutput` for the open question behind it.
	unshapedWait, ok := v1.OutputNames(&v1.Node{Kind: &v1.Node_Wait{Wait: &v1.Wait{
		Kind: &v1.Wait_Signal{Signal: &v1.Signal{Name: "go"}},
	}}}, nil)
	require.True(t, ok)
	for _, n := range unshapedWait {
		require.NotEmpty(t, n.Name,
			"an unshaped wait_for_signal stopped answering with certainty; if so, the exception this test "+
				"documents no longer applies and unknownStepOutput's silence there should be revisited")
	}
}

// certainKindNodes builds one minimal [v1.Node] per kind
// [TestACertainKindsAreExactlyWhatOutputNamesAnswersWithCertainty] checks, keyed the
// same way `unknownStepOutput`'s own switch names them.
func certainKindNodes(t *testing.T) map[string]*v1.Node {
	t.Helper()

	return map[string]*v1.Node{
		"switch": {Kind: &v1.Node_Switch{Switch: &v1.Switch{
			Value: v1.NewLiteral("x"),
		}}},
		"for_each": {Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewLiteralList("a"),
		}}},
		// A callee that declares at least one output. A callee declaring none is a
		// separate, still-certain case (see [certainStepOutput]'s emptyMessage for
		// "call"), not the fully-certain-and-nonempty shape this test checks.
		"call": {Kind: &v1.Node_Call{Call: &v1.Call{
			Workflow: &v1.Workflow{
				Name: "callee",
				DeclaredOutputs: []*v1.OutputDeclaration{
					{Name: "url", Value: v1.NewLiteral("https://example.com")},
				},
			},
		}}},
		"parallel": {Kind: &v1.Node_Parallel{Parallel: &v1.Parallel{}}},
		"loop": {Kind: &v1.Node_Loop{Loop: &v1.Loop{
			Until: v1.NewLiteral(true),
		}}},
	}
}

// TestAToleratedStepsErrorOutputIsAllowed is the case this check refused for a whole
// review cycle, and the reason is worth more than the fix.
//
// `error` comes from the step's *policy*, not from its task: when `continue_on_error:`
// is set and the step fails, both drivers synthesise it in place of the task's outputs,
// which is the whole point — a later step branches on it. Checking the task's
// descriptor alone therefore reported a documented, working pattern as a mistake.
//
// Every exemption above was one this check's author designed and then tested. This one
// belongs to a feature written by someone else, years apart, and only shows up where
// the two *join*. Testing each half of a join and not the join is how a validator comes
// to refuse files the engine runs.
func TestAToleratedStepsErrorOutputIsAllowed(t *testing.T) {
	t.Parallel()

	tolerated := `
edition: v2026.3
name: t
steps:
  - id: risky
    continue_on_error: true
    http:
      url: https://example.com
  - id: report
    log:
      message: ${steps.risky.error}
`

	require.Empty(t, diagnose(t, tolerated),
		"a step read `error` from a step allowed to fail, which is what the policy is for")

	// And without the policy it is still an unknown output, because then nothing
	// produces it. The exemption is the policy's, not the name's.
	untolerated := `
edition: v2026.3
name: t
steps:
  - id: risky
    http:
      url: https://example.com
  - id: report
    log:
      message: ${steps.risky.error}
`

	require.Contains(t, diagnose(t, untolerated), `step "risky" has no output "error"`,
		"`error` was allowed on a step that cannot produce it")
}

// Compound steps take the fixed-output path before the generic tolerated-error
// allowance, so each kind must include the output contributed by its policy.
func TestToleratedCompoundStepsErrorOutputIsAllowed(t *testing.T) {
	t.Parallel()

	kinds := map[string]string{
		"switch":   "switch:\n      value: ${'x'}\n      cases:\n        - case: x\n          steps: []",
		"for_each": "for_each:\n      items: []\n      steps:\n        - id: nested\n          log:\n            message: hi",
		"parallel": "parallel:\n      - steps:\n          - id: nested\n            log:\n              message: hi",
		"loop":     "loop:\n      until: true\n      steps:\n        - id: nested\n          log:\n            message: hi",
	}
	for name, kind := range kinds {
		t.Run(name, func(t *testing.T) {
			src := "edition: v2026.3\nname: t\nsteps:\n  - id: risky\n    continue_on_error: true\n    " + kind + "\n  - id: report\n    log:\n      message: ${steps.risky.error}\n"
			require.Empty(t, diagnose(t, src))
		})
	}
}

// A stateful loop whose carried-state name happens to be `error` collides, by
// spelling alone, with the tolerated-error output a `continue_on_error:` loop
// also carries. Where the policy is set, `steps.<loop>.error` is the real
// tolerated output, not a mistaken reach for the loop's own `as:` name, so it
// must not be rejected by the state-name-collision message.
func TestToleratedLoopWithStateNamedErrorStillExposesTheToleratedOutput(t *testing.T) {
	t.Parallel()

	src := "edition: v2026.3\nname: t\nsteps:\n" +
		"  - id: risky\n" +
		"    continue_on_error: true\n" +
		"    loop:\n" +
		"      as: error\n" +
		"      init: ${''}\n" +
		"      update: ${error}\n" +
		"      until: true\n" +
		"      steps:\n" +
		"        - id: nested\n" +
		"          log:\n" +
		"            message: hi\n" +
		"  - id: report\n" +
		"    log:\n" +
		"      message: ${steps.risky.error}\n"
	require.Empty(t, diagnose(t, src))
}

// TestAToleratedStepListsTheErrorOutput checks the message, not only the verdict.
//
// An author who wrote `${steps.risky.reslt}` on a tolerated step and is shown a list
// without `error` in it has been told something false about what is available — and the
// next thing they need is often exactly that name.
func TestAToleratedStepListsTheErrorOutput(t *testing.T) {
	t.Parallel()

	src := `
edition: v2026.3
name: t
steps:
  - id: risky
    continue_on_error: true
    log:
      message: hi
  - id: report
    log:
      message: ${steps.risky.nonsense}
`

	require.Contains(t, diagnose(t, src), "error",
		"a tolerated step's diagnostic did not mention the output its policy gives it")
}
