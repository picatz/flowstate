package flowfile

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The domain checks are all gated on one boolean, and nothing used to assert
// that the boolean is ever true for a file this repository ships.
//
// [switchDomain] reports whether a discriminant's domain is a property of the
// file, and every diagnostic that makes `switch:` worth writing over three
// `if:`s — the impossible case, the missing case, the unreachable `default:`,
// the type mismatch — is skipped when it says no. Saying no is not an error and
// produces no output: an open domain is deliberately silent. So a shaping
// expression edited into a form the inference cannot read turns five checks off
// at once, and every existing test stays green, because each of them either
// supplies its own closed fixture or asserts silence.
//
// That is the shape CLAUDE.md names — a bound nothing reaches is a bound nothing
// tests — pointed at an inference rather than a limit. These two tests reach it
// from both ends: the shipped example really is inferable, and an expression
// outside the readable form really does open the domain.

// repoRoot locates the repository from this package's directory.
func repoRoot() string { return filepath.Join("..", "..", "..", "..") }

// approvalGateSwitch returns the shipped example's parsed workflow and the
// dispatch that reads the gate's outcome.
func approvalGateSwitch(t *testing.T) (*v1.Workflow, *v1.Switch) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(repoRoot(), "examples", "approval-gate", "workflow.yaml"))
	require.NoError(t, err, "examples/approval-gate/workflow.yaml moved and this test did not")

	wf, err := Unmarshal(data)
	require.NoError(t, err)

	for _, node := range wf.GetSteps() {
		if sw := node.GetSwitch(); sw != nil {
			return wf, sw
		}
	}
	t.Fatal("examples/approval-gate/workflow.yaml no longer dispatches on a `switch:`\n" +
		"  the domain diagnostics only exist for a switch, so converting that dispatch back\n" +
		"  to sibling `if:` steps silently removes all five of them")
	return nil, nil
}

// The positive direction: the example this repository ships as the closed-domain
// demonstration actually has a domain the validator can read.
//
// Asserted on the values and their order, not merely on the boolean, because the
// order is what the diagnostics enumerate: "the values are "deployed",
// "rejected", "undecided"" is the sentence an author reads, and it is first-
// appearance order in the gate's own ternary.
func TestApprovalGateDomainIsInferable(t *testing.T) {
	t.Parallel()

	wf, sw := approvalGateSwitch(t)

	domain, known := switchDomain(sw.GetValue(), domainScope(wf))
	require.True(t, known,
		"examples/approval-gate's discriminant no longer has an inferable domain\n"+
			"  the gate's `outcome:` must stay conditionals over string literals all the way down —\n"+
			"  an optMap, an orValue, or a value read from the payload computes the same strings and\n"+
			"  silently disables the impossible-case, exhaustiveness, unreachable-default and\n"+
			"  type-mismatch diagnostics for every switch reading it")
	assert.Equal(t, []string{"deployed", "rejected", "undecided"}, domain,
		"the domain the diagnostics enumerate, in the order the gate's own expression reads")
}

// The same property from the outside: with the domain readable, a case value the
// gate can never produce is fatal.
//
// Between this and the test above, an edit that opens the domain cannot pass
// quietly — the first names the cause, this one names what it costs.
func TestApprovalGateRefusesACaseItCannotProduce(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join(repoRoot(), "examples", "approval-gate", "workflow.yaml"))
	require.NoError(t, err)

	require.Contains(t, string(data), "- case: rejected",
		"the fixture substitution below found nothing, so this test would assert against the file unmodified")
	typo := strings.Replace(string(data), "- case: rejected", "- case: rejcted", 1)

	ds, err := ValidateSource([]byte(typo))
	require.NoError(t, err)

	var text string
	for _, d := range ds {
		text += d.Message + "\n"
	}
	assert.Contains(t, text, `case "rejcted" is not a value`)
	assert.Contains(t, text, `did you mean "rejected"?`)
	assert.Contains(t, text, `cases do not handle "rejected"`)
}

// loadExampleSwitch reads a workflow under examples/ and returns the parsed
// workflow plus the `switch:` node with the given step id.
func loadExampleSwitch(t *testing.T, exampleDir, stepID string) (*v1.Workflow, *v1.Switch) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(repoRoot(), "examples", exampleDir, "workflow.yaml"))
	require.NoError(t, err, "examples/%s/workflow.yaml moved and this test did not", exampleDir)

	wf, err := Unmarshal(data)
	require.NoError(t, err)

	node := nodeWithID(stepID, wf)
	require.NotNil(t, node, "examples/%s/workflow.yaml no longer has a step %q", exampleDir, stepID)
	sw := node.GetSwitch()
	require.NotNil(t, sw, "examples/%s/workflow.yaml's %q step is no longer a `switch:`", exampleDir, stepID)
	return wf, sw
}

// The positive direction, extended to a discriminant named by a `value:` step
// rather than a wait's shaped output.
//
// examples/optional-dispatch names its discriminant with a `value:` step whose
// expression is itself conditionals over string literals — exactly the shape
// the wait tier already reads, just reached through the other step kind this
// issue adds. Asserted on values and first-appearance order, for the same
// reason as the approval gate: that order is the sentence the diagnostics
// print.
func TestOptionalDispatchDomainIsInferable(t *testing.T) {
	t.Parallel()

	wf, sw := loadExampleSwitch(t, "optional-dispatch", "report")

	domain, known := switchDomain(sw.GetValue(), domainScope(wf))
	require.True(t, known,
		"examples/optional-dispatch's discriminant no longer has an inferable domain\n"+
			"  the `outcome` value step's expression must stay conditionals over string literals")
	assert.Equal(t, []string{"no_response", "approved", "rejected"}, domain,
		"the domain in the order the `outcome` step's own ternary reads")
}

// The same, over a fixture whose conditions are list comprehensions
// (`all`/`exists`) rather than boolean fields. Those comprehensions sit in the
// conditional's *condition* argument, which the walk ignores — proving that
// stays true when the discriminant is named by a `value:` step too.
func TestListComprehensionsDomainIsInferable(t *testing.T) {
	t.Parallel()

	wf, sw := loadExampleSwitch(t, "list-comprehensions", "report")

	domain, known := switchDomain(sw.GetValue(), domainScope(wf))
	require.True(t, known,
		"examples/list-comprehensions's discriminant no longer has an inferable domain")
	assert.Equal(t, []string{"healthy", "degraded", "down"}, domain,
		"the domain in the order the `status` step's own ternary reads")
}

// The cost of the domain being open, reproduced against examples/optional-dispatch
// exactly as TestApprovalGateRefusesACaseItCannotProduce reproduces it against
// the wait tier: a misspelled case is fatal, with a spelling suggestion, only
// because the `value:` step's domain is now readable.
func TestOptionalDispatchRefusesACaseItCannotProduce(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join(repoRoot(), "examples", "optional-dispatch", "workflow.yaml"))
	require.NoError(t, err)

	require.Contains(t, string(data), "- case: approved",
		"the fixture substitution below found nothing, so this test would assert against the file unmodified")
	typo := strings.Replace(string(data), "- case: approved", "- case: aproved", 1)

	ds, err := ValidateSource([]byte(typo))
	require.NoError(t, err)

	var text string
	for _, d := range ds {
		text += d.Message + "\n"
	}
	assert.Contains(t, text, `case "aproved" is not a value`)
	assert.Contains(t, text, `did you mean "approved"?`)
}

// The ripple guard: both fixtures validate clean unmodified, and — the part
// silence alone cannot distinguish — their domains are known rather than open.
// Asserting only `ValidateSource` silence would also pass with the domain
// still closed off, which is the exact vacuity this issue is about.
func TestValueStepDispatchExamplesValidateCleanWithKnownDomains(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		dir    string
		stepID string
	}{
		{"optional-dispatch", "report"},
		{"list-comprehensions", "report"},
	} {
		t.Run(tc.dir, func(t *testing.T) {
			t.Parallel()

			data, err := os.ReadFile(filepath.Join(repoRoot(), "examples", tc.dir, "workflow.yaml"))
			require.NoError(t, err)

			ds, err := ValidateSource(data)
			require.NoError(t, err)
			assert.Empty(t, ds, "examples/%s/workflow.yaml should validate clean: %v", tc.dir, ds)

			wf, sw := loadExampleSwitch(t, tc.dir, tc.stepID)
			_, known := switchDomain(sw.GetValue(), domainScope(wf))
			assert.True(t, known,
				"examples/%s's domain must be known, not merely silent — silence alone is also\n"+
					"  what an open domain produces", tc.dir)
		})
	}
}

// The pin CLAUDE.md calls "test the traversal, not just the step": webhook-routing
// dispatches on `${inputs.action}`, a provider-owned open set no validator on this
// machine can enumerate, and its domain must stay open. This pins the tier's edge
// so that enum-typed inputs (#332) change it deliberately, in a test diff someone
// reads — rather than an inference silently widening to cover it.
func TestWebhookRoutingDomainStaysOpen(t *testing.T) {
	t.Parallel()

	wf, sw := loadExampleSwitch(t, "webhook-routing", "on_event")

	_, known := switchDomain(sw.GetValue(), domainScope(wf))
	assert.False(t, known,
		"examples/webhook-routing dispatches on a workflow input, an open set no validator\n"+
			"  on this machine can enumerate; it must stay open until enum-typed inputs land")
}

// The negative direction, which is the one that actually guards the inference:
// a shaping expression outside the readable form opens the domain, and opening
// it is silent.
//
// Each fixture below is a plausible edit rather than an absurd one — a leaf that
// is a computed string, a leaf that is a payload field, an expression that is not
// a conditional at all. All three compute a value the author would describe as
// "one of three strings", and all three take the checks with them, which is
// exactly why the shipped file's spelling is load-bearing rather than stylistic.
func TestAShapingExpressionOutsideTheReadableFormOpensTheDomain(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		shape string
	}{
		{
			name:  "a computed string leaf",
			shape: `${has(payload.approved) ? string(payload.approved) : "undecided"}`,
		},
		{
			name:  "a leaf read from the payload",
			shape: `${has(payload.outcome) ? payload.outcome : "undecided"}`,
		},
		{
			name:  "not a conditional at all",
			shape: `${payload.outcome}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := `edition: v2026.3
name: t
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
      outputs:
        outcome: >-
          ` + tc.shape + `
  - id: decision
    switch:
      value: ${steps.approval.outcome}
      cases:
        - case: rejcted
          steps: []
`

			wf, err := Unmarshal([]byte(src))
			require.NoError(t, err)

			_, known := switchDomain(wf.GetSteps()[1].GetSwitch().GetValue(), domainScope(wf))
			assert.False(t, known,
				"this shape is not conditionals over string literals, so the domain must be open")

			// And what an open domain costs, stated where a reader can see it:
			// the same misspelling the test above reports is accepted in
			// silence, and so is a switch handling one value out of three with
			// no `default:`.
			ds, err := ValidateSource([]byte(src))
			require.NoError(t, err)
			assert.Empty(t, ds,
				"an open domain is deliberately silent — which is precisely why the shipped gate's\n"+
					"  spelling has to stay in the readable form: %v", ds)
		})
	}
}

// The negative direction extended to the tier this issue adds: a `value:` step
// naming the discriminant, but outside the shape `switchDomain` can read.
//
// Each fixture is deliberately plausible rather than absurd, and each also
// asserts that a misspelled case draws none of the domain diagnostics —
// naming the cost of the domain being open, not only the fact of it, per
// CLAUDE.md. Two of the four fixtures (the wrong-output-name one and the
// log-step one) also draw an unrelated unresolved-reference diagnostic from
// the ordinary output-resolution check, which is correct and expected: that
// check is not the one this issue changes, so its presence is asserted
// alongside the domain's silence rather than papered over.
func TestAValueStepOutsideTheReadableFormOpensTheDomain(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		src           string
		wantNoOtherDs bool
	}{
		{
			// A `value:` step whose expression is a bare reference, not
			// conditionals over string literals at all.
			name: "a value step reading an input directly",
			src: `edition: v2026.3
name: t
inputs:
  x:
    type: string
    required: true
steps:
  - id: outcome
    value: ${inputs.x}
  - id: decision
    switch:
      value: ${steps.outcome.value}
      cases:
        - case: rejcted
          steps: []
`,
			wantNoOtherDs: true,
		},
		{
			// A `value:` step holding a literal rather than an expression —
			// refused on purpose: a switch over a constant is degenerate, and
			// inventing a singleton domain would fire the exhaustiveness
			// checks on a file whose real mistake is the dispatch itself.
			name: "a value step holding a literal",
			src: `edition: v2026.3
name: t
steps:
  - id: outcome
    value: 3
  - id: decision
    switch:
      value: ${steps.outcome.value}
      cases:
        - case: rejcted
          steps: []
`,
			wantNoOtherDs: true,
		},
		{
			// The discriminant names the right step but the wrong output —
			// a `value:` step produces only `value`, never `other`.
			name: "a discriminant naming a value step's non-value output",
			src: `edition: v2026.3
name: t
steps:
  - id: outcome
    value: >-
      ${true ? "a" : "b"}
  - id: decision
    switch:
      value: ${steps.outcome.other}
      cases:
        - case: rejcted
          steps: []
`,
		},
		{
			// The discriminant names a step that is neither a wait nor a
			// value step at all.
			name: "a discriminant naming a log step",
			src: `edition: v2026.3
name: t
steps:
  - id: outcome
    log:
      message: hello
  - id: decision
    switch:
      value: ${steps.outcome.value}
      cases:
        - case: rejcted
          steps: []
`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			wf, err := Unmarshal([]byte(tc.src))
			require.NoError(t, err)

			decision := nodeWithID("decision", wf)
			require.NotNil(t, decision)

			_, known := switchDomain(decision.GetSwitch().GetValue(), domainScope(wf))
			assert.False(t, known,
				"this shape is outside what switchDomain reads, so the domain must be open")

			ds, err := ValidateSource([]byte(tc.src))
			require.NoError(t, err)

			var text string
			for _, d := range ds {
				text += d.Message + "\n"
			}
			assert.NotContains(t, text, "is not a value",
				"an open domain is deliberately silent, even over a misspelled case: %v", ds)
			if tc.wantNoOtherDs {
				assert.Empty(t, ds, "this fixture has nothing else wrong with it: %v", ds)
			}
		})
	}
}

// The rest of this file covers issue #578's second slice: `optMap`,
// `optFlatMap` and `orValue` extending the readable tier, with optionality
// discharged only by `orValue`.
//
// `optMap`/`optFlatMap` are cel-go *macros* — expanded at parse time into a
// comprehension over `@result`/`hasValue()` — so every fixture below is written
// with the exact expectation that [switchDomain] resolves the stored tree back
// through [resolveMacros] before walking it. A regression that starts walking
// the raw AST again does not produce a wrong domain; it produces an open one,
// because a bare `call.GetFunction() == "optMap"` check never fires against the
// expanded form. That failure mode is silent by the rule this whole file
// exists to guard (an open domain draws no diagnostics), which is what makes
// the anti-vacuity guard below load-bearing rather than decorative.

// optMapOrValueWait is a wait fixture whose shaped output is exactly the
// `optMap`+`orValue` chain the issue specifies, dispatched by a switch with all
// three of the chain's strings as cases.
const optMapOrValueWait = `edition: v2026.3
name: t
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
      outputs:
        outcome: >-
          ${payload.?approved.optMap(approved, approved ? "deployed" : "rejected").orValue("expired")}
  - id: decision
    switch:
      value: ${steps.approval.outcome}
      cases:
        - case: deployed
          steps: []
        - case: rejected
          steps: []
        - case: expired
          steps: []
`

// TestOptMapOrValueDomainIsInferable is the positive case for the chain the
// issue names: an `optMap` computing the present-value strings, discharged by
// `orValue` into the absent-value string, all under a bare
// `${steps.<id>.<name>}` discriminant.
//
// Order matters as much as membership: asserted in the order the expression
// reads — the optional chain's two branches, walked receiver-first per
// totalStringLeaves's `orValue` case, then the fallback — because that is the
// order the diagnostics enumerate.
func TestOptMapOrValueDomainIsInferable(t *testing.T) {
	t.Parallel()

	wf, err := Unmarshal([]byte(optMapOrValueWait))
	require.NoError(t, err)

	decision := nodeWithID("decision", wf)
	require.NotNil(t, decision)

	approval := nodeWithID("approval", wf)
	require.NotNil(t, approval)
	shaped := approval.GetWait().GetSignal().GetOutputs()["outcome"]
	require.NotNil(t, shaped)

	// The anti-vacuity guard: this fixture's shaping expression really does
	// go through cel-go's macro expander, so the domain below is exercising
	// [resolveMacros] and not silently reading the raw AST — which would also
	// report an open domain, indistinguishable from this test doing nothing.
	require.NotEmpty(t, shaped.GetExpr().GetSourceInfo().GetMacroCalls(),
		"this fixture's `optMap` must still be tracked as a macro call; if a future parse path "+
			"stops tracking macros, switchDomain's resolve-then-walk silently narrows to nothing, "+
			"and this is the test meant to catch that as a red test rather than as no test at all")

	domain, known := switchDomain(decision.GetSwitch().GetValue(), domainScope(wf))
	require.True(t, known, "an optMap/orValue chain is exactly the shape totalStringLeaves/optionalLeaves read")
	assert.Equal(t, []string{"deployed", "rejected", "expired"}, domain,
		"the domain in the order the chain reads: the optional's branches, then orValue's fallback")
}

// TestOptMapOrValueRefusesACaseItCannotProduce is the cost of the domain above
// being known, the same pairing every other positive test in this file keeps:
// a misspelled case is fatal, with a spelling suggestion.
func TestOptMapOrValueRefusesACaseItCannotProduce(t *testing.T) {
	t.Parallel()

	require.Contains(t, optMapOrValueWait, "- case: expired",
		"the fixture substitution below found nothing, so this test would assert against the file unmodified")
	typo := strings.Replace(optMapOrValueWait, "- case: expired", "- case: expierd", 1)

	ds, err := ValidateSource([]byte(typo))
	require.NoError(t, err)

	var text string
	for _, d := range ds {
		text += d.Message + "\n"
	}
	assert.Contains(t, text, `case "expierd" is not a value`)
	assert.Contains(t, text, `did you mean "expired"?`)
	assert.Contains(t, text, `cases do not handle "expired"`)
}

// TestOptFlatMapDomainIsInferable covers `optFlatMap`, whose body is itself
// optional-typed rather than total — an inner `optMap` chain under an outer
// `orValue`, so [optionalLeaves] has to recurse into itself through
// `optFlatMap` and not merely delegate to [totalStringLeaves].
func TestOptFlatMapDomainIsInferable(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: probe
    wait_for_signal:
      name: config-reported
      timeout: 24h
      outputs:
        outcome: >-
          ${payload.?config.optFlatMap(v, dyn(v).?state.optMap(s, s ? "on" : "off")).orValue("unknown")}
  - id: decision
    switch:
      value: ${steps.probe.outcome}
      cases:
        - case: "on"
          steps: []
        - case: "off"
          steps: []
        - case: unknown
          steps: []
`
	wf, err := Unmarshal([]byte(src))
	require.NoError(t, err)

	decision := nodeWithID("decision", wf)
	require.NotNil(t, decision)

	domain, known := switchDomain(decision.GetSwitch().GetValue(), domainScope(wf))
	require.True(t, known, "optFlatMap's body is itself optional-typed, which optionalLeaves must recurse through")
	assert.Equal(t, []string{"on", "off", "unknown"}, domain)

	ds, err := ValidateSource([]byte(src))
	require.NoError(t, err)
	assert.Empty(t, ds, "the fixture validates clean: %v", ds)
}

// TestUndischargedOptMapOpensTheDomain is the trap the issue's correction #2
// warns about, written out where a reader can see the cost: an `optMap` chain
// with nothing discharging it can evaluate to `optional.none`, which matches
// no string case and falls to `default:`. Reporting a closed domain here — the
// mistake the two-function split exists to prevent — would draw the
// unreachable-`default:` diagnostic on a `default:` that is genuinely
// reachable whenever the signal never arrives with `approved` set.
func TestUndischargedOptMapOpensTheDomain(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
      outputs:
        outcome: >-
          ${payload.?approved.optMap(approved, approved ? "deployed" : "rejected")}
  - id: decision
    switch:
      value: ${steps.approval.outcome}
      cases:
        - case: rejcted
          steps: []
      default:
        steps: []
`
	wf, err := Unmarshal([]byte(src))
	require.NoError(t, err)

	decision := nodeWithID("decision", wf)
	require.NotNil(t, decision)

	_, known := switchDomain(decision.GetSwitch().GetValue(), domainScope(wf))
	assert.False(t, known, "nothing discharges optionality here, so optional.none is a real outcome the domain must include")

	ds, err := ValidateSource([]byte(src))
	require.NoError(t, err)
	var text string
	for _, d := range ds {
		text += d.Message + "\n"
	}
	assert.NotContains(t, text, "is not a value",
		"an open domain is deliberately silent, even over a misspelled case: %v", ds)
}

// TestOptMapReadingItsOwnVariableOpensTheDomain: `optMap(v, v)` puts the bound
// identifier in leaf position. That is correctly *not* a string literal, so
// the walk returns false — fail-safe, not a special case.
func TestOptMapReadingItsOwnVariableOpensTheDomain(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
      outputs:
        outcome: ${payload.?approved.optMap(v, v).orValue("expired")}
  - id: decision
    switch:
      value: ${steps.approval.outcome}
      cases:
        - case: rejcted
          steps: []
      default:
        steps: []
`
	wf, err := Unmarshal([]byte(src))
	require.NoError(t, err)

	decision := nodeWithID("decision", wf)
	require.NotNil(t, decision)

	_, known := switchDomain(decision.GetSwitch().GetValue(), domainScope(wf))
	assert.False(t, known, "optMap(v, v)'s body is the bound identifier, not a string literal")

	ds, err := ValidateSource([]byte(src))
	require.NoError(t, err)
	var text string
	for _, d := range ds {
		text += d.Message + "\n"
	}
	assert.NotContains(t, text, "is not a value", "an open domain is deliberately silent: %v", ds)
}

// TestValueDotValueOpensTheDomain covers `.value()`: it aborts evaluation on
// `optional.none` rather than producing a value, so recognizing it would
// encode a runtime claim the validator cannot check. Excluded on purpose; see
// [optionalLeaves]'s doc comment.
func TestValueDotValueOpensTheDomain(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
      outputs:
        outcome: >-
          ${payload.?approved.optMap(approved, approved ? "deployed" : "rejected").value()}
  - id: decision
    switch:
      value: ${steps.approval.outcome}
      cases:
        - case: rejcted
          steps: []
      default:
        steps: []
`
	wf, err := Unmarshal([]byte(src))
	require.NoError(t, err)

	decision := nodeWithID("decision", wf)
	require.NotNil(t, decision)

	_, known := switchDomain(decision.GetSwitch().GetValue(), domainScope(wf))
	assert.False(t, known, "value() is deliberately not recognized: see optionalLeaves's doc comment")

	ds, err := ValidateSource([]byte(src))
	require.NoError(t, err)
	var text string
	for _, d := range ds {
		text += d.Message + "\n"
	}
	assert.NotContains(t, text, "is not a value", "an open domain is deliberately silent: %v", ds)
}

// TestOrValueOfANonLiteralOpensTheDomain: `orValue`'s argument has to be a
// string literal like every other leaf — a payload read is not knowable, so
// the whole chain's domain is not either, even though the optMap half alone
// would be.
func TestOrValueOfANonLiteralOpensTheDomain(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
      outputs:
        outcome: >-
          ${payload.?approved.optMap(approved, approved ? "deployed" : "rejected").orValue(payload.fallback)}
  - id: decision
    switch:
      value: ${steps.approval.outcome}
      cases:
        - case: rejcted
          steps: []
      default:
        steps: []
`
	wf, err := Unmarshal([]byte(src))
	require.NoError(t, err)

	decision := nodeWithID("decision", wf)
	require.NotNil(t, decision)

	_, known := switchDomain(decision.GetSwitch().GetValue(), domainScope(wf))
	assert.False(t, known, "orValue's argument is a payload read, not a string literal")

	ds, err := ValidateSource([]byte(src))
	require.NoError(t, err)
	var text string
	for _, d := range ds {
		text += d.Message + "\n"
	}
	assert.NotContains(t, text, "is not a value", "an open domain is deliberately silent: %v", ds)
}

// TestHasValueInLeafPositionOpensTheDomain: `hasValue()` produces a bool, never
// a string, so it is refused wherever a leaf is expected — as opposed to
// deciding a conditional's *condition*, which the walk ignores regardless of
// what it is.
func TestHasValueInLeafPositionOpensTheDomain(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
      outputs:
        outcome: >-
          ${true ? payload.?approved.hasValue() : "b"}
  - id: decision
    switch:
      value: ${steps.approval.outcome}
      cases:
        - case: rejcted
          steps: []
      default:
        steps: []
`
	wf, err := Unmarshal([]byte(src))
	require.NoError(t, err)

	decision := nodeWithID("decision", wf)
	require.NotNil(t, decision)

	_, known := switchDomain(decision.GetSwitch().GetValue(), domainScope(wf))
	assert.False(t, known, "hasValue() in leaf position produces a bool, never a string")

	ds, err := ValidateSource([]byte(src))
	require.NoError(t, err)
	var text string
	for _, d := range ds {
		text += d.Message + "\n"
	}
	assert.NotContains(t, text, "is not a value", "an open domain is deliberately silent: %v", ds)
}

// TestValueStepOptMapChainDomainIsKnown is the join test CLAUDE.md asks for:
// slice 1's tier (a `value:` step naming the discriminant) crossed with slice
// 2's tier (the shaping expression being an optMap/orValue chain rather than a
// bare ternary), both exercised at once rather than each in isolation.
func TestValueStepOptMapChainDomainIsKnown(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
inputs:
  approved:
    type: bool
    required: false
steps:
  - id: outcome
    value: >-
      ${inputs.?approved.optMap(approved, approved ? "deployed" : "rejected").orValue("expired")}
  - id: decision
    switch:
      value: ${steps.outcome.value}
      cases:
        - case: deployed
          steps: []
        - case: rejected
          steps: []
        - case: expired
          steps: []
`
	wf, err := Unmarshal([]byte(src))
	require.NoError(t, err)

	decision := nodeWithID("decision", wf)
	require.NotNil(t, decision)

	domain, known := switchDomain(decision.GetSwitch().GetValue(), domainScope(wf))
	require.True(t, known, "a value: step's optMap/orValue chain is exactly slice 1's tier crossed with slice 2's")
	assert.Equal(t, []string{"deployed", "rejected", "expired"}, domain)

	ds, err := ValidateSource([]byte(src))
	require.NoError(t, err)
	assert.Empty(t, ds, "the fixture validates clean: %v", ds)
}

// domainScope is the scope a switch is checked against in the walk: every
// top-level step recorded under its own id, the way validateWorkflow records
// one before validating the step after it. switchDomain reads a discriminant's
// step from the scope rather than searching the file for its id (#323), so the
// tests that call it directly have to hand it the same thing the walk would.
func domainScope(wf *v1.Workflow) refScope {
	scope := newRefScope(wf)
	for _, node := range wf.GetSteps() {
		recordStepInScope(scope, node)
	}
	return scope
}

// The rest of this file covers #674: a discriminant decomposed into its own
// `value:` step used to open the domain unconditionally, because the walk had
// no case for a leaf that is itself a `steps.<id>.<name>` select rather than a
// literal. [resolveStepOutputExpr] and the recursive case it adds to
// [totalStringLeaves] are what follow that leaf, bounded by
// [maxSwitchDomainDepth].

// TestExpenseApprovalDomainIsInferable is the real-world case the issue was
// filed over: `examples/expense-approval`'s `outcome` step names its domain
// through `steps.escalation_outcome.value`, a nested `value:` step, rather
// than inline. The positive direction, over the shipped file rather than a
// synthetic fixture — the same discipline TestApprovalGateDomainIsInferable
// and TestOptionalDispatchDomainIsInferable already hold their tiers to.
func TestExpenseApprovalDomainIsInferable(t *testing.T) {
	t.Parallel()

	wf, sw := loadExampleSwitch(t, "expense-approval", "settle")

	domain, known := switchDomain(sw.GetValue(), domainScope(wf))
	require.True(t, known,
		"examples/expense-approval's discriminant no longer has an inferable domain\n"+
			"  `outcome` must stay conditionals over string literals, or over a leaf naming\n"+
			"  another value: step's output, all the way down")
	assert.Equal(t, []string{"denied_no_response", "approved_after_escalation", "denied", "approved_by_manager"}, domain,
		"the domain in the order outcome's own ternary reads, following the escalation_outcome and direct_outcome hops")
}

// TestExpenseApprovalRefusesACaseItCannotProduce is the cost of the domain
// above being known, reproduced against the shipped fixture exactly the way
// #674 reported the regression: before the fix this typo drew nothing.
func TestExpenseApprovalRefusesACaseItCannotProduce(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join(repoRoot(), "examples", "expense-approval", "workflow.yaml"))
	require.NoError(t, err)

	require.Contains(t, string(data), "case: approved_by_manager",
		"the fixture substitution below found nothing, so this test would assert against the file unmodified")
	typo := strings.Replace(string(data), "case: approved_by_manager", "case: approved_by_managr", 1)

	ds, err := ValidateSource([]byte(typo))
	require.NoError(t, err)

	var text string
	for _, d := range ds {
		text += d.Message + "\n"
	}
	assert.Contains(t, text, `case "approved_by_managr" is not a value`)
	assert.Contains(t, text, `did you mean "approved_by_manager"?`)
}

// chainedValueSteps builds a workflow with n `value:` steps in a chain — v0
// holds the closed-domain literal ternary, and v1..v(n-1) each read the
// previous one bare (`value: ${steps.v<k-1>.value}`) — plus a `decision`
// switch dispatching on the last step in the chain. It is the generalized
// shape of the issue's own example: not one decomposition hop but an
// arbitrary number, which is what the depth-bound tests below need.
func chainedValueSteps(n int) (src string, lastID string) {
	var b strings.Builder
	b.WriteString("edition: v2026.3\nname: t\nsteps:\n")
	b.WriteString("  - id: v0\n    value: >-\n      ${true ? \"a\" : \"b\"}\n")
	for i := 1; i < n; i++ {
		b.WriteString("  - id: v" + strconv.Itoa(i) + "\n    value: ${steps.v" + strconv.Itoa(i-1) + ".value}\n")
	}
	lastID = "v" + strconv.Itoa(n-1)
	b.WriteString("  - id: decision\n    switch:\n      value: ${steps." + lastID + ".value}\n      cases:\n        - case: a\n          steps: []\n")
	return b.String(), lastID
}

// TestDecomposedDiscriminantDomainIsInferable is the generic form of the
// issue's own before/after: a discriminant read through a chain of `value:`
// steps, none of which is itself a conditional over string literals at the
// point the switch names — only the step at the bottom of the chain is. Each
// hop count from one (the issue's own example, one step of indirection) up to
// [maxSwitchDomainDepth] (the bound's edge) must still resolve.
func TestDecomposedDiscriminantDomainIsInferable(t *testing.T) {
	t.Parallel()

	for hops := 1; hops <= maxSwitchDomainDepth; hops++ {
		hops := hops
		t.Run(strconv.Itoa(hops)+"_hops", func(t *testing.T) {
			t.Parallel()

			src, _ := chainedValueSteps(hops + 1) // hops+1 steps: v0 literal, v1..v(hops) each one hop
			wf, err := Unmarshal([]byte(src))
			require.NoError(t, err)

			decision := nodeWithID("decision", wf)
			require.NotNil(t, decision)

			domain, known := switchDomain(decision.GetSwitch().GetValue(), domainScope(wf))
			assert.True(t, known, "%d hops is within maxSwitchDomainDepth (%d); the chain should still resolve", hops, maxSwitchDomainDepth)
			assert.Equal(t, []string{"a", "b"}, domain)
		})
	}
}

// TestDecomposedDiscriminantBeyondTheDepthBoundOpensTheDomain is the other
// edge: a chain one hop past [maxSwitchDomainDepth] opens the domain rather
// than resolving it or recursing without limit. Silent, per the rule this
// whole feature exists to keep — see [maxSwitchDomainDepth]'s doc comment for
// why a bound exists here at all even though the document's own step
// ordering already makes the walk terminate.
func TestDecomposedDiscriminantBeyondTheDepthBoundOpensTheDomain(t *testing.T) {
	t.Parallel()

	src, _ := chainedValueSteps(maxSwitchDomainDepth + 2)
	wf, err := Unmarshal([]byte(src))
	require.NoError(t, err)

	decision := nodeWithID("decision", wf)
	require.NotNil(t, decision)

	_, known := switchDomain(decision.GetSwitch().GetValue(), domainScope(wf))
	assert.False(t, known, "a chain one hop past maxSwitchDomainDepth must open the domain rather than resolve or hang")

	// And the cost, stated the way every other negative fixture in this file
	// states it: the same file validates clean even with a case the closed
	// end of the chain could never produce.
	typo := strings.Replace(src, "case: a", "case: c", 1)
	ds, err := ValidateSource([]byte(typo))
	require.NoError(t, err)
	var text string
	for _, d := range ds {
		text += d.Message + "\n"
	}
	assert.NotContains(t, text, "is not a value",
		"an open domain past the bound is deliberately silent, same as any other open domain: %v", ds)
}

// TestChainedDiscriminantRefusesACaseItCannotProduce is
// TestDecomposedDiscriminantDomainIsInferable's cost-of-being-known pairing:
// a chain within the bound still catches a misspelled case, not merely
// reports a domain in a unit test nothing else reads.
func TestChainedDiscriminantRefusesACaseItCannotProduce(t *testing.T) {
	t.Parallel()

	src, _ := chainedValueSteps(maxSwitchDomainDepth + 1) // exactly at the bound
	require.Contains(t, src, "case: a")
	typo := strings.Replace(src, "case: a", "case: c", 1)

	ds, err := ValidateSource([]byte(typo))
	require.NoError(t, err)

	var text string
	for _, d := range ds {
		text += d.Message + "\n"
	}
	assert.Contains(t, text, `case "c" is not a value`)
}
