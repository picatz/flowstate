package flowfile

import (
	"os"
	"path/filepath"
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

	domain, known := switchDomain(sw.GetValue(), wf)
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

	domain, known := switchDomain(sw.GetValue(), wf)
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

	domain, known := switchDomain(sw.GetValue(), wf)
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
			_, known := switchDomain(sw.GetValue(), wf)
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

	_, known := switchDomain(sw.GetValue(), wf)
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

			_, known := switchDomain(wf.GetSteps()[1].GetSwitch().GetValue(), wf)
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

			_, known := switchDomain(decision.GetSwitch().GetValue(), wf)
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
