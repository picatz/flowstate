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
