package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The six switch diagnostics, one fixture each, per #357's gate — plus the two
// decisions the design said to force with a test rather than doctrine: the
// step-id namespace across sibling case bodies, and open-domain silence.

// gateHeader is the closed-domain fixture the domain checks run against: the
// approval gate's shape, whose ternary yields exactly deployed | rejected |
// undecided — the inferable tier the design names.
const gateHeader = `edition: v2026.3
name: t
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
      outputs:
        outcome: >-
          ${timed_out
            ? "undecided"
            : (has(payload.approved) && payload.approved ? "deployed" : "rejected")}
`

func validateSwitchSrc(t *testing.T, src string) flowfile.Diagnostics {
	t.Helper()
	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	return ds
}

func diagnosticMessages(ds flowfile.Diagnostics) string {
	var b strings.Builder
	for _, d := range ds {
		b.WriteString(d.Message)
		b.WriteString("\n")
	}
	return b.String()
}

// Diagnostic 1: an impossible value against a knowable domain, with the nearest
// legal spelling — the typo'd literal that is legal CEL and never matches,
// which is the silent-nothing failure the construct exists to prevent.
func TestSwitchImpossibleCaseGetsNearestSpelling(t *testing.T) {
	t.Parallel()

	ds := validateSwitchSrc(t, gateHeader+`  - id: after
    switch:
      value: ${steps.approval.outcome}
      cases:
        - case: deployed
          steps: []
        - case: undecidd
          steps: []
      default:
        steps: []
`)
	require.NotEmpty(t, ds, "a case the domain cannot produce must be refused")
	text := diagnosticMessages(ds)
	assert.Contains(t, text, `case "undecidd" is not a value`)
	assert.Contains(t, text, `"undecided", "deployed", "rejected"`)
	assert.Contains(t, text, `did you mean "undecided"?`)

	// Positioned: the diagnostic lands on the case literal, not on line 1.
	for _, d := range ds {
		if strings.Contains(d.Message, "undecidd") {
			assert.Positive(t, d.Line, "the impossible-value diagnostic must carry a position")
			assert.Equal(t, "after", d.Step)
			assert.Equal(t, "undecidd", d.Value, "Value carries the offending literal")
		}
	}
}

// Diagnostic 2: a duplicate case after flattening a `case: [a, b]` list — the
// second occurrence can never match, a mistake by construction. The domain does
// not matter, so the fixture keeps it open.
func TestSwitchDuplicateCaseAfterFlattening(t *testing.T) {
	t.Parallel()

	ds := validateSwitchSrc(t, `edition: v2026.3
name: t
inputs:
  env:
    type: string
    required: true
steps:
  - id: route
    switch:
      value: ${inputs.env}
      cases:
        - case: prod
          steps: []
        - case: [staging, prod]
          steps: []
`)
	require.NotEmpty(t, ds, "a duplicate case must be refused")
	text := diagnosticMessages(ds)
	assert.Contains(t, text, `case "prod" is already handled`)
	assert.Contains(t, text, "the first match wins")
}

// Diagnostic 3: exhaustiveness. No `default:` claims every value is handled,
// and against a knowable domain the claim is checked, with the missing values
// named and the written-down opt-out spelled in the remedy.
func TestSwitchExhaustivenessNamesTheMissingValues(t *testing.T) {
	t.Parallel()

	ds := validateSwitchSrc(t, gateHeader+`  - id: after
    switch:
      value: ${steps.approval.outcome}
      cases:
        - case: deployed
          steps: []
`)
	require.NotEmpty(t, ds, "an inexhaustive switch with no default must be refused")
	text := diagnosticMessages(ds)
	assert.Contains(t, text, `cases do not handle "undecided", "rejected"`)
	assert.Contains(t, text, "default: {steps: []}")
}

// Diagnostic 4: an unreachable default. Cases that already exhaust the domain
// make `default:` dead code, the same mistake class as a duplicate case.
func TestSwitchUnreachableDefaultIsRefused(t *testing.T) {
	t.Parallel()

	ds := validateSwitchSrc(t, gateHeader+`  - id: after
    switch:
      value: ${steps.approval.outcome}
      cases:
        - case: [deployed, rejected, undecided]
          steps: []
      default:
        steps: []
`)
	require.NotEmpty(t, ds, "a default no value can reach must be refused")
	assert.Contains(t, diagnosticMessages(ds), "`default:` can never run")
}

// Diagnostic 5: a type mismatch — a case literal of a type the discriminant can
// never produce.
func TestSwitchTypeMismatchIsRefused(t *testing.T) {
	t.Parallel()

	ds := validateSwitchSrc(t, gateHeader+`  - id: after
    switch:
      value: ${steps.approval.outcome}
      cases:
        - case: 1
          steps: []
      default:
        steps: []
`)
	require.NotEmpty(t, ds, "an int case against a string domain must be refused")
	text := diagnosticMessages(ds)
	assert.Contains(t, text, "case 1 is not a string")
	assert.Contains(t, text, "can never match")
}

// Diagnostic 6: a computed case, refused with the settled sentence — and its
// two range-looking cousins, which are the first thing someone routing HTTP
// statuses will try. These need no domain, so they fire against an open one.
func TestSwitchComputedAndRangeCasesAreRefused(t *testing.T) {
	t.Parallel()

	ds := validateSwitchSrc(t, `edition: v2026.3
name: t
inputs:
  status:
    type: string
    required: true
vars:
  expected: prod
steps:
  - id: route
    switch:
      value: ${inputs.status}
      cases:
        - case: ${vars.expected}
          steps: []
        - case: 2xx
          steps: []
        - case: 400-499
          steps: []
`)
	require.NotEmpty(t, ds, "computed and range-looking cases must be refused")
	text := diagnosticMessages(ds)
	assert.Contains(t, text, "cases are literals; a computed comparison is what `if:` is for")
	assert.Contains(t, text, `"2xx" looks like a range`)
	assert.Contains(t, text, `"400-499" looks like a range`)
}

// Open domain: the validator stays silent, per the report-what-the-file-owns
// rule — the runtime record covers the gap. The same shapes that are fatal
// against the gate's domain are legal against an input's.
func TestSwitchOpenDomainStaysSilent(t *testing.T) {
	t.Parallel()

	ds := validateSwitchSrc(t, `edition: v2026.3
name: t
inputs:
  action:
    type: string
    required: true
steps:
  - id: route
    switch:
      value: ${inputs.action}
      cases:
        - case: opened
          steps:
            - id: triage
              log:
                message: hi
        - case: [closed, merged]
          steps: []
`)
	assert.Empty(t, ds, "an open domain must produce no domain diagnostics: %v", ds)
}

// The forced answer on step-id reuse across sibling case bodies: refused, the
// parallel-branch rule for the parallel-branch reason. Exactly one body runs,
// but every body's outputs merge into one namespace, so a reference has to mean
// one step — reuse would make `steps.notify` ambiguous to the validator and to
// every reader.
func TestSwitchStepIDReuseAcrossCaseBodiesIsRefused(t *testing.T) {
	t.Parallel()

	ds := validateSwitchSrc(t, `edition: v2026.3
name: t
inputs:
  env:
    type: string
    required: true
steps:
  - id: route
    switch:
      value: ${inputs.env}
      cases:
        - case: prod
          steps:
            - id: notify
              log:
                message: prod
        - case: dev
          steps:
            - id: notify
              log:
                message: dev
`)
	require.NotEmpty(t, ds, "an id reused across sibling case bodies must be refused")
	assert.Contains(t, diagnosticMessages(ds), "switch bodies share one output namespace")
}

// And the merge itself: a later step may reference a case-body step by id,
// exactly as it may reference a parallel branch's - the other half of the
// namespace decision above, asserted so the refusal cannot be satisfied by
// simply hiding body ids from the enclosing scope.
func TestSwitchBodyOutputsAreReferenceableAfterTheBlock(t *testing.T) {
	t.Parallel()

	ds := validateSwitchSrc(t, `edition: v2026.3
name: t
inputs:
  env:
    type: string
    required: true
steps:
  - id: route
    switch:
      value: ${inputs.env}
      cases:
        - case: prod
          steps:
            - id: gated
              value: ${1}
        - case: dev
          steps: []
  - id: after
    if: ${steps.route.case != null}
    log:
      message: ${string(steps.gated.value)}
`)
	assert.Empty(t, ds, "a case-body step must be referenceable after the switch: %v", ds)
}

// The duplicate check compares integers exactly. 9007199254740992 and
// 9007199254740993 (2^53 and 2^53+1) are distinct int64 values a float64
// cannot tell apart, so a matcher comparing through float64 flagged the second
// as a duplicate of the first — and dispatched both to the first body. Neither
// is a duplicate; a genuine duplicate at the same magnitude still is.
func TestSwitchIntegerCasesAboveDoublePrecisionAreNotDuplicates(t *testing.T) {
	t.Parallel()

	ds := validateSwitchSrc(t, `edition: v2026.3
name: t
steps:
  - id: route
    switch:
      value: ${9007199254740993}
      cases:
        - case: 9007199254740992
          steps: []
        - case: 9007199254740993
          steps: []
`)
	assert.Empty(t, ds, "distinct int64 cases one float64 apart from nothing must both be legal: %v", ds)

	ds = validateSwitchSrc(t, `edition: v2026.3
name: t
steps:
  - id: route
    switch:
      value: ${9007199254740993}
      cases:
        - case: 9007199254740993
          steps: []
        - case: 9007199254740993
          steps: []
`)
	require.NotEmpty(t, ds, "an exact integer duplicate must still be refused")
	assert.Contains(t, diagnosticMessages(ds), "case 9007199254740993 is already handled")
}

// The nested half of the merge decision TestSwitchBodyOutputsAreReferenceableAfterTheBlock
// pins at the top level: inside a for_each body, a later sibling may reference
// a case-body step too. The nested walk used to record only the switch's own
// id, so the same reference execution resolves was legal after a top-level
// switch and refused after a nested one.
func TestSwitchNestedInForEachMergesCaseBodySteps(t *testing.T) {
	t.Parallel()

	ds := validateSwitchSrc(t, `edition: v2026.3
name: t
steps:
  - id: process
    for_each:
      items: ${['bucket']}
      as: resource
      steps:
        - id: dispatch
          switch:
            value: ${resource}
            cases:
              - case: bucket
                steps:
                  - id: mark
                    value: ${resource + "!"}
        - id: readback
          value: ${steps.mark.value + "?"}
`)
	assert.Empty(t, ds, "a case-body step must be referenceable by a later sibling in the loop body: %v", ds)
}

// A case-body step may not reuse the switch's own id: the switch records its
// `value` and `case` outputs under that id, so a body step wearing it would
// validate clean and then have its outputs silently replaced — the id is taken
// before any body runs, even though enclosing.steps does not hold it yet.
func TestSwitchBodyStepReusingTheSwitchIDIsRefused(t *testing.T) {
	t.Parallel()

	ds := validateSwitchSrc(t, `edition: v2026.3
name: t
inputs:
  env:
    type: string
    required: true
steps:
  - id: route
    switch:
      value: ${inputs.env}
      cases:
        - case: prod
          steps:
            - id: route
              log:
                message: prod
`)
	require.NotEmpty(t, ds, "a body step reusing the switch's own id must be refused")
	text := diagnosticMessages(ds)
	assert.Contains(t, text, "id is already used outside case 1")
	for _, d := range ds {
		if strings.Contains(d.Message, "already used outside") {
			assert.Equal(t, "route", d.Step)
			assert.Positive(t, d.Line, "the collision diagnostic must carry a position")
		}
	}
}

// A switch that is only a default dispatches on nothing and is refused; an
// empty `cases:` list is the same mistake spelled at the parser.
func TestSwitchWithOnlyADefaultIsRefused(t *testing.T) {
	t.Parallel()

	_, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: t
inputs:
  env:
    type: string
    required: true
steps:
  - id: route
    switch:
      value: ${inputs.env}
      cases: []
      default:
        steps: []
`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one case")
}

// The composition fixture from the design: a switch inside a for_each body
// dispatching on the loop's own binding validates cleanly — the binding is in
// scope for the discriminant, and the bodies see it too.
func TestSwitchInsideAForEachReadsTheLoopBinding(t *testing.T) {
	t.Parallel()

	ds := validateSwitchSrc(t, `edition: v2026.3
name: t
steps:
  - id: process
    for_each:
      items: ${['bucket', 'instance']}
      as: resource
      steps:
        - id: dispatch
          switch:
            value: ${resource.kind}
            cases:
              - case: bucket
                steps:
                  - id: check_bucket
                    log:
                      message: ${resource.name}
              - case: instance
                steps: []
`)
	assert.Empty(t, ds, "a switch on the loop binding must validate: %v", ds)
}

// An unknown key inside the construct is reported with the keys that belong
// there, at each of the three levels - the misspelled-key rule this repo's
// diagnostics doctrine leads with.
func TestSwitchUnknownKeysAreReportedPerLevel(t *testing.T) {
	t.Parallel()

	_, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: t
steps:
  - id: route
    switch:
      valu: ${inputs.env}
      cases:
        - caze: prod
          steps: []
      default:
        stepz: []
`))
	require.Error(t, err)
	text := err.Error()
	assert.Contains(t, text, `unknown key "valu"`)
	assert.Contains(t, text, `unknown key "caze"`)
	assert.Contains(t, text, `unknown key "stepz"`)
}

// A null case is refused: it would make the step's own `case` record - null
// when and only when no case matched - ambiguous.
func TestSwitchNullCaseIsRefused(t *testing.T) {
	t.Parallel()

	ds := validateSwitchSrc(t, `edition: v2026.3
name: t
inputs:
  env:
    type: string
    required: true
steps:
  - id: route
    switch:
      value: ${inputs.env}
      cases:
        - case: null
          steps: []
`)
	require.NotEmpty(t, ds)
	assert.Contains(t, diagnosticMessages(ds), "null, which is not a value to dispatch on")
}
