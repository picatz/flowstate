package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// An iteration is narrowed to the nodes directly in the body (onlyBodyOutputs and
// bodyOutputs in the two drivers), so the keys of an entry are those and no others.
func TestAResultsEntryHoldsOnlyTheNodesDirectlyInTheBody(t *testing.T) {
	t.Parallel()

	source := func(body string) string {
		return `edition: ` + flowfile.CurrentEdition + `
name: t
inputs:
  names:
    type: list(string)
    required: true
steps:
  - id: fan
    for_each:
      items: ${inputs.names}
      as: n
      steps:
        - id: direct
          value: ${n}
        - id: fork
          parallel:
            - steps:
                - id: left
                  value: ${n}
            - steps:
                - id: right
                  value: ${n}
        - id: inner
          for_each:
            items: ${[n]}
            as: m
            steps:
              - id: nested
                value: ${m}
  - id: use
    value: ${` + body + `}
`
	}

	for _, test := range []struct{ name, body, want string }{
		{"a direct body step", `steps.fan.results.map(r, r.direct.value)`, ""},
		{"a nested loop's own results", `steps.fan.results.map(r, r.inner.results)`, ""},
		{"a step nested in a loop of the body", `steps.fan.results.map(r, r.nested.value)`, `has no step "nested"`},
		{"a step in a branch of a parallel", `steps.fan.results.map(r, r.left.value)`, `has no step "left"`},
		{"the parallel itself", `steps.fan.results.map(r, r.fork)`, `has no step "fork"`},
		{"a macro variable that rebinds the name to another list", `steps.fan.results.map(r, inputs.names.map(r, r.size()))`, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assertDiagnosis(t, source(test.body), test.want)
		})
	}
}

// A step id that is not unique cannot be told apart from its namesake, so reads
// of its `results` stay unjudged.
func TestAResultsEntryOfADuplicatedLoopIdIsNotJudged(t *testing.T) {
	t.Parallel()

	src := `edition: ` + flowfile.CurrentEdition + `
name: t
steps:
  - id: fan
    for_each:
      items: ${[1]}
      as: n
      steps:
        - id: a
          value: ${n}
  - id: fan
    for_each:
      items: ${[2]}
      as: n
      steps:
        - id: b
          value: ${n}
  - id: use
    value: ${steps.fan.results.map(r, r.zzz)}
`
	wf, _, err := flowfile.Parse([]byte(src))
	if err != nil {
		// The duplicate id is refused first; either way no key is judged.
		assert.NotContains(t, err.Error(), "has no step")

		return
	}
	require.NotNil(t, wf)
	assert.NotContains(t, flowfile.Validate(wf).Error(), "has no step")
}
