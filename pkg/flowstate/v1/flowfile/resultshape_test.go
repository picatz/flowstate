package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A loop's `results` entry is a map of the body's step ids to their outputs, and a
// read through a macro variable that names anything else is a missing key at run
// time. The checks below hold the validator to refusing it where it is written,
// and to staying silent about every read the run resolves.
func TestALoopResultsEntryIsCheckedAgainstTheBody(t *testing.T) {
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
        - id: greet
          value: ${n}
        - id: shout
          value: ${n + "!"}
  - id: use
    value: ${` + body + `}
`
	}

	for _, test := range []struct {
		name, body, want string // empty want accepts
	}{
		{"a body step read through filter and map", `steps.fan.results.filter(r, r.greet.value == "a").map(q, q.shout.value)`, ""},
		{"a presence test of a body step", `steps.fan.results.filter(r, has(r.greet))`, ""},
		{"an index into the list", `steps.fan.results[0].greet.value`, ""},
		{"a string-indexed read", `steps.fan.results.map(r, r["greet"]["value"])`, ""},
		{"the failure a tolerated step records", `steps.fan.results.filter(r, has(r.greet.error)).map(r, r.greet.item)`, ""},
		{"a macro variable of the same name over another list", `inputs.names.map(r, r.size())`, ""},
		{"a misspelled step id", `steps.fan.results.filter(r, r.greeet.value == "a")`, `the body of "fan" has no step "greeet"; it has "greet", "shout". Did you mean "greet"?`},
		{"a name that is no step", `steps.fan.results.map(r, r.nonexistent)`, `the body of "fan" has no step "nonexistent"`},
		{"a misspelled output of a step", `steps.fan.results.map(r, r.greet.valu)`, `step "greet" has no output "valu"; it has "value"`},
		{"a misspelled output suggests the nearest", `steps.fan.results.map(r, r.greet.valu)`, `Did you mean "value"?`},
		{"a misspelled id in an optional read", `steps.fan.results.map(r, r.?greeet)`, `has no step "greeet"`},
		{"a misspelled id after an index", `steps.fan.results[0].greeet.value`, `has no step "greeet"`},
		{"a misspelled id after a filter", `steps.fan.results.filter(r, true).map(q, q.greeet.value)`, `has no step "greeet"`},
		{"a misspelled id inside a presence test", `steps.fan.results.filter(r, has(r.greeet.value))`, `has no step "greeet"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assertDiagnosis(t, source(test.body), test.want)
		})
	}
}

// A `loop:` reports the same entry, and a task's outputs are the ones it declares.
func TestALoopStepsResultsEntryIsChecked(t *testing.T) {
	t.Parallel()

	source := func(body string) string {
		return `edition: ` + flowfile.CurrentEdition + `
name: t
steps:
  - id: count
    loop:
      as: i
      init: 0
      update: ${i + 1}
      until: ${i >= 1}
      max_iterations: 3
      steps:
        - id: tick
          value: ${i}
        - id: shell
          exec:
            dir: /tmp
            argv: [echo, hi]
  - id: use
    value: ${` + body + `}
`
	}

	for _, test := range []struct {
		name, body, want string
	}{
		{"a body step", `steps.count.results.map(r, r.tick.value)`, ""},
		{"a declared output of a task", `steps.count.results.map(r, r.shell.exit_code)`, ""},
		{"a misspelled output of a task", `steps.count.results.map(r, r.shell.exit_cod)`, `Did you mean "exit_code"?`},
		{"a misspelled id", `steps.count.results.map(r, r.tik.value)`, `the body of "count" has no step "tik"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assertDiagnosis(t, source(test.body), test.want)
		})
	}
}

// assertDiagnosis validates source and holds it to want: no diagnostic when want is
// empty, and one containing want otherwise.
func assertDiagnosis(t *testing.T, source, want string) {
	t.Helper()

	wf, _, err := flowfile.Parse([]byte(source))
	require.NoError(t, err)

	got := flowfile.Validate(wf).Error()
	if want == "" {
		assert.Empty(t, got)

		return
	}
	assert.Contains(t, got, want)
}
