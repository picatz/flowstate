package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/require"
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
name: t
steps:
  - id: a
    echo:
      message: hi
  - id: b
    echo:
      message: ${steps.a.nonsense}
`,
			want: `step "a" has no output "nonsense"; it produces result`,
		},
		{
			name: "a near miss gets a suggestion rather than a list",
			src: `
name: t
steps:
  - id: a
    echo:
      message: hi
  - id: b
    echo:
      message: ${steps.a.reslt}
`,
			want: `did you mean "result"?`,
		},
		{
			// The reason this check exists. A task with no outputs makes *every*
			// reference to it wrong, so the message explains the design rather than
			// listing an empty set — "it produces: " teaches nothing.
			name: "a task that produces nothing says why",
			src: `
name: t
steps:
  - id: say
    log:
      message: hi
  - id: b
    echo:
      message: ${steps.say.result}
`,
			want: "the log task produces no outputs, because a log step is an effect rather than a value",
		},
		{
			// One level down is the language's; deeper is CEL selecting into a value,
			// which this cannot and should not check.
			name: "selecting into a real output is fine",
			src: `
name: t
steps:
  - id: a
    echo:
      message: hi
  - id: b
    echo:
      message: ${steps.a.result.something.deeper}
`,
		},
		{
			// The whole outputs mapping, which any step has — including one with
			// nothing in it.
			name: "the mapping itself is fine",
			src: `
name: t
steps:
  - id: say
    log:
      message: hi
  - id: b
    echo:
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
name: t
steps:
  - id: fetch
    http:
      url: https://example.com
      outputs:
        anything: ${status_code}
  - id: use
    echo:
      message: ${steps.fetch.anything}
`

	require.Empty(t, diagnose(t, src),
		"a step reading an output the fetch step named for itself was reported as unknown")
}

// TestABlockStepsOutputsAreNotSecondGuessed keeps the check inside what it knows.
//
// A `for_each` reports `results`, a `parallel` merges its branches, and a
// `wait_for_signal` carries whatever a sender sent. None of them is a task, so none has
// a declared outputs message to check against — and guessing at their shapes here would
// be a second, quieter definition of what they produce, drifting from the engine's.
func TestABlockStepsOutputsAreNotSecondGuessed(t *testing.T) {
	t.Parallel()

	src := `
name: t
steps:
  - id: each
    for_each:
      items: ${["a"]}
      iterator: name
      steps:
        - id: inner
          echo:
            message: ${name}
  - id: gate
    wait_for_signal:
      name: go
      timeout: 1h
  - id: use
    echo:
      message: ${string(steps.each.results) + string(steps.gate.payload)}
`

	require.Empty(t, diagnose(t, src),
		"a block step's outputs were checked against a task's descriptor, which it does not have")
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
name: t
steps:
  - id: risky
    continue_on_error: true
    http:
      url: https://example.com
  - id: report
    echo:
      message: ${steps.risky.error}
`

	require.Empty(t, diagnose(t, tolerated),
		"a step read `error` from a step allowed to fail, which is what the policy is for")

	// And without the policy it is still an unknown output, because then nothing
	// produces it. The exemption is the policy's, not the name's.
	untolerated := `
name: t
steps:
  - id: risky
    http:
      url: https://example.com
  - id: report
    echo:
      message: ${steps.risky.error}
`

	require.Contains(t, diagnose(t, untolerated), `step "risky" has no output "error"`,
		"`error` was allowed on a step that cannot produce it")
}

// TestAToleratedStepListsTheErrorOutput checks the message, not only the verdict.
//
// An author who wrote `${steps.risky.reslt}` on a tolerated step and is shown a list
// without `error` in it has been told something false about what is available — and the
// next thing they need is often exactly that name.
func TestAToleratedStepListsTheErrorOutput(t *testing.T) {
	t.Parallel()

	src := `
name: t
steps:
  - id: risky
    continue_on_error: true
    log:
      message: hi
  - id: report
    echo:
      message: ${steps.risky.nonsense}
`

	require.Contains(t, diagnose(t, src), "error",
		"a tolerated step's diagnostic did not mention the output its policy gives it")
}
