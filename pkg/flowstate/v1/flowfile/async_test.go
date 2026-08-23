package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `async:` from the position an author actually writes it (issue #418).
//
// The engine cases in `pkg/flowstate/v1/internal/conformance` prove that both drivers agree
// about what an async step does; none of them proves anybody can write one. That
// is the whole of the house rule about reachability: a test that builds
// `&v1.Node{Async: true}` in Go is a test of the engine, and the feature is the
// path from a file someone types.
//
// So what is pinned here is the path: the key compiles, `flow fmt` writes it back
// unchanged, `flow validate` accepts a file that uses it, and each placement the
// engine refuses is refused *with a position* rather than at run time.

// TestAsyncCompiles pins that the marker an author writes is the marker that
// arrives, and that Marshal is its exact inverse.
func TestAsyncCompiles(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: build
    async: true
    log:
      message: building
  - id: use
    value: ${has(steps.build)}
`

	wf, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err)
	require.True(t, wf.GetSteps()[0].GetAsync(), "`async: true` compiled to nothing")
	require.False(t, wf.GetSteps()[1].GetAsync(), "a step that says nothing became async")

	// The inverse, which is what keeps `flow fix` and `flow fmt` from dropping the
	// one marker that decides whether a file's steps may overlap — a rewrite that
	// silently removed it would produce a file that still validates, still runs,
	// and is slower for reasons nothing in it explains.
	out, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	assert.Contains(t, string(out), "async: true")

	back, err := flowfile.Unmarshal(out)
	require.NoError(t, err)
	assert.True(t, back.GetSteps()[0].GetAsync(), "the marker did not survive a round trip")
	assert.False(t, back.GetSteps()[1].GetAsync())
}

// TestAsyncFalseIsTheSameFileAsNoAsync pins the round trip's other direction: a
// step spelling out the default must be equal to one that says nothing, or two
// identical workloads would compare unequal on the strength of a word.
func TestAsyncFalseIsTheSameFileAsNoAsync(t *testing.T) {
	t.Parallel()

	spelled, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: t
steps:
  - id: a
    async: false
    log:
      message: hi
`))
	require.NoError(t, err)

	out, err := flowfile.Marshal(spelled)
	require.NoError(t, err)
	assert.NotContains(t, string(out), "async",
		"`async: false` was written back, so the default has two spellings")
}

// TestAsyncNGraphValidates is the shape the whole issue is about, accepted: two
// steps started, and two later steps that each name only one of them.
func TestAsyncNGraphValidates(t *testing.T) {
	t.Parallel()

	wf, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: t
steps:
  - id: build
    async: true
    log:
      message: building
  - id: provision
    async: true
    log:
      message: provisioning
  - id: test
    value: ${has(steps.build)}
  - id: deploy
    value: ${has(steps.provision)}
`))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf), "the N-graph was reported as wrong")
}

// TestAsyncRefusalsArePositioned is the diagnostics half.
//
// Each of these is a placement [v1.CheckAsyncPlacement] refuses, and each is
// refused here with a line, a column, the step it belongs to and a sentence
// saying what to do instead — because the alternative to reporting it is a run
// that fails on its first step, which is where an author who wrote a wait
// `async:` would otherwise meet it.
func TestAsyncRefusalsArePositioned(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		src      string
		contains string
	}{
		{
			name: "a wait",
			src: `edition: v2026.3
name: t
steps:
  - id: approve
    async: true
    wait_for_signal:
      name: go
`,
			contains: "a deadlock nothing in the file shows",
		},
		{
			name: "a value",
			src: `edition: v2026.3
name: t
steps:
  - id: total
    async: true
    value: ${1 + 1}
`,
			contains: "there is nothing to overlap",
		},
		{
			name: "a block",
			src: `edition: v2026.3
name: t
steps:
  - id: both
    async: true
    parallel:
      - steps:
          - id: left
            log:
              message: left
`,
			contains: "mark the task steps inside it instead",
		},
		{
			name: "inside a parallel branch",
			src: `edition: v2026.3
name: t
steps:
  - id: both
    parallel:
      - steps:
          - id: left
            async: true
            log:
              message: left
`,
			contains: "is not supported inside a `for_each` body or a `parallel` branch",
		},
		{
			name: "inside a for_each body",
			src: `edition: v2026.3
name: t
steps:
  - id: each
    for_each:
      items: ${[1, 2]}
      steps:
        - id: inner
          async: true
          log:
            message: ${string(item)}
`,
			contains: "is not supported inside a `for_each` body or a `parallel` branch",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Through ValidateSource rather than Validate, because half the
			// diagnostic is where it points: the source is what carries the
			// positions, and a refusal that names a problem without a place has
			// moved an author's search rather than ended it.
			ds, err := flowfile.ValidateSource([]byte(test.src))
			require.NoError(t, err, "the file did not parse, so the refusal under test was never reached")
			require.NotEmpty(t, ds, "an `async:` the engine refuses was accepted by the validator")

			var found *flowfile.Diagnostic
			for i, d := range ds {
				if d.Field == "async" {
					found = &ds[i]

					break
				}
			}
			require.NotNil(t, found, "no diagnostic landed on the `async:` key; got %v", ds)
			assert.Equal(t, v1.DiagnosticCodePlacementRefusal, found.Code)
			assert.Contains(t, found.Message, test.contains,
				"the diagnostic does not say what to do instead")
			assert.NotZero(t, found.Line, "the diagnostic has no line, so it names a problem without a place")
		})
	}
}

// TestAsyncOnAWorkingFileIsNotReported is the false-positive direction, which
// matters more than the refusals: a validator that reported a legitimate `async:`
// would train authors to ignore it.
//
// Deliberately includes the shape that looks suspicious and is not — a step
// joined by the very next step, which buys nothing and is a perfectly ordinary
// thing to have in a file being written.
func TestAsyncOnAWorkingFileIsNotReported(t *testing.T) {
	t.Parallel()

	wf, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: t
steps:
  - id: a
    async: true
    log:
      message: hi
  - id: b
    value: ${has(steps.a)}
`))
	require.NoError(t, err)

	for _, d := range flowfile.Validate(wf) {
		assert.NotEqual(t, "async", d.Field,
			"a legitimate `async:` was reported: "+d.Message)
	}
}

// TestAsyncMustBeABoolean pins the one thing the parser itself decides about the
// key, with a position: `async:` is a marker, not an expression, so anything
// other than true or false is answered where it was written rather than compiled
// into something that happens to be truthy.
func TestAsyncMustBeABoolean(t *testing.T) {
	t.Parallel()

	_, err := flowfile.Unmarshal([]byte(`edition: v2026.3
name: t
steps:
  - id: a
    async: maybe
    log:
      message: hi
`))
	require.Error(t, err, "`async: maybe` was accepted")
	assert.Contains(t, err.Error(), "async")
	assert.True(t, strings.Contains(err.Error(), "true or false"),
		"the diagnostic does not say what a legal value is: %v", err)
}
