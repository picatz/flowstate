package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Diagnostics are a feature, and a compensation is the block where a wrong file is
// most expensive: a saga written incorrectly is a workload that believes it is
// transactional and is not, and the way an author finds out is a run that failed
// and left half a world behind. So each of the ways `undo:` can be written wrong is
// answered by name and at a position, and each one is pinned here.
//
// Written against the source rather than the compiled workflow, because the
// position is half the diagnostic — a message that says the compensation is in the
// wrong place and does not say *where* has moved the search rather than ended it.

// TestUndoCompiles pins that the shape an author writes is the shape that arrives.
func TestUndoCompiles(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: t
steps:
  - id: make
    log:
      message: made
    undo:
      log:
        message: unmade
`

	wf, err := flowfile.Unmarshal([]byte(src))
	require.NoError(t, err)

	undo := wf.GetSteps()[0].GetUndo()
	require.NotNil(t, undo, "`undo:` compiled to nothing")
	assert.Equal(t, "log", undo.GetTask().GetName())
	assert.Equal(t, "unmade", undo.GetTask().GetInputs()["message"].GetLiteral().GetStringValue())

	// Marshal is the inverse, which is what keeps `flow fix` and `flow fmt` from
	// dropping a compensation on their way through a file they were only asked to
	// reformat.
	out, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	assert.Contains(t, string(out), "undo:")

	back, err := flowfile.Unmarshal(out)
	require.NoError(t, err)
	assert.Equal(t, "log", back.GetSteps()[0].GetUndo().GetTask().GetName())
}

// TestUndoReadsItsOwnStepsOutputs is the reference that resolves nowhere else.
//
// A step naming itself is a forward reference everywhere in a Flowfile and is
// refused as one. Inside its own `undo:` it is the ordinary case, and it is the
// reference a compensation almost always needs — the thing to delete is named by
// the step that created it. So the validator has to model what the engine does
// rather than apply the general rule, and this is the pin on that.
func TestUndoReadsItsOwnStepsOutputs(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: t
steps:
  - id: make
    http:
      url: https://example.com/create
      outputs: '${ {"id": response.body} }'
    undo:
      log:
        message: ${steps.make.id}
`

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	assert.Empty(t, ds, "a compensation reading its own step's output was reported as a bad reference")
}

// TestUndoDiagnostics covers every way the block can be written wrong.
func TestUndoDiagnostics(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		src  string
		line int
		want string
	}{
		{
			name: "nothing under it",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
`,
			line: 7,
			want: "must name the task that takes this step back",
		},
		{
			name: "two tasks",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
      log:
        message: one
      http:
        url: https://example.com
`,
			line: 10,
			want: "a compensation is a single task",
		},
		{
			name: "control flow under it",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
      for_each:
        items: ${[1]}
        steps:
          - id: b
            log:
              message: hi
`,
			line: 8,
			want: "is control flow rather than a task",
		},
		{
			name: "the retired spelling",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
      task:
        name: log
`,
			line: 8,
			want: "a step names its task directly now",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := flowfile.Parse([]byte(tt.src))
			require.Error(t, err, "the file compiled and should not have")

			var ds flowfile.Diagnostics
			require.ErrorAs(t, err, &ds, "the compiler's error is not diagnostics")
			require.NotEmpty(t, ds)

			var found bool
			for _, d := range ds {
				if strings.Contains(d.Error(), tt.want) {
					found = true
					assert.Equal(t, tt.line, d.Line,
						"the diagnostic is on the wrong line:\n%s", d.Error())
				}
			}
			assert.True(t, found, "no diagnostic said %q; got:\n%s", tt.want, ds.Error())
		})
	}
}

// TestUndoValidationDiagnostics covers what only the validator can see: a task
// that does not exist, an input it does not have, and a reference that cannot
// resolve. Each is positioned on the `undo:` key, which is the one token the whole
// block hangs from.
func TestUndoValidationDiagnostics(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		src  string
		line int
		want string
	}{
		{
			name: "unknown task",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
      shell:
        command: rm -rf /
`,
			line: 7,
			want: `unknown task "shell"`,
		},
		{
			name: "an input the task does not have",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
      log:
        mesage: typo
`,
			line: 7,
			want: `input "mesage"`,
		},
		{
			name: "a reference to a step that does not exist",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
      log:
        message: ${steps.nope.thing}
`,
			line: 7,
			want: `"nope"`,
		},
		{
			name: "a reference to a step that has not run yet",
			src: `edition: v2026.2
name: t
steps:
  - id: a
    log:
      message: hi
    undo:
      log:
        message: ${steps.later.thing}
  - id: later
    log:
      message: hi
`,
			line: 7,
			want: `"later"`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(tt.src))
			require.NoError(t, err)
			require.NotEmpty(t, ds, "the file validated and should not have")

			var found bool
			for _, d := range ds {
				if strings.Contains(d.Error(), tt.want) {
					found = true
					assert.Equal(t, tt.line, d.Line,
						"the diagnostic is on the wrong line:\n%s", d.Error())
					assert.Contains(t, d.Error(), `undo`,
						"the diagnostic does not say the problem is in the compensation:\n%s", d.Error())
				}
			}
			assert.True(t, found, "no diagnostic said %q; got:\n%s", tt.want, ds.Error())
		})
	}
}

// TestUndoPlacementIsRefusedWithAPosition covers the two shapes this version does
// not support, at the position an author wrote them.
//
// The refusals themselves are [v1.CheckUndoPlacement]'s, which both execution
// drivers also call — one rule, three enforcement points, and the only difference
// between them is that this one has a line number.
func TestUndoPlacementIsRefusedWithAPosition(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		src  string
		line int
		want string
	}{
		{
			name: "inside a for_each body",
			src: `edition: v2026.2
name: t
steps:
  - id: loop
    for_each:
      items: ${[1]}
      steps:
        - id: inner
          log:
            message: hi
          undo:
            log:
              message: bye
`,
			line: 11,
			// The for_each/parallel refusal is about registration order under
			// concurrency, and must say so rather than reusing the loop's wording —
			// [TestUndoInsideANamedLoopBodyIsRefused] pins the accurate loop message.
			want: "the order work registers in inside concurrent control flow is not the same",
		},
		{
			name: "inside a parallel branch",
			src: `edition: v2026.2
name: t
steps:
  - id: fan
    parallel:
      - steps:
          - id: inner
            log:
              message: hi
            undo:
              log:
                message: bye
`,
			line: 10,
			want: "the order work registers in inside concurrent control flow is not the same",
		},
		{
			name: "on control flow",
			src: `edition: v2026.2
name: t
steps:
  - id: loop
    for_each:
      items: ${[1]}
      steps:
        - id: inner
          log:
            message: hi
    undo:
      log:
        message: bye
`,
			line: 11,
			want: "only supported on a step that runs a task",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(tt.src))
			require.NoError(t, err)

			var found bool
			for _, d := range ds {
				if strings.Contains(d.Error(), tt.want) {
					found = true
					assert.Equal(t, tt.line, d.Line,
						"the refusal is on the wrong line:\n%s", d.Error())
				}
			}
			assert.True(t, found, "no diagnostic said %q; got:\n%s", tt.want, ds.Error())
		})
	}
}

// TestUndoInsideANamedLoopBodyIsRefused pins the loop-specific refusal — issue
// #219's problem statement that a `loop:` body must not be told it is "inside a
// for_each body or a parallel branch", which is simply false for it and would
// send an author looking for a fan-out that is not there. A loop's own message
// names carried state, which is the actual, distinct reason it is refused: a
// sequential loop's registration order is perfectly well defined, unlike a
// for_each's or a parallel's, so the two placements need different sentences even
// though both are refused.
func TestUndoInsideANamedLoopBodyIsRefused(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.2
name: t
steps:
  - id: pages
    loop:
      as: cursor
      init: "${0}"
      update: "${cursor + 1}"
      until: "${cursor >= 1}"
      steps:
        - id: inner
          log:
            message: hi
          undo:
            log:
              message: bye
`

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)

	var found bool
	for _, d := range ds {
		if strings.Contains(d.Error(), "is inside a loop body") {
			found = true
			assert.NotContains(t, d.Error(), "for_each body or a parallel branch",
				"a loop body's refusal borrowed the for_each/parallel wording, naming a "+
					"construct that is not in this file:\n%s", d.Error())
			assert.Contains(t, d.Error(), "carries state between iterations",
				"the loop refusal does not say why a loop is different from concurrent control flow:\n%s", d.Error())
		}
	}
	assert.True(t, found, "no diagnostic refused the compensation inside the loop body; got:\n%s", ds.Error())
}

// TestUndoOnACallStepIsRefused pins issue #219 problem 1: a compensation written
// on the `call:` step itself is still refused — a call has no effect of its own
// to take back under compose-through any more than it did before — but the
// message must now point at the callee's own steps rather than lump a call in
// with "a wait and a parallel block have no effect of their own", which names the
// wrong construct for a call.
func TestUndoOnACallStepIsRefused(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "callee.yaml", simpleCalleeSource)
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      tenant: acme
    undo:
      log:
        message: bye
`)

	wf, _, err := flowfile.ParseFile(caller)
	require.NoError(t, err)

	ds := flowfile.Validate(wf)
	require.NotEmpty(t, ds, "a compensation on a `call:` step validated and should not have")

	var found bool
	for _, d := range ds {
		if strings.Contains(d.Error(), "only supported on a step that runs a task") {
			found = true
			assert.Contains(t, d.Error(), "callee's own steps",
				"the refusal does not point an author at where the compensation belongs:\n%s", d.Error())
			assert.NotContains(t, d.Error(), "a wait and a parallel block",
				"a `call:` step's refusal reused the control-flow wording, which names the wrong construct:\n%s", d.Error())
		}
	}
	assert.True(t, found, "no diagnostic refused the compensation on the call step; got:\n%s", ds.Error())
}

// TestUndoInsideACalleeValidates is issue #219 problem 2's author-time face: a
// callee's own task-step `undo:` must be accepted, whether the callee is
// validated on its own or reached through a caller's `call:` — both are
// [v1.UndoScopeTopLevel] / [v1.UndoScopeCall] respectively, and
// [v1.CheckUndoPlacement] allows both. Before compose-through this validated
// (validateAtDepth always treated a callee's own top level as unnested) while the
// engine refused it at run time — a false accept that would have sent an author
// straight into a run failing on its first step; this test also stands as the
// regression pin for that half of the fix now that both agree.
func TestUndoInsideACalleeValidates(t *testing.T) {
	calleeSrc := `edition: v2026.2
name: callee
inputs:
  tenant:
    type: string
    required: true
steps:
  - id: provision
    http:
      url: https://example.com/create
      outputs: '${ {"id": response.body} }'
    undo:
      log:
        message: ${steps.provision.id}
outputs:
  greeting:
    value: ${'hello ' + inputs.tenant}
`

	t.Run("standalone", func(t *testing.T) {
		ds, err := flowfile.ValidateSource([]byte(calleeSrc))
		require.NoError(t, err)
		assert.Empty(t, ds, "a callee's own task-step undo was refused when validated on its own")
	})

	t.Run("reached through a call", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "callee.yaml", calleeSrc)
		caller := writeFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: provision
    call: ./callee.yaml
    with:
      tenant: acme
`)

		wf, _, err := flowfile.ParseFile(caller)
		require.NoError(t, err)

		ds := flowfile.Validate(wf)
		assert.Empty(t, ds, "a callee's own task-step undo was refused when reached through a caller's call: %s", ds.Error())
	})
}

// TestUndoInsideACallInsideConcurrentControlFlowIsRefused is the negative
// direction [TestUndoInsideACalleeValidates] needs beside it — test that A
// cannot reach B, not that A can reach A. A callee's `undo:` validates when the
// `call:` reaching it sits at the top level; this pins that it stays refused
// when the `call:` itself sits inside a `for_each` body or a `parallel` branch,
// with the concurrency message rather than a silent accept. A call must not be
// usable to launder a compensation out of a scope that already refuses one —
// see [v1.UndoScope.IntoCall], which both drivers and this validator compose
// through identically.
func TestUndoInsideACallInsideConcurrentControlFlowIsRefused(t *testing.T) {
	calleeSrc := `edition: v2026.2
name: callee
steps:
  - id: provision
    log:
      message: hi
    undo:
      log:
        message: bye
`

	t.Run("call inside a for_each body", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "callee.yaml", calleeSrc)
		caller := writeFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: loop
    for_each:
      items: ${[1]}
      steps:
        - id: provision
          call: ./callee.yaml
`)

		wf, _, err := flowfile.ParseFile(caller)
		require.NoError(t, err)

		ds := flowfile.Validate(wf)
		require.NotEmpty(t, ds, "a callee's undo reached through a call inside a for_each body validated and should not have")

		var found bool
		for _, d := range ds {
			if strings.Contains(d.Error(), "the order work registers in inside concurrent control flow is not the same") {
				found = true
			}
		}
		assert.True(t, found, "no diagnostic gave the concurrency reason for refusing the callee's undo; got:\n%s", ds.Error())
	})

	t.Run("call inside a parallel branch", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "callee.yaml", calleeSrc)
		caller := writeFile(t, dir, "caller.yaml", `edition: v2026.2
name: caller
steps:
  - id: fan
    parallel:
      - steps:
          - id: provision
            call: ./callee.yaml
`)

		wf, _, err := flowfile.ParseFile(caller)
		require.NoError(t, err)

		ds := flowfile.Validate(wf)
		require.NotEmpty(t, ds, "a callee's undo reached through a call inside a parallel branch validated and should not have")

		var found bool
		for _, d := range ds {
			if strings.Contains(d.Error(), "the order work registers in inside concurrent control flow is not the same") {
				found = true
			}
		}
		assert.True(t, found, "no diagnostic gave the concurrency reason for refusing the callee's undo; got:\n%s", ds.Error())
	})
}

// TestUndoIsReservedAgainstTaskNames keeps the grammar unambiguous.
//
// A step key is a property or a task name, told apart by asking the registry,
// which only works while the two sets are disjoint. `undo` moved from the words
// held for later into the words the grammar speaks, and a plugin registering a
// task by that name would make `undo:` mean two incompatible things in one
// position.
func TestUndoIsReservedAgainstTaskNames(t *testing.T) {
	t.Parallel()

	assert.True(t, v1.IsReservedStepKey("undo"),
		"`undo:` is grammar and a task could still be registered under the name")
	assert.False(t, v1.IsFutureStepKey("undo"),
		"`undo:` is built, so reporting it as held for a later version would refuse a file that works")
}
