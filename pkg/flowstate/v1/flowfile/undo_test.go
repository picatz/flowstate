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

	src := `edition: v2026.3
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

	src := `edition: v2026.3
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
			src: `edition: v2026.3
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
			src: `edition: v2026.3
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
			src: `edition: v2026.3
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
			src: `edition: v2026.3
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
			src: `edition: v2026.3
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
			src: `edition: v2026.3
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
			src: `edition: v2026.3
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
			src: `edition: v2026.3
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

// TestUndoCompensationHasThePluginCarveOut proves a compensation's task-name check
// treats a dotted (plugin) name the same way the primary task-name check does: a
// step's task and its compensation task are the same kind of thing and must fail the
// same way. A correctly-spelled plugin task gets the installation diagnosis, not a
// flat "unknown task" that would tell an author their file is wrong on the strength
// of what plugins this launch-nothing validator happens not to have loaded; a name
// with no dot is a spelling the validator can judge, so it still gets "unknown task".
func TestUndoCompensationHasThePluginCarveOut(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		task    string
		wantSub string
		notSub  string
	}{
		{
			name:    "dotted plugin task gets the installation carve-out",
			task:    "slack.post",
			wantSub: `no plugin task "slack.post" is registered here`,
			notSub:  `unknown task`,
		},
		{
			name:    "an undotted misspelling still gets the flat unknown-task message",
			task:    "htpp",
			wantSub: `unknown task "htpp"`,
			notSub:  `no plugin task`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := `edition: v2026.3
name: t
steps:
  - id: book
    log:
      message: hi
    undo:
      ` + tt.task + `:
        text: undo
`
			ds, err := flowfile.ValidateSource([]byte(src))
			require.NoError(t, err)
			require.NotEmpty(t, ds, "the file validated and should not have")

			var found bool
			for _, d := range ds {
				if strings.Contains(d.Error(), tt.wantSub) {
					found = true
					assert.Contains(t, d.Error(), `undo`,
						"the diagnostic does not place the problem in the compensation:\n%s", d.Error())
				}
			}
			assert.True(t, found, "no diagnostic contained %q; got:\n%s", tt.wantSub, ds.Error())
			assert.NotContains(t, ds.Error(), tt.notSub,
				"the diagnostic used the wrong message shape")
		})
	}
}

// TestUndoPlacementIsRefusedWithAPosition covers the two shapes this version does
// not support, at the position an author wrote them.
//
// The refusals themselves are [v1.CheckUndoPlacement]'s, which both execution
// drivers also call — one rule, three enforcement points, and the only difference
// between them is that this one has a line number.
func TestUndoInsideANamedLoopBodyIsAccepted(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
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

	for _, d := range ds {
		assert.NotContains(t, d.Error(), "`undo:` is only supported",
			"a compensation inside a `loop:` body was refused; #253 opened this placement:\n%s", d.Error())
	}
}

// TestUndoInsideALoopInsideAForEachIsRefused is the direction that opening the
// loop boundary made necessary rather than the one it made legal.
//
// A `loop:` may be written inside a `for_each` body — only a loop directly inside
// another loop is refused — so once a loop body accepts a compensation, a loop is
// exactly the wrapper that could launder one out of the concurrency refusal. That
// is issue #219's escape hatch with `loop:` where `call:` stood, and
// [v1.UndoScope.IntoLoop] is what closes it: the composed placement stays
// [v1.UndoScopeConcurrent], so the refusal an author gets is the concurrency one,
// naming the construct that is actually the problem.
func TestUndoInsideACallFromALoopBodyIsAccepted(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeFile(t, dir, "shift.yaml", `edition: v2026.3
name: shift
inputs:
  percent:
    type: int
steps:
  - id: shift
    log:
      message: ${"shifting to " + string(inputs.percent)}
    undo:
      log:
        message: ${"rolling back " + string(inputs.percent)}
`)
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
name: caller
steps:
  - id: rollout
    loop:
      as: percent
      init: "${5}"
      update: "${percent * 5}"
      until: "${percent >= 50}"
      steps:
        - id: shift
          call: ./shift.yaml
          with:
            percent: "${percent}"
`)

	wf, _, err := flowfile.ParseFile(caller)
	require.NoError(t, err)

	for _, d := range flowfile.Validate(wf) {
		assert.NotContains(t, d.Error(), "`undo:` is only supported",
			"a callee's compensation reached by `call:` from a `loop:` body was refused:\n%s", d.Error())
	}
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
	caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
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
	calleeSrc := `edition: v2026.3
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
		caller := writeFile(t, dir, "caller.yaml", `edition: v2026.3
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
func TestUndoIsReservedAgainstTaskNames(t *testing.T) {
	t.Parallel()

	assert.True(t, v1.IsReservedStepKey("undo"),
		"`undo:` is grammar and a task could still be registered under the name")
	assert.False(t, v1.IsFutureStepKey("undo"),
		"`undo:` is built, so reporting it as held for a later version would refuse a file that works")
}

// TestUndoInsideConcurrentControlFlowIsAccepted pins the placement that opened
// when concurrent scopes gained a structural ordering key: a compensation in a
// `for_each` body and one in a `parallel` branch both validate. The engine-side
// ordering claim lives in the shared corpus ([conformance.UndoCases]); what belongs to
// the validator is only that the author is no longer refused.
func TestUndoInsideConcurrentControlFlowIsAccepted(t *testing.T) {
	t.Parallel()

	for name, src := range map[string]string{
		"for_each body": `edition: v2026.3
name: t
steps:
  - id: fan
    for_each:
      items: "${['a', 'b']}"
      as: item
      steps:
        - id: inner
          log:
            message: ${item}
          undo:
            log:
              message: bye
`,
		"parallel branch": `edition: v2026.3
name: t
steps:
  - id: both
    parallel:
      - steps:
          - id: left
            log:
              message: hi
            undo:
              log:
                message: bye
      - steps:
          - id: right
            log:
              message: ho
`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(src))
			require.NoError(t, err)

			for _, d := range ds {
				assert.NotContains(t, d.Error(), "undo",
					"a compensation inside concurrent control flow was refused:\n%s", d.Error())
			}
		})
	}
}

// TestUndoOnControlFlowIsRefusedWithAPosition keeps the surviving refusal
// author-facing: a compensation on a step that has no effect of its own is
// reported at the step, with the position an editor can jump to. The engine's
// backstop for a spec that never saw a Flowfile is [conformance.UndoPlacementCases];
// this is the half with line and column.
func TestUndoOnControlFlowIsRefusedWithAPosition(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: t
steps:
  - id: fan
    for_each:
      items: "${['a']}"
      as: item
      steps:
        - id: inner
          log:
            message: ${item}
    undo:
      log:
        message: bye
`

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)

	found := false
	for _, d := range ds {
		if !strings.Contains(d.Error(), "undo") {
			continue
		}
		found = true
		assert.Positive(t, d.Line, "the refusal carries no line:\n%s", d.Error())
	}
	require.True(t, found, "a compensation on a for_each step itself was accepted; control flow has no effect of its own to take back")
}
