package flowfile_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// TestParseWait covers the three spellings of a wait, which is the surface an
// author actually touches.
//
// The engine and both drivers handled waiting before any of this existed, and none
// of it was reachable: a capability is not done until a Flowfile can express it.
func TestParseWait(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		src   string
		check func(t *testing.T, wait *v1.Wait)
	}{
		{
			name: "a sleep",
			src: `edition: v2026.3
name: w
steps:
  - id: pause
    sleep: 30s
`,
			check: func(t *testing.T, wait *v1.Wait) {
				require.Equal(t, 30*time.Second, wait.GetDuration().AsDuration())
				require.Nil(t, wait.GetTimeout())
			},
		},
		{
			// The headline case. Go's own duration parser stops at hours, so this
			// would have been rejected with "not a duration" — for the exact thing
			// the feature is advertised as doing.
			name: "a sleep of a week",
			src: `edition: v2026.3
name: w
steps:
  - id: pause
    sleep: 7d
`,
			check: func(t *testing.T, wait *v1.Wait) {
				require.Equal(t, 7*24*time.Hour, wait.GetDuration().AsDuration())
			},
		},
		{
			name: "a sleep mixing days and hours",
			src: `edition: v2026.3
name: w
steps:
  - id: pause
    sleep: 1d12h
`,
			check: func(t *testing.T, wait *v1.Wait) {
				require.Equal(t, 36*time.Hour, wait.GetDuration().AsDuration())
			},
		},
		{
			name: "a wait_until expression",
			src: `edition: v2026.3
name: w
steps:
  - id: window
    wait_until: ${"2030-01-01T00:00:00Z"}
`,
			check: func(t *testing.T, wait *v1.Wait) {
				require.NotNil(t, wait.GetUntil(), "wait_until produced no expression")
				require.NotNil(t, wait.GetUntil().GetExpr(), "wait_until was not compiled as an expression")
			},
		},
		{
			// The form someone writes first, and the one that has to be short.
			name: "a signal, written as a scalar",
			src: `edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal: deploy-approved
`,
			check: func(t *testing.T, wait *v1.Wait) {
				require.Equal(t, "deploy-approved", wait.GetSignal().GetName())
				require.Nil(t, wait.GetTimeout(), "a scalar signal gained a timeout from nowhere")
			},
		},
		{
			name: "a signal with a timeout",
			src: `edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 24h
`,
			check: func(t *testing.T, wait *v1.Wait) {
				require.Equal(t, "deploy-approved", wait.GetSignal().GetName())
				require.Equal(t, 24*time.Hour, wait.GetTimeout().AsDuration())
			},
		},
		{
			name: "a signal with an underscore in its name",
			src: `edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal: deploy_approved_2
`,
			check: func(t *testing.T, wait *v1.Wait) {
				require.Equal(t, "deploy_approved_2", wait.GetSignal().GetName())
			},
		},
		{
			// The fence is what makes it code, so this is the case that pins the
			// rule the other duration positions could not have: `sleep: 30s` above
			// stays a literal in `duration`, and only this one reaches
			// `duration_expr`. Both readings live on the same key.
			name: "a computed sleep",
			src: `edition: v2026.3
name: w
steps:
  - id: pause
    sleep: ${duration(inputs.grace)}
`,
			check: func(t *testing.T, wait *v1.Wait) {
				require.Nil(t, wait.GetDuration(),
					"a fenced value was read as a literal duration")
				require.NotNil(t, wait.GetDurationExpr(),
					"a fenced value did not compile to an expression")
				require.Nil(t, wait.GetTimeout())
			},
		},
		{
			// `now` in the position that did not previously bind it. Parsed here
			// and *accepted* by the validator in TestValidateAcceptsWaits below —
			// two different claims, and the second is the one that used to fail.
			name: "a computed sleep reading the clock",
			src: `edition: v2026.3
name: w
steps:
  - id: pause
    sleep: ${(now + duration("1h")) - now}
`,
			check: func(t *testing.T, wait *v1.Wait) {
				require.NotNil(t, wait.GetDurationExpr())
			},
		},
		{
			name: "a signal with a computed timeout",
			src: `edition: v2026.3
name: w
steps:
  - id: gate
    wait_for_signal:
      name: sign-off
      timeout: ${deadline - now}
`,
			check: func(t *testing.T, wait *v1.Wait) {
				require.Equal(t, "sign-off", wait.GetSignal().GetName())
				require.Nil(t, wait.GetTimeout(),
					"a fenced timeout was read as a literal duration")
				require.NotNil(t, wait.GetTimeoutExpr(),
					"a fenced timeout did not compile to an expression")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			workflow, _, err := flowfile.Parse([]byte(test.src))
			require.NoError(t, err)
			require.Len(t, workflow.GetSteps(), 1)

			wait := workflow.GetSteps()[0].GetWait()
			require.NotNil(t, wait, "the step is not a wait")
			test.check(t, wait)

			// It has to survive a round trip, or `flow fmt` silently rewrites the
			// author's file into something else.
			out, err := flowfile.Marshal(workflow)
			require.NoError(t, err, "a wait could not be written back out")

			again, _, err := flowfile.Parse(out)
			require.NoError(t, err, "a written-out wait could not be read back:\n%s", out)
			require.Empty(t, cmpWorkflows(workflow, again), "a wait changed shape through a round trip:\n%s", out)
		})
	}
}

// TestParseWaitDiagnostics covers what an author gets wrong, since the diagnostic
// is the part of the DSL they meet most often.
func TestParseWaitDiagnostics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "a sleep that is not a duration",
			src: `edition: v2026.3
name: w
steps:
  - id: pause
    sleep: soon
`,
			want: `"soon" is not a duration`,
		},
		{
			name: "a sleep of no time at all",
			src: `edition: v2026.3
name: w
steps:
  - id: pause
    sleep: 0s
`,
			want: "must be greater than zero",
		},
		{
			name: "a signal with no name",
			src: `edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal:
      timeout: 1h
`,
			want: "needs a name",
		},
		{
			name: "a signal name with a space in it",
			src: `edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal: deploy approved
`,
			want: "may only contain letters, digits, dashes, and underscores",
		},
		{
			// A step timeout does nothing to a wait, and an author who wrote one
			// believed it bounded something.
			name: "a step timeout on a wait",
			src: `edition: v2026.3
name: w
steps:
  - id: pause
    sleep: 1h
    timeout: 5m
`,
			want: "does nothing on a waiting step",
		},
		{
			name: "a retry on a wait",
			src: `edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal: deploy-approved
    retry:
      attempts: 3
`,
			want: "no activity to attempt again",
		},
		{
			// The other key on the same arm: a retry on a sleep is a no-op for the
			// same reason a retry on a signal is — a timer has nothing to attempt
			// again.
			name: "a retry on a sleep",
			src: `edition: v2026.3
name: w
steps:
  - id: pause
    sleep: 1h
    retry:
      attempts: 3
`,
			want: "no activity to attempt again",
		},
		{
			// The third arm gets the same refusal: wait_until schedules no activity
			// a step-level timeout could bound.
			name: "a step timeout on a wait_until",
			src: `edition: v2026.3
name: w
steps:
  - id: hold
    wait_until: ${now}
    timeout: 5m
`,
			want: "does nothing on a waiting step",
		},
		{
			// A wait's own bound is `wait_for_signal:`'s own `timeout:`, inside the
			// wait — so a `timeout:` on the *step* is the confusion the diagnostic
			// exists to catch, and its advice points at the inside-the-wait spelling.
			name: "a step timeout on a wait_for_signal",
			src: `edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal: deploy-approved
    timeout: 5m
`,
			want: "does nothing on a waiting step",
		},
		{
			name: "two kinds of work at once",
			src: `edition: v2026.3
name: w
steps:
  - id: confused
    sleep: 1h
    wait_for_signal: deploy-approved
`,
			want: "a step does exactly one kind of work",
		},
		{
			name: "an unknown key inside a signal",
			src: `edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      after: 1h
`,
			want: `unknown key "after"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := flowfile.Parse([]byte(test.src))
			require.Error(t, err, "an unusable wait was accepted")
			require.Contains(t, err.Error(), test.want)

			// Every diagnostic names a position, which is what makes it
			// actionable in an editor rather than merely true.
			require.Regexp(t, `\d+:\d+:`, err.Error(),
				"the diagnostic does not name a line and column")
		})
	}
}

// TestRetryTimeoutStayOnTaskSteps is the positive half of the wait refusal: the
// keys `checkWaitPolicy` refuses beside a wait are exactly the keys a task step is
// meant to carry, so bounding and retrying a task must stay clean. Refusing them on
// waits by narrowing the grammar rather than by check would have taken them off
// tasks too; this proves it did not.
func TestRetryTimeoutStayOnTaskSteps(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: w
steps:
  - id: fetch
    http:
      method: GET
      url: https://example.com/
    retry:
      attempts: 3
    timeout: 10s
`
	_, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err, "retry/timeout on a task step should be accepted")

	ds, err := flowfile.ValidateSource([]byte(src))
	require.NoError(t, err)
	require.Empty(t, ds, "a task carrying retry/timeout should validate clean:\n%s", ds.Error())
}

// TestValidateAcceptsWaits checks the other half of reachable: that `flow validate`
// accepts a workload built out of waits.
//
// The validator walks node kinds separately from the parser, and its fallthrough
// reported "step must have one of task, for_each, or parallel" — so before this, a
// Flowfile that parsed correctly still failed validation, which is the same as not
// working.
func TestValidateAcceptsWaits(t *testing.T) {
	t.Parallel()

	src := []byte(`edition: v2026.3
name: gated
steps:
  - id: start
    log:
      message: starting
  - id: settle
    sleep: 1s
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 1h
  - id: after
    if: ${!steps.approval.timed_out}
    log:
      message: ${steps.approval.by}
  - id: pauses
    for_each:
      items: ${[1, 2]}
      steps:
        - id: inner-pause
          sleep: 1s
`)

	ds, err := flowfile.ValidateSource(src)
	require.NoError(t, err)
	require.Empty(t, ds, "a workload built out of waits was refused:\n%s", ds.Error())
}

// TestValidateReportsUnresolvableWaitUntil checks that a wait's own expression is
// held to the same standard as any other.
func TestValidateReportsUnresolvableWaitUntil(t *testing.T) {
	t.Parallel()

	src := []byte(`edition: v2026.3
name: w
steps:
  - id: window
    wait_until: ${nonexistent.deadline}
`)

	ds, err := flowfile.ValidateSource(src)
	require.NoError(t, err)
	require.NotEmpty(t, ds, "a wait_until naming a step that does not exist was accepted")
	require.Contains(t, ds.Error(), "nonexistent")
}

// TestValidateAcceptsComputedDurations is the "reachable from a Flowfile" half of
// expression-valued durations: parsing one and having the validator accept it are
// two claims, and the second is the one `now` used to fail.
//
// The four spellings the feature was specified against, in one file so that a
// regression in any of them fails here rather than in an example somebody deletes.
func TestValidateAcceptsComputedDurations(t *testing.T) {
	t.Parallel()

	src := []byte(`edition: v2026.3
name: computed
inputs:
  grace:
    type: string
    required: true
  plan:
    type: string
    required: true
steps:
  - id: from_input
    sleep: ${duration(inputs.grace)}
  - id: bare_input
    sleep: ${inputs.grace}
  - id: branched
    sleep: '${inputs.plan == "enterprise" ? duration("720h") : duration("168h")}'
  - id: gate
    wait_for_signal:
      name: sign-off
      timeout: ${(now + days(1)) - now}
  - id: literal
    sleep: 5m
`)

	ds, err := flowfile.ValidateSource(src)
	require.NoError(t, err)
	require.Empty(t, ds, "a computed duration was refused:\n%s", ds.Error())
}

// TestValidateRefusesNowOutsideAWait is the standing guard the widened binding
// makes worth restating.
//
// `now` moved from one position to three, and the failure mode of widening a
// binding is widening it too far — a name that resolves everywhere is a clock
// readable from any expression in the language, which is precisely what invariant 4
// and the DSL's own bet refuse. Each case below is a position that must keep
// saying no.
func TestValidateRefusesNowOutsideAWait(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
		want string
	}{
		{
			// The one ARCHITECTURE.md argues from: a task input may be resolved
			// inside an activity, where each retry would read a different value.
			name: "a task input",
			src: `edition: v2026.3
name: w
steps:
  - id: t
    log:
      message: ${string(now)}
`,
			want: "only available inside a wait",
		},
		{
			// Evaluated before the step's own vars exist, in workflow code but not
			// in a wait — and a condition that read a clock would put a
			// nondeterministic branch in history.
			name: "a step condition",
			src: `edition: v2026.3
name: w
steps:
  - id: t
    if: ${now > timestamp("2020-01-01T00:00:00Z")}
    log:
      message: hi
`,
			want: "only available inside a wait",
		},
		{
			// An activity, and the one seam replay does not cover: re-evaluated at
			// the top of every Continue-As-New segment.
			name: "a workflow var",
			src: `edition: v2026.3
name: w
vars:
  x: ${now}
steps:
  - id: t
    log:
      message: hi
`,
			want: `unknown name "now"`,
		},
		{
			// The collision that rooting cannot fix: an iterator is bare too, so it
			// and the clock genuinely share a namespace.
			name: "a loop iterator",
			src: `edition: v2026.3
name: w
steps:
  - id: t
    for_each:
      items: ${[1, 2]}
      as: now
      steps:
        - id: inner
          log:
            message: hi
`,
			want: "choose another iterator",
		},
		{
			// A step's own vars are bare within that step, so one called `now`
			// would shadow the clock inside that step's own wait.
			name: "a step var",
			src: `edition: v2026.3
name: w
steps:
  - id: t
    vars:
      now: ${1}
    log:
      message: hi
`,
			want: "rename this one",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(test.src))
			require.NoError(t, err)
			require.NotEmpty(t, ds, "`now` was accepted outside a wait")
			require.Contains(t, ds.Error(), test.want)
		})
	}
}

// TestValidateReportsUnresolvableComputedDurations holds a computed duration to the
// same reference standard `wait_until:` is held to.
//
// Widening a position is the moment a check gets forgotten: the expression is new,
// the walk over it is not, and a field missing from `validateWait` would accept a
// reference to a step nobody wrote.
func TestValidateReportsUnresolvableComputedDurations(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
	}{
		{
			name: "a computed sleep",
			src: `edition: v2026.3
name: w
steps:
  - id: pause
    sleep: ${steps.nonexistent.grace}
`,
		},
		{
			name: "a computed timeout",
			src: `edition: v2026.3
name: w
steps:
  - id: gate
    wait_for_signal:
      name: sign-off
      timeout: ${steps.nonexistent.deadline}
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ds, err := flowfile.ValidateSource([]byte(test.src))
			require.NoError(t, err)
			require.NotEmpty(t, ds, "a computed duration naming a step that does not exist was accepted")
			require.Contains(t, ds.Error(), "nonexistent")
		})
	}
}

// cmpWorkflows reports how two workflows differ, for the round-trip assertions.
func cmpWorkflows(a, b *v1.Workflow) string {
	if a.String() == b.String() {
		return ""
	}
	return "before:\n" + a.String() + "\nafter:\n" + b.String()
}

// TestWaitOutputsAreReferenceable checks that a step after a wait may reference it,
// which is what makes a gate compose rather than being a special form.
func TestWaitOutputsAreReferenceable(t *testing.T) {
	t.Parallel()

	src := []byte(`edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal: deploy-approved
  - id: deploy
    if: ${steps.approval.approved}
    log:
      message: deploying
`)

	ds, err := flowfile.ValidateSource(src)
	require.NoError(t, err)

	// The reference resolves: a wait's outputs are available to later steps. What
	// keys a payload carries is not knowable from the file, so only the step id is
	// checked — which is the honest limit of what a validator can say here.
	for _, d := range ds {
		require.False(t, strings.Contains(d.Message, "approval"),
			"a reference to a wait's outputs was reported as unresolvable: %s", d.Message)
	}
}

// TestParseWaitOutputsShaping covers the `outputs:` key on a gate: the shape an
// author writes, and that it survives `flow fmt` unchanged.
//
// A round trip is the assertion that matters most here, for the reason
// `varsToYAML` records: `Marshal` is the inverse of `Unmarshal` and `flow fix`
// rewrites files through it, so a block nothing writes back is a block the command
// silently *deletes* — and deleting this one would turn a stated gate back into
// four copies of a predicate, quietly.
func TestParseWaitOutputsShaping(t *testing.T) {
	t.Parallel()

	src := []byte(`edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 1h
      outputs:
        approved: ${has(payload.approved) && payload.approved}
        answered: ${!timed_out}
        who: ${sender.identity.subject}
        kind: approval
`)

	workflow, _, err := flowfile.Parse(src)
	require.NoError(t, err)

	shaped := workflow.GetSteps()[0].GetWait().GetSignal().GetOutputs()
	require.Len(t, shaped, 4)
	require.NotNil(t, shaped["approved"].GetExpr(), "a fenced value is an expression")
	require.NotNil(t, shaped["kind"].GetLiteral(), "an unfenced value is a literal, as everywhere else")

	// Still on the signal, not on the wait: the placement is what makes shaping
	// unrepresentable on a `sleep:` and a `wait_until:`, so a refactor that moved
	// the field would silently reopen that.
	require.Equal(t, time.Hour, workflow.GetSteps()[0].GetWait().GetTimeout().AsDuration())

	out, err := flowfile.Marshal(workflow)
	require.NoError(t, err)
	require.Contains(t, string(out), "outputs:", "shaping was dropped writing the file back out")

	again, _, err := flowfile.Parse(out)
	require.NoError(t, err, "a written-out gate could not be read back:\n%s", out)
	require.Empty(t, cmpWorkflows(workflow, again),
		"a gate's outputs shaping changed shape through a round trip:\n%s", out)
}

// TestValidateWaitOutputsShapingScope pins what a shaping expression may name.
//
// The wait's own result is bound bare and `now` with it, over the ordinary scope —
// so this is really two assertions in one: the three names resolve, and everything
// that resolves in an `if:` still resolves here.
func TestValidateWaitOutputsShapingScope(t *testing.T) {
	t.Parallel()

	src := []byte(`edition: v2026.3
name: w
inputs:
  approver:
    type: string
    required: true
vars:
  label: release
steps:
  - id: first
    log:
      message: hello
  - id: approval
    wait_for_signal:
      name: deploy-approved
      outputs:
        approved: ${has(payload.approved) && payload.approved}
        lapsed: ${timed_out}
        who: ${sender.identity.subject}
        expected: ${inputs.approver}
        label: ${vars.label}
        after: ${now}
`)

	ds, err := flowfile.ValidateSource(src)
	require.NoError(t, err)
	require.Empty(t, ds, "a shaping expression over names the engine binds was reported")
}

// TestValidateWaitOutputsShapingDiagnostics covers what an author gets wrong here,
// including the two negative directions replace semantics create.
func TestValidateWaitOutputsShapingDiagnostics(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		src  string
		want string
	}{
		{
			// The replace-semantics diagnostic, and the reason replace is safe to
			// ship at all: shaping *drops* `payload`, so a later reference to it
			// reads nothing. Reported, with the one-line fix named.
			name: "a later step reads a name the shaping dropped",
			src: `edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      outputs:
        approved: ${has(payload.approved) && payload.approved}
  - id: deploy
    if: ${steps.approval.payload.approved}
    log:
      message: deploying
`,
			want: "re-expose it",
		},
		{
			// The same check from the other side: a name that is merely misspelled
			// gets the suggestion rather than the re-exposure advice, because
			// `approvd` is not one of the wait's own outputs.
			name: "a later step misspells a shaped name",
			src: `edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      outputs:
        approved: ${has(payload.approved) && payload.approved}
  - id: deploy
    if: ${steps.approval.approvd}
    log:
      message: deploying
`,
			want: `did you mean "approved"?`,
		},
		{
			// Bound only inside the shaping block. Outside it `payload` is an
			// ordinary unknown name, which is what keeps a step legitimately called
			// `payload` from being shadowed everywhere.
			name: "the wait's result is not bound outside the shaping block",
			src: `edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      outputs:
        approved: ${has(payload.approved) && payload.approved}
  - id: deploy
    if: ${payload.approved}
    log:
      message: deploying
`,
			want: "unknown name",
		},
		{
			name: "a shaping expression names a step that does not exist",
			src: `edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      outputs:
        approved: ${steps.nonexistent.ok}
`,
			want: "nonexistent",
		},
		{
			// An empty block would silently produce a step with no outputs at all,
			// since shaping replaces rather than extends.
			name: "an empty outputs block",
			src: `edition: v2026.3
name: w
steps:
  - id: approval
    wait_for_signal:
      name: deploy-approved
      outputs: {}
`,
			want: "replaces what the wait produces",
		},
		{
			// The grammar refuses shaping on the two arms whose result is only the
			// passage of time — structurally, since neither takes a mapping at all.
			name: "a sleep cannot carry shaping",
			src: `edition: v2026.3
name: w
steps:
  - id: pause
    sleep:
      duration: 30s
      outputs:
        done: ${!timed_out}
`,
			want: "duration",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// A compile-time refusal comes back as an error and a check that runs
			// on a compiled workflow comes back in the diagnostics, and which of
			// the two a case lands in is a property of *where* the mistake is
			// catchable rather than of how serious it is. Both are read, so a
			// diagnostic moving between them is not a silent pass.
			ds, err := flowfile.ValidateSource([]byte(test.src))
			reported := ds.Error()
			if err != nil {
				reported = err.Error()
			}
			require.NotEmpty(t, reported, "nothing was reported")
			require.Contains(t, reported, test.want)
		})
	}
}
