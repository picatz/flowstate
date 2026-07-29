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
			src: `name: w
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
			src: `name: w
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
			src: `name: w
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
			src: `name: w
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
			src: `name: w
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
			src: `name: w
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
			src: `name: w
steps:
  - id: approval
    wait_for_signal: deploy_approved_2
`,
			check: func(t *testing.T, wait *v1.Wait) {
				require.Equal(t, "deploy_approved_2", wait.GetSignal().GetName())
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
			src: `name: w
steps:
  - id: pause
    sleep: soon
`,
			want: `"soon" is not a duration`,
		},
		{
			name: "a sleep of no time at all",
			src: `name: w
steps:
  - id: pause
    sleep: 0s
`,
			want: "must be greater than zero",
		},
		{
			name: "a signal with no name",
			src: `name: w
steps:
  - id: approval
    wait_for_signal:
      timeout: 1h
`,
			want: "needs a name",
		},
		{
			name: "a signal name with a space in it",
			src: `name: w
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
			src: `name: w
steps:
  - id: pause
    sleep: 1h
    timeout: 5m
`,
			want: "does nothing on a waiting step",
		},
		{
			name: "a retry on a wait",
			src: `name: w
steps:
  - id: approval
    wait_for_signal: deploy-approved
    retry:
      attempts: 3
`,
			want: "no activity to attempt again",
		},
		{
			name: "two kinds of work at once",
			src: `name: w
steps:
  - id: confused
    sleep: 1h
    wait_for_signal: deploy-approved
`,
			want: "a step does exactly one kind of work",
		},
		{
			name: "an unknown key inside a signal",
			src: `name: w
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

// TestValidateAcceptsWaits checks the other half of reachable: that `flow validate`
// accepts a workload built out of waits.
//
// The validator walks node kinds separately from the parser, and its fallthrough
// reported "step must have one of task, for_each, or parallel" — so before this, a
// Flowfile that parsed correctly still failed validation, which is the same as not
// working.
func TestValidateAcceptsWaits(t *testing.T) {
	t.Parallel()

	src := []byte(`name: gated
steps:
  - id: start
    echo:
      message: starting
  - id: settle
    sleep: 1s
  - id: approval
    wait_for_signal:
      name: deploy-approved
      timeout: 1h
  - id: after
    if: ${!steps.approval.timed_out}
    echo:
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

	src := []byte(`name: w
steps:
  - id: window
    wait_until: ${nonexistent.deadline}
`)

	ds, err := flowfile.ValidateSource(src)
	require.NoError(t, err)
	require.NotEmpty(t, ds, "a wait_until naming a step that does not exist was accepted")
	require.Contains(t, ds.Error(), "nonexistent")
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

	src := []byte(`name: w
steps:
  - id: approval
    wait_for_signal: deploy-approved
  - id: deploy
    if: ${steps.approval.approved}
    echo:
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
