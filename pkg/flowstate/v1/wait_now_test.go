package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A wait deadline is computed from the driver's clock, never from the wall clock.
//
// That is what makes `${now + days(1)}` safe to write: under Temporal the value
// comes from workflow.Now, which replays to the same instant, so a run that is
// replayed or continued as new computes the same deadline it computed the first
// time. These tests pin the binding and the units, because a duration that
// silently wraps or a `now` read from the wrong clock both produce a wait that
// looks right and fires at the wrong time.

// waitUntil compiles a `wait_until:` expression the way a Flowfile would, so
// these tests exercise the path an author actually reaches rather than a hand-built
// protobuf node.
func waitUntil(t *testing.T, expression string) *v1.Value {
	t.Helper()

	source := "name: t\nsteps:\n  - id: hold\n    wait_until: " + expression + "\n"

	workflow, err := flowfile.Unmarshal([]byte(source))
	require.NoError(t, err, "the expression did not compile")

	until := workflow.GetSteps()[0].GetWait().GetUntil()
	require.NotNil(t, until, "no wait_until expression was compiled")

	return until
}

func TestWaitDeadlineBindsNowToTheClockItIsGiven(t *testing.T) {
	t.Parallel()

	// A fixed instant rather than time.Now: if the binding ever read the wall
	// clock instead of this, the assertion below would drift rather than fail
	// cleanly, and a flaky test is how that bug would get ignored.
	now := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

	deadline, err := v1.EvalWaitDeadline(t.Context(), waitUntil(t, "${now + days(1)}"), nil, now)
	require.NoError(t, err)
	require.Equal(t, now.Add(24*time.Hour), deadline.UTC(),
		"the deadline was not computed from the clock the caller passed")
}

func TestWaitDeadlineDurationUnits(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

	for expression, want := range map[string]time.Duration{
		"${now + seconds(30)}":        30 * time.Second,
		"${now + minutes(5)}":         5 * time.Minute,
		"${now + hours(2)}":           2 * time.Hour,
		"${now + days(3)}":            72 * time.Hour,
		"${now + weeks(1)}":           7 * 24 * time.Hour,
		"${now + days(1) + hours(6)}": 30 * time.Hour,

		// A day is a fixed offset here rather than a calendar day, which is the
		// whole reason these exist separately from a date library.
		"${now - days(1)}": -24 * time.Hour,
	} {
		deadline, err := v1.EvalWaitDeadline(t.Context(), waitUntil(t, expression), nil, now)
		require.NoError(t, err, "%s did not evaluate", expression)
		require.Equal(t, now.Add(want).UTC(), deadline.UTC(), "%s computed the wrong moment", expression)
	}
}

// TestWaitDeadlineRefusesADurationThatWouldWrap is the bound that matters most.
//
// int64 nanoseconds run out at about 292 years, and without a check days(400000)
// becomes a *negative* duration — so a wait meant for the far future would already
// be in the past and would release immediately. Failing is recoverable; silently
// not waiting is not.
func TestWaitDeadlineRefusesADurationThatWouldWrap(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)

	for _, expression := range []string{
		"${now + days(400000)}",
		"${now + weeks(100000)}",
		"${now - days(400000)}",
	} {
		_, err := v1.EvalWaitDeadline(t.Context(), waitUntil(t, expression), nil, now)
		require.Error(t, err, "%s produced a duration instead of failing", expression)
		require.ErrorContains(t, err, "out of range")
	}
}

// TestWaitDeadlineStillTakesAMomentFromData checks the shape wait_until exists
// for, which the `now` binding is an addition to rather than a replacement of.
func TestWaitDeadlineStillTakesAMomentFromData(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	moment := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			"schedule": {NamedValues: map[string]*v1.Value{
				"opens_at": v1.NewLiteral(moment.Format(time.RFC3339)),
			}},
		},
	})

	deadline, err := v1.EvalWaitDeadline(t.Context(), waitUntil(t, "${steps.schedule.opens_at}"), scope, now)
	require.NoError(t, err)
	require.Equal(t, moment, deadline.UTC())
}

// TestAStepMayBeCalledNow is what the rule above became.
//
// A step called `now` used to be refused, because bound names win over step
// outputs: it would have worked everywhere except inside a `wait_until:`, where
// it would silently have meant the clock. Rooting removes the possibility rather
// than the permission — the step is `steps.now` and the clock is `now`, so
// neither can be written where the other was meant, and there is nothing left for
// a rule to forbid.
//
// The refusal is not merely dropped here; the two names are asserted to resolve
// to *different things* in the one scope where both exist. That is the property
// the old rule was protecting, and it is now structural.
func TestAStepMayBeCalledNow(t *testing.T) {
	t.Parallel()

	source := "name: t\nsteps:\n" +
		"  - id: now\n    cel:\n      expr: \"'2001-01-01T00:00:00Z'\"\n" +
		"  - id: hold\n    wait_until: ${now}\n"

	workflow, err := flowfile.Unmarshal([]byte(source))
	require.NoError(t, err, "the workflow did not compile")
	require.Empty(t, flowfile.Validate(workflow), "a step named `now` must be accepted now")

	// Both names, in the scope a wait evaluates against: the step through the
	// root, the clock bare.
	clock := time.Date(2030, 6, 1, 12, 0, 0, 0, time.UTC)
	scope := v1.NewScope(v1.CurrentProfile, &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			"now": {NamedValues: map[string]*v1.Value{
				"result": v1.NewLiteral("2001-01-01T00:00:00Z"),
			}},
		},
	})

	fromClock, err := v1.EvalWaitDeadline(t.Context(), waitUntil(t, "${now}"), scope, clock)
	require.NoError(t, err)
	assert.Equal(t, clock, fromClock.UTC(), "bare `now` is the clock")

	fromStep, err := v1.EvalWaitDeadline(t.Context(), waitUntil(t, "${steps.now.result}"), scope, clock)
	require.NoError(t, err)
	assert.Equal(t, time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC), fromStep.UTC(),
		"the rooted name is the step, and the clock did not shadow it")
}

// A loop iterator is the other name an author picks that lands in a wait's scope,
// and it reaches it by a different route: a step id resolves through the scope's
// outputs, while an iterator is bound into its vars. The clock is bound over both.
//
// So this is the same test as the one above pointed at the other route. It asserts
// the refusal *and* the shadowing that makes the refusal necessary, because the
// refusal alone would keep passing if the binding order were ever reversed — and
// reversing it is the change that looks like a fix from the wrong end.
func TestWaitDeadlineNowDoesNotShadowALoopIterator(t *testing.T) {
	t.Parallel()

	// The shadowing itself, at the level the drivers share. The item is a moment
	// carried as data — the shape a for_each over embargo times has — so if the
	// iterator won, the deadline would be the item.
	item := "2001-01-01T00:00:00Z"
	clock := time.Date(2030, 6, 1, 12, 0, 0, 0, time.UTC)

	scope := v1.NewScope(v1.CurrentProfile, nil).WithLocal(v1.NowIdentifier, v1.NewLiteral(item))

	deadline, err := v1.EvalWaitDeadline(t.Context(), waitUntil(t, "${now}"), scope, clock)
	require.NoError(t, err)
	require.Equal(t, clock, deadline.UTC(),
		"the loop variable won, so the clock a wait deadline needs is not the one it got")

	// Which is why the name cannot be chosen. Without this the workflow above is
	// authorable, and the only symptom is a wait that ends at the wrong moment.
	source := "name: t\nsteps:\n" +
		"  - id: targets\n    cel:\n      expr: \"['a']\"\n" +
		"  - id: sweep\n    for_each:\n      items: ${steps.targets.result}\n      iterator: now\n" +
		"      steps:\n        - id: hold\n          wait_until: ${now}\n"

	workflow, err := flowfile.Unmarshal([]byte(source))
	require.NoError(t, err, "the workflow did not compile")

	diagnostics := flowfile.Validate(workflow)
	require.NotEmpty(t, diagnostics, "a loop iterator named `now` was accepted")
	require.Contains(t, diagnostics.Error(), "choose another iterator")
}
