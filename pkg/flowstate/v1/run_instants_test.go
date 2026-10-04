package flowstatev1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// The local half of [conformance.AssertRunInstants]: `run.started_at` is the
// instant the run's clock read when the run began, and `trigger.scheduled_at` is
// the epoch for a run that has no schedule.
func TestRunInstantsLocal(t *testing.T) {
	started := time.Date(2026, 8, 2, 7, 30, 0, 0, time.UTC)
	ctx := v1.NewContextWithClock(t.Context(), v1.NewVirtualClock(started))

	outputs, err := v1.Run(ctx, conformance.RunAddressWorkflow())
	require.NoError(t, err)

	conformance.AssertRunInstants(t, outputs, started, time.Unix(0, 0))
}

// A scheduled run's slot is read from its trigger context, and a start is not a
// slot: the two differ under a backfill.
func TestRunInstantsLocalScheduledSlotIsNotTheStart(t *testing.T) {
	started := time.Date(2026, 8, 3, 9, 0, 0, 0, time.UTC)
	slot := time.Date(2026, 8, 2, 7, 0, 0, 0, time.UTC)

	trigger := v1.NewScheduleTriggerContext("nightly", "ops")
	trigger.ScheduledAt = timestamppb.New(slot)
	ctx := v1.NewContextWithClock(t.Context(), v1.NewVirtualClock(started))
	ctx = v1.NewContextWithTrigger(ctx, trigger)

	outputs, err := v1.Run(ctx, conformance.RunAddressWorkflow())
	require.NoError(t, err)

	conformance.AssertRunInstants(t, outputs, started, slot)
}

// A host that starts the same program twice (a reversible debugger's replay) pins
// the start, and the pin wins over a clock that has moved on in between.
func TestRunInstantsLocalPinnedStartOutlivesTheClock(t *testing.T) {
	pinned := time.Date(2026, 8, 2, 7, 30, 0, 0, time.UTC)
	clock := v1.NewVirtualClock(pinned.Add(time.Hour))
	ctx := v1.NewContextWithClock(t.Context(), clock)
	ctx = v1.NewContextWithRunStart(ctx, pinned)

	outputs, err := v1.Run(ctx, conformance.RunAddressWorkflow())
	require.NoError(t, err)

	conformance.AssertRunInstants(t, outputs, pinned, time.Unix(0, 0))
}
