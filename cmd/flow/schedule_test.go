package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
)

type scheduleCreateRecorder struct {
	flowstatev1connect.UnimplementedWorkflowServiceHandler

	request *v1.CreateScheduleRequest
}

func (r *scheduleCreateRecorder) CreateSchedule(_ context.Context, req *connect.Request[v1.CreateScheduleRequest]) (*connect.Response[v1.CreateScheduleResponse], error) {
	r.request = req.Msg

	return connect.NewResponse(&v1.CreateScheduleResponse{Schedule: &v1.ScheduleDescription{}}), nil
}

// TestScheduleCreateChecksPluginTasksAgainstACatalog proves submission accepts
// plugin descriptors without launching plugin code in the CLI process. The
// server receiving the compiled task is the join: merely exposing the flag
// would pass even if workflow loading still used the built-in registry.
func TestScheduleCreateChecksPluginTasksAgainstACatalog(t *testing.T) {
	bin := buildFlowBinary(t)
	catalog := pluginCatalogFor(t, bin)

	dir := t.TempDir()
	workflow := filepath.Join(dir, "workflow.yaml")
	require.NoError(t, os.WriteFile(workflow, []byte(`edition: v2026.3
name: plugin-schedule
triggers:
  schedule:
    every: 1h
steps:
  - id: hello
    example.greet:
      greeting: Hello
      name: schedule
outputs: {}
`), 0o600))

	recorder := &scheduleCreateRecorder{}
	mux := http.NewServeMux()
	mux.Handle(flowstatev1connect.NewWorkflowServiceHandler(recorder))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	res := runFlowBinary(t, bin, "schedule", "create", workflow,
		"--"+pluginCatalogFlag, catalog, "--address", server.URL, "--output", "json")
	require.NoError(t, res.Err, "a scheduled plugin task was refused:\n%s", res.Output())
	require.NotNil(t, recorder.request, "schedule creation never reached the server")
	require.Len(t, recorder.request.GetWorkflow().GetSteps(), 1)
	assert.Equal(t, "example.greet", recorder.request.GetWorkflow().GetSteps()[0].GetTask().GetName())
}

// backfillCommand is a command carrying only the flag under test.
//
// Built here rather than reached for out of the real tree because what is being
// tested is the parsing of one flag's values, and a command that also needs an
// address, a workflow file and a server would be testing everything except that.
func backfillCommand(t *testing.T, values ...string) *cobra.Command {
	t.Helper()

	cmd := &cobra.Command{Use: "create"}
	cmd.Flags().StringSlice("backfill", nil, "")
	for _, value := range values {
		require.NoError(t, cmd.Flags().Set("backfill", value))
	}

	return cmd
}

// TestBackfillFlagsAreReadAndBounded covers `--backfill START..END` at the point
// an operator's typing becomes a request.
//
// The bounds are asserted here *and* against the server, which is the point rather
// than duplication: the CLI check exists so the message arrives before a round
// trip, and the server check exists because the RPC is public and a bound only the
// CLI applies is not a bound (TestABackfillBeyondItsBoundsIsRefusedByTheServer is
// the other half). Both call [v1.CheckScheduleBackfill], so there is one spelling
// of every rule and the two cannot drift into disagreeing.
func TestBackfillFlagsAreReadAndBounded(t *testing.T) {
	t.Parallel()

	t.Run("no flag is no backfill", func(t *testing.T) {
		t.Parallel()

		backfills, err := scheduleBackfillFlags(backfillCommand(t))
		require.NoError(t, err)
		assert.Empty(t, backfills)
	})

	t.Run("a range becomes a request", func(t *testing.T) {
		t.Parallel()

		backfills, err := scheduleBackfillFlags(backfillCommand(t,
			"2026-08-01T00:00:00Z..2026-08-02T00:00:00Z",
			"2026-08-04T00:00:00Z..2026-08-05T00:00:00Z"))
		require.NoError(t, err)
		require.Len(t, backfills, 2)
		assert.Equal(t, "2026-08-01T00:00:00Z", backfills[0].GetStartAt().AsTime().UTC().Format(time.RFC3339))
		assert.Equal(t, "2026-08-05T00:00:00Z", backfills[1].GetEndAt().AsTime().UTC().Format(time.RFC3339))
	})

	for _, tt := range []struct {
		name    string
		values  []string
		message string
	}{
		{
			name:    "no separator at all",
			values:  []string{"2026-08-01T00:00:00Z"},
			message: "must be START..END",
		},
		{
			name:    "a start that is not a timestamp",
			values:  []string{"yesterday..2026-08-02T00:00:00Z"},
			message: "backfill start",
		},
		{
			name:    "an end that is not a timestamp",
			values:  []string{"2026-08-01T00:00:00Z..tomorrow"},
			message: "backfill end",
		},
		{
			name:    "a range that runs backwards",
			values:  []string{"2026-08-02T00:00:00Z..2026-08-01T00:00:00Z"},
			message: "the start must come first",
		},
		{
			name:    "more history than the span allows",
			values:  []string{"2026-01-01T00:00:00Z..2026-06-01T00:00:00Z"},
			message: "more than 744h0m0s of history",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := scheduleBackfillFlags(backfillCommand(t, tt.values...))
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.message)
		})
	}

	// The count bound, reached and then passed: ten ranges are the most an operator
	// may ask for, and each is short enough that the span bound is not what refuses
	// the eleventh.
	t.Run("the count bound is reached and then exceeded", func(t *testing.T) {
		t.Parallel()

		var values []string
		for i := range v1.MaxScheduleBackfills {
			day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, i*2)
			values = append(values, day.Format(time.RFC3339)+".."+day.Add(time.Hour).Format(time.RFC3339))
		}

		backfills, err := scheduleBackfillFlags(backfillCommand(t, values...))
		require.NoError(t, err, "exactly the maximum number of ranges must be accepted")
		assert.Len(t, backfills, v1.MaxScheduleBackfills)

		values = append(values, "2026-09-01T00:00:00Z..2026-09-01T01:00:00Z")
		_, err = scheduleBackfillFlags(backfillCommand(t, values...))
		require.Error(t, err)
		assert.ErrorContains(t, err, "at most 10 are accepted")
	})
}

// TestDescribeCadenceSaysWhatWasAskedFor covers the line `flow schedule describe`
// prints, including the fields the bounded recovery controls added.
//
// A describe answers one question: is this the schedule I meant. A rendering that
// says "1 calendar specification(s)" answers a different one, so the values are
// printed in the notation the Flowfile writes them in.
func TestDescribeCadenceSaysWhatWasAskedFor(t *testing.T) {
	t.Parallel()

	line := describeCadence(&v1.ScheduleTrigger{
		Cron:     []string{"0 7 * * MON-FRI"},
		TimeZone: "Europe/Dublin",
		Overlap:  v1.ScheduleTrigger_OVERLAP_SKIP,
		Calendars: []*v1.ScheduleTrigger_Calendar{{
			Hour:       []*v1.ScheduleTrigger_Calendar_Range{{Start: 9, End: 17, Step: 2}},
			DayOfMonth: []*v1.ScheduleTrigger_Calendar_Range{{Start: 1}},
			Comment:    "office hours",
		}},
		StartAt:        timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
		EndAt:          timestamppb.New(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)),
		CatchupWindow:  durationpb.New(6 * time.Hour),
		PauseOnFailure: true,
	})

	assert.Contains(t, line, "0 7 * * MON-FRI")
	assert.Contains(t, line, "calendar hour 9-17/2 day_of_month 1 (office hours)")
	assert.Contains(t, line, "from 2026-01-01T00:00:00Z")
	assert.Contains(t, line, "through 2027-01-01T00:00:00Z")
	assert.Contains(t, line, "catch up within 6h0m0s")
	assert.Contains(t, line, "pause on failure")

	// A trigger that says none of it says none of it: an absent field must not be
	// rendered as the value it would default to, or a describe becomes a claim
	// about what somebody wrote.
	plain := describeCadence(&v1.ScheduleTrigger{Every: durationpb.New(time.Hour)})
	assert.Equal(t, "every 1h0m0s", plain)
}
