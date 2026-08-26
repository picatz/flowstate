package main

import (
	"fmt"
	"text/tabwriter"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// `flow timeline`, which asks a run what it did rather than what it is doing.
//
// A verb rather than a flag on `flow get`, and that is the decision worth
// stating. `get` answers about a run's present — status, position, what is
// mid-retry, what it is parked on — and every field of its answer is about now.
// An account of the past is a different shape (many rows, ordered, each about a
// moment), a different bound (a history rather than one Describe), and a
// different reason to be called. Folding it in would give `flow get` two answers
// and make every consumer of the first parse around the second.
//
// The cost, stated rather than glossed: a verb somebody has to tell apart from
// `get`, and help text that has to keep saying which is which.

// newTimelineCommand builds the `flow timeline` command.
func newTimelineCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "timeline [workflow-id]",
		Short: "Report what a run did, in the order it did it",
		Long: "Read a run's own account of itself: which step ran, which attempt, what it " +
			"waited for, what failed and with what sentence.\n\n" +
			"`flow get` answers what a run is doing. This answers what it did, which is the " +
			"question left when a run has already finished and there is no present to " +
			"report. It starts nothing, signals nothing and changes nothing.\n\n" +
			"A run that continued as new has an account per segment. `nextRunId` and " +
			"`previousRunId` name the neighbours and `firstRunId` names where the workload " +
			"began; pass one with --run-id to read it. Both directions, because omitting " +
			"--run-id reads the *latest* segment, which by definition has no next one.\n\n" +
			"`truncated` says the account is not the whole of that segment — resume with " +
			"--after-event-id set to the last row's event id. Raising --max-entries is not " +
			"the way past it: the ceiling is a ceiling, and one segment can hold several " +
			"times the largest answer this returns.",
		Args: cobra.ExactArgs(1),
		RunE: runTimeline,
		Example: `# What did this run actually do?
flow timeline flowstate-workflow-3f7c

# Just the failures, for a script:
flow timeline flowstate-workflow-3f7c -o json | jq '.entries[] | select(.failure != null)'

# The next segment of a workload that continued as new:
flow timeline flowstate-workflow-3f7c --run-id 0198f1e2-...

# Continue an account the server clipped:
flow timeline flowstate-workflow-3f7c --after-event-id 4821`,
	}

	addOutputFlag(cmd)

	cmd.Flags().String("run-id", "",
		"read one segment of the workload; unset reads whichever is current")
	cmd.Flags().Int32("max-entries", 0,
		"stop after this many entries; unset uses the server's default")
	cmd.Flags().Int64("after-event-id", 0,
		"resume past an entry already read, by its event id; unset starts at the beginning")

	return cmd
}

// runTimeline answers `flow timeline`.
func runTimeline(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	surface := newSurface(cmd)
	server := serverFlagsOf(cmd)
	workflowID := args[0]

	runID, _ := cmd.Flags().GetString("run-id")
	maxEntries, _ := cmd.Flags().GetInt32("max-entries")
	afterEventID, _ := cmd.Flags().GetInt64("after-event-id")

	request := &v1.GetTimelineRequest{
		WorkflowId:   workflowID,
		RunId:        runID,
		MaxEntries:   maxEntries,
		AfterEventId: afterEventID,
	}
	if err := v1.Validate(request); err != nil {
		return err
	}

	response, err := newWorkflowServiceClient(server).GetTimeline(cmd.Context(), connect.NewRequest(request))
	if err != nil {
		return refusedRun("reading the timeline of", workflowID, server, err)
	}

	if format != FormatText {
		return writeJSON(surface, format, response.Msg)
	}

	renderTimeline(surface, response.Msg)

	return nil
}

// renderTimeline writes the account for a person.
func renderTimeline(surface *ui.UI, msg *v1.GetTimelineResponse) {
	if len(msg.GetEntries()) == 0 {
		fmt.Fprintln(surface.Err, "this run has recorded nothing yet")
	}

	table := tabwriter.NewWriter(surface.Out, 0, 8, 2, ' ', 0)
	if len(msg.GetEntries()) > 0 {
		fmt.Fprintln(table, "TIME\tWHAT\tSTEP\tDETAIL")
	}

	for _, entry := range msg.GetEntries() {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\n",
			formatRunTime(entry.GetTime().AsTime(), entry.GetTime() != nil),
			timelineKindLabel(entry.GetKind()),
			entry.GetStep(),
			timelineDetail(entry),
		)
	}
	_ = table.Flush()

	// All of these go to the error stream, because they are about the answer
	// rather than part of it: a reader piping this into `jq` or a file wants
	// the rows.
	if msg.GetTruncated() {
		// The command to run next, not the fact that there is more. A reader
		// told only that an answer is partial has to work out how to continue
		// it, and the whole point of reporting the event id is that they do not
		// have to.
		if entries := msg.GetEntries(); len(entries) > 0 {
			fmt.Fprintf(surface.Err,
				"this is not the whole of this run's account — continue with "+
					"--after-event-id %d\n", entries[len(entries)-1].GetEventId())
		} else {
			fmt.Fprintln(surface.Err,
				"this run's history is longer than this server will walk, so nothing past "+
					"here is readable; the same request will do the same thing")
		}
	}
	if next := msg.GetNextRunId(); next != "" {
		fmt.Fprintf(surface.Err,
			"this run continued as new; the next segment is --run-id %s\n", next)
	}
	if previous := msg.GetPreviousRunId(); previous != "" {
		fmt.Fprintf(surface.Err,
			"it continued from --run-id %s (the workload began at --run-id %s)\n",
			previous, msg.GetFirstRunId())
	}
}

// timelineKindLabel is the one-word column, which is what makes the table
// scannable: a reader looks down it for `failed` and stops there.
func timelineKindLabel(kind v1.TimelineEntry_Kind) string {
	switch kind {
	case v1.TimelineEntry_KIND_STEP_SCHEDULED:
		return "step"
	case v1.TimelineEntry_KIND_STEP_COMPLETED:
		return "done"
	case v1.TimelineEntry_KIND_STEP_FAILED:
		return "failed"
	case v1.TimelineEntry_KIND_STEP_TIMED_OUT:
		return "timed out"
	case v1.TimelineEntry_KIND_STEP_CANCELED:
		return "canceled"
	case v1.TimelineEntry_KIND_TIMER_STARTED:
		return "waiting"
	case v1.TimelineEntry_KIND_TIMER_FIRED:
		return "waited"
	case v1.TimelineEntry_KIND_SIGNAL_RECEIVED:
		return "signal"
	case v1.TimelineEntry_KIND_RUN_CONTINUED:
		return "continued"
	case v1.TimelineEntry_KIND_RUN_ENDED:
		return "ended"
	default:
		return "?"
	}
}

// timelineDetail is the rightmost column: the attempt when there was more than
// one, and the failure's sentence when there was one.
//
// The attempt is shown only past the first, because "attempt 1" on every row is
// a column of noise, and a run that never retried should read as one that never
// retried.
func timelineDetail(entry *v1.TimelineEntry) string {
	switch {
	case entry.GetAttempt() > 1 && entry.GetFailure() != "":
		return fmt.Sprintf("attempt %d after: %s", entry.GetAttempt(), entry.GetFailure())
	case entry.GetAttempt() > 1:
		return fmt.Sprintf("attempt %d", entry.GetAttempt())
	default:
		return entry.GetFailure()
	}
}
