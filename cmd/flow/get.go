package main

import (
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// runGet reports what a run is doing, and what it produced if it is finished.
//
// `flow run` already polls this while it waits, which covered the case where the
// person who started a workload is still watching it. That is the case durable
// execution is least needed for. A workload that outlives the terminal that
// started it has to be askable about afterwards, and an approval gate is the
// clearest example: it is waiting precisely because nobody is watching.
func runGet(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	surface := newSurface(cmd)
	workflowID := args[0]

	request := &v1.GetRequest{WorkflowId: workflowID}

	// Left absent rather than empty when unset. The schema requires a run id to be
	// a UUID when present, so sending "" would be refused for not looking like one
	// instead of meaning "whichever run is current".
	server := serverFlagsOf(cmd)

	if runID, _ := cmd.Flags().GetString("run-id"); runID != "" {
		request.RunId = &runID
	}

	if err := v1.Validate(request); err != nil {
		return fmt.Errorf("%w\n  a run id is the UUID Temporal gave one attempt at the workload; "+
			"omit it to ask about whichever attempt is current", err)
	}

	response, err := newWorkflowServiceClient(server).Get(cmd.Context(), connect.NewRequest(request))
	if err != nil {
		return refusedRun("reading", workflowID, server, err)
	}

	msg := response.Msg

	// A machine reader gets the whole answer as one document — status, both ids,
	// outputs, and the failure if there was one — rather than the outputs alone
	// with the status split off onto another stream. Splitting is right for a
	// person, who is reading two things; it is wrong for a program, which is
	// reading one.
	//
	// The exit status is still the run's outcome, so `flow get x -o json && ...`
	// behaves the way the shell reader expects either way.
	if format.Machine() {
		if err := writeJSON(surface, format, msg); err != nil {
			return err
		}

		if failure := msg.GetError(); failure != nil {
			return fmt.Errorf("run %s ended %s: %s",
				workflowID, strings.ToLower(statusLabel(msg.GetStatus())), failure.GetMessage())
		}

		return nil
	}

	// The status goes to stderr and the outputs to stdout, so `flow get x | jq`
	// receives a workload's data and nothing else, while somebody watching a
	// terminal still sees what happened. A run that is still going produces no
	// stdout at all, which is the honest answer to "what did it produce".
	// The status is the one value on this line worth finding before the line is
	// read, which is what a filled label is for. It is the word either way: strip
	// the styling and the sentence still says what happened.
	//
	// ErrTheme, because this line goes to stderr: `flow get x | jq` has a piped
	// stdout and a terminal stderr, and the palette for one is not the palette for
	// the other.
	fmt.Fprintf(surface.Err, "%s workflow %s run %s%s%s\n",
		surface.ErrTheme.Pill(statusTone(msg.GetStatus()), statusLabel(msg.GetStatus())),
		msg.GetWorkflowId(), msg.GetRunId(), runAge(msg, time.Now()),
		runPosition(surface.ErrTheme, msg.GetProgress()))

	// The why beneath the where. Position says which step a running run is on;
	// this says an attempt count is climbing and what the last one died of,
	// which is the difference between "working" and "stuck" — the question that
	// used to require leaving Flowstate for the temporal CLI.
	for _, pending := range msg.GetPendingActivities() {
		line := fmt.Sprintf("retrying, attempt %d", pending.GetAttempt())
		if failure := pending.GetLastFailure(); failure != "" {
			line += ": " + failure
		}
		if next := pending.GetNextAttemptScheduledTime(); next != nil {
			if wait := time.Until(next.AsTime()); wait > 0 {
				line += fmt.Sprintf(" (next attempt in %s)", wait.Round(time.Second))
			}
		}
		fmt.Fprintf(surface.Err, "  %s\n", surface.ErrTheme.Muted.Render(line))
	}

	if outputs := msg.GetOutputs(); outputs != nil {
		encoded, err := protojson.Marshal(outputs)
		if err != nil {
			return fmt.Errorf("formatting the outputs of %s: %w", workflowID, err)
		}
		fmt.Fprintf(surface.Out, "%s\n", encoded)
	}

	// A run that failed is reported as a failure, so `flow get x && ...` behaves
	// the way a shell reader expects. The query itself succeeded; what is being
	// reported is the workload's outcome, which is what was asked about.
	if failure := msg.GetError(); failure != nil {
		return fmt.Errorf("run %s ended %s: %s",
			workflowID, strings.ToLower(statusLabel(msg.GetStatus())), failure.GetMessage())
	}

	return nil
}

// runAge renders how long a run has been going, or how long it took.
//
// It is the answer to the question a status alone cannot reach. `RUNNING` is the same
// word for a workload three seconds in and one wedged since Tuesday, and telling those
// apart is the whole reason somebody runs `flow get` on a particular run rather than
// listing everything.
//
// Elapsed rather than an instant, because the reader is asking about *this* run rather
// than about the calendar. The machine-readable output carries the timestamps
// themselves, where a consumer can do its own arithmetic.
//
// Empty when there is no start time, which is what an older server answers. A CLI that
// printed "0s" there would be inventing a fact about a run it was told nothing about.
func runAge(msg *v1.GetResponse, now time.Time) string {
	if msg.GetStartTime() == nil {
		return ""
	}

	started := msg.GetStartTime().AsTime()
	if closed := msg.GetCloseTime(); closed != nil {
		return fmt.Sprintf(" (took %s)", roundedDuration(closed.AsTime().Sub(started)))
	}

	// The caller supplies the clock rather than this reading it, because a
	// running run's age is measured against *some* moment, and a test that
	// cannot say which moment can only assert it within a racy window.
	return fmt.Sprintf(" (running for %s)", roundedDuration(now.Sub(started)))
}

// roundedDuration renders a duration at a precision somebody reads rather than
// computes: whole seconds, and no more than that.
//
// A run's age is prose. `1m23.4917s` is a measurement of the clock this command
// happened to be run at, and the extra digits say nothing about the workload.
func roundedDuration(d time.Duration) time.Duration {
	if d < 0 {
		// A clock skew between the server and this machine, which is not something
		// to report as a negative age.
		return 0
	}

	return d.Round(time.Second)
}

// statusLabel renders a run status the way a person would say it.
//
// The generated enum names carry a STATUS_ prefix that exists to keep the
// constants distinct in the schema, and repeating it on a terminal only makes a
// line harder to scan.
func statusLabel(status v1.RunResponse_Status) string {
	return strings.TrimPrefix(status.String(), "STATUS_")
}

// runPosition renders where a running run has got to.
//
// Empty where the server said nothing, which is not the same as the beginning: a
// run whose worker did not answer, or one on an interpreter built before the query
// existed, reports no position. Rendering that as "on step 1" would be a fact
// invented on the run's behalf, and it would be wrong exactly when somebody is
// looking because something seems stuck.
//
// The path is joined with a separator rather than nested, because this is one line
// beside a status and the whole value of it is being readable at a glance —
// `on deploy > each > upload` says the shape without spending three lines on it.
func runPosition(theme ui.Theme, progress *v1.RunProgress) string {
	if progress.GetStepId() == "" {
		return ""
	}

	position := progress.GetStepId()
	for _, step := range progress.GetPath() {
		position += " > " + step
	}

	return theme.Muted.Render(" on " + position)
}
