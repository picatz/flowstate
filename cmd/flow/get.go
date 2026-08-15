package main

import (
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

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
	rendering, err := resolveRunRendering(cmd)
	if err != nil {
		return err
	}

	format := rendering.format

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

	// `flow get` asks about a run by id alone, on a separate invocation from
	// whatever started it — it never holds the workflow specification that
	// declared which of these outputs are sensitive. That is exactly the
	// fail-closed case [redactGetResponse] documents: workflow is nil, so every
	// declared output is withheld unless --reveal-sensitive asked otherwise.
	reveal := revealSensitiveRequested(cmd)
	if reveal {
		noteRevealedSensitiveValues(surface)
	}

	msg := redactGetResponse(response.Msg, nil, reveal)

	// A machine reader gets the whole answer as one document — status, both ids,
	// outputs, and the failure if there was one — rather than the outputs alone
	// with the status split off onto another stream. Splitting is right for a
	// person, who is reading two things; it is wrong for a program, which is
	// reading one.
	//
	// The exit status is still the run's outcome, so `flow get x -o json && ...`
	// behaves the way the shell reader expects either way.
	if format.Machine() {
		if err := writeRunJSON(surface, rendering, msg); err != nil {
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
	//
	// The same sentence `flow watch` shows, from the same function: a person who
	// asks once and a person who watches are reading about one run, and two
	// renderers for it would eventually describe it two ways.
	for _, line := range pendingActivityLines(msg.GetPendingActivities(), time.Now()) {
		fmt.Fprintf(surface.Err, "  %s\n", surface.ErrTheme.Muted.Render(line))
	}

	// What the run is waiting for a person to do. A gate names the signal an
	// operator has to send, which is the one thing a position cannot say: `on
	// approval` tells somebody where the run is and not what would move it.
	for _, line := range pendingWaitLines(msg.GetProgress(), time.Now()) {
		fmt.Fprintf(surface.Err, "  %s\n", surface.ErrTheme.Muted.Render(line))
	}

	// What the workflow said it would report, named before the transcript is written
	// out beneath it — the same section `flow run` and `flow watch` finish with,
	// through the same function, so one finished run reads the same however it was
	// asked about.
	writeRunOutputs(surface, msg)

	// Through the one function that writes this document, rather than through a
	// bare protojson.Marshal of its own, which is what stood here. That was the
	// divergence runlocal.go's own doc comment warns about, live in a third verb:
	// `flow get x | jq` received a document with unpopulated fields dropped while
	// `flow run local x | jq` received them, so the same expression answered
	// differently depending on which verb a script had asked.
	if err := writeStepOutputs(surface, rendering, msg); err != nil {
		return fmt.Errorf("formatting the outputs of %s: %w", workflowID, err)
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
//
// Delegated rather than trimmed here, because this stopped being the only place
// that needed the short name: `--filter` compares against it, so a filter and a
// printed line have to agree about what a status is called. Two trims of one
// prefix is the shape CLAUDE.md describes as a value with one meaning written
// down twice — harmless until one of them changes.
func statusLabel(status v1.RunResponse_Status) string {
	return v1.StatusName(status)
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
	position := positionPath(progress)
	if position == "" {
		return ""
	}

	return theme.Muted.Render(" on " + position)
}

// positionPath is the position as bare text, with no styling and no sentence
// around it.
//
// Split out because two surfaces want the same value in different frames: `flow get`
// and the line-per-change shape of `flow watch` append it to a status line, which is
// what [runPosition] renders; the live view puts it on a line of its own, and it is
// also what that view compares against to decide a run has moved. Joining the path
// twice is how the two would come to disagree about a run inside a loop.
//
// Empty where the server said nothing, which is not the same as the beginning — see
// [runPosition].
func positionPath(progress *v1.RunProgress) string {
	if progress.GetStepId() == "" {
		return ""
	}

	position := progress.GetStepId()
	for _, step := range progress.GetPath() {
		position += " > " + step
	}

	return position
}

// pendingWaitLines renders the gates a run is parked on, one sentence each.
//
// The signal name is the point of the line: it is the argument `flow signal`
// takes, and before this it could only be recovered from the file the run was
// compiled from. The deadline is rendered as a countdown for [runAge]'s reason,
// against a moment the caller supplies rather than one this reads, so a rendered
// line is a fact about the answer rather than about when it happened to print.
//
// Nothing is printed for a run with no gates open, and nothing for a run whose
// worker did not answer: both are the empty set here, and the difference between
// them is one the position beside this already reports (an unset progress is
// "nobody answered", see [runPosition]).
//
// A truncated answer says so rather than quietly reading as the whole of it,
// which is the rule the schema states for the flag itself.
func pendingWaitLines(progress *v1.RunProgress, now time.Time) []string {
	waits := progress.GetPendingWaits()
	if len(waits) == 0 {
		return nil
	}

	lines := make([]string, 0, len(waits)+1)
	for _, wait := range waits {
		where := wait.GetStepId()
		if path := wait.GetPath(); len(path) > 0 {
			where = strings.Join(path, " > ") + " > " + where
		}

		line := fmt.Sprintf("waiting at %s for signal %q", where, wait.GetSignalName())
		if wait.GetPoliced() {
			// Said because the two ways a signal fails to arrive look identical
			// from here: nobody sent one, and the server refused the one that
			// was sent.
			line += " (authorized senders only)"
		}
		if deadline := wait.GetDeadline(); deadline != nil {
			if left := deadline.AsTime().Sub(now); left > 0 {
				line += fmt.Sprintf(", lapsing in %s", roundedDuration(left))
			} else {
				line += ", lapsing now"
			}
		}

		lines = append(lines, line)
	}

	if progress.GetPendingWaitsTruncated() {
		lines = append(lines, "and more gates than this run reports")
	}

	return lines
}

// pendingActivityLines renders what Temporal is retrying, one sentence each.
//
// now is supplied rather than read, because the countdown is measured against
// *some* moment: `flow get` reads the clock as it prints, and a watch uses the time
// the answer was observed — which is what makes the line a fact about the poll
// rather than about when it happened to be rendered.
func pendingActivityLines(pending []*v1.PendingActivity, now time.Time) []string {
	if len(pending) == 0 {
		return nil
	}

	lines := make([]string, 0, len(pending))
	for _, activity := range pending {
		line := fmt.Sprintf("retrying, attempt %d", activity.GetAttempt())
		if failure := activity.GetLastFailure(); failure != "" {
			line += ": " + failure
		}
		if next := activity.GetNextAttemptScheduledTime(); next != nil {
			if wait := next.AsTime().Sub(now); wait > 0 {
				line += fmt.Sprintf(" (next attempt in %s)", wait.Round(time.Second))
			}
		}

		// What the attempt running right now last said it was doing, which is the
		// difference between "this has been going for four minutes" and knowing
		// which end of it is slow.
		//
		// Appended last, after the failure and the countdown, because those two
		// describe attempts that are over and this describes the one that is not.
		// Absent when the attempt is waiting to be retried rather than running,
		// when it has not reported yet, or when the worker predates the field —
		// none of which is "doing nothing", so nothing is printed rather than a
		// word that would claim one of them.
		if phase := activity.GetPhase(); phase != "" {
			line += ", " + phase
		}

		lines = append(lines, line)
	}

	return lines
}
