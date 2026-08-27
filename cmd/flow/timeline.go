package main

import (
	"context"
	"fmt"
	"text/tabwriter"
	"time"

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
			"--run-id and --after-event-id set to the last row's event id, which the " +
			"command prints for you. Both, because event ids restart in each segment: a " +
			"cursor means nothing until the segment it counts within is named. Raising " +
			"--max-entries is not the way past it either: the ceiling is a ceiling, and " +
			"one segment can hold several times the largest answer this returns.\n\n" +
			"One fact is missing from every account by construction, and this says so on " +
			"stderr when it applies. A step waiting out a retry backoff has not failed " +
			"anywhere history can see: Temporal records that failure on the next attempt's " +
			"start, so the most recent one has no row until that attempt begins, and reading " +
			"further with --after-event-id will not find it. `flow get` reports it, and the " +
			"note names the command to run.\n\n" +
			"That note is a second read, taken after the rows, and it says so: it is the run's " +
			"present rather than the account's last line. A step can stop retrying between the " +
			"two, in which case no note is printed for a gap the rows really had.",
		Args: cobra.ExactArgs(1),
		RunE: runTimeline,
		Example: `# What did this run actually do?
flow timeline flowstate-workflow-3f7c

# Just the failures, for a script. Non-empty rather than non-null: this
# command emits unpopulated fields, so every entry has a failure and a step
# that succeeded carries it as the empty string.
flow timeline flowstate-workflow-3f7c -o json | jq '.entries[] | select(.failure != "")'

# The next segment of a workload that continued as new:
flow timeline flowstate-workflow-3f7c --run-id 0198f1e2-...

# Continue an account the server clipped, which names the segment as well
# because event ids restart in each one (the command prints both for you):
flow timeline flowstate-workflow-3f7c --run-id 0198f1e2-... --after-event-id 4821`,
	}

	addOutputFlag(cmd)

	cmd.Flags().String("run-id", "",
		"read one segment of the workload; unset reads whichever is current")
	cmd.Flags().Int32("max-entries", 0,
		"stop after this many entries; unset uses the server's default")
	cmd.Flags().Int64("after-event-id", 0,
		"resume past an entry already read, by its event id; requires --run-id, since "+
			"event ids restart in each segment; unset starts at the beginning")

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

	// Built once and used for both calls this verb can make, the way [runList]
	// already builds one for a sequence of them: resolving a credential source
	// is cheap but it is not free, and a second client would mint a second
	// token to answer one question.
	client := newWorkflowServiceClient(server)

	response, err := client.GetTimeline(cmd.Context(), connect.NewRequest(request))
	if err != nil {
		return refusedRun("reading the timeline of", workflowID, server, err)
	}

	if format != FormatText {
		// The footer below is prose, and prose has no place in a document a
		// program indexes into — `flow get -o json` withholds its own retrying
		// lines for exactly this reason, and there the fact is a *field* of the
		// answer rather than a sentence beside it. So the second call is not
		// made either: a round trip whose only output is suppressed is a cost
		// with no reader.
		return writeJSON(surface, format, response.Msg)
	}

	// Whether this answer continues one the caller already read, which decides
	// what an empty answer means.
	renderTimeline(surface, afterEventID > 0, response.Msg)

	// Last, after everything renderTimeline has to say about the account's
	// extent. Truncation and continued-as-new are facts *about the account*;
	// this one is about something no account can hold, so it reads as the
	// ending rather than as a second afterthought — and its own sentence says
	// outright that continuing the rows above will not produce it.
	noteRetryingSteps(cmd.Context(), surface, client, workflowID, response.Msg)

	return nil
}

// The account's honest gap, and why it is answered with a sentence rather than
// with a row.
//
// An attempt that has failed and is waiting out its retry backoff has no
// history event yet. Temporal writes that failure onto the *next* attempt's
// start, so a step in backoff shows a scheduling that never ends and one
// failure per attempt that already completed — every failure except the most
// recent one, which is the one somebody reading a stuck run came for. The
// schema states that boundary on [v1.TimelineEntry_KIND_STEP_FAILED] itself,
// and names [v1.GetResponse.PendingActivities] as the field that answers it.
//
// A footer rather than a synthesized row, and this is the part worth writing
// down. Every row is addressed by its event id and a caller resumes an account
// with --after-event-id, so a row invented here would be one no cursor can
// name: give it an id and it is indistinguishable from a recorded event, give
// it none and it breaks the resumption the truncation note tells people to
// use. It would also be a fabricated entry in a record whose whole worth is
// that it is one — the rule `cmd/flow/internal/mcp`'s ReduceTranscript already
// states where it drops steps from a transcript, that nothing is synthesized to
// stand in for what was left out, because a reader cannot tell an invented row
// from one the workload produced.

// timelineRunReader is the one extra RPC this footer costs.
//
// Narrow on purpose. The client the CLI builds satisfies it, and a type naming
// a single method is the plainest available statement of what asking for this
// sentence buys and what it spends.
type timelineRunReader interface {
	Get(context.Context, *connect.Request[v1.GetRequest]) (*connect.Response[v1.GetResponse], error)
}

// noteRetryingSteps asks what the run is doing now, when the account just
// printed leaves room for that to matter.
//
// Conditional, because this is a second round trip on a verb that had one. The
// gate is the account's own ending: [v1.GetResponse.PendingActivities] is
// populated only for a RUNNING run, and a segment whose account holds its
// ending has stopped being one. That is the common case for this verb — its
// whole reason to exist is the question left once a run has finished — so the
// ordinary `flow timeline` costs exactly what it did before.
//
// The residual, stated rather than glossed: an account clipped before its own
// ending, or resumed past it, pays for a call that answers "nothing pending".
// That is the direction to be wrong in. Overrunning the true scope costs a
// round trip; underrunning it hides the one fact this footer exists to report.
//
// Two reads, two moments, and the footer says which one it came from rather
// than pretending otherwise. GetTimeline read a history and this reads the
// run's present afterwards, so nothing here continues the rows above: a step
// can finish its retry between the two calls, in which case no footer is
// printed for a gap the account really had, and a step can begin retrying after
// them, in which case the footer describes something no row above mentions
// (Codex, #1142).
//
// The order is chosen rather than suffered, and it is the half of this that is
// fixable here. Asking first and rendering afterwards would make the footer
// *older* than the rows, so it could contradict a completion the reader can see
// three lines up — a sentence about a step the account already shows finishing.
// Asking second means the footer is never staler than the account, only
// fresher, and [retryingStepsFooter] leads with where its answer came from so
// that "fresher" reads as a second observation rather than as the account's
// last line.
//
// Which residual is shipped, said plainly, because they are not equally bad. A
// missing footer costs a reader a hint about a state that has since resolved,
// and the next `flow timeline` shows the whole story. A present-tense sentence
// that reads as part of the account would be a claim about the run that nothing
// synchronised. The first is cheap and the second is not, so the wording buys
// the first.
//
// The sound fix is one server-side snapshot — the retry fact carried in the
// timeline response itself — which is a schema change with both drivers to
// consider and belongs in its own change, in the same read path #1135 is
// already deciding the shape of.
func noteRetryingSteps(
	ctx context.Context,
	surface *ui.UI,
	reader timelineRunReader,
	workflowID string,
	msg *v1.GetTimelineResponse,
) {
	if timelineHoldsTheSegmentsEnding(msg) {
		return
	}

	request := &v1.GetRequest{WorkflowId: workflowID}

	// The segment the rows above are about, not whichever is current. A
	// timeline is per segment and a workload can continue as new between two
	// calls, so an unnamed run would let this sentence describe a different
	// segment from the account it is appended to — the same reasoning the
	// truncation note gives for naming both of its flags.
	if runID := msg.GetRunId(); runID != "" {
		request.RunId = &runID
	}

	response, err := reader.Get(ctx, connect.NewRequest(request))
	if err != nil {
		// Said, not swallowed. Once this footer exists its absence reads as
		// "nothing is mid-retry", and a check that never happened must not be
		// reported as a check that came back empty.
		//
		// Not an error, though. The account above is what the command was asked
		// for and it arrived, so a failed aside about the run's present does
		// not turn a successful read into a failed one.
		fmt.Fprintf(surface.Err,
			"whether a step is retrying is not known here, because reading this run's "+
				"present failed: %s\n", ui.EscapeControl(err.Error()))

		return
	}

	// Two fields of that answer are read and the rest of it — a workload's
	// outputs among them — is dropped here rather than rendered. Which of those
	// are sensitive is declared by the specification that produced them, which
	// `flow get` has a flag for and this verb neither holds nor asks about.
	if footer := retryingStepsFooter(workflowID, msg.GetRunId(), response.Msg, time.Now()); footer != "" {
		fmt.Fprintln(surface.Err, footer)
	}
}

// timelineHoldsTheSegmentsEnding reports whether this account carries the row
// that says the segment stopped.
//
// Both kinds, because a segment that continued as new has ended as surely as
// one that completed: the workload goes on under the next run id, and this
// account is about this one.
func timelineHoldsTheSegmentsEnding(msg *v1.GetTimelineResponse) bool {
	for _, entry := range msg.GetEntries() {
		if entry.GetKind() == v1.TimelineEntry_KIND_RUN_ENDED ||
			entry.GetKind() == v1.TimelineEntry_KIND_RUN_CONTINUED {
			return true
		}
	}

	return false
}

// retryingStepsFooter is the sentence, or the empty string when there is
// nothing to say.
//
// It opens by naming where its answer came from, and that clause is load
// bearing rather than throat clearing. This is a second read taken after the
// rows above, so a sentence that opened with the claim would be asserting a
// synchronisation between the two that does not exist — see [noteRetryingSteps]
// for the race and for which half of it is shipped. Said this way, a retry that
// began after the account was read is a later observation honestly labelled,
// rather than the account's last line contradicting itself.
//
// Never a step name, and that is a refusal rather than an omission. A pending
// activity is deliberately not named by step in the schema: Temporal
// identifies one by a generated id and a registered function name, a loop
// running iterations in parallel schedules one step id several times at once,
// and pairing a pending activity with an un-ended scheduling above would be a
// guess this surface would then print as a fact. The count is what can be said
// truthfully, and `flow get` is where the rest of the answer lives.
func retryingStepsFooter(workflowID, runID string, msg *v1.GetResponse, now time.Time) string {
	// Two conditions, each saying something the other cannot, and a pending
	// activity has to pass both before this note is owed anything about it.
	//
	// A count past the first says there *is* a previous attempt and it failed.
	// Temporal reports every activity that is scheduled or started and not yet
	// finished, so a healthy run on its first attempt at one step is in this
	// list too, and that step's scheduling is already a row with nothing
	// missing from it.
	//
	// A next attempt still ahead says that failure has no row *yet*, and this
	// is the condition the note actually turns on — an attempt count alone gets
	// it backwards. The server writes the previous attempt's failure into the
	// account the moment the next attempt *starts*, on the ActivityTaskStarted
	// arm of its timeline conversion, so a step whose retry is already running
	// has that failure on the reader's screen and a note about it would be
	// pointing three lines up (Codex, #1142). The only state this note exists
	// for is the one in between: failed, and waiting.
	//
	// A time already past is not evidence of waiting either, and is deliberately
	// read as silence rather than as an overdue attempt. The schema says this
	// field is unset while an attempt runs, but the server fills it from
	// Temporal's `scheduled_time` rather than its `next_attempt_schedule_time`,
	// and only the latter is documented to be null for an activity that is
	// scheduled or started — so a running attempt reaches here carrying the
	// moment *it* was scheduled, which is behind us. Staying quiet costs a hint
	// about a state that is already changing; speaking would be a claim about a
	// row the reader can see.
	//
	// The comparison is against this machine's clock, which is not the authority
	// on the server's schedule. Skew larger than a backoff interval mis-sorts
	// the two states in whichever direction it leans — the cost of deciding this
	// from a timestamp, and the same clock `flow get` already counts down
	// against.
	retrying := make([]*v1.PendingActivity, 0, len(msg.GetPendingActivities()))
	for _, activity := range msg.GetPendingActivities() {
		if activity.GetAttempt() <= 1 {
			continue
		}

		if next := activity.GetNextAttemptScheduledTime(); next == nil || !next.AsTime().After(now) {
			continue
		}

		retrying = append(retrying, activity)
	}

	// A clipped list of pending activities is the other way this sentence can
	// be owed. The server reports at most a fixed number and says when it cut
	// the list short, and the ones it did not report may be retrying — so a
	// clipped list with nothing retrying in it is "not known" rather than
	// "nothing", which is the rule the bound itself is documented under: a
	// reader must never mistake some of the retrying steps for all of them.
	clipped := msg.GetPendingActivitiesTruncated()

	if len(retrying) == 0 && !clipped {
		return ""
	}

	var head string

	switch {
	case len(retrying) == 0:
		head = "this run has more steps pending than it reports, so whether one of them " +
			"is retrying cannot be told from here"

	case len(retrying) == 1:
		// The countdown is unconditional here, because the filter above is what
		// established it: nothing reaches this slice without a next attempt
		// still ahead of the same `now`.
		//
		// "attempt N is due in" rather than "attempt N, next attempt in",
		// because those are one attempt and not two. The schema's attempt is
		// the one about to run, so naming a *next* one after it would invent a
		// further try nobody is waiting for — and the try whose failure is
		// missing from the account is the one before it.
		head = fmt.Sprintf("one step is retrying — attempt %d is due in %s",
			retrying[0].GetAttempt(),
			roundedDuration(retrying[0].GetNextAttemptScheduledTime().AsTime().Sub(now)))

	default:
		// The furthest, because that is what an attempt count is read for: a
		// run retrying four steps where one is on attempt 9 is a different
		// situation from four steps all on attempt 2.
		furthest := int32(0)
		for _, activity := range retrying {
			furthest = max(furthest, activity.GetAttempt())
		}

		head = fmt.Sprintf("%d steps are retrying — the furthest on attempt %d",
			len(retrying), furthest)
	}

	if clipped && len(retrying) > 0 {
		// Pending rather than retrying, because that is all the flag says: the
		// steps past the bound may be on their first attempt.
		head += ", and more steps are pending than this reports"
	}

	// Escaped, both of them. The workflow id is whatever the caller typed and
	// the run id is whatever the server answered, and this line is about to be
	// read by a terminal — the same reason the rows above escape a step name
	// and a failure sentence.
	command := "flow get " + ui.EscapeControl(workflowID)
	if runID != "" {
		command += " --run-id " + ui.EscapeControl(runID)
	}

	// One invariant tail under all three heads, and it does two jobs. It says
	// why the failure is missing rather than only that it is, and "no row of
	// its own here until the next attempt starts" is what tells a reader who
	// has just been offered --after-event-id that continuing the account will
	// not produce it either.
	return retryNoteProvenance + head +
		fmt.Sprintf("; a failed attempt has no row of its own here until the "+
			"next one starts, so `%s` is what reports it", command)
}

// retryNoteProvenance opens the footer with the moment its answer belongs to.
//
// "the run's present" is the vocabulary the rest of this CLI already uses for
// what `flow get` answers, and it is what the note about a failed check next
// door says too. "asked after the rows above" is the part that refuses a claim
// nothing here can make: these are two reads at two moments, and a sentence
// that read as the account's own conclusion would be asserting they were one.
const retryNoteProvenance = "the run's present, asked after the rows above: "

// renderTimeline writes the account for a person.
func renderTimeline(surface *ui.UI, resumed bool, msg *v1.GetTimelineResponse) {
	// Three different facts behind one empty answer, and the truncation flag is
	// what tells two of them apart.
	//
	// An empty first answer means the run has done nothing yet. An empty
	// *continuation* means the account ended at the cursor — which happens for
	// real, since an answer that exactly fills its entry ceiling is reported as
	// truncated whether or not anything follows it. And an empty answer that is
	// itself *truncated* is neither: it is the documented dead end, where the
	// scan budget was spent before reaching the cursor.
	//
	// The last of those is reported below with everything else truncation has
	// to say, so this stays quiet about it rather than announcing an end. An
	// earlier version returned here and skipped that entirely, which presented
	// an incomplete account as a complete one — the same defect this whole
	// paragraph exists to prevent, introduced while fixing its sibling (Codex,
	// #1119).
	if len(msg.GetEntries()) == 0 && !msg.GetTruncated() {
		if resumed {
			fmt.Fprintln(surface.Err, "no further entries: that was the end of this run's account")
		} else {
			fmt.Fprintln(surface.Err, "this run has recorded nothing yet")
		}
	}

	table := tabwriter.NewWriter(surface.Out, 0, 8, 2, ' ', 0)
	if len(msg.GetEntries()) > 0 {
		fmt.Fprintln(table, "TIME\tWHAT\tSTEP\tDETAIL")
	}

	for _, entry := range msg.GetEntries() {
		// Both of the last two columns carry text this process did not write —
		// a failure's message is the workload's, and a signal row's step is the
		// name whoever sent it chose. Handed to a tabwriter bare, a newline in
		// either fabricates rows that look like this command's own output, a
		// tab breaks the alignment a reader scans down, and an escape restyles
		// the terminal (Codex, #1119). The JSON form below keeps the value as
		// it is: a consumer parsing JSON is not a terminal interpreting bytes.
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\n",
			formatRunTime(entry.GetTime().AsTime(), entry.GetTime() != nil),
			timelineKindLabel(entry.GetKind()),
			ui.EscapeControl(entry.GetStep()),
			ui.EscapeControl(timelineDetail(entry)),
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
			// Both flags, always. An event id counts within one segment, so
			// resuming without naming the segment is refused — and it would be
			// wrong even if it were not, since an unnamed run resolves to
			// whichever is latest and a workload can continue as new between
			// two calls.
			fmt.Fprintf(surface.Err,
				"this is not the whole of this run's account — continue with "+
					"--run-id %s --after-event-id %d\n",
				msg.GetRunId(), entries[len(entries)-1].GetEventId())
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

// timelineDetail is the rightmost column: which try this row is about, and the
// failure's sentence when there was one.
//
// The two halves have different rules, and getting that wrong is how a column
// becomes noise or a fact goes missing.
//
// A row carrying a failure always names its attempt, because that is the whole
// reason to read a failure row: a step failing on attempt 1 and a step failing
// on attempt 5 are different situations, and the sentence is usually identical
// in both. Zero is the exception — a run-level failure is not an attempt at
// anything — so it prints the sentence alone.
//
// A row that succeeded names its attempt only past the first, because every
// scheduling now carries attempt 1 and "attempt 1" down the whole column says
// nothing. A run that never retried should read as one that never retried.
func timelineDetail(entry *v1.TimelineEntry) string {
	failure := entry.GetFailure()
	attempt := entry.GetAttempt()

	switch {
	case failure != "" && attempt > 0:
		return fmt.Sprintf("attempt %d: %s", attempt, failure)
	case failure != "":
		return failure
	case attempt > 1:
		return fmt.Sprintf("attempt %d", attempt)
	default:
		return ""
	}
}
