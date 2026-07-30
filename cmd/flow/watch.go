package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Watching a run is the one place this CLI has a reason to hold the terminal.
//
// Every other command answers a question and exits, which is what makes them
// scriptable. A workload is different: it takes as long as the work takes, and
// `flow get` in a shell loop is what people do instead. That is worse in ways worth
// naming — it reprints the whole answer, so the eye has to diff two screens to find
// what changed; it cannot say "nothing has changed yet" without saying it again; and
// it cannot stop by itself when the run stops.
//
// # What it can actually show, which is less than it should be
//
// Not which step the run is on. `Get` answers a running execution with three
// scalars — the two ids and the status — and reports outputs only once the run has
// finished, so there is no per-step progress to display while it matters. See
// `pkg/flowstate/v1/server/server.go`, the STATUS_RUNNING case.
//
// So what a follow reports while a run is going is the status, how long it has been
// watched, and — the moment the run ends — the steps that produced outputs. That is
// worth having: the elapsed time is what distinguishes a run that is working from a
// watch that has frozen, the exit is automatic, and the step list is a summary on the
// final frame. It is not the live step-by-step this ought to be, and the missing piece
// is the server's, not this file's. Anything here that renders steps is written to be
// right the day the server can answer, and is exercised meanwhile by tests that supply
// a response shape the server does not yet produce — which is stated rather than left
// for somebody to discover.
//
// # It must not become the only way to watch
//
// A TUI that a script inherits is a script that hangs. So the terminal is a
// *capability* here, not an assumption: where there is one, this draws a live view;
// where there is not, it prints one line per change and exits when the run
// finishes. Same command, same information, both shapes useful.
//
// The live view draws to *stderr*, which is what makes the two shapes compose
// rather than compete. The answer — the outputs the run produced — goes to stdout
// exactly as `flow get` writes it, so `flow watch x | jq .stepValues` shows a live
// view on the terminal and pipes the answer to jq from one invocation. --output
// json or jsonl suppresses the view entirely, because somebody who asked for a
// document asked for a document.
//
// # Polling, and why that is not a compromise
//
// There is no streaming RPC to subscribe to, so this asks `Get` on an interval.
// That is not obviously worse for this job: a run's interesting transitions are
// step boundaries, which are seconds to minutes apart, and a poll that coalesces
// several of them into one redraw is showing the same information with less
// flicker. What polling must not do is hammer the server, so the interval is
// bounded below and the request is the same one `flow get` makes.

// watchPoller is the run state this command renders, behind an interface so both
// shapes can be driven without a server.
//
// The parts most likely to be wrong are the ones a fake can exercise: an off-by-one
// in a step list, a terminal status that does not stop the loop, a transient error
// that ends a watch it should have survived. What a fake cannot tell us is whether
// `Get` returns what we think, which is why the wiring below issues the same
// request `flow get` does rather than a second opinion about it.
type watchPoller interface {
	Poll(ctx context.Context) (*v1.GetResponse, error)
}

// clientPoller polls the real service.
type clientPoller struct {
	workflowID string
	runID      string
}

func (p clientPoller) Poll(ctx context.Context) (*v1.GetResponse, error) {
	request := &v1.GetRequest{WorkflowId: p.workflowID}
	if p.runID != "" {
		request.RunId = &p.runID
	}

	response, err := newWorkflowServiceClient().Get(ctx, connect.NewRequest(request))
	if err != nil {
		return nil, classifyPollError(p.workflowID, err)
	}

	return response.Msg, nil
}

// transientError marks a poll failure worth asking about again.
type transientError struct{ error }

// classifyPollError explains a refused poll and records whether it is worth another
// attempt.
//
// Classified here, where the connect code is still legible, and *before* the advice
// is added: refusedRun answers CodeNotFound with prose rather than a wrapped error,
// so the code cannot be recovered from what it returns. A watch that could not tell
// the difference would sit out its whole outage allowance on a mistyped id, saying
// nothing useful.
func classifyPollError(workflowID string, err error) error {
	refused := refusedRun("reading", workflowID, err)
	if worthAskingAgain(connect.CodeOf(err)) {
		return transientError{refused}
	}

	return refused
}

// worthAskingAgain reports whether a refusal is one a second later might not be.
//
// The split is between a server that is briefly unable to answer and a request
// that will be refused however many times it is made. Retrying the second kind
// turns a clear refusal into a pause followed by the same refusal, so the default
// here is deliberately *not* to retry: a code that has not been thought about is
// reported, which is the outcome that gets it thought about.
func worthAskingAgain(code connect.Code) bool {
	switch code {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded,
		connect.CodeResourceExhausted, connect.CodeAborted,
		connect.CodeInternal, connect.CodeUnknown:
		return true
	default:
		// CodeNotFound is a mistyped id or a run outside this tenant.
		// CodeUnauthenticated and CodePermissionDenied are credentials that will
		// not become acceptable by being presented again a second later — and the
		// token file is re-read per request, so a rotation is already picked up
		// without anything here retrying.
		return false
	}
}

const (
	// minWatchInterval bounds how often the server is asked.
	//
	// A person cannot read faster than this and a server should not be asked to
	// answer faster on the strength of somebody typing a smaller number. The flag
	// is clamped rather than rejected: refusing `--interval 10ms` teaches nothing,
	// and quietly asking a server twenty times a second is the outcome that
	// matters.
	minWatchInterval = 250 * time.Millisecond

	// defaultWatchInterval is slow enough to be unnoticeable server-side and fast
	// enough that a step boundary appears while the eye is still on the screen.
	defaultWatchInterval = time.Second

	// outageAllowance is how long the server may be unable to answer before the
	// watch gives up on it.
	//
	// Some allowance is not indulgence: the reason to watch rather than to loop
	// `flow get` is that a watch lasts as long as the run, and over an hour a
	// server restart or a dropped connection is close to certain. A watch that dies
	// on the first one sends people back to the shell loop, which retries by
	// construction.
	//
	// Measured as elapsed time, from the clock, and not as a number of attempts.
	// Attempts were the first attempt at this and they were wrong twice over. They
	// are only equal to time if every attempt returns promptly — so an interval of
	// ten seconds gave up after twenty while reporting thirty, and a server that
	// accepted a connection and then said nothing produced no attempt at all, which
	// left the allowance never starting and the watch hanging until somebody killed
	// it. Whenever a bound is stated in one unit and enforced in another, the
	// difference is where the peer gets to live.
	outageAllowance = 30 * time.Second
)

// addFollowFlags declares the flags of a command that follows a run.
//
// Shared between `flow watch` and `flow run`, which follow the same way for the same
// reasons — and which would otherwise drift into two spellings of one idea, the way
// `--address` did before it was declared from one list.
func addFollowFlags(cmd *cobra.Command) {
	cmd.Flags().Duration("interval", defaultWatchInterval,
		"how often to ask the server, clamped to a floor of "+minWatchInterval.String())

	// The escape hatch that makes following a terminal safe.
	//
	// Detecting a terminal is right for the common case and wrong for somebody who
	// wants a scrollable transcript, is driving this under `script(1)`, or is reading
	// with a screen reader that a repainting view fights. Without a way to ask, a
	// terminal is a trap — and the answer "pipe it to cat" is a worse interface than
	// a flag.
	cmd.Flags().Bool("plain", false,
		"print one line per change instead of drawing a live view, even on a terminal")
}

// newWatchCommand builds the watch sub-command.
func newWatchCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "watch [workflow-id]",
		Short: "Follow a run until it finishes",
		Long: "Follow a run until it finishes.\n\n" +
			"Where there is a terminal this draws a live view of the run, on stderr, so the " +
			"outputs it produced still go to stdout the way `flow get` writes them — one " +
			"invocation can show progress on a terminal and pipe its answer to jq. Where " +
			"there is not, it prints one line per change instead, so it is safe in a script " +
			"or a CI job. --output json or jsonl draws no view at all: json is the final " +
			"state as one document, jsonl is one document per change, which is a live event " +
			"stream a program can read as it arrives.\n\n" +
			"The exit code reports the run: 0 when it completed, non-zero when it failed, " +
			"was canceled, terminated, or timed out — so `flow watch` can gate a pipeline " +
			"without anything having to parse its output.",
		Example: `# Follow a run on a terminal.
flow watch flowstate-workflow-3f7c

# Follow one attempt rather than whichever is current.
flow watch flowstate-workflow-3f7c --run-id 0198f1c4-8f0e-7d3a-9b21-6c1f4a2e5d77

# Live view on the terminal, the outputs into jq, from one invocation.
flow watch flowstate-workflow-3f7c | jq .stepValues

# As an event stream, for a script or an agent: one document per change.
flow watch flowstate-workflow-3f7c -o jsonl | jq -c '{status, steps: (.outputs.stepValues // {} | keys)}'

# Gate on the outcome; the exit code is the run's.
flow watch flowstate-workflow-3f7c >/dev/null && ./promote.sh`,
		Args:         cobra.ExactArgs(1),
		RunE:         runWatch,
		SilenceUsage: true,
	}

	addOutputFlag(cmd)
	addFollowFlags(cmd)

	cmd.Flags().String("run-id", "",
		"pin the watch to one run of the workload; unset follows whichever run is current")

	return cmd
}

func runWatch(cmd *cobra.Command, args []string) error {
	runID, _ := cmd.Flags().GetString("run-id")
	interval, _ := cmd.Flags().GetDuration("interval")
	plain, _ := cmd.Flags().GetBool("plain")

	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	workflowID := args[0]

	// Validated before anything is watched, so a malformed --run-id is refused in
	// the same breath as `flow get` refuses it rather than on the first poll,
	// several hundred milliseconds into a live view.
	request := &v1.GetRequest{WorkflowId: workflowID}
	if runID != "" {
		request.RunId = &runID
	}
	if err := v1.Validate(request); err != nil {
		return fmt.Errorf("%w\n  a run id is the UUID Temporal gave one attempt at the workload; "+
			"omit it to follow whichever attempt is current", err)
	}

	// Nothing known yet: `flow watch` is asked about a run it did not start, so the
	// first poll is the first thing it learns.
	return watchRun(cmd.Context(), newSurface(cmd), format,
		clientPoller{workflowID: workflowID, runID: runID},
		clampWatchInterval(interval), plain, workflowID, nil)
}

// clampWatchInterval enforces the floor.
func clampWatchInterval(interval time.Duration) time.Duration {
	return max(interval, minWatchInterval)
}

// watchRun follows a run to a terminal state, in whichever shape the surface and
// the requested format can carry.
//
// Split from the command so every shape is reachable from a test without a terminal
// or a server, and so the choice between them is one visible branch rather than a
// condition buried in a render path.
func watchRun(
	ctx context.Context,
	surface *ui.UI,
	format OutputFormat,
	poller watchPoller,
	interval time.Duration,
	plain bool,
	workflowID string,
	known *v1.GetResponse,
) error {
	// Both flags decide before the terminal does. A document was asked for
	// explicitly and a terminal was not, so drawing a view over somebody's requested
	// JSON would be this command guessing against a flag — which is the mistake
	// --output exists to prevent, and --plain is the same rule said out loud.
	if plain || format.Machine() || !surface.ErrCaps.TTY {
		return followPlainly(ctx, surface, format, poller, interval, workflowID, known)
	}

	return followLive(ctx, surface, poller, interval, workflowID, known)
}

// followPlainly is the shape a script, a CI job, and a program each receive.
//
// One line per change rather than per poll, which is the whole difference from a
// `flow get` loop: a run that sits on the same step for four minutes produces four
// minutes of silence here and 120 identical lines there.
func followPlainly(
	ctx context.Context,
	surface *ui.UI,
	format OutputFormat,
	poller watchPoller,
	interval time.Duration,
	workflowID string,
	known *v1.GetResponse,
) error {
	state := newWatchState(workflowID, known)

	// A ticker rather than a sleep so the interval is the period between requests
	// rather than the period plus however long the server took to answer — which is
	// the difference between an interval and a lower bound on one.
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		response, err := poller.Poll(ctx)

		// Checked before the answer is folded in, because a poll cut short by ctrl+c
		// fails with a cancelled context — and connect reports that as a refusal like
		// any other, which absorb would treat as the server having stopped answering.
		// The exit code would then say a run failed when what actually happened is
		// that somebody stopped watching it.
		if ctx.Err() != nil {
			return interrupted(surface, format, state)
		}

		// The clock is read here rather than inside absorb, so the state machine is a
		// function of what it is handed. Read after the poll returns, because what the
		// allowance measures is how long the server has been *observed* unable to
		// answer, and an answer that took twenty seconds to fail was twenty seconds
		// nobody knew about.
		progress := state.absorb(time.Now(), response, err)

		if progress.Changed {
			if err := reportChange(surface, format, state, response); err != nil {
				return err
			}
		}

		if progress.Done {
			return finishWatch(surface, format, state)
		}

		select {
		case <-ctx.Done():
			return interrupted(surface, format, state)
		case <-ticker.C:
		}
	}
}

// interrupted ends a follow that stopped before the run did.
//
// Nothing about the run went wrong — this is ctrl+c, or a CI job being stopped — so it
// is not an error and the exit status stays zero. Reporting the run as failed because
// watching it was interrupted would be a lie a pipeline would act on.
//
// The single-document form still owes its reader a document, and this is the case that
// makes it matter rather than a nicety. `flow run -o json` starts a durable workload
// and then follows it; interrupted before the first change, it used to write nothing
// at all, leaving a caller with no machine-readable name for a run that is still
// going — unwatchable, uncancellable, and unterminatable except by hand. What it gets
// now is the last state known, which is at worst the run as it was started.
func interrupted(surface *ui.UI, format OutputFormat, state *watchState) error {
	if format != FormatJSON || state.response == nil {
		// jsonl has already emitted every change including the last, and the text
		// shapes have said what they knew on stderr as they went.
		return nil
	}

	return writeJSON(surface, format, state.response)
}

// reportChange writes one change, in the shape the format asks for.
func reportChange(surface *ui.UI, format OutputFormat, state *watchState, response *v1.GetResponse) error {
	switch format {
	case FormatJSONL:
		// The server's own message, so a reader is indexing documented fields
		// rather than a shape invented here for the occasion.
		return writeJSON(surface, format, response)

	case FormatJSON:
		// One document per invocation, so nothing is written until the last change
		// is known.
		return nil

	default:
		// stderr, because this is the account of the run and not its answer: the
		// answer is the outputs, and it goes to stdout when the run has produced
		// it. A pipe therefore receives the outputs alone.
		_, err := fmt.Fprintln(surface.Err, state.line(surface.ErrTheme))

		return err
	}
}

// finishWatch writes whatever the shape owes a reader at the end, then reports the
// run's outcome as this command's exit status.
func finishWatch(surface *ui.UI, format OutputFormat, state *watchState) error {
	// A watch that gave up knows nothing final about the run, so it writes nothing
	// final about it. Emitting the last state it happened to see as though it were
	// the answer is how a program concludes a run is still RUNNING for good.
	if state.gaveUp {
		return state.lastError
	}

	switch format {
	case FormatJSON:
		if err := writeJSON(surface, format, state.response); err != nil {
			return err
		}

	case FormatJSONL:
		// Every change was written as it happened, including the last.

	default:
		// The same bytes `flow get` writes to stdout, for the same reason: a run
		// that produced outputs has an answer, and the answer belongs on the
		// stream a pipe reads.
		if err := writeStepOutputs(surface, state.response); err != nil {
			return err
		}
	}

	return outcomeError(state.status, state.workflowID, state.failure)
}

// writeStepOutputs writes a finished run's outputs, and nothing at all when it has
// none.
//
// Nothing rather than an empty document, because a failed run produced no outputs
// and `{}` would claim it produced none *successfully* — a distinction a shell
// reader has only the exit code to recover.
func writeStepOutputs(surface *ui.UI, response *v1.GetResponse) error {
	outputs := response.GetOutputs()
	if outputs == nil {
		return nil
	}

	encoded, err := marshalJSON(outputs, false)
	if err != nil {
		return fmt.Errorf("formatting the outputs of %s: %w", response.GetWorkflowId(), err)
	}

	_, err = fmt.Fprintf(surface.Out, "%s\n", encoded)

	return err
}

// watchState is the run as the watch has seen it, and the decision of when to stop.
//
// One state machine, folded into by both shapes, because "has anything changed",
// "is this over", and "has the server been quiet too long" are three questions that
// must not get two answers. The live view and the plain lines then differ only in
// how they render it — which is what keeps a bug fixed in one from surviving in the
// other.
type watchState struct {
	workflowID string

	runID  string
	status v1.RunResponse_Status

	// steps are the ids of steps that have produced outputs, sorted.
	steps []string

	// failure is a failed run's message, empty until there is one.
	failure string

	// response is the last answer the server gave, kept for the shapes that emit
	// the server's own message rather than prose about it.
	response *v1.GetResponse

	// outageSince is when the server was first observed unable to answer, zero once
	// it answers again.
	//
	// A time rather than a count, because the allowance is stated in seconds and has
	// to be enforced in seconds. See [outageAllowance] for the two ways a count got
	// it wrong.
	outageSince time.Time

	// lastError is the most recent failure, so a live view can say why nothing is
	// moving instead of appearing to have frozen.
	lastError error

	// gaveUp records that lastError is why the walk ended rather than something it
	// survived, which is what the live shape has to know: a bubbletea program that
	// quits carries no error out, so the reason travels on the state.
	gaveUp bool
}

// newWatchState begins a walk, optionally already knowing something about the run.
//
// `flow run` knows the run exists and what its ids are before it starts following,
// and seeding that matters for a reason that is not cosmetic: a machine-readable
// caller interrupted before the first poll would otherwise be given nothing at all,
// while a durable workload it can no longer name goes on running. See [finishWatch].
func newWatchState(workflowID string, known *v1.GetResponse) *watchState {
	state := &watchState{workflowID: workflowID}
	if known != nil {
		state.response = known
		state.runID = known.GetRunId()
		state.status = known.GetStatus()
	}

	return state
}

// watchProgress is what one poll means for a reader.
type watchProgress struct {
	// Changed reports that a reader has something new to be told. False for a poll
	// that found the run exactly where it was, which is most of them.
	Changed bool

	// Done reports that the walk is over.
	Done bool

	// Err is why it ended, when it ended badly. A Done with no Err is the run
	// having reached a terminal status, which is success at watching whatever the
	// run itself did.
	Err error
}

// absorb folds one poll result into the state.
//
// at is when the result was observed, taken by the caller rather than read here. Both
// shapes already have it — the plain loop reads the clock, the live view has the time
// on the tick that scheduled the poll — and taking it makes the whole state machine a
// function of its inputs, so a test can state exactly when it should give up rather
// than wait to find out.
func (s *watchState) absorb(at time.Time, response *v1.GetResponse, err error) watchProgress {
	if err != nil {
		return s.absorbError(at, err)
	}

	// A status the schema forbids is a peer that is not answering the question.
	// Treating it as "still running" would wait forever on a server that will never
	// say otherwise, and the whole point of a status is that it distinguishes those.
	if response.GetStatus() == v1.RunResponse_STATUS_UNSPECIFIED {
		return s.stop(fmt.Errorf(
			"the server reported no status for run %q, which is a status the schema does not permit; "+
				"ask `flow get %s` and report it if it persists", s.workflowID, s.workflowID))
	}

	recovered := !s.outageSince.IsZero()
	s.outageSince, s.lastError = time.Time{}, nil

	steps := completedSteps(response)

	// No separate "is this the first answer" flag, and the reason is the guard above
	// rather than an oversight: the zero value of status is UNSPECIFIED, and an
	// UNSPECIFIED answer never reaches here, so the first answer to get this far
	// always has a status differing from the one held. A flag saying the same thing
	// would be a field whose value can never change the outcome — which is a thing
	// that reads as load-bearing and is not, and which no test can hold honest.
	changed := recovered ||
		s.status != response.GetStatus() ||
		s.runID != response.GetRunId() ||
		!slices.Equal(s.steps, steps)

	s.response = response
	s.runID = response.GetRunId()
	s.status = response.GetStatus()
	s.steps = steps
	if failure := response.GetError(); failure != nil {
		s.failure = failure.GetMessage()
	}

	return watchProgress{Changed: changed, Done: terminalStatus(s.status)}
}

// absorbError folds a refused poll in, deciding whether to keep asking.
func (s *watchState) absorbError(at time.Time, err error) watchProgress {
	var transient transientError
	if !errors.As(err, &transient) {
		return s.stop(err)
	}

	first := s.outageSince.IsZero()
	if first {
		s.outageSince = at
	}
	s.lastError = err

	// The measured elapsed time, and the allowance therefore always gets its full
	// span whatever the interval and however long a request took to fail. A first
	// failure never ends a watch, because no time has passed since itself — so there
	// is always a second attempt, without a floor on attempts to say so.
	if elapsed := at.Sub(s.outageSince); elapsed >= outageAllowance {
		return s.stop(fmt.Errorf(
			"gave up watching %q after %s of the server being unable to answer: %w",
			s.workflowID, elapsed.Round(time.Second), err))
	}

	// A change, so the reader is told the server went quiet rather than watching a
	// still screen and guessing. Only the first one: an outage that persists is not
	// news each second. The recovery is a change too, which is why absorb reports
	// one when the outage ends.
	return watchProgress{Changed: first}
}

// stop records why the walk ended and reports it.
//
// Written onto the state as well as returned because the two shapes collect it
// differently: the plain loop reads the return value, and a bubbletea program that
// quits carries nothing out but its model. One assignment here is what keeps those
// from disagreeing about whether a watch ended well.
func (s *watchState) stop(err error) watchProgress {
	s.lastError, s.gaveUp = err, true

	return watchProgress{Done: true, Err: err}
}

// line renders the state as one line of prose, for the shape that prints a line per
// change.
func (s *watchState) line(theme ui.Theme) string {
	if s.lastError != nil {
		return fmt.Sprintf("%s %s", theme.Pill(ui.ToneWarning, "unreachable"), s.lastError)
	}

	line := fmt.Sprintf("%s workflow %s run %s",
		theme.Pill(statusTone(s.status), statusLabel(s.status)), s.workflowID, s.runID)

	if len(s.steps) > 0 {
		line += fmt.Sprintf(" after %s", strings.Join(s.steps, ", "))
	}
	if s.failure != "" {
		line += fmt.Sprintf(": %s", s.failure)
	}

	return line
}

// terminalStatus reports whether a run has stopped moving.
//
// UNSPECIFIED is deliberately absent: it is not a run in progress, it is a server
// that has not answered the question, and absorb refuses it rather than waiting on
// it.
func terminalStatus(status v1.RunResponse_Status) bool {
	switch status {
	case v1.RunResponse_STATUS_COMPLETED, v1.RunResponse_STATUS_FAILED,
		v1.RunResponse_STATUS_CANCELED, v1.RunResponse_STATUS_TERMINATED,
		v1.RunResponse_STATUS_TIMED_OUT:
		return true
	default:
		return false
	}
}

// outcomeError turns a finished run's status into this command's exit code.
//
// A watch that exits 0 on a failed run is a watch nobody can gate a pipeline on,
// which is most of the reason to have the plain shape at all. The message names the
// status rather than restating it as a failure, because "terminated" and "timed
// out" are different things to go and look at.
func outcomeError(status v1.RunResponse_Status, workflowID, failure string) error {
	if !terminalStatus(status) {
		// Watching stopped before the run did — a cancelled context, or a person
		// pressing q. Not the run's outcome, so not an error about the run.
		return nil
	}

	if status == v1.RunResponse_STATUS_COMPLETED {
		return nil
	}

	// Appended only when it says something the status has not.
	//
	// The guard used to be a workaround: the server answered a terminal run's failure
	// message with the status name itself, so appending it unguarded read
	// `run "x" failed: STATUS_FAILED` — a sentence restating its own subject while
	// looking like the reason had been retrieved. The server reads the run's actual
	// failure now, so the ordinary case says something.
	//
	// The check stays, because the fallback stays: a terminal run whose error cannot
	// be read is still answered with its status, which is the honest answer and the
	// one this must not print twice.
	if failure != "" && !restatesStatus(failure, status) {
		return fmt.Errorf("run %q %s: %s", workflowID, statusWord(status), failure)
	}

	return fmt.Errorf("run %q %s", workflowID, statusWord(status))
}

// restatesStatus reports whether a failure message only names the status again.
func restatesStatus(failure string, status v1.RunResponse_Status) bool {
	failure = strings.TrimSpace(failure)

	return strings.EqualFold(failure, status.String()) ||
		strings.EqualFold(failure, statusLabel(status)) ||
		strings.EqualFold(failure, statusWord(status))
}

// statusWord renders a status the way prose wants it.
//
// statusLabel is the same value shouted, which is right for a column and wrong
// mid-sentence: "run x TIMED_OUT" reads as a different vocabulary from every other
// sentence this CLI writes.
func statusWord(status v1.RunResponse_Status) string {
	return strings.ToLower(strings.ReplaceAll(statusLabel(status), "_", " "))
}

// completedSteps lists the ids of steps that have produced outputs, in order.
//
// Sorted because the outputs arrive in a protobuf map, which has no iteration
// order — an unsorted list would reshuffle itself on every redraw and read as
// though the run were going backwards.
func completedSteps(response *v1.GetResponse) []string {
	values := response.GetOutputs().GetStepValues()
	if len(values) == 0 {
		return nil
	}

	ids := make([]string, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	return ids
}
