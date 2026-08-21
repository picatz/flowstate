package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/cmd/flow/internal/watch"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
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
// # What it shows, and where each part comes from
//
// `Get` fills Progress and PendingActivities for a running execution
// (`pkg/flowstate/v1/server/server.go`, the STATUS_RUNNING case), so every poll
// carries where the run is — the top-level step and the path into it — and whether
// an activity is on its fourth attempt and what the last one died of. Both are
// folded in by [watch.State.Absorb] and rendered by both shapes, through the same
// helpers `flow get` prints them with: [runPosition] and [pendingActivityLines]. A
// follow therefore says at least what a `flow get` says, continuously.
//
// The position is also part of what *changed* means. A run that spends four minutes
// moving between steps under one status would otherwise be four minutes of a screen
// nobody can distinguish from a frozen one — and in the line-per-change shape, four
// minutes of nothing printed at all. Status, run id, position, retry state and the
// steps that have produced outputs are each grounds for telling a reader something.
//
// What the position is not is a percentage. It is the step a run is on, because that
// is the fact the workflow can answer for itself; a run does not know how many steps
// remain any more than a program knows how many statements it has left. The elapsed
// time is the other half of the same job, and it belongs to the watch rather than to
// the run: it is what distinguishes a run that is working from a watch that has
// frozen even when nothing else on screen moves.
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
// exactly as `flow get` writes it, so `flow watch x | jq .steps` shows a live
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
//
// # What lives here versus in cmd/flow/internal/watch
//
// Split by #410: the state machine and the bubbletea model that draws the live
// view moved to internal/watch, so a test can drive either with a fake poller and
// nothing else — no cobra command, no client, no --output flag. What stays here is
// the transport ([clientPoller], which needs [serverFlags] and [newFollowClient]),
// the classification of a refusal against the connect code ([classifyPollError]),
// the cobra command and its flags, and the writing of the run document `--output`
// asked for ([reportChange], [finishWatch]) — none of which a state machine should
// need to know about to be tested.

// watchPoller is the run state this command renders, behind an interface so both
// shapes can be driven without a server. See [watch.Poller].
type watchPoller = watch.Poller

// watchState is the run as a watch has seen it. See [watch.State].
type watchState = watch.State

// watchProgress is what one poll means for a reader. See [watch.Progress].
type watchProgress = watch.Progress

// transientError marks a poll failure worth asking about again. See
// [watch.TransientError].
type transientError = watch.TransientError

// watchOption adjusts how a walk describes the run it is following. See
// [watch.Option].
type watchOption = watch.Option

// outageAllowance is how long the server may be unable to answer before a watch
// gives up on it. See [watch.OutageAllowance].
const outageAllowance = watch.OutageAllowance

// completedSteps lists the ids of steps that have produced outputs, in order. See
// [watch.CompletedSteps].
var completedSteps = watch.CompletedSteps

// watchDeps supplies internal/watch the rendering functions it borrows from this
// package rather than owning a second copy of — see [watch.Deps].
func watchDeps() watch.Deps {
	return watch.Deps{
		StatusTone:           statusTone,
		StatusLabel:          statusLabel,
		PositionPath:         positionPath,
		RunPosition:          runPosition,
		PendingActivityLines: pendingActivityLines,
		PendingWaitLines:     pendingWaitLines,
	}
}

// newWatchState begins a walk, optionally already knowing something about the run,
// supplying this package's [watch.Deps]. See [watch.NewState].
func newWatchState(workflowID string, known *v1.GetResponse, options ...watch.Option) *watchState {
	return watch.NewState(watchDeps(), workflowID, known, options...)
}

// namedRun says what to call a run in prose, for the verb that knows the workflow's
// own name. See [watch.Named].
var namedRun = watch.Named

// clientPoller polls the real service.
type clientPoller struct {
	workflowID string
	runID      string

	// server is carried rather than read at poll time, because a poller runs
	// after its command has returned control to the follow loop and has no
	// command to ask. Still needed alongside client: [classifyPollError]
	// reads it to explain a refusal (address, tenant) even though the client
	// itself has already been built.
	server serverFlags

	// client is the transport this poller asks through, built once by
	// [newFollowClient] before the follow loop starts rather than by Poll on
	// every tick — see that function for why. Left nil in tests that
	// construct a clientPoller directly and want the single-call default
	// instead; see [clientPoller.Poll].
	client flowstatev1connect.WorkflowServiceClient

	// spec is the specification this run actually executed, when this poller's
	// caller can show that it holds it. `flow run` parsed a file and submitted
	// it in this same process *and* the server attested that the run executes
	// that copy unchanged, so it passes it here and every declared-sensitive
	// output is redacted precisely — see [executedSpecification] for why the
	// attestation is load-bearing and holding the file is not enough. `flow
	// watch <id>` is a separate, later invocation that never had the file, and a
	// run whose specification the deployment substituted is a file that did not
	// run; both leave this nil, which is the fail-closed case
	// [redactGetResponse] documents: every declared output is withheld because
	// nothing here can say which ones the executed workflow marked.
	spec *v1.Workflow

	// reveal is --reveal-sensitive, read once by the command that built this
	// poller. The one thing that defeats both the precise and the fail-closed
	// path above.
	reveal bool
}

func (p clientPoller) Poll(ctx context.Context) (*v1.GetResponse, error) {
	request := &v1.GetRequest{WorkflowId: p.workflowID}
	if p.runID != "" {
		request.RunId = &p.runID
	}

	// A poller built by [newWatchCommand] or [runWorkflow] carries a client
	// built once, before the follow loop started — see [newFollowClient] for
	// why that matters. A poller built directly, the way this package's own
	// tests do for a single Poll, falls back to building one inline: correct
	// either way, and the difference only shows up over many polls, which
	// those tests do not make.
	client := p.client
	if client == nil {
		client = newWorkflowServiceClient(p.server)
	}

	response, err := client.Get(ctx, connect.NewRequest(request))
	if err != nil {
		return nil, classifyPollError(p.workflowID, p.server, err)
	}

	return redactGetResponse(response.Msg, p.spec, p.reveal), nil
}

// classifyPollError explains a refused poll and records whether it is worth another
// attempt.
//
// Classified here, where the connect code is still legible, and *before* the advice
// is added: refusedRun answers CodeNotFound with prose rather than a wrapped error,
// so the code cannot be recovered from what it returns. A watch that could not tell
// the difference would sit out its whole outage allowance on a mistyped id, saying
// nothing useful.
func classifyPollError(workflowID string, server serverFlags, err error) error {
	refused := refusedRun("reading", workflowID, server, err)
	if worthAskingAgain(connect.CodeOf(err)) {
		return watch.NewTransientError(refused)
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

	// Shared for the same reason the two flags above are: `flow run` and
	// `flow watch` follow a run through the same [clientPoller], so the escape
	// hatch that decides whether it redacts declared-sensitive outputs has to be
	// one flag rather than two that could drift apart.
	addRevealSensitiveFlag(cmd)
}

// newWatchCommand builds the watch sub-command.
func newWatchCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "watch [workflow-id]",
		Short: "Follow a run until it finishes",
		Long: "Follow a run until it finishes.\n\n" +
			"Where there is a terminal this draws a live view of the run, on stderr, so the " +
			"outputs it produced still go to stdout the way `flow get` writes them: one " +
			"invocation can show progress on a terminal and pipe its answer to jq. Where " +
			"there is not, it prints one line per change instead, so it is safe in a script " +
			"or a CI job. --output json or jsonl draws no view at all: json is the final " +
			"state as one document, jsonl is one document per change, which is a live event " +
			"stream a program can read as it arrives.\n\n" +
			"The exit code reports the run: 0 when it completed, non-zero when it failed, " +
			"was canceled, terminated, or timed out, so `flow watch` can gate a pipeline " +
			"without anything having to parse its output." + runDocumentHelp,
		Example: `# Follow a run on a terminal.
flow watch flowstate-workflow-3f7c

# Follow one attempt rather than whichever is current.
flow watch flowstate-workflow-3f7c --run-id 0198f1c4-8f0e-7d3a-9b21-6c1f4a2e5d77

# Live view on the terminal, the outputs into jq, from one invocation.
flow watch flowstate-workflow-3f7c | jq .steps

# As an event stream, for a script or an agent: one document per change.
flow watch flowstate-workflow-3f7c -o jsonl | jq -c '{status, steps: (.outputs.steps // {} | keys)}'

# Gate on the outcome; the exit code is the run's.
flow watch flowstate-workflow-3f7c >/dev/null && ./promote.sh`,
		Args:         cobra.ExactArgs(1),
		RunE:         runWatch,
		SilenceUsage: true,
	}

	addOutputFlag(cmd)
	addRawOutputFlag(cmd)
	addFollowFlags(cmd)

	cmd.Flags().String("run-id", "",
		"pin the watch to one run of the workload; unset follows whichever run is current")

	return cmd
}

func runWatch(cmd *cobra.Command, args []string) error {
	runID, _ := cmd.Flags().GetString("run-id")
	interval, _ := cmd.Flags().GetDuration("interval")
	plain, _ := cmd.Flags().GetBool("plain")

	rendering, err := resolveRunRendering(cmd)
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

	surface := newSurface(cmd)

	// `flow watch` asks about a run it did not start, by id alone — no spec to
	// consult, so [clientPoller] fails closed and withholds every declared
	// output unless told otherwise.
	reveal := revealSensitiveRequested(cmd)
	if reveal {
		noteRevealedSensitiveValues(surface)
	}

	server := serverFlagsOf(cmd)

	// Built once, before the follow loop starts, and reused for every poll —
	// see [newFollowClient]. An unknown --credential-source or a
	// github-actions source with no --audience is refused right here, before
	// a single poll happens, rather than surfacing thirty seconds into the
	// follow loop's outage allowance the way a transport-level error would.
	client, err := newFollowClient(server)
	if err != nil {
		return err
	}

	// Nothing known yet: `flow watch` is asked about a run it did not start, so the
	// first poll is the first thing it learns.
	return watchRun(cmd.Context(), surface, rendering,
		clientPoller{workflowID: workflowID, runID: runID, server: server, client: client, reveal: reveal},
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
	rendering runRendering,
	poller watchPoller,
	interval time.Duration,
	plain bool,
	workflowID string,
	known *v1.GetResponse,
	options ...watch.Option,
) error {
	// Both flags decide before the terminal does. A document was asked for
	// explicitly and a terminal was not, so drawing a view over somebody's requested
	// JSON would be this command guessing against a flag — which is the mistake
	// --output exists to prevent, and --plain is the same rule said out loud.
	if plain || rendering.WantsDocument() || !surface.ErrCaps.TTY {
		return followPlainly(ctx, surface, rendering, poller, interval, workflowID, known, options...)
	}

	return followLive(ctx, surface, rendering, poller, interval, workflowID, known, options...)
}

// followPlainly is the shape a script, a CI job, and a program each receive.
//
// One line per change rather than per poll, which is the whole difference from a
// `flow get` loop: a run that sits on the same step for four minutes produces four
// minutes of silence here and 120 identical lines there.
func followPlainly(
	ctx context.Context,
	surface *ui.UI,
	rendering runRendering,
	poller watchPoller,
	interval time.Duration,
	workflowID string,
	known *v1.GetResponse,
	options ...watch.Option,
) error {
	state := newWatchState(workflowID, known, options...)

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
			return interrupted(surface, rendering, state)
		}

		// The clock is read here rather than inside absorb, so the state machine is a
		// function of what it is handed. Read after the poll returns, because what the
		// allowance measures is how long the server has been *observed* unable to
		// answer, and an answer that took twenty seconds to fail was twenty seconds
		// nobody knew about.
		progress := state.Absorb(time.Now(), response, err)

		if progress.Changed {
			if err := reportChange(surface, rendering, state, response); err != nil {
				return err
			}
		}

		if progress.Done {
			return finishWatch(surface, rendering, state)
		}

		select {
		case <-ctx.Done():
			return interrupted(surface, rendering, state)
		case <-ticker.C:
		}
	}
}

// newWatchModel builds the live view, supplying this package's [watch.Deps].
func newWatchModel(
	ctx context.Context,
	surface *ui.UI,
	poller watchPoller,
	interval time.Duration,
	workflowID string,
	known *v1.GetResponse,
	options ...watch.Option,
) watch.Model {
	return watch.NewModel(ctx, surface, watchDeps(), poller, interval, workflowID, known, options...)
}

// followLive draws a run until it finishes, through [watch.Run], and then writes
// whatever the shape owes a reader at the end from the model it comes back with.
func followLive(
	ctx context.Context,
	surface *ui.UI,
	rendering runRendering,
	poller watchPoller,
	interval time.Duration,
	workflowID string,
	known *v1.GetResponse,
	options ...watch.Option,
) error {
	model, err := watch.Run(ctx, surface, watchDeps(), poller, interval, workflowID, known, options...)
	if err != nil {
		return err
	}

	return watchEnding(surface, rendering, model)
}

// watchEnding is what a finished live view owes its caller.
//
// Separate from followLive because it is the whole of what the live shape decides
// for itself, and the only part of it a test can reach without a terminal to draw
// on.
func watchEnding(surface *ui.UI, rendering runRendering, model watch.Model) error {
	if model.Quit() {
		// Watching stopped before the run did, so there is no outcome to report and
		// nothing to write to stdout: the run has not produced its outputs yet.
		return nil
	}

	state := model.State()

	// What the live view drew is transient: bubbletea erases its last frame on the
	// way out, so a durable run watched on a terminal used to end having said
	// nothing that stayed on screen about how it ended. `flow run local` says
	// `COMPLETED workflow <name>` and stops, and a person moving between the two
	// drivers should not find that the durable one is the one that goes quiet
	// (picatz/flowstate#544).
	//
	// The same sentence the plain shape writes, from the same state, rather than a
	// second rendering of the outcome: [watch.State.Line] is the one place a run's
	// status becomes prose. Terminal statuses only — a walk that gave up knows
	// nothing final about the run, and finishWatch answers that with the error it
	// stopped on rather than with a line claiming the last thing it happened to see.
	//
	// This is the *first* thing this shape writes that stays, so it is where every
	// run id the walk saw comes due — including each continue-as-new handover's,
	// which the erased frames were the only place to have shown. `flow get --run-id`
	// and `flow watch --run-id` both take one, so an attempt whose id never reached
	// the reader is an attempt they cannot ask about. See [watch.State.Line].
	if rendering.format == FormatText && !state.GaveUp() && watch.TerminalStatus(state.Status()) {
		fmt.Fprintln(surface.Err, state.Line(surface.ErrTheme))
	}

	// Otherwise the same ending the plain shape reaches, from the same state, so the
	// outputs on stdout and the exit code are identical whichever shape drew the
	// progress. A TUI that reports differently from a pipe is two commands.
	return finishWatch(surface, rendering, state)
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
//
// rendering.WantsDocument(), not a bare `format == FormatJSON`, is what decides
// whether this owes a document at all: `--raw` on the default text format asked
// for one explicitly, through [runRendering.WantsDocument], exactly as
// followLive/followPlainly's own routing decision already honours it — an
// interrupted raw watch owes its reader the same last-known-state document a
// `-o json` watch does, not the silence the text shape gets when nothing was
// asked for.
func interrupted(surface *ui.UI, rendering runRendering, state *watchState) error {
	if rendering.format == FormatJSONL || !rendering.WantsDocument() || state.Response() == nil {
		// jsonl has already emitted every change including the last, the text
		// shapes have said what they knew on stderr as they went, and nothing
		// asked for a document owes nobody one.
		return nil
	}

	return writeRunJSON(surface, rendering, state.Response())
}

// reportChange writes one change, in the shape the format asks for.
//
// response is this poll's own answer, not state.Response(): the two agree after a
// successful poll, but a transient refusal is a change too — the outage itself,
// [State.Absorb]'s reasoning for why it must not go unreported — and that poll's
// response is nil. Reading state.Response() there would resend the last answer the
// server actually gave, stale and a second time, as though the server had just
// repeated itself; a caller reading the event stream for what changed would see a
// duplicate transition rather than the "nothing new, the server went quiet" this
// change means. Passed the raw poll response keeps this shape doing the one thing
// the event stream promises: one document per change, in the change's own words.
func reportChange(surface *ui.UI, rendering runRendering, state *watchState, response *v1.GetResponse) error {
	switch rendering.format {
	case FormatJSONL:
		// The server's own message, so a reader is indexing documented fields
		// rather than a shape invented here for the occasion.
		return writeRunJSON(surface, rendering, response)

	case FormatJSON:
		// One document per invocation, so nothing is written until the last change
		// is known.
		return nil

	default:
		// stderr, because this is the account of the run and not its answer: the
		// answer is the outputs, and it goes to stdout when the run has produced
		// it. A pipe therefore receives the outputs alone.
		_, err := fmt.Fprintln(surface.Err, state.Line(surface.ErrTheme))

		return err
	}
}

// finishWatch writes whatever the shape owes a reader at the end, then reports the
// run's outcome as this command's exit status.
func finishWatch(surface *ui.UI, rendering runRendering, state *watchState) error {
	// A watch that gave up knows nothing final about the run, so it writes nothing
	// final about it. Emitting the last state it happened to see as though it were
	// the answer is how a program concludes a run is still RUNNING for good.
	if state.GaveUp() {
		return state.LastError()
	}

	// The line-per-change shape has already written every change including the last,
	// so there is nothing final left to say. Every other shape owes its reader one
	// document, and [writeRun] is the one that writes it — the same function
	// `flow run local` finishes through, which is what keeps a caller reading
	// `.outputs.stepValues` reading the same field from both drivers.
	if rendering.format != FormatJSONL {
		if err := writeRun(surface, rendering, state.Response()); err != nil {
			return err
		}
	}

	return outcomeError(state.Status(), state.WorkflowID(), state.Failure())
}

// outcomeError turns a finished run's status into this command's exit code.
//
// A watch that exits 0 on a failed run is a watch nobody can gate a pipeline on,
// which is most of the reason to have the plain shape at all. The message names the
// status rather than restating it as a failure, because "terminated" and "timed
// out" are different things to go and look at.
func outcomeError(status v1.RunResponse_Status, workflowID, failure string) error {
	if !watch.TerminalStatus(status) {
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
