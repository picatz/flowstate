package main

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/cmd/flow/internal/debugtui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// `flow debug attach`, `flow debug get` and `flow debug do`: the terminal's
// door to a durable run's debug session, over the server's debug RPCs.
//
// The vocabulary is the prompt's, spoken by [flowdebug.Driver], so a person who
// debugged a local run knows these commands already. The session lives in the
// run: this process holds a session id and a heartbeat, and a second process
// can rejoin with `--session`.

const debugAttachLong = `Attach a debugger to a durable run, hold it at its next step boundary, and
drive it: step in, over and out of calls, run until a step, set conditional
breakpoints, and evaluate read-only CEL against the held scope.

The run's workflow must declare ` + "`debug:`" + ` naming you, and your credentials must
carry workload.debug (and workload.debug_inspect to evaluate). A hold parks
workflow code at a step boundary only: work already dispatched keeps running,
and so does time. The session is leased: this command renews it while it runs,
and a session nobody renews lapses and the run resumes on its own.

At a terminal of at least 60x12 the debugger is a full-screen view with keyboard and
mouse. --tui=false keeps the line editor. The view is never used under --script, with a
piped stdin or stdout, with a machine --output, in CI (the CI environment variable) or
with TERM=dumb: there the commands are read from the input and answered as text, exactly
as before.

Commands are read from the terminal, or from --script. Leaving with ` + "`detach`" + `, or
at the end of input, releases the run; ` + "`disconnect`" + ` leaves the session attached
for a later ` + "`flow debug attach --session <id>`" + `.

With --history --run-id the run is not held at all: the debugger opens its record, every
point of it reachable both ways with next, back, goto and the timeline, whether the run
is still going or closed. Nothing runs and no session is taken; what it shows is
reconstructed from the history, a name of the scope is marked rec, an expression typed or
watched is marked hyp (computed now, never held by the run), and a value that cannot be
known is marked n/a. until, break, pause and the other verbs that need a run executing are
refused by name.`

const debugAttachExample = `# Attach, stop at the next step, and drive it interactively:
flow debug attach order-1234

# The same, as a reproducible script, with each answer as a JSON line:
flow debug attach order-1234 --script debug.txt -o jsonl

# Rejoin a session another process left attached:
flow debug attach order-1234 --session 5d3f…

# Walk a closed run's record, both ways, in the full-screen debugger:
flow debug attach order-1234 --history --run-id 5d3f…

# The line editor instead of the full-screen debugger:
flow debug attach order-1234 --tui=false`

func addDebugRemoteCommands(debugCmd *cobra.Command) {
	attachCmd := &cobra.Command{
		Use:     "attach <workflow-id>",
		Short:   "Attach a debugger to a durable run and drive it",
		Long:    debugAttachLong,
		Args:    cobra.ExactArgs(1),
		RunE:    runDebugAttach,
		Example: debugAttachExample,
	}
	addServerFlags(attachCmd)
	addOutputFlag(attachCmd)
	attachCmd.Flags().String("run-id", "", "pin the run, as the first run id of its chain; unset follows the current one")
	addRecordFlag(attachCmd)
	attachCmd.Flags().String("session", "", "rejoin this session instead of attaching a new one")
	attachCmd.Flags().Duration("lease", 2*time.Minute, "how long each renewal holds the session; the engine bounds it")
	attachCmd.Flags().String("script", "", "read commands from this file instead of the terminal")
	attachCmd.Flags().String("program", "", "the Flowfile the run was started from, for source lines; used only if it compiles to the program the run executes, "+
		"the deployment's plugin and task pins aside")
	attachCmd.Flags().Duration("wait", time.Minute, "how long a movement waits for the next stop before reporting the run still running")
	attachCmd.Flags().Bool("history", false, "walk the recorded run named by --run-id instead of holding it: every point is reachable both ways, "+
		"nothing runs, and what it shows is reconstructed from its history")
	addTUIFlag(attachCmd)

	getCmd := &cobra.Command{
		Use:   "get <workflow-id>",
		Short: "Show a durable run's debug session",
		Long: "Read a durable run's debug state: whether a session is attached, where the run " +
			"is held and why, its frames, breakpoints and recent observations. It changes nothing.",
		Args: cobra.ExactArgs(1),
		RunE: runDebugGet,
		Example: `# Where a run's debug session stands, and what it last did:
flow debug get order-1234

# The same snapshot, as the schema's JSON:
flow debug get order-1234 -o json`,
	}
	addServerFlags(getCmd)
	addOutputFlag(getCmd)
	getCmd.Flags().String("run-id", "", "pin the run, as the first run id of its chain")

	doCmd := &cobra.Command{
		Use:   "do <workflow-id> <command...>",
		Short: "Run one debugger command against a session",
		Long: "Run one debugger command — next, inspect steps.fetch, break charge if amount > 500 — " +
			"against a session a previous attach left attached, and print the answer. For " +
			"scripts and agents that drive a session one step at a time.",
		Args: cobra.MinimumNArgs(2),
		RunE: runDebugDo,
		Example: `flow debug do order-1234 --session 5d3f… next
flow debug do order-1234 --session 5d3f… inspect steps.quote.total -o json`,
	}
	addServerFlags(doCmd)
	addOutputFlag(doCmd)
	doCmd.Flags().String("run-id", "", "pin the run, as the first run id of its chain")
	doCmd.Flags().String("session", "", "the session to act in (required)")
	_ = doCmd.MarkFlagRequired("session")
	doCmd.Flags().Duration("wait", 30*time.Second, "how long a movement waits for the next stop")

	historyCmd := &cobra.Command{
		Use:   "history <workflow-id> --run-id <run-id>",
		Short: "Show a durable run as it was at a past point",
		Long: "Read a durable run, open or closed, as it was at one workflow-task boundary of its " +
			"recorded history: where it was held, its frames, its progress. It replays the " +
			"interpreter over the history with no worker attached, so it dispatches nothing and " +
			"changes nothing. What the session held is reconstructed; a point the history cannot be replayed " +
			"to is refused. --inspect evaluates an expression over the scope the run held at the point, " +
			"which needs the debug_inspect action; the answer is labelled hypothetical, since the " +
			"expression is evaluated now and never happened in the run. --points lists the points a " +
			"run can be read at.",
		Args: cobra.ExactArgs(1),
		RunE: runDebugHistory,
		Example: `# The last point of a run:
flow debug history order-1234 --run-id 5d3f…

# The points it can be read at, then one of them:
flow debug history order-1234 --run-id 5d3f… --points
flow debug history order-1234 --run-id 5d3f… --at 17

# A value at a point, and the same expression at an earlier one:
flow debug history order-1234 --run-id 5d3f… --inspect steps.quote.total
flow debug history order-1234 --run-id 5d3f… --at 17 \\
  --inspect steps.quote.total

# The answer as the schema's JSON:
flow debug history order-1234 --run-id 5d3f… -o json`,
	}
	addServerFlags(historyCmd)
	addOutputFlag(historyCmd)
	historyCmd.Flags().String("run-id", "", "the execution to read; `flow get` prints a run's id (required)")
	_ = historyCmd.MarkFlagRequired("run-id")
	historyCmd.Flags().Int64("at", 0, "the event id of the point to read; 0 is the last")
	historyCmd.Flags().StringArray("inspect", nil, "evaluate an expression at the point; repeatable, at most 16")
	historyCmd.Flags().Bool("points", false, "list the points the run can be read at instead of reading one")

	debugCmd.AddCommand(attachCmd, getCmd, doCmd, historyCmd)
}

func runDebugAttach(cmd *cobra.Command, args []string) (err error) {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}
	runID, _ := cmd.Flags().GetString("run-id")
	session, _ := cmd.Flags().GetString("session")
	lease, _ := cmd.Flags().GetDuration("lease")
	script, _ := cmd.Flags().GetString("script")
	program, _ := cmd.Flags().GetString("program")
	wait, _ := cmd.Flags().GetDuration("wait")
	record, _ := cmd.Flags().GetString("record")
	history, _ := cmd.Flags().GetBool("history")
	if history {
		if err := refuseHistoryAttach(cmd, runID); err != nil {
			return err
		}
	}
	if script != "" {
		if err := refuseRecordingOver(cmd, script); err != nil {
			return err
		}
	}

	// The screen is the default at a terminal and nowhere else: wherever it
	// could not be drawn the attach runs as it did before it existed, byte for
	// byte on stdout, and says so on stderr only if --tui was spelled out.
	surface := newSurface(cmd)
	wantTUI, note := debugScreen(cmd, cmd.InOrStdin(), surface.Out, script, format, os.Getenv)
	fmt.Fprint(surface.Err, note)

	var (
		sourceMap *v1.DebugSourceMap
		parsed    *v1.Workflow
		documents []debugtui.Document
	)
	if program != "" {
		workflow, source, err := loadMappedWorkflow(program)
		if err != nil {
			return err
		}
		sourceMap = source.sourceMap(workflow)
		parsed = workflow
		if wantTUI {
			documents = source.documents(sourceMap)
		}
	}

	ctx := cmd.Context()
	var (
		remote  attachedTarget
		opening string
	)
	if history {
		// A record is read, not held: no session is taken, so there is nothing
		// to renew and nothing a lapse can release.
		recorded, err := flowdebug.OpenHistorical(ctx,
			flowdebug.RemoteHistory(newWorkflowServiceClient(serverFlagsOf(cmd)), args[0], runID),
			flowdebug.WithSourceMap(sourceMap))
		if err != nil {
			return fmt.Errorf("reading the history of %s: %w", args[0], err)
		}
		remote = recordedRun{recorded}
		opening = fmt.Sprintf("reading the record of %s, run %s — %d points, reconstructed from its history; nothing runs",
			args[0], runID, len(recorded.Points()))
	} else {
		live, receipt, err := flowdebug.AttachRemote(ctx, newWorkflowServiceClient(serverFlagsOf(cmd)), args[0], runID,
			flowdebug.RemoteOptions{SessionID: session, Lease: lease, SourceMap: sourceMap, Wait: 5 * time.Second})
		if err != nil {
			return err
		}
		remote = live
		opening = fmt.Sprintf("attached to %s — session %s (%s)", args[0], live.SessionID(),
			strings.TrimSpace(flowdebug.FormatReceipt(receipt)))
	}
	// Every way out below releases the run unless it has already been
	// detached or deliberately left attached: a failed write, a bound
	// reached, an error from the target. Close after Disconnect or Close is
	// a no-op, so this only acts where no path chose.
	defer func() { _ = remote.Close() }()
	answers := &driveAnswers{out: surface.Out, format: format}
	defer func() { err = errors.Join(err, answers.flush()) }()
	if !format.Machine() {
		fmt.Fprintln(surface.Out, opening)
		if program != "" && !remote.SourceMapVerified() {
			fmt.Fprintf(surface.Out, "%s does not match the program this run executes; lines are not shown\n", program)
		}
	}

	driver := flowdebug.NewDriver(remote)
	driver.Wait = wait

	// The lines the run accepted, written however the session ends. Registered
	// after the release above, so it runs first.
	recording := &attachRecording{}
	if record != "" {
		defer func() { writeRecording(record, recording.lines, recording.truncated, surface.Err) }()
	}

	// The first stop, or the news that the run has not reached a boundary.
	// A failure here detaches through the deferred Close: nothing has told
	// the caller the session's id, so there is nobody to rejoin it.
	first, err := driver.Do(ctx, "status")
	if err != nil {
		return err
	}
	if first.Snapshot.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD {
		if held, err := remote.WaitSnapshot(withWait(ctx, wait), first.Snapshot.GetRevision()); err == nil {
			first = &flowdebug.DriveResult{Snapshot: held, Text: flowdebug.FormatSnapshot(held)}
		}
	}
	if err := answers.add("status", first); err != nil {
		return err
	}

	// The screen drives the run itself and ends the way the loop below does.
	if wantTUI {
		return attachWithTUI(ctx, cmd, remote, driver, parsed, sourceMap, documents, surface, recording, args[0], first.Snapshot.GetCapabilities())
	}

	in, interactive := io.Reader(cmd.InOrStdin()), stdinIsInteractive(cmd)
	if script != "" {
		file, err := os.Open(script)
		if err != nil {
			_ = remote.Close()

			return err
		}
		defer file.Close()
		in, interactive = file, false
	}

	// The prompt is for the person typing, never part of the answers: with a
	// machine format it goes to stderr, so stdout stays the JSON it promises.
	prompt := surface.Out
	if format.Machine() {
		prompt = surface.Err
	}
	keep := false
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 4096), flowdebug.MaxCommandBytes+1)

	// A person at a terminal gets the line editor and tab completion; a script
	// or a pipe keeps the scanner and the bytes it has always produced. The
	// console reads stdin itself, so it is wanted only where stdin is the
	// terminal and neither a script nor a machine format has taken its place.
	var console *debugConsole
	if script == "" && !format.Machine() {
		var restore func()
		console, _, restore = debugConsoleFor(cmd.InOrStdin(), surface.Out, surface.Theme)
		defer restore()
	}
	if console != nil {
		console.SetCompleter(attachCompleter(ctx, driver))
	}

	// The panes `flow test --debug` paints, on the console and nowhere else:
	// with none, panes is nil and nothing below it runs. The program's step
	// list is offered only when it is the program this run executes, so a
	// mismatched file cannot name steps the run does not have.
	var panesOut io.Writer = surface.Out
	if console != nil {
		panesOut = console
	}
	_, panes := debugPanesFor(ctx, console, panesOut, surface.Theme, surface.Caps, func(string, flowdebug.Tone) {})
	frames := flowdebug.FrameOptions{Program: parsed}
	if parsed != nil && remote.SourceMapVerified() {
		frames.SourceMap = sourceMap
		frames.Inventory = stepList(parsed)
	}
	panes.setTarget(remote, frames)
	panes.paintStop(first.Snapshot)
	// next is one line of input. Both the end of input and an interrupt at the
	// console end the session the same way: the run is released, because a
	// debugger that is gone must not keep a production run held. Any other
	// failure to read the terminal is kept in consoleErr and handled where a
	// scanner's read failure is.
	var consoleErr error
	next := func() (string, bool) {
		if console != nil {
			text, err := console.Prompt()
			consoleErr = unexpectedPromptError(err)

			return text, err == nil
		}
		if interactive {
			fmt.Fprint(prompt, flowdebug.Prompt)
		}

		if !scanner.Scan() {
			return "", false
		}

		return scanner.Text(), true
	}
	for {
		text, more := next()
		if !more {
			break
		}
		line := strings.TrimSpace(text)
		switch line {
		case "":
			continue
		case "disconnect":
			keep = true
		case "quit", "q", "exit":
			line = "detach"
		}
		if keep {
			break
		}

		result, err := driver.Do(ctx, line)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			// A person at a terminal reads the error and types the line
			// again. A script cannot, and the lines after this one were
			// written assuming it ran, so the attach fails — and the run is
			// released on the way out — rather than exiting 0 on a script
			// that ran in part.
			if !interactive {
				return fmt.Errorf("%q: %w", line, err)
			}
			fmt.Fprintf(surface.Err, "%v\n", err)

			continue
		}
		if err := answers.add(line, result); err != nil {
			return err
		}
		panes.paintStop(result.Snapshot)
		if notDone(result) == nil {
			recording.add(line)
		}
		if line == "detach" {
			// A detach the run did not accept leaves it held; Close sends
			// one that cannot be refused as stale, rather than walking
			// away from it.
			if !flowdebug.Accepted(result.Receipt) {
				return remote.Close()
			}

			return remote.Disconnect()
		}
		// The same holds for a line the run answered but did not do: a
		// refused command, or a breakpoint it would not arm.
		if !interactive {
			if err := notDone(result); err != nil {
				return fmt.Errorf("%q: %w", line, err)
			}
		}
		if terminalDebugState(result.Snapshot.GetState()) {
			return remote.Disconnect()
		}
	}

	// A line the reader could not take — longer than a command may be — is
	// not the end of input: the rest of the script was never read. The
	// session is left attached for a rejoin, its lease bounding the hold,
	// and the command fails rather than reporting a run it did not drive.
	if err := cmp.Or(scanner.Err(), consoleErr); err != nil {
		_ = remote.Disconnect()
		if errors.Is(err, bufio.ErrTooLong) {
			err = fmt.Errorf("a command is at most %d bytes", flowdebug.MaxCommandBytes)
		}

		if history {
			return fmt.Errorf("reading commands: %w", err)
		}

		return fmt.Errorf("reading commands: %w; session %s is left attached until its lease lapses", err, sessionOf(remote))
	}

	if keep {
		if !format.Machine() && !history {
			fmt.Fprintf(surface.Out, "left session %s attached; rejoin with `flow debug attach %s --session %s` before its lease lapses\n",
				sessionOf(remote), args[0], sessionOf(remote))
		}

		return remote.Disconnect()
	}

	// The end of input releases the run: a debugger that is gone must not
	// keep a production run held.
	return remote.Close()
}

// attachedTarget is what `flow debug attach` drives: a [flowdebug.Remote]
// holding a durable run, or, for --history, a recorded run read through a
// [flowdebug.Historical]. The two are one front to the loop and the screen below,
// which is the point: a post-mortem is the same debugger over another target.
type attachedTarget interface {
	flowdebug.Target

	// SourceMapVerified is whether the source map given names the program the
	// target is at.
	SourceMapVerified() bool

	// Disconnect leaves the target the way `disconnect` does: a live session
	// stays attached for a rejoin, and a record has nothing to leave.
	Disconnect() error
}

// recordedRun is a [flowdebug.Historical] as an [attachedTarget]. A record holds
// no session, so leaving it is closing it.
type recordedRun struct{ *flowdebug.Historical }

// Disconnect closes the record: there is no session to leave attached.
func (r recordedRun) Disconnect() error { return r.Close() }

// sessionOf is the session id of a live target, or "" for a record, which has none.
func sessionOf(target attachedTarget) string {
	if live, ok := target.(interface{ SessionID() string }); ok {
		return live.SessionID()
	}

	return ""
}

// refuseHistoryAttach is why --history cannot be honoured with these flags, or
// nil. A record is one execution's, so the run id is required; it holds no
// session, so the flags that name, renew or wait on one mean nothing.
func refuseHistoryAttach(cmd *cobra.Command, runID string) error {
	if runID == "" {
		return errors.New("--history needs --run-id: a point of a recorded run is a point of one execution; `flow get` prints a run's id")
	}

	var given []string
	for _, name := range []string{"session", "lease", "wait"} {
		if cmd.Flags().Changed(name) {
			given = append(given, "--"+name)
		}
	}
	if len(given) > 0 {
		return fmt.Errorf("--history reads a recorded run and holds no session, so %s does not apply; "+
			"leave it out, or drop --history to attach to the live run", strings.Join(given, ", "))
	}

	return nil
}

// unexpectedPromptError is the part of a console prompt's failure that is not
// the person ending the session: the end of input and an interrupt both mean
// "release the run", and anything else is a terminal that failed to be read.
func unexpectedPromptError(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, flowdebug.ErrConsoleInterrupted) {
		return nil
	}

	return err
}

// completionTimeout bounds what one tab press may spend asking the target.
const completionTimeout = 2 * time.Second

// attachCompleter completes at the console over what the attached run says
// about itself, with the driver's rules: names and never values, and nothing
// the caller's own `inspect` could not reach. A target that does not answer
// within [completionTimeout] leaves the keystroke with nothing to offer instead
// of holding the terminal.
func attachCompleter(ctx context.Context, driver *flowdebug.Driver) func(line string, pos int) flowdebug.Completion {
	return func(line string, pos int) flowdebug.Completion {
		if pos >= 0 && pos < len(line) {
			line = line[:pos]
		}
		ctx, cancel := context.WithTimeout(ctx, completionTimeout)
		defer cancel()

		answer, err := driver.Complete(ctx, line)
		if err != nil {
			return flowdebug.Completion{}
		}

		return answer
	}
}

func terminalDebugState(state v1.DebugRunState) bool {
	switch state {
	case v1.DebugRunState_DEBUG_RUN_STATE_COMPLETED, v1.DebugRunState_DEBUG_RUN_STATE_FAILED,
		v1.DebugRunState_DEBUG_RUN_STATE_EXPIRED, v1.DebugRunState_DEBUG_RUN_STATE_DETACHED:
		return true
	default:
		return false
	}
}

func withWait(ctx context.Context, wait time.Duration) context.Context {
	if wait <= 0 {
		return ctx
	}
	ctx, cancel := context.WithTimeout(ctx, wait)
	context.AfterFunc(ctx, cancel)

	return ctx
}

func runDebugGet(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}
	runID, _ := cmd.Flags().GetString("run-id")

	response, err := newWorkflowServiceClient(serverFlagsOf(cmd)).DebugGet(cmd.Context(),
		newDebugGetRequest(args[0], runID))
	if err != nil {
		return err
	}

	snapshot := response.Msg.GetSnapshot()
	surface := newSurface(cmd)
	if format.Machine() {
		return writeJSON(surface, format, snapshot)
	}
	fmt.Fprint(surface.Out, formatDebugGet(snapshot))

	return nil
}

// formatDebugGet renders `flow debug get`'s text: the snapshot, then every
// observation it did not already print. A notice [flowdebug.FormatSnapshot]
// prints itself is not listed again.
func formatDebugGet(snapshot *v1.DebugSnapshot) string {
	var b strings.Builder
	b.WriteString(flowdebug.FormatSnapshot(snapshot))
	for _, observation := range snapshot.GetObservations() {
		if flowdebug.FormatSnapshotShows(snapshot, observation) {
			continue
		}
		fmt.Fprintf(&b, "  · %s\n", observation.GetText())
	}

	return b.String()
}

func runDebugHistory(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}
	runID, _ := cmd.Flags().GetString("run-id")
	at, _ := cmd.Flags().GetInt64("at")
	points, _ := cmd.Flags().GetBool("points")
	expressions, _ := cmd.Flags().GetStringArray("inspect")
	inspections := make([]*v1.DebugHistoryInspection, len(expressions))
	for i, expression := range expressions {
		inspections[i] = &v1.DebugHistoryInspection{Expression: expression}
	}

	response, err := newWorkflowServiceClient(serverFlagsOf(cmd)).DebugHistory(cmd.Context(),
		connect.NewRequest(&v1.DebugHistoryRequest{WorkflowId: args[0], RunId: runID, EventId: at, Inspections: inspections}))
	if err != nil {
		return err
	}

	surface := newSurface(cmd)
	if format.Machine() {
		return writeJSON(surface, format, response.Msg)
	}
	fmt.Fprint(surface.Out, formatDebugHistory(response.Msg, points, expressions...))

	return nil
}

// formatDebugHistory renders `flow debug history`'s text: the point and how it
// is known, then the snapshot, or with points the list of points instead, then
// each expression asked at the point with its answer.
func formatDebugHistory(response *v1.DebugHistoryResponse, points bool, expressions ...string) string {
	var b strings.Builder
	if points {
		for _, id := range response.GetBoundaries() {
			fmt.Fprintf(&b, "%d\n", id)
		}

		return b.String()
	}

	fidelity := strings.ToLower(strings.TrimPrefix(response.GetFidelity().String(), "DEBUG_FIDELITY_"))
	fmt.Fprintf(&b, "at event %d of %d points · %s\n", response.GetEventId(), len(response.GetBoundaries()), fidelity)
	if response.GetSnapshot() == nil {
		fmt.Fprintf(&b, "the run had not installed its debug session yet; %d steps completed\n", response.GetProgress().GetCompletedSteps())
	} else {
		b.WriteString(formatDebugGet(response.GetSnapshot()))
	}
	for i, answered := range response.GetInspected() {
		expression := "(scope)"
		if i < len(expressions) && expressions[i] != "" {
			expression = expressions[i]
		}
		kind := strings.ToLower(strings.TrimPrefix(answered.GetFidelity().String(), "DEBUG_FIDELITY_"))
		switch result := answered.GetResult(); {
		case result.GetError() != "":
			fmt.Fprintf(&b, "%s: %s\n", expression, result.GetError())
		case result.GetValue() != nil:
			fmt.Fprintf(&b, "%s = %s (%s) · %s\n", expression, result.GetValue().GetRendered(), result.GetValue().GetType(), kind)
		default:
			fmt.Fprintf(&b, "%s: %d names · %s\n", expression, result.GetTotal(), kind)
			for _, child := range result.GetChildren() {
				fmt.Fprintf(&b, "  %s = %s\n", child.GetName(), child.GetValue().GetRendered())
			}
		}
	}

	return b.String()
}

func runDebugDo(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}
	runID, _ := cmd.Flags().GetString("run-id")
	session, _ := cmd.Flags().GetString("session")
	wait, _ := cmd.Flags().GetDuration("wait")

	remote, _, err := flowdebug.AttachRemote(cmd.Context(), newWorkflowServiceClient(serverFlagsOf(cmd)), args[0], runID,
		flowdebug.RemoteOptions{SessionID: session, Wait: 5 * time.Second})
	if err != nil {
		return err
	}
	// One command, then leave the session as it was found: attached.
	defer func() { _ = remote.Disconnect() }()

	driver := flowdebug.NewDriver(remote)
	driver.Wait = wait
	line := strings.Join(args[1:], " ")
	result, err := driver.Do(cmd.Context(), line)
	if err != nil {
		return err
	}
	if err := notDone(result); err != nil {
		_ = writeDriveResult(newSurface(cmd).Out, format, line, result)

		return err
	}

	return writeDriveResult(newSurface(cmd).Out, format, line, result)
}

// notDone is why a line the target answered did not do what it asked: a
// receipt refusing the command, or a breakpoint the line set that the target
// took but would not arm. Nil when the line was done, or is pending.
func notDone(result *flowdebug.DriveResult) error {
	if receipt := result.Receipt; receipt != nil && !flowdebug.Accepted(receipt) {
		return errors.New("the command was not applied: " + strings.TrimSpace(flowdebug.FormatReceipt(receipt)))
	}
	if state := result.Unarmed; state != nil {
		return fmt.Errorf("the breakpoint %s was not armed: %s", state.GetId(), state.GetMessage())
	}

	return nil
}

// driveAnswers writes an attach's answers as its format asks: each rendering or
// JSON line as it comes, or, for `-o json`, one array of every answer at the
// end, since that format is one document per invocation.
type driveAnswers struct {
	out    io.Writer
	format OutputFormat
	held   [][]byte
	// heldBytes is what held holds, for [maxAttachJSONBytes].
	heldBytes int
}

// maxAttachJSONBytes bounds the `-o json` document an attach holds until it
// ends. A session long enough to pass it is one to stream with `-o jsonl`.
const maxAttachJSONBytes = 16 << 20

func (a *driveAnswers) add(command string, result *flowdebug.DriveResult) error {
	if a.format != FormatJSON {
		return writeDriveResult(a.out, a.format, command, result)
	}
	encoded, err := encodeDriveResult(command, result)
	if err != nil {
		return err
	}
	if a.heldBytes+len(encoded) > maxAttachJSONBytes {
		return fmt.Errorf("the -o json document would pass %d bytes; stream a session this long with -o jsonl", maxAttachJSONBytes)
	}
	a.held = append(a.held, encoded)
	a.heldBytes += len(encoded)

	return nil
}

// flush writes the `-o json` document, once, if any answer was held.
func (a *driveAnswers) flush() error {
	if len(a.held) == 0 {
		return nil
	}
	held := a.held
	a.held = nil
	_, err := fmt.Fprintf(a.out, "[%s]\n", bytes.Join(held, []byte(",")))

	return err
}

// writeDriveResult writes one command's answer: its rendering, or the typed
// messages it carried as one JSON object.
func writeDriveResult(out io.Writer, format OutputFormat, command string, result *flowdebug.DriveResult) error {
	if !format.Machine() {
		_, err := fmt.Fprint(out, result.Text)

		return err
	}
	encoded, err := encodeDriveResult(command, result)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "%s\n", encoded)

	return err
}

// encodeDriveResult is one command's answer as a JSON object: the command, and
// each typed message it carried in the schema's JSON.
func encodeDriveResult(command string, result *flowdebug.DriveResult) ([]byte, error) {
	name, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	parts := []string{`"command":` + string(name)}
	for _, part := range []struct {
		name    string
		message proto.Message
		present bool
	}{
		{"receipt", result.Receipt, result.Receipt != nil},
		{"snapshot", result.Snapshot, result.Snapshot != nil},
		{"inspect", result.Inspect, result.Inspect != nil},
	} {
		if !part.present {
			continue
		}
		encoded, err := v1.MarshalSchemaJSON(part.message, false)
		if err != nil {
			return nil, err
		}
		parts = append(parts, fmt.Sprintf("%q:%s", part.name, encoded))
	}

	return []byte("{" + strings.Join(parts, ",") + "}"), nil
}

func newDebugGetRequest(workflowID, runID string) *connect.Request[v1.DebugGetRequest] {
	return connect.NewRequest(&v1.DebugGetRequest{WorkflowId: workflowID, RunId: runID})
}
