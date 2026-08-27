package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// `flow debug replay`, the half of #928's record-and-replay that did not exist
// (#1111, item 3).
//
// A session has recorded every command it accepted since slice 1 —
// [flowdebug.Session.Script] — and until now the recording was spent in exactly
// one place, rendered into the `flowstate_debug` answer (`mcpdebug.go`). So a
// debugging session could be written down and nothing could play one back: the
// script in an issue was prose.
//
// # It is a local run, not a fifth front
//
// This command is not a new debugger. It is `flow run local --debug` with its
// command stream read from a file instead of from the terminal, and it says so
// by *being* that: the whole of it below is a pre-flight over the script, a
// reader handed to the shared path, and an account of what the script did. The
// run itself is [runLocalWorkflow] — the same telemetry, the same egress and
// task policies, the same plugins, the same secret providers, the same inputs
// binding and the same answer document.
//
// That is the design call, and the alternative is what makes it one. A replay
// that built its own narrower run would be a second rehearsal beside the one
// `flow run local` gives, and a reproduction that runs under a different
// posture than the session it reproduces is not a reproduction — which is
// CLAUDE.md's two-drivers rule applied to two fronts over one driver. It costs
// the flag surface: this verb declares what a local run declares, through the
// same helpers `flow task run` composes its own local execution from
// (`taskrun.go`), because a flag the rehearsal takes and the replay does not is
// a run the replay cannot reproduce.

// newDebugCommand builds `flow debug` and the one verb under it.
//
// A group with a single member, which reads like ceremony and is not — the same
// judgement `flow task` records next door. `replay` is a verb about a debugging
// session rather than about a workflow, so it belongs under a noun naming the
// debugger, and that noun is where the family grows: a `record` verb, and
// slice 2's durable pause and resume, are all about the same thing.
func newDebugCommand() *cobra.Command {
	debugCmd := &cobra.Command{
		Use:   "debug",
		Short: "Work with a recorded debugging session",
		Long: "Work with the step debugger's recordings.\n\n" +
			"The debugger itself is reached as `flow run local --debug` (a real run, at a " +
			"terminal), as `flow test --debug` (one test case), and as `flow dap` (from an " +
			"editor). Every one of those records the commands it accepted; this is where a " +
			"recording is played back.",
	}

	replayCmd := &cobra.Command{
		Use:     "replay <script-file> <workflow-file>",
		Short:   "Replay a recorded debugging session against a workflow",
		Long:    debugReplayLong,
		Args:    cobra.ExactArgs(2),
		RunE:    replayDebugScript,
		Example: debugReplayExample,
	}

	// The flags a local run takes, because this *is* a local run — see the
	// header. The same list, through the same helpers, in the same order
	// `flow task run` composes them in.
	addOutputFlag(replayCmd)
	addRawOutputFlag(replayCmd)
	addInputFlags(replayCmd)
	addRevealSensitiveFlag(replayCmd)
	addEgressPolicyFlag(replayCmd)
	addTaskPolicyFlag(replayCmd)
	addSecretFlags(replayCmd)
	addPluginFlags(replayCmd)
	addLocalRehearsalFlags(replayCmd)
	addLocalSignalFlags(replayCmd)

	// How this command tells the shared local-run path that its run is
	// debugged. It is not part of the verb's surface — a replay with no
	// debugger is `flow run local` — so it is hidden, and [replayDebugScript]
	// sets it unconditionally rather than reading it, which is what keeps
	// `--debug=false` from being a way to ask this command to do nothing.
	replayCmd.Flags().Bool("debug", true,
		"held true by this command: a replay is a debugging session, so there is nothing to choose")
	replayCmd.Flags().Lookup("debug").Hidden = true

	debugCmd.AddCommand(replayCmd)

	return debugCmd
}

// debugReplayLong is the help, and it carries the two things somebody has to
// know before the arguments mean anything: what a script file is, and that this
// runs the workflow for real.
const debugReplayLong = "Replay a recorded debugging session: read a script of debugger commands and " +
	"drive a real local run with them, stopping where the recorded session stopped and " +
	"asking what it asked.\n\n" +
	"A script is the commands themselves, one per line — exactly what a session reads " +
	"from a terminal, and exactly what the `script` field of the `flowstate_debug` tool's " +
	"answer holds. A line starting with `#` is a comment, at the prompt as well as here, " +
	"because the file is the command stream rather than a format wrapped around it. A " +
	"blank line is refused: at a prompt it means `step`, in a file it reads as spacing, " +
	"and a recorded script never contains one.\n\n" +
	"A script names no workflow, so the workflow is the second argument: a path recorded " +
	"on one machine is not a path on another, and a header naming one that nothing " +
	"checked would be worse than no header at all.\n\n" +
	"The script is checked against the workflow before anything runs, so a `break` naming " +
	"a step this workflow does not declare, or a misspelled command, is a diagnostic with " +
	"a line and a column rather than a run that quietly did something else.\n\n" +
	"This is a real run under this machine's own posture: the same policies, plugins, " +
	"secrets and inputs `flow run local` uses, because a reproduction that runs under a " +
	"different posture reproduces a different thing. The console's account goes to " +
	"stderr and the answer stays the document on stdout, exactly as `flow run local " +
	"--debug` leaves them."

// debugReplayExample shows the shape of a script as well as the invocation,
// because the file is the part nobody can guess.
const debugReplayExample = `# Replay the session recorded beside one of the examples:
flow debug replay examples/loop-accumulate/debug.script examples/loop-accumulate/workflow.yaml

# What such a file holds — a comment, then the commands a session accepted:
#   # why the last term never lands in the sum
#   break term if acc.n == 3
#   continue
#   inspect acc
#   continue

# Replay with the arguments the recorded run was started with:
flow debug replay session.script examples/computed-outputs/workflow.yaml --input release=2026.9.0

# Record one to replay later, by keeping what the console read:
flow run local examples/loop-accumulate/workflow.yaml --debug < session.script`

// addLocalSignalFlags declares the flags that answer a workflow's approval
// gates up front.
//
// Supplying signals ahead of time is what makes an approval gate something an
// author can exercise on their laptop rather than first meeting in production.
// A local run is a process, so there is nobody to signal it after it starts;
// the local waiter buffers what is given here, so a gate reached later still
// finds its answer waiting — the same behavior the durable driver has because
// Temporal buffers signals for a run.
//
// And who those answers are from. A gate whose `signals:` policy names an
// approver is unreachable without these: a delivery attesting nobody matches no
// `allow:` rule, so the only rehearsal available was the refusal. They name the
// approver every --signal of this run stands in for, and the same check
// production runs then admits or refuses it here — including
// `distinct_from_starter:`, compared against --as-subject/--as-issuer.
//
// Spelled to rhyme with --as-subject and its siblings, which name the starter,
// because they answer the same shape of question about the other party.
// Deliberately no --signal-as-deployment: no `signals:` rule can match on a
// deployment, so a flag for it would rehearse nothing.
//
// A function rather than two copies, because `flow run local` and `flow debug
// replay` are one execution verb with two command streams, and a flag one of
// them takes and the other does not is a run the replay cannot reproduce. It
// lives here rather than in main.go for the reason [addLocalRehearsalFlags]
// lives in taskrun.go: a flag set belongs beside the verb whose shape it is.
func addLocalSignalFlags(cmd *cobra.Command) {
	cmd.Flags().StringArray("signal", nil,
		`answer a wait_for_signal step, as name=json (repeatable), e.g. --signal deploy-approved='{"approved": true}'`)
	cmd.Flags().String("signal-as-subject", "",
		"authenticated subject to deliver --signal as, with --signal-as-issuer (local runs only)")
	cmd.Flags().String("signal-as-issuer", "",
		"authenticated issuer to deliver --signal as, with --signal-as-subject (local runs only)")
	cmd.Flags().String("signal-as-namespace", "",
		"tenant namespace to deliver --signal as (local runs only)")
	cmd.Flags().StringArray("signal-as-claim", nil,
		"authenticated string claim NAME=VALUE to deliver --signal as (repeatable)")
}

// replayDebugScript reads a script, refuses what the run would disagree with,
// and then runs the workflow with the script as its command stream.
func replayDebugScript(cmd *cobra.Command, args []string) error {
	scriptPath, workflowPath := args[0], args[1]

	lines, err := readDebugScript(scriptPath)
	if err != nil {
		return err
	}

	// The workflow, read here as well as inside the run.
	//
	// Two checks below need it before a step has run, and the same cost is
	// paid for the same reason two commands over: `flow test --debug` loads
	// its test file twice so that it can refuse three cases by name rather
	// than silently debug the first. A file that does not parse contributes
	// nothing and says nothing here — the run is about to report that
	// properly, with positions, and a second complaint would be the same news
	// told worse (see workflowStepIDs).
	workflow, _, parseErr := flowfile.ParseFile(workflowPath)

	var steps []string
	if parseErr == nil {
		steps = stepIDs(workflow)
	}

	if problems, total := flowdebug.CheckScript(lines, steps); len(problems) > 0 {
		return scriptProblemsError(scriptPath, problems, total)
	}

	// The reveal question, answered by the function `flow run local --debug`
	// answers it with rather than by a second rule.
	//
	// A debugger is a reveal: it narrates each step's values as they complete
	// and `inspect` reaches anything in scope. Replay narrates exactly the
	// same values, so it takes exactly the same refusal — a weaker answer here
	// would be a second door around the redaction the first one closes, which
	// is the shape Codex found on #1109 and the shape CLAUDE.md's two-drivers
	// section is about.
	//
	// Asked here as well as there only so that the remedy names this verb:
	// the shared path's wording offers "drop --debug", and there is nothing to
	// drop from a command whose whole job is the debugger. The decision is
	// [decideCarriedValues]'s either way, so the two cannot disagree, and the
	// shared check still stands behind this one for a workflow that could not
	// be parsed here.
	if parseErr == nil && decideCarriedValues(workflow, revealSensitiveRequested(cmd)) != carriedValuesShown {
		return fmt.Errorf("a replay narrates step values and evaluates expressions over them, and "+
			"%q declares sensitive inputs or outputs whose transcript this command would otherwise "+
			"withhold; add --reveal-sensitive to replay it with values shown, or run it without a "+
			"debugger with `flow run local`",
			workflow.GetName())
	}

	// The script becomes the session's command stream. Set on this command
	// rather than passed, because the shared path reads it as
	// `cmd.InOrStdin()` — a session's input has always been a stream, and this
	// is that stream coming off disk instead of a terminal.
	stream := newScriptStream(lines)
	cmd.SetIn(stream)

	// Held true rather than read: see the flag's own declaration.
	if err := cmd.Flags().Set("debug", "true"); err != nil {
		return err
	}

	runErr := runLocalWorkflow(cmd, []string{workflowPath})

	// Whatever the run did, what the *script* did is worth saying — including
	// after a failure, where a script that ran out is one explanation for it.
	reportScriptRemainder(cmd, scriptPath, stream)

	return runErr
}

// readDebugScript reads and bounds one script file.
//
// The bounds are [flowdebug.ReadScript]'s, which are the session's own
// recording bounds read in the other direction; this adds only the file.
func readDebugScript(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	lines, err := flowdebug.ReadScript(file)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(lines) == 0 {
		return nil, fmt.Errorf("%s: this script has no commands, and an empty one would run the "+
			"workflow unattended — which is what `flow run local` already does", path)
	}

	return lines, nil
}

// scriptProblemsError renders every problem as its own `file:line:column:
// message` line.
//
// Through [positionLine], the one join every rendered diagnostic in this CLI
// goes through, so a script's findings are as clickable in a terminal and as
// greppable in a CI log as a Flowfile's are (#384). The shape is
// [diagnosticsError]'s deliberately: one error whose every line names its own
// file, because a line that travels on its own has to say which file it is
// about.
//
// Not a usage error. The invocation was correct and the *file* named on it is
// what was refused, which is the boundary execute.go draws and
// TestIsUsageErrorDoesNotMarkAFileFinding pins.
func scriptProblemsError(path string, problems []flowdebug.ScriptProblem, total int) error {
	lines := make([]string, 0, len(problems)+1)
	for _, problem := range problems {
		lines = append(lines, positionLine(path, problem.String()))
	}
	if total > len(problems) {
		lines = append(lines, fmt.Sprintf("%s: %d more problems were found and %d are shown",
			path, total-len(problems), len(problems)))
	}

	return fmt.Errorf("%s cannot be replayed against this workflow:\n%s", path, strings.Join(lines, "\n"))
}

// reportScriptRemainder says what the script and the run disagreed about, once
// the run is over.
//
// Two shapes, and both are silent failures without it — which is the one wrong
// answer a diagnostic can give (CLAUDE.md):
//
//   - The script ran out while the run was still held. The session already says
//     the run continued unattended, because that is a fact about the *run*;
//     this says what is wrong with the *script* and how to fix it, which is a
//     different sentence for a different reader.
//   - The run ended with commands still in the file. Nothing says so at all
//     otherwise: the session simply never asks for them, and a reproduction
//     that stopped short looks exactly like one that finished.
//
// An account rather than a refusal, on stderr with the rest of the run's
// account. The run happened and its answer is real, and this command's exit
// status means what `flow run local`'s means — whether the run succeeded — so a
// short script must not turn a run that completed into a failure.
func reportScriptRemainder(cmd *cobra.Command, path string, stream *scriptStream) {
	delivered, unread, first, exhausted := stream.progress()

	// Nothing was read at all: the run failed before it reached a boundary, or
	// refused to start. Saying "every command is unread" there would be a
	// diagnostic about the script for a failure that has nothing to do with
	// it, and a false diagnostic is worse than a missing one.
	if delivered == 0 {
		return
	}

	surface := newSurface(cmd)
	pill := surface.ErrTheme.Pill(ui.ToneWarning, "script")

	if exhausted {
		fmt.Fprintf(surface.Err, "%s %s ran out while the run was still held at a step boundary, so the "+
			"rest of the run continued unattended. End the script with `continue` to run to the end "+
			"deliberately, or `quit` to stop there.\n", pill, path)

		return
	}

	if unread > 0 {
		fmt.Fprintf(surface.Err, "%s the run ended with %d of %s's commands unread, from line %d (%q). "+
			"The run reached fewer step boundaries than the script has commands: shorten it, or check "+
			"that this is the workflow it was recorded against.\n",
			pill, unread, path, first.number, first.text)
	}
}

// scriptStream is the script, handed to the session one line at a time.
//
// It is an [io.Reader] because that is the seam the shared local-run path
// installs ([flowdebug.Options.In]), and a reader is all a session needs: the
// [Console] interface exists for a terminal's editing keys, and a file has
// none.
//
// # One line per read, and why the count is exact
//
// A read never crosses a line boundary. That is what makes the count
// trustworthy rather than approximate: the session's scanner reads only when it
// needs a token, and it needs one only when a boundary asked for a command
// ([flowdebug.Session] drives its reader from a `wants` channel, one line per
// request). Hand it several lines at once and it buffers them, so the bytes
// this reader gave up would stop being the commands the session took. Counting
// a line only when its last byte is gone keeps that true even where the scanner
// asks for less than a whole line, which it does while its buffer is still
// growing.
//
// The counters are read after the run returns and written on the session's
// reader goroutine, which [flowdebug.Session.Close] releases but does not wait
// for — so they are held under a mutex. "The run is over, so nothing is
// reading" is an argument, not a synchronization edge.
type scriptStream struct {
	mu        sync.Mutex
	lines     []string
	line      int  // index of the line being handed over
	offset    int  // bytes of that line already handed over
	delivered int  // lines handed over whole
	exhausted bool // the session asked for a line after the last one
}

// newScriptStream returns a reader over lines.
func newScriptStream(lines []string) *scriptStream {
	return &scriptStream{lines: lines}
}

// Read hands over at most the rest of the current line.
func (s *scriptStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.line >= len(s.lines) {
		// Only ever reached because something asked, which is the whole
		// signal: a session asks for a line at a boundary it decided to stop
		// at, so an end reached here is an end reached with the run held.
		s.exhausted = true

		return 0, io.EOF
	}

	// The newline is the session's line terminator, so it travels with the
	// line rather than being something the reader adds between them — a script
	// whose last line had none would otherwise lose its last command.
	text := s.lines[s.line] + "\n"
	n := copy(p, text[s.offset:])
	s.offset += n

	if s.offset >= len(text) {
		s.line++
		s.offset = 0
		s.delivered++
	}

	return n, nil
}

// unreadLine names the first command the run never asked for.
type unreadLine struct {
	number int
	text   string
}

// progress reports what the session took: how many lines it read, how many
// commands are left unread, which is the first of them, and whether it asked
// for one past the end.
//
// Commands rather than lines in the count, because a comment is not a command
// and a script that ends in a sentence about itself has nothing left undone. A
// blank line *is* counted: it is a `step`. What a comment is gets asked of
// [flowdebug.IsComment] rather than spelled again here — a second reading of
// the marker is a line this would count as unread while the session read it as
// nothing.
func (s *scriptStream) progress() (delivered, unread int, first unreadLine, exhausted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := s.line; i < len(s.lines); i++ {
		if flowdebug.IsComment(s.lines[i]) {
			continue
		}
		if unread == 0 {
			first = unreadLine{number: i + 1, text: s.lines[i]}
		}
		unread++
	}

	return s.delivered, unread, first, s.exhausted
}
