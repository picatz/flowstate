package flowdebug

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/celcomplete"
)

// The bounds. A debugger reads what a person or a replay script types, which
// is untrusted input like any other (CLAUDE.md), and each of these bounds the
// resource that input controls rather than a proxy for it.
const (
	// MaxCommandBytes bounds one command line. The resource is memory in the
	// scanner, and the attacker-controlled quantity is a line with no newline
	// in it — a file that is one very long "line" would otherwise be read
	// whole before anything looked at it.
	MaxCommandBytes = 64 << 10

	// MaxBreakpoints bounds how many step ids a session may hold at once.
	// The resource is the set itself, and a replay script is a loop away from
	// growing it without end.
	MaxBreakpoints = 1024

	// MaxScriptCommands bounds how many accepted commands a session records
	// for replay. The resource is the recording, which grows with what the
	// session is told to do rather than with the workflow: a `step` at every
	// boundary of a long loop is a legitimate session that records a command
	// per iteration. Past the bound the session keeps working and stops
	// recording, and [Session.ScriptTruncated] says so — a replay script that
	// silently dropped its tail would reproduce a *different* run, which is
	// worse than one that admits it is partial.
	MaxScriptCommands = 100_000

	// MaxScriptBytes bounds the recording's *size*, which the count above does
	// not.
	//
	// Bounding one resource does not bound another the peer controls the ratio
	// to (CLAUDE.md): a hundred thousand commands each just under
	// [MaxCommandBytes] satisfies both advertised bounds and is six gigabytes
	// held for the life of the session. Nobody types that, and a console is
	// not the only way in — either CLI front reads whatever is redirected at
	// its stdin, and the MCP adapter's own `maxDebugScriptBytes` bounds one
	// *call* rather than the session a caller builds out of several (Codex,
	// #1109).
	//
	// A megabyte, the size this repository already uses for "a document a
	// person wrote": a replay script is meant to be re-run and pasted into an
	// issue, and one larger than a Flowfile has stopped being either. Past it
	// the session keeps working and stops recording, exactly as past the count
	// — the tail is admitted by [Session.ScriptTruncated] rather than dropped
	// in silence.
	MaxScriptBytes = 1 << 20

	// MaxInspectRunes bounds the rendered length of one inspection's answer.
	// The expression's own cost is bounded by the evaluator; this bounds the
	// *rendering* of a result that was cheap to compute and is large to print
	// — `${inputs.rows}` on a long list.
	MaxInspectRunes = 4096

	// MaxScopeNames bounds how many names one `scope` line lists before it
	// says how many more there are.
	//
	// A count rather than a rune cap, unlike [MaxInspectRunes], because the
	// thing being bounded is a *list* and cutting it by width truncates a
	// name down the middle — the reader learns neither what the name was nor
	// that there were others. Twenty is what a terminal line holds while
	// staying scannable.
	//
	// `scope` answers "what can I name right now", and past a few dozen names
	// a complete answer stops answering it: a run of hundreds of steps
	// produced one unbroken comma-separated wall, unreadable in a terminal
	// and expensive in an agent's context. So the bound is not a loss of the
	// command's purpose but the thing that restores it — orientation here,
	// enumeration through tab completion and `inspect steps.`, which are
	// bounded and prefix-filtered for exactly this.
	MaxScopeNames = 20
)

// Tone classifies one fragment of session output, so a terminal can colour
// the session the way `flow test` already colours its transcript.
//
// Deliberately this package's own vocabulary rather than flowtest's
// [flowtest.TranscriptTone], and the reason is the member set: a transcript
// has no prompt and no breakpoint, and extending its tones with members no
// transcript line can carry would muddy what that type means. The consumer
// (`flow test --debug`) maps both vocabularies onto the one theme, which is
// where they meet.
type Tone int

const (
	// ToneInfo is the account: step outcomes, scope listings, inspection
	// results, help. The zero value, so an unclassified fragment reads as
	// ordinary text rather than as an outcome it is not.
	ToneInfo Tone = iota

	// ToneBreak is the session's heading: the "break at <step>" line a
	// reader scans for when deciding where they are.
	ToneBreak

	// TonePrompt is the input affordance ("debug> "), secondary by nature —
	// it should recede so the run's own account stands out.
	TonePrompt

	// ToneWarning is a fact worth noticing that is not a run failure: a
	// tolerated step failure, an inspection that did not compile, an
	// unknown command, a console that ran out.
	ToneWarning

	// ToneDanger is a step failure the run does not absorb.
	ToneDanger
)

// Options configures a [Session].
type Options struct {
	// In is where commands are read from: a terminal, or a recorded script
	// being replayed. A nil In means every breakpoint resumes immediately,
	// which is what makes a session with no console harmless rather than
	// hung.
	In io.Reader

	// Out is where the session writes. A nil Out discards, so a session can
	// be driven for its control effects alone.
	Out io.Writer

	// Breakpoints are step ids to stop at, set before the run starts.
	//
	// Whether any were set decides where the session first stops: with none,
	// it stops at the first step, because `--debug` with no other argument
	// means "let me look before anything happens". With breakpoints, it runs
	// to the first one, because naming a step is how an author says they do
	// not want to walk there.
	Breakpoints []string

	// Clock, when set, is read to timestamp each stop, so a session under a
	// virtual clock reports the run's own time rather than the wall's. Nil
	// reads no clock and prints no timestamps.
	Clock v1.Clock

	// Emit, when set, receives every fragment the session would have written
	// to Out, classified by [Tone], and Out is then not written to at all —
	// the fragment's bytes (newlines included) are the caller's to render.
	// This is how `flow test --debug` colours the session through its theme
	// without this package knowing a terminal exists. Nil keeps the plain
	// writes to Out.
	Emit func(text string, tone Tone)

	// Console, when set, is prompted through instead of reading lines from
	// In, and In is then not read at all. See [Console].
	Console Console

	// Steps are the ids this run may reach, for completing `break` and
	// `until` before the run has been anywhere.
	//
	// Optional, and a session with none still completes over the steps it has
	// watched go past — see [Session.reachableSteps]. It is a caller's answer
	// rather than something this package derives because the seam it is
	// installed on ([v1.Debugger]) is handed one step at a time and never the
	// workflow: `flow run local --debug` has the specification in hand, and an
	// embedder with only the seam honestly does not.
	//
	// Unbounded here, deliberately: these are ids the caller already holds a
	// whole workflow's worth of, so storing them costs nothing new. What is
	// bounded is the *answer*, in [Session.Complete].
	Steps []string
}

// Console is a line editor a session prompts through.
//
// It exists so that this package can be given a real prompt — completion,
// history, the editing keys — without knowing that a terminal exists. What a
// console is handed back is the same string a line of a script would have been,
// so a session driven by one and a session driven by `< script.txt` take
// exactly the same path from there. That is the property [Session] was built
// on and this must not break: a debugger that only worked on a terminal would
// take `flow test --debug < script.txt` and the `flowstate_debug` tool with it.
//
// A console is prompted from the session's own reader goroutine, one call at a
// time, and never concurrently with itself. [Session.Complete] is not: a
// console's completion callback runs on whichever goroutine is inside Prompt,
// while the boundary that asked for the line is parked elsewhere.
//
// # A console owes [MaxCommandBytes]
//
// It is the one bound on this surface that moves with the reader. The other
// four are the session's wherever the line came from — [MaxBreakpoints] and
// [MaxInspectRunes] bound what a command does, [MaxScriptCommands] and
// [MaxScriptBytes] bound the recording — but this one bounds the *read*, and
// the [bufio.Scanner] that enforces it is not in a console's path. So a
// console owes it: whatever it returns must be a line the session would have
// accepted, however that is arranged.
type Console interface {
	// Prompt writes the prompt and reads one line, without its newline.
	//
	// It returns [io.EOF] when the input ends — a redirected script that ran
	// out, or somebody pressing ctrl-D — which the session answers by
	// resuming the run and saying so. It returns [ErrConsoleInterrupted] when
	// somebody interrupted the prompt instead, which the session answers by
	// ending the run, because the two must not be confused: resuming an
	// unattended `flow run local --debug` is how a ctrl-C comes to run the
	// rest of somebody's workflow for real.
	Prompt() (string, error)
}

// Prompt is what a session asks with, and what a [Console] should draw.
//
// Exported so that a console styling it is styling the same string the plain
// path writes, rather than a second copy that can drift into a different word.
const Prompt = "debug> "

// ErrConsoleInterrupted is what a [Console] returns when somebody interrupted
// the prompt — ctrl-C at a terminal — rather than ending the input.
//
// The session treats it exactly as `quit`: the run stops here and the session
// is over. That is the reading with no bad outcome. Answering an interrupt the
// way a finished script is answered would resume the run and let it finish
// unattended, and under `flow run local --debug` the rest of that run is real
// HTTP requests against real systems — so the one key a person presses to stop
// something would be the key that lets it go.
var ErrConsoleInterrupted = errors.New("the debug console was interrupted")

// mode is what the session does at the next boundary it reaches.
type mode int

const (
	// modeStop stops at the next boundary, whatever it is: `step`.
	modeStop mode = iota
	// modeRun stops only at a breakpoint: `continue`.
	modeRun
	// modeUntil stops at one named step, or at a breakpoint reached first:
	// `until <id>`.
	modeUntil
)

// A Session is one interactive debugging session over one local run.
//
// It implements [v1.Debugger] and [v1.RunObserver]; install both on the run's
// context and the session both holds the run at each boundary and narrates
// what each step produced.
//
// Safe for concurrent use. The local driver is sequential today, but a
// debugger is installed on a context that loop bodies, parallel branches and
// called workflows all share, and [v1.RunObserver] states plainly that its
// callbacks may arrive on a different goroutine than the boundary did.
type Session struct {
	out  io.Writer
	emit func(text string, tone Tone)
	// in is nil when no stream was given, and unread when a console was: a
	// console owns its own reader.
	in *bufio.Scanner
	// console is the line editor to prompt through, or nil to read lines from
	// in. See [Console].
	console Console
	clock   v1.Clock

	// lines carries what the reader goroutine read, and closes when the input
	// ends; wants is how a prompt asks it for one, and readOnce starts it at
	// the first prompt. A goroutine rather than a synchronous read, because
	// the run being held is cancellable and neither a Scanner nor a terminal
	// is — see [Session.readCommand].
	lines    chan string
	wants    chan struct{}
	readOnce sync.Once
	// done releases the reader goroutine; see [Session.Close].
	done     chan struct{}
	stopOnce sync.Once

	// promptMu admits one goroutine at a time to a step boundary, and outMu
	// to the output.
	//
	// Neither is paranoia about a driver that "runs branches in written
	// order": [v1.RunObserver] says in as many words that its callbacks
	// arrive on the step's own goroutine and that an implementation storing
	// events must synchronize itself when a workflow has `parallel:`
	// branches or `async:` steps, and this session both stores them and
	// hands them to a caller's Emit. Two branches finishing at once raced
	// this session's own fields and the MCP transcript's slice alike, on a
	// type whose doc claims it is safe for concurrent use (Codex, #1109).
	//
	// promptMu covers the *whole* of [Session.BeforeStep] rather than the
	// prompt inside it, because deciding whether to stop reads and writes
	// the same mode two branches would be deciding against — and because a
	// second branch prompting while the first is parked would have both of
	// them pulling lines off one command stream, splitting one script
	// between two run positions. A debugger holds *the* run; while it is
	// stopped, the run is stopped.
	//
	// They are separate locks, and the order is only ever promptMu → outMu
	// → mu: an account arriving on another branch (StepFinished, which never
	// prompts) must still be able to print while a boundary is parked
	// waiting for a command, or the account of a run would deadlock behind
	// the run's own pause.
	promptMu sync.Mutex
	outMu    sync.Mutex

	mu          sync.Mutex
	mode        mode
	until       string
	breakpoints map[string]breakpoint

	// notedUnbound remembers which breakpoints have already reported a
	// condition they could not evaluate, so the notice is one line rather than
	// one per iteration. See [Session.noteDeclined].
	notedUnbound map[string]struct{}
	// script records accepted commands, in order, for replay, and scriptBytes
	// is what they weigh — see [MaxScriptBytes], which the count bound beside
	// it does not imply.
	script          []string
	scriptBytes     int
	scriptTruncated bool
	// started is the clock reading at the first boundary, so stops report
	// time relative to the run rather than an absolute date.
	started  time.Time
	hasStart bool
	// finished is the id of the most recent step whose outcome arrived, and
	// the text of it, so a `step` can report what it just ran before asking
	// what to do next.
	lastOutcome string
	// closed is set once the command stream ended, so the session stops
	// trying to read from it.
	closed bool
	// readErr is why the stream ended, when it ended for a reason. A
	// [bufio.Scanner] that meets a line longer than [MaxCommandBytes] stops
	// exactly as it stops at EOF, and a session that cannot tell those apart
	// answers a refused command with "no more commands" and runs the rest of
	// the workflow unattended (Codex, #1109).
	readErr error
	// redact is what every printed line passes through — see [Session.SetRedactor].
	redact func(string) string
	// redactValue is what every *structured* value passes through before it is
	// rendered — see [Session.SetValueRedactor].
	redactValue func(any) any
	// ended is set when the session itself abandoned the run (`quit` at a
	// breakpoint): the one command advertised as leaving must not be
	// answered with another prompt, so the autopsy checks it and stays shut
	// (Codex, #1107).
	ended bool
	// interrupted is set when a console reported [ErrConsoleInterrupted]
	// rather than a stream that ran out. The two must not be confused: a
	// stream that ends resumes the run and lets it finish, and doing that on
	// a ctrl-C would be answering "stop" by running the rest of somebody's
	// workflow unattended.
	interrupted bool

	// at is what the session is prompting about right now, so that
	// [Session.Complete] — called from the console's own goroutine while a
	// boundary is parked — can answer against the scope the prompt is for.
	at promptSubject

	// steps are the ids a caller said this run may reach ([Options.Steps]),
	// and seen are the ids this session has watched go past. Both feed
	// completion for `break` and `until`; see [Session.reachableSteps].
	steps []string
	seen  map[string]struct{}

	// seenShort reports that seen refused an id it had not already got,
	// which makes it a *prefix* of the run rather than the run. Completion
	// reads it; see [Session.sawStep] for why it cannot be inferred later.
	seenShort bool
}

// A promptSubject is what one prompt is about: the scope to answer questions
// against, whatever is bound around it, and which of the two prompts it is.
//
// A struct rather than three fields, because they are set and read as one thing
// and a prompt holding this scope with that binding would answer about a
// position the run is not in.
type promptSubject struct {
	// scope is what an inspection evaluates against.
	scope *v1.Scope

	// extra are the bare bindings layered over it — the autopsy's `run` and
	// `vars`, and nothing at a breakpoint.
	extra map[string]ref.Val

	// autopsy reports which of the two prompts this is, which decides the
	// verbs there are to offer.
	autopsy bool
}

// New returns a session configured by opts.
func New(opts Options) (*Session, error) {
	if len(opts.Breakpoints) > MaxBreakpoints {
		return nil, fmt.Errorf("a session may hold %d breakpoints, and %d were named", MaxBreakpoints, len(opts.Breakpoints))
	}

	s := &Session{
		out:         opts.Out,
		emit:        opts.Emit,
		console:     opts.Console,
		clock:       opts.Clock,
		breakpoints: make(map[string]breakpoint, len(opts.Breakpoints)),
		done:        make(chan struct{}),
		steps:       slices.Clone(opts.Steps),
		seen:        map[string]struct{}{},
	}
	if s.out == nil {
		s.out = io.Discard
	}
	if opts.In != nil && opts.Console == nil {
		s.in = bufio.NewScanner(opts.In)
		// One byte over [MaxCommandBytes], and the byte is the line
		// terminator rather than command room: bufio.Scanner grows its buffer
		// to at most this and must hold the delimiter as well as the token,
		// so a buffer of exactly MaxCommandBytes rejects a line of exactly
		// MaxCommandBytes with ErrTooLong — a command every other bound on
		// this surface accepts, refused by the reader (Codex, #1109).
		s.in.Buffer(make([]byte, 0, 4096), MaxCommandBytes+len("\n"))
	}
	for _, id := range opts.Breakpoints {
		if id = strings.TrimSpace(id); id != "" {
			s.breakpoints[id] = breakpoint{}
		}
	}

	// Where the first stop lands, and why it depends on nothing else: an
	// author who named a step asked to go there.
	s.mode = modeStop
	if len(s.breakpoints) > 0 {
		s.mode = modeRun
	}

	return s, nil
}

// Script returns the commands this session accepted, in order. Feeding them
// to a new session's In reproduces the same decisions against the same
// workflow — the replay half of #928's record-and-replay.
func (s *Session) Script() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.script...)
}

// ScriptTruncated reports whether the recording stopped early at
// [MaxScriptCommands]. A truncated script replays a prefix of the session, not
// the session.
func (s *Session) ScriptTruncated() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.scriptTruncated
}

// errQuit is what a `quit` command returns through [v1.Debugger.BeforeStep],
// which is how a session ends a run it is holding.
//
// It wraps [v1.ErrDebugSessionEnded] so a harness can tell an *abandoned* run
// from a failed one — see that sentinel for why the distinction decides a
// verdict — while the message stays this session's own, naming the command the
// person actually typed.
var errQuit = fmt.Errorf("debug session ended by the `quit` command: %w", v1.ErrDebugSessionEnded)

// BeforeStep implements [v1.Debugger]: the run is held here for as long as the
// session's reader takes to say otherwise.
func (s *Session) BeforeStep(ctx context.Context, node *v1.Node, scope *v1.Scope) error {
	// Before shouldStop, deliberately: see [Session.promptMu]. Deciding to
	// stop is a read-modify of the same mode a sibling branch is deciding
	// against, and one script cannot answer two prompts.
	s.promptMu.Lock()
	defer s.promptMu.Unlock()

	s.sawStep(node.GetId())

	if !s.shouldStop(ctx, node.GetId(), scope) {
		return nil
	}

	// What this prompt is about, so that a completion arriving from a console's
	// own goroutine answers against the scope the run is actually held in.
	// Cleared on the way out: a session that kept the last scope alive would
	// answer questions about a position the run has left.
	s.prompting(promptSubject{scope: scope})
	defer s.prompting(promptSubject{})

	s.announce(node)

	for {
		line, ok, readErr := s.readCommand(ctx)
		if readErr != nil {
			// Cancelled mid-prompt: the person interrupted the run this
			// session was holding, and the engine unwinds it as the
			// cancellation it is rather than as a console that wandered off.
			return readErr
		}
		if !ok {
			// Interrupted at the prompt — ctrl-C at a terminal — which ends
			// the run exactly as `quit` does. Checked before the arms below,
			// because those resume the run, and answering "stop" by running
			// the rest of somebody's workflow unattended is the one outcome
			// this must not have. Recorded as `quit` so the replay script
			// says what the session did.
			if s.wasInterrupted() {
				s.record("quit")
				s.mu.Lock()
				s.ended = true
				s.mu.Unlock()
				s.printfTone(ToneWarning, "(interrupted — ending the run here, as `quit` does)\n")

				return errQuit
			}

			// The console is gone: a replay script that ran out, or a
			// terminal that closed. The run resumes and finishes rather than
			// being held by a debugger that is not there — #928's own answer
			// to its question 4, that a run held paused by a vanished
			// debugger is an availability incident. Said out loud, because a
			// run that finished the rest of itself unattended is something
			// the reader has to know happened.
			if why := s.consoleEnded(); why != "" {
				s.printfTone(ToneDanger,
					"(%s — continuing to the end of the run, unattended)\n", why)
			} else {
				s.printfTone(ToneWarning, "(no more commands — continuing to the end of the run)\n")
			}
			s.resume(modeRun, "")

			return nil
		}

		resumed, err := s.dispatch(ctx, line, node, scope)
		if err != nil {
			return err
		}
		if resumed {
			return nil
		}
	}
}

// StepFinished implements [v1.RunObserver]. The account is what a session
// prints after `step`: an author who advanced one step wants to see what it
// produced, and this is the same record `flow test`'s transcript renders, from
// the same seam, rather than a second bookkeeping of it.
func (s *Session) StepFinished(id string, outputs *v1.Node_Outputs, err error, tolerated bool) {
	s.sawStep(id)

	text := s.stepOutcomeText(outputs, err, tolerated)

	s.mu.Lock()
	s.lastOutcome = id + " " + text
	s.mu.Unlock()

	// The tone is the outcome's, matching the transcript's reading of the
	// same three cases: a failure the run absorbs is a warning, one it does
	// not is danger, and everything else is account.
	tone := ToneInfo
	switch {
	case err != nil && tolerated:
		tone = ToneWarning
	case err != nil:
		tone = ToneDanger
	}
	s.printfTone(tone, "  %s %s\n", id, text)
}

// StepSkipped implements [v1.RunObserver]. A skipped step never reaches
// [Session.BeforeStep] — there is no work to hold — so this is the only place
// a session can say the `if:` decided against it.
func (s *Session) StepSkipped(id string) {
	// Remembered as a step this run reaches even though it did not run: a
	// breakpoint on a step whose `if:` was false this time is exactly what
	// somebody sets when they are trying to find out why.
	s.sawStep(id)

	s.printf("  %s skipped (`if:` was false)\n", id)
}

// WaitStarted implements [v1.RunObserver], reporting a wait as it parks.
func (s *Session) WaitStarted(id, signal string, timeout time.Duration, bounded bool) {
	switch {
	case signal == "":
		s.printf("  %s waiting %s\n", id, timeout)
	case bounded:
		s.printf("  %s waiting for signal %q (timeout %s)\n", id, signal, timeout)
	default:
		s.printf("  %s waiting for signal %q (no timeout)\n", id, signal)
	}
}

// shouldStop answers the mode question for one step id.
//
// A pure query, deliberately: every path out of [Session.BeforeStep] sets the
// mode on its way — a command that resumes sets it, and the exhausted-console
// path sets it too — so there is no state for this to carry forward and an
// `until` cannot outlive its arrival. An earlier draft cleared the mode here
// as well, which read like the thing keeping a stale `until` from stopping at
// a step nobody named; it could not, because no boundary is reached without a
// resume in between, and a mutation removing it failed no test. Answering the
// question and changing nothing is the honest shape.
func (s *Session) shouldStop(ctx context.Context, id string, scope *v1.Scope) bool {
	s.mu.Lock()
	at, isBreakpoint := s.breakpoints[id]
	mode, until := s.mode, s.until
	s.mu.Unlock()

	if isBreakpoint && s.breakpointHolds(ctx, id, at, scope) {
		return true
	}

	switch mode {
	case modeStop:
		return true
	case modeUntil:
		return until == id
	default:
		return false
	}
}

// A breakpoint is a step id and, optionally, the condition that decides
// whether reaching it stops the run.
//
// The condition is a [v1.Value] holding a parsed expression rather than the
// source text, because it is compiled once when `break` accepts it and
// evaluated at every arrival — see [Session.addBreakpoint] for why those are
// not the same moment. Source is kept beside it for `breakpoints` to print and
// for the replay script to reproduce.
type breakpoint struct {
	source    string
	condition *v1.Value
}

// breakpointHolds answers whether an arrival at a breakpoint should stop.
//
// Evaluated outside s.mu — the condition is the author's own CEL and calling
// into the evaluator under the session lock would hold it for the length of an
// expression somebody else wrote. [Session.BeforeStep] already serialises
// boundaries through promptMu, so nothing here races a second arrival.
//
// The condition is the step's own `if:`, deliberately: same function, same
// scope, same refusal of a non-boolean ([v1.EvalConditionInScope]). A second
// bool-coercion rule beside the engine's would be two answers to one question,
// and a breakpoint that disagreed with the `if:` written on the same step is a
// debugger lying about the language it debugs.
//
// An error stops the run and says why. That is fail-closed read for a
// debugger: the component that allows when it cannot decide is the one that
// lets a run proceed unattended past the point somebody asked to hold it, and
// a breakpoint that looks set and silently never fires is the outcome with no
// symptom. Stopping also makes the reporting free — the session is parked, so
// the reason prints once rather than once per arrival.
func (s *Session) breakpointHolds(ctx context.Context, id string, at breakpoint, scope *v1.Scope) bool {
	if at.condition == nil {
		return true
	}

	// # A condition that cannot be evaluated does not stop
	//
	// A step id names a *visibility domain* rather than a step: two sibling
	// loops may each declare a body step called `page`, and this map is keyed
	// by id, so one `break page if total == 3` is a breakpoint at both. In the
	// loop that binds no `total` the condition cannot be asked at all — a
	// different thing from a question whose answer is no — and holding the run
	// there parks it in the loop the author was not debugging (Codex, #1116).
	//
	// An earlier version answered that by requiring every name the condition
	// reads to be bound before evaluating. That was wrong in both directions
	// at once, which is what settled this design. Too strict:
	// `n == 3 || fallback == 4` is true whenever `n` is 3, because CEL
	// short-circuits, and a preflight over both names declined it. Too weak:
	// `steps.setup.ok` reads the root `steps`, which resolves everywhere, so
	// the check passed and evaluation failed on the member anyway. No set of
	// names satisfies both, because *which references a condition needs* is a
	// question only the evaluator can answer: it depends on the values.
	//
	// So the evaluator answers it. An error means this occurrence could not
	// answer, and the run is not held here.
	//
	// That reverses this change's first fail-closed reading, and the notice is
	// what makes the reversal safe. The argument for stopping was that a
	// breakpoint which looks armed and never fires is a failure with no
	// symptom — true, and it is the *silence* that makes it so rather than the
	// not-stopping. A declined arrival says why, once per breakpoint, so a
	// mistyped name reports and never fires while a condition about a sibling
	// domain reports and fires where it belongs. Stopping bought nothing the
	// notice does not, and cost a hold in the wrong loop on every legal
	// workflow that reuses an id.
	holds, err := v1.EvalConditionInScope(ctx, at.condition, scope)
	if err != nil {
		s.noteDeclined(id, err)

		return false
	}

	return holds
}

// resume sets what happens at the next boundary.
func (s *Session) resume(m mode, until string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mode = m
	s.until = until
}

// announce prints where the run has stopped.
func (s *Session) announce(node *v1.Node) {
	at := ""
	if s.clock != nil {
		now := s.clock.Now()

		s.mu.Lock()
		if !s.hasStart {
			s.started, s.hasStart = now, true
		}
		elapsed := now.Sub(s.started)
		s.mu.Unlock()

		at = fmt.Sprintf("   t=%s", elapsed)
	}

	s.printfTone(ToneBreak, "break at %s (%s)%s\n", node.GetId(), NodeKind(node), at)
}

// Close releases the session's reader.
//
// A session reads its console on a goroutine (see [Session.readCommand]), and
// that goroutine parks on an unbuffered send whenever a line arrives with
// nobody left to take it — which is every session whose run ended while a
// command was still in flight. In a process that is about to exit, parked is
// free. In a server that answers debug calls for as long as it is up, it is a
// goroutine, a scanner and a script retained per call, without bound (Codex,
// #1109).
//
// So a caller that outlives its sessions closes them. Idempotent, safe to call
// from any goroutine, and safe to call on a session whose reader never
// started: it releases what exists rather than requiring the caller to know
// what that was.
//
// It does not close the underlying reader — a session does not own the
// caller's io.Reader, and closing stdin under `flow test --debug` would end
// far more than the session.
func (s *Session) Close() error {
	s.stopOnce.Do(func() { close(s.done) })

	return nil
}

// readCommand reads one line. ok is false once the input has ended, and err is
// the context's own error when cancellation ended the wait instead.
//
// The read runs on a goroutine of its own, because the run being held is
// cancellable and neither a [bufio.Scanner] nor a terminal is: ctrl-C's first
// signal cancels the command's context for a graceful stop, and a session
// blocked synchronously in a read would hold the process hostage for a second,
// harder signal (Codex, #1109). One reader goroutine per session, started at
// the first prompt, and it exits when the session is done — either because the
// input ended, or because [Session.Close] or a cancelled prompt released it.
//
// It reads on request rather than continuously, which a [Console] requires and
// a stream is no worse for. A console draws its own prompt as part of reading,
// so a reader running ahead would paint a `debug> ` over the account of the
// step that had just finished; and a reader that has already consumed a line
// nobody asked for has taken it out of a stream the process may still own after
// the session ends.
func (s *Session) readCommand(ctx context.Context) (line string, ok bool, err error) {
	s.mu.Lock()
	closed, scanner, console := s.closed, s.in, s.console
	s.mu.Unlock()

	if closed || (scanner == nil && console == nil) {
		return "", false, nil
	}

	s.readOnce.Do(func() {
		s.lines = make(chan string)
		// Buffered by one, so that asking for a line never blocks: at most one
		// request is outstanding, and a reader that has already exited leaves
		// the request sitting in the buffer where nothing waits on it.
		s.wants = make(chan struct{}, 1)
		go s.read(scanner, console)
	})

	select {
	case s.wants <- struct{}{}:
	case <-s.done:
		return "", false, nil
	}

	// The prompt is the session's to draw only when it is reading a stream. A
	// console draws its own, because a line editor has to know the prompt's
	// width to put the cursor anywhere.
	if console == nil {
		s.printfTone(TonePrompt, Prompt)
	}

	select {
	case line, open := <-s.lines:
		if !open {
			s.mu.Lock()
			s.closed = true
			s.mu.Unlock()

			return "", false, nil
		}

		return line, true, nil

	case <-ctx.Done():
		// The run is being torn down around this prompt; closed so no later
		// boundary or autopsy asks a console the person already interrupted,
		// and the reader released so it does not outlive the run it was for.
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		_ = s.Close()

		return "", false, ctx.Err()
	}
}

// read is the reader goroutine: one line per request, until the input ends.
func (s *Session) read(scanner *bufio.Scanner, console Console) {
	// The reader's own verdict, published before the close that a receiver
	// reads as "the input is gone" — so a session that finds it closed can
	// tell an input that ended from one that broke, and an interrupt from
	// either. Set first, closed second: a receiver woken by the close must
	// find the reason already there.
	var readErr error
	defer func() {
		s.mu.Lock()
		s.readErr = readErr
		s.interrupted = errors.Is(readErr, ErrConsoleInterrupted)
		s.mu.Unlock()

		close(s.lines)
	}()

	for {
		select {
		case <-s.wants:
		case <-s.done:
			return
		}

		var line string
		if console != nil {
			text, err := console.Prompt()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					// io.EOF is the console saying the input ended, which is
					// not a reason worth naming: it is the ordinary way a
					// script or a terminal finishes. Anything else is.
					readErr = err
				}

				return
			}
			line = text
		} else {
			if !scanner.Scan() {
				readErr = scanner.Err()

				return
			}
			line = scanner.Text()
		}

		// Never a bare send: with the session done there is no receiver, and a
		// bare send would park this goroutine for the life of the process
		// holding everything it closed over.
		select {
		case s.lines <- line:
		case <-s.done:
			return
		}
	}
}

// wasInterrupted reports whether the input ended because somebody interrupted
// the prompt rather than because it ran out.
func (s *Session) wasInterrupted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.interrupted
}

// prompting records what the session is currently prompting about, for
// [Session.Complete] to answer against.
func (s *Session) prompting(at promptSubject) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.at = at
}

// sawStep remembers a step id this session has watched go past, so that
// `break` and `until` complete over the run even where the caller named no
// steps.
//
// Bounded, because the number of ids a run produces is the workflow's rather
// than this session's: a `call:` chain reaches steps no one file lists. Past the
// bound it stops remembering rather than growing.
//
// What it refuses, it records. The bound used to be justified by the answer
// being cut at the same number anyway — which is wrong in the one direction
// that matters, and wrong for the same reason bounding before filtering was
// (see [Session.reachableSteps]): the answer's cap runs *after* the prefix
// filter, so `break zzz` over a run whose 513th id was `zzz_step` matches
// nothing, and would say so as though nothing were there (Codex, #1114). The
// dropped id cannot be recovered — this cache is filled in arrival order by a
// run that has since moved past those steps, and a later prompt's scope no
// longer carries their outputs — so what is owed the author is the notice that
// the list is short.
//
// Only an id the cache does not already hold sets it: refusing a repeat loses
// nothing, and a run that loops over one step for an hour should not report
// itself truncated.
func (s *Session) sawStep(id string) {
	if id == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.seen) >= celcomplete.MaxCandidates {
		if _, held := s.seen[id]; !held {
			s.seenShort = true
		}

		return
	}
	s.seen[id] = struct{}{}
}

// noteDeclined reports, once per breakpoint, that a condition could not be
// evaluated at a step, so the run was not held there.
//
// Once, because the alternative is a line per arrival — and the case this
// exists for is a loop, where that is a line per iteration. Once is enough to
// tell a mistyped name from a condition about a different `page`: the first
// never fires anywhere and says so, the second fires where it belongs.
//
// This notice is what makes not-stopping safe rather than silent — see
// [Session.breakpointHolds], which reversed a fail-closed rule on the strength
// of it.
func (s *Session) noteDeclined(id string, err error) {
	s.mu.Lock()
	if s.notedUnbound == nil {
		s.notedUnbound = map[string]struct{}{}
	}
	_, already := s.notedUnbound[id]
	s.notedUnbound[id] = struct{}{}
	s.mu.Unlock()

	if already {
		return
	}

	s.printfTone(ToneWarning, "breakpoint at %s: the condition could not be evaluated here, so the run was not held: %v\n", id, err)
}

// consoleEnded says why the command stream stopped, in words an author can
// act on, or "" where it simply ran out.
//
// The one reason that is not a broken pipe is the one worth naming: a command
// longer than [MaxCommandBytes] is a bound this package advertises, and an
// author who hit it deserves to be told which bound they hit rather than to
// watch their run finish without them.
func (s *Session) consoleEnded() string {
	s.mu.Lock()
	err := s.readErr
	s.mu.Unlock()

	switch {
	case err == nil:
		return ""

	case errors.Is(err, bufio.ErrTooLong):
		return fmt.Sprintf("a command was longer than the %d bytes one may be, and a scanner "+
			"cannot carry on past it", MaxCommandBytes)

	default:
		return "the console could not be read: " + err.Error()
	}
}

// record appends an accepted command to the replay script.
func (s *Session) record(line string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.script) >= MaxScriptCommands || s.scriptBytes+len(line) > MaxScriptBytes {
		s.scriptTruncated = true

		return
	}
	s.scriptBytes += len(line)
	s.script = append(s.script, line)
}

// printfTone writes one classified fragment: through Emit when the caller
// installed one, to Out otherwise. Every write in this package goes through
// here, so a session is colourable without a second output path.
func (s *Session) printfTone(tone Tone, format string, args ...any) {
	text := s.redactText(fmt.Sprintf(format, args...))

	// Held across the call, not just around a field read: Emit is the
	// caller's, and the two this repository ships both accumulate — the MCP
	// adapter appends to a slice and adds up bytes, the CLI writes to a
	// terminal. Serializing here is what makes the contract [v1.RunObserver]
	// states this session's to keep rather than every embedder's to
	// rediscover.
	s.outMu.Lock()
	defer s.outMu.Unlock()

	if s.emit != nil {
		s.emit(text, tone)

		return
	}
	fmt.Fprint(s.out, text)
}

// SetRedactor installs what every line this session prints passes through, or
// clears it with nil.
//
// A debugger is a reveal — it narrates each step's values as they arrive and
// `inspect` reaches whatever is in scope — so on a surface whose ordinary
// rendering withholds a value, the session must withhold it too or it is a
// second door around the first (Codex, #1109). It cannot decide that itself:
// what is sensitive is a property of the workflow and the case, which the
// caller running them knows and this package deliberately does not.
//
// So the caller hands the rule in, and hands in nil when the run it applies to
// is over. Applied to the *rendered line* rather than to values on the way in,
// because that is the one place everything passes: a step's account, an
// inspection's answer, a failure at the autopsy. A redactor installed here can
// only ever make output smaller, so a caller that installs none is exactly as
// this behaved before.
//
// Evaluation is untouched. A `${...}` in the file still sees the real value,
// and an inspection still compares against it — only what prints withholds,
// which is the same split [flowtest]'s transcript already lives by.
func (s *Session) SetRedactor(redact func(string) string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.redact = redact
}

// redactText applies the installed redactor, if any.
// SetValueRedactor installs what every structured value this session renders
// passes through, or clears it with nil.
//
// The companion to [Session.SetRedactor], and it exists because a text
// backstop cannot see a shape. A caller's redaction set holds both the values
// it recognises and a set of substrings to catch them in rendered prose, and
// the substring half deliberately omits short ones: replacing every "7" in
// every line would make a transcript unreadable, and that omission is correct
// for text. But a *value* is matched by equality, not by looking like
// something — so a sensitive `credentials: [7]` is recognised whole here while
// no substring in the world would have caught the 7 (Codex, #1109).
//
// So values redact by identity before rendering, and the rendered line still
// goes through the text redactor afterwards. Two seams because there are two
// questions: is this the value, and does this line contain it.
func (s *Session) SetValueRedactor(redact func(any) any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.redactValue = redact
}

// redactedValue is v through the installed value redactor, or v.
func (s *Session) redactedValue(v any) any {
	s.mu.Lock()
	redact := s.redactValue
	s.mu.Unlock()

	if redact == nil {
		return v
	}

	return redact(v)
}

func (s *Session) redactText(text string) string {
	s.mu.Lock()
	redact := s.redact
	s.mu.Unlock()

	if redact == nil {
		return text
	}

	return redact(text)
}

// noteWithholding says, once, that this scope is not the one the checks read.
//
// The bindings an autopsy evaluates against are the redacted ones, so
// `inspect vars.token` answers `[redacted]` — which is legible — and
// `inspect vars.token == "the real value"` answers *false*, which is not: the
// same expression in `expect.check` saw the real binding and may well have
// been true. That disagreement is deliberate and stays. Handing the raw value
// to CEL and redacting only the printed line would restore the agreement and
// turn a withheld value into a programmable oracle — `startsWith`, `size()`
// and a slice each answer truthfully about a secret, one call at a time,
// which is a worse door than the one printing was closed against (Codex,
// #1109). Fail closed, and say so, rather than being quietly wrong.
func (s *Session) noteWithholding() {
	s.mu.Lock()
	withholding := s.redact != nil
	s.mu.Unlock()

	if !withholding {
		return
	}

	s.printfTone(ToneWarning,
		"(this case withholds sensitive values: they are withheld from these bindings, not just "+
			"from what prints, so a comparison against a real value answers false here even where "+
			"the same check was true)\n")
}

func (s *Session) printf(format string, args ...any) {
	s.printfTone(ToneInfo, format, args...)
}

// stepOutcomeText renders one step's recorded outcome for the console.
func (s *Session) stepOutcomeText(outputs *v1.Node_Outputs, err error, tolerated bool) string {
	if err != nil {
		if tolerated {
			return "failed (tolerated by continue_on_error): " + err.Error()
		}

		return "FAILED: " + err.Error()
	}

	named := outputs.GetNamedValues()
	if len(named) == 0 {
		return "completed"
	}

	names := make([]string, 0, len(named))
	for name := range named {
		names = append(names, name)
	}
	sort.Strings(names)

	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+": "+s.valueText(named[name]))
	}

	// Redacted *before* the cap, not after. capRunes keeps the first
	// MaxInspectRunes of the rendering, and a secret longer than that survives
	// truncation as a prefix no substring match can find — so a cap applied
	// first would expose the first 4096 runes of exactly the value the
	// redactor exists to withhold (Codex, #1109). Order is the whole fix.
	return "-> " + capRunes(s.redactText(strings.Join(parts, ", ")), MaxInspectRunes)
}

// valueText renders one output value. A value that is not a resolved literal
// — a secret reference above all — renders as what it is rather than as what
// it points at.
func (s *Session) valueText(value *v1.Value) string {
	if ref := value.GetSecretRef(); ref != nil {
		return fmt.Sprintf("secret(%s://%s)", ref.GetScheme(), ref.GetName())
	}
	lit := value.GetLiteral()
	if lit == nil {
		return "…"
	}
	native, err := v1.LiteralToGo(lit)
	if err != nil {
		return "…"
	}

	return nativeText(s.redactedValue(native))
}

// nativeText renders a plain Go value the way an author reads data: as JSON,
// which is the encoding every other surface in this repo hands them. Anything
// JSON cannot encode falls back to Go's own rendering rather than to nothing,
// because a debugger that answers "…" has failed at its one job.
func nativeText(native any) string {
	encoded, err := json.Marshal(native)
	if err != nil {
		return fmt.Sprint(native)
	}

	return string(encoded)
}

// refValText renders an inspection's result through the same conversion a
// `value:` step's result takes — [cel.RefValueToValue] then [v1.LiteralToGo],
// exactly as EvalValueNode does — so what an inspection prints and what the
// same expression would produce in the file are one rendering of one value,
// rather than two that can drift.
func (s *Session) refValText(out ref.Val) string {
	lit, err := cel.RefValueToValue(out)
	if err != nil {
		return fmt.Sprint(out.Value())
	}
	native, err := v1.LiteralToGo(lit)
	if err != nil {
		return fmt.Sprint(out.Value())
	}

	// Redacted as a *value*, before it becomes text — see
	// [Session.SetValueRedactor]. The two fallbacks above deliberately do not:
	// a value this conversion could not read is not one an equality can
	// recognise either, and what covers them is the text redactor the caller
	// of this applies to whatever comes back.
	return nativeText(s.redactedValue(native))
}

// capRunes truncates text to at most limit runes, saying that it did.
func capRunes(text string, limit int) string {
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}

	return string(runes[:limit]) + fmt.Sprintf("… (%d more)", len(runes)-limit)
}

// Autopsy holds the session open one last time, after a case's run has ended
// and its expectations have been judged: the failures print, and the scope is
// still there to be questioned. It is the other half of "debug tests / use
// tests to debug" — a breakpoint stops a run *before* it surprises you, and
// the autopsy is for the case that already did: explore the finished run,
// craft the claim with `inspect`, and leave with the `expect.check:` entry
// the file was missing.
//
// extra carries the post-run bindings the case's own `expect.check:` was
// judged under — the file's `vars` and the `run` root extended with
// failed/error — so an inspection here answers exactly as the failing check
// read (Codex, #1107): diagnosing `vars.x == ...` with a prompt that cannot
// see `vars` would be an autopsy of a different body. Nil binds nothing
// extra, which is every caller outside flowtest.
//
// failures are the rendered verdicts, printed as the failures they are. The
// commands are the session's ordinary ones; the movement verbs (`step`,
// `continue`, `until`) and `quit` all just leave, because there is no run
// left to move — and leaving changes nothing: the verdict was reached before
// this was called, so an autopsy can never turn a red case green or a green
// one red. Commands accepted here are recorded like any others, so a
// replayed script re-runs the same questions over the same corpse.
//
// flowtest calls this only for a failing case under `--debug`, discovering
// it by capability the way it discovers a session that observes — a
// [v1.Debugger] that does not implement it simply ends when the run ends.
func (s *Session) Autopsy(ctx context.Context, scope *v1.Scope, extra map[string]ref.Val, failures []string) {
	// The same admission [Session.BeforeStep] takes, for the same reason:
	// this prompts, and a prompt reads the one command stream. The run is
	// over by the time this is called and no boundary should still be
	// parked, but "should" is not what a lock is for.
	s.promptMu.Lock()
	defer s.promptMu.Unlock()

	// A session that quit at a breakpoint said it was done: the case fails
	// (abandoning a run is a verdict), and answering the command advertised
	// as leaving with another prompt would make `quit` a lie (Codex, #1107).
	s.mu.Lock()
	ended := s.ended
	s.mu.Unlock()
	if ended {
		return
	}

	// What this prompt is about — the finished run's scope and the bindings
	// its checks were judged under — so that completion here offers the same
	// names an inspection here resolves. Cleared on the way out, as the
	// boundary's is.
	s.prompting(promptSubject{scope: scope, extra: extra, autopsy: true})
	defer s.prompting(promptSubject{})

	s.printfTone(ToneBreak, "autopsy: the case failed %d expectation(s); the run is over, but its scope is still here\n", len(failures))
	for _, failure := range failures {
		s.printfTone(ToneDanger, "  %s\n", failure)
	}
	s.printf("(`inspect` questions the finished run; `quit` or `continue` leaves — the verdict is already in)\n")
	s.noteWithholding()

	for {
		line, ok, readErr := s.readCommand(ctx)
		if readErr != nil || !ok {
			// A cancellation or a finished stream both end the autopsy the
			// same way: there is no run left for either to change. A stream
			// that *broke* still says so before going — there is no run to
			// rescue here, but an author whose command was refused should
			// learn that rather than watch the prompt vanish (Codex, #1109).
			//
			// An interrupt is a third way to leave and needs no sentence: the
			// verdict was reached before this was called, so ctrl-C here ends
			// a conversation rather than a run.
			if readErr == nil && !s.wasInterrupted() {
				if why := s.consoleEnded(); why != "" {
					s.printfTone(ToneDanger, "(%s — leaving the autopsy)\n", why)
				}
			}

			return
		}

		verb, rest := split(line)
		switch verb {
		case "", "step", "s", "continue", "c", "until", "u", "quit", "q":
			s.record("quit")

			return

		case "inspect", "p":
			expression := strings.TrimSpace(rest)
			if expression == "" {
				s.printfTone(ToneWarning, "inspect needs an expression: inspect steps.build.artifact\n")
				continue
			}
			s.record("inspect " + expression)
			s.inspectWith(ctx, expression, scope, extra)

		case "scope":
			s.record("scope")
			s.showScopeWith(scope, extra)

		case "help", "h", "?":
			s.printf(`inspect <expr>   evaluate a CEL expression against the finished run
scope            list what the run can still name
quit, q          leave the autopsy (so do step/continue — the run is over)
`)

		default:
			s.printfTone(ToneWarning, "unknown command %q — try `help`\n", verb)
		}
	}
}
