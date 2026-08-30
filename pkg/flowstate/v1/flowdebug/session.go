package flowdebug

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
	"google.golang.org/protobuf/proto"

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

	// MaxScopeBindings bounds how many names one [Session.ScopeProto] answer
	// carries, and therefore how many values it resolves, whatever its caller
	// asked for.
	//
	// A caller's budget is a *request* and this is the producer's ceiling. They
	// are different things for the reason CLAUDE.md gives twice over, because
	// this bound was arrived at in two steps and each step is the same lesson:
	// bounding one resource does not bound another the peer controls the ratio
	// to.
	//
	// First, [v1.DefaultCostLimit] bounds a single evaluation and nothing
	// bounded how many of them one answer performs — a workload chooses how
	// many names a scope holds, and a negative limit asked for all of them, so
	// a caller could buy unbounded compilation with one message. Then, capping
	// the *evaluations* still left a message materializing a binding and an
	// expression string per name, so the CPU, the allocation and the response
	// size were bounded by nothing an evaluation ceiling could see (Codex,
	// #1194, twice).
	//
	// So the ceiling is on the bindings, and the evaluations follow: a value
	// can only be resolved for a binding that was carried, so one number bounds
	// both rather than two numbers that have to be kept in agreement.
	//
	// 500 rather than something smaller because a producer ceiling narrower
	// than what a front legitimately asks for would quietly hand it less than
	// it requested: this repository's two renderers cap themselves at 200
	// (`debugpane.MaxScopeEvaluations`) and 500 (`flowdap.MaxScopeVariables`),
	// so this is the larger of them. Nothing here has a use for more.
	//
	// The totals are untouched by it. [Session.ScopeProto] still reports what
	// the run can reach, so an elision says how much it elided rather than
	// reporting the bound back as the scope's size — the same distinction
	// [MaxScopeNames] draws for a line somebody reads.
	MaxScopeBindings = 500

	// MaxStepWindow bounds how many rows one [Session.StepWindowProto] answer
	// carries, whatever its caller asked for.
	//
	// [Session.Steps] takes its window from its caller because an in-process
	// caller pays for its own copy; a wire answer is a *message*, and the
	// inventory's size is the workload's choice, so a negative or enormous
	// limit bought an O(N) response from a caller this API explicitly treats as
	// untrusted (Codex, #1194). The same argument as [MaxScopeBindings], on the
	// other listing.
	//
	// Deliberately the same number as that one and written as that one, so the
	// two cannot drift: they answer one question — how many entries may one
	// debug answer carry — for two listings. A pane asks for a terminal's
	// height and a wire client pages by [DebugStepWindow.total], so nothing
	// here has a use for more.
	MaxStepWindow = MaxScopeBindings
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

	// Steps are the steps this run may reach, for completing `break` and
	// `until` before the run has been anywhere, and for the list
	// [Session.Steps] answers.
	//
	// Optional, and a session with none still completes over the steps it has
	// watched go past — see [Session.reachableSteps]. It is a caller's answer
	// rather than something this package derives because the seam it is
	// installed on ([v1.Debugger]) is handed one step at a time and never the
	// workflow: `flow run local --debug` has the specification in hand, and an
	// embedder with only the seam honestly does not.
	//
	// Unbounded here, deliberately: these are steps the caller already holds a
	// whole workflow's worth of, so storing them costs nothing new. What is
	// bounded is the *answer* — in [Session.Complete], and in the window
	// [Session.Steps] copies.
	//
	// Each entry names the workflow that declares it as well as its id, because
	// an id is not an identity across a `call:`: a caller and a callee may both
	// declare `build` (`eval.go:1804-1812`), and a flattened inventory of the
	// two holds two rows nothing can tell apart. [Step.State] is ignored here —
	// nothing has happened to any of them yet.
	Steps []Step

	// Controlled says this session is driven by [Session.Control] as well as,
	// or instead of, by lines of text.
	//
	// It has to be declared rather than inferred, because the two things a
	// session does when it has nothing to read are opposites. A session with
	// no In and no Console resumes at every stop — that is what makes an
	// unconfigured session harmless rather than hung, and it is checked
	// before anything is pending, so "wait, a command may arrive later" is
	// not something the session can work out for itself.
	//
	// The cost is the other side of the same coin, and it is the caller's to
	// accept: a controlled session with nobody controlling it holds the run
	// at its first stop until [Session.Close]. That is the same bargain a
	// terminal makes, and the same escape — see [Session.Control].
	Controlled bool
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

	mu    sync.Mutex
	mode  mode
	until string
	// untilCondition optionally gates the stop `until` names, exactly as a
	// breakpoint's condition gates its arrival: same compiler, same evaluator,
	// same declined-arrival notice. One-shot with the mode that carries it —
	// every resume clears both.
	untilCondition *v1.Value
	breakpoints    map[string]breakpoint

	// notedUnbound remembers which condition-gated stops — breakpoints and
	// `until` — have already reported a condition they could not evaluate, so
	// the notice is one line rather than one per iteration. See
	// [Session.noteDeclined].
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
	// and seen are the ids this session has watched go past, each against what
	// it was last watched doing. Both feed completion for `break` and `until`
	// (see [Session.reachableSteps]); the states feed [Session.Steps].
	//
	// One map rather than a second one beside it, because the two questions
	// have one answer: an id this session has seen is exactly an id it has
	// watched *do* something, and a separate state map would be a second thing
	// to bound, a second thing to truncate, and a second place for the two to
	// disagree about which ids the run reached.
	steps []Step
	seen  map[string]StepState

	// seenOrder is the same ids in arrival order, for a caller that named no
	// steps: a map has none, and a step list in map order is a different list
	// every time it is drawn. Held under the same bound as seen — an id seen
	// refuses is one this never hears about. Its entries carry no workflow,
	// because nothing that reaches [Session.sawStep] knows one.
	seenOrder []Step

	// sharedIDs are the ids more than one workflow in steps declares, and
	// sharedCount how many rows carry one.
	//
	// Computed once in [New] because steps is a caller's answer and does not
	// change. They are what makes [Session.Steps] fail closed on an outcome
	// nothing can attribute: [v1.RunObserver] hands over bare ids, so a
	// `StepFinished("build")` across a `call:` boundary names two rows and
	// belongs to one.
	sharedIDs   map[string]struct{}
	sharedCount int

	// seenShort reports that seen refused an id it had not already got,
	// which makes it a *prefix* of the run rather than the run. Completion
	// reads it; see [Session.sawStep] for why it cannot be inferred later.
	seenShort bool

	// wantOutstanding says the reader has been asked for a line and has not
	// delivered one yet.
	//
	// The buffered `wants` channel was documented as meaning "at most one
	// request is outstanding", and that was true while a line could only ever
	// arrive from the reader. Once a control command can satisfy a boundary
	// instead, the reader keeps owing a line that nobody collects — so the
	// next boundary would queue a *second* token, and the one after that would
	// block forever on a full channel, in a select with no context and no
	// control arm. A session with a blocking console and a controller
	// deadlocked on its third stop (Codex, #1122).
	wantOutstanding bool

	// controlled is [Options.Controlled], and control carries the lines
	// [Session.Control] delivers. See control.go.
	controlled bool
	control    chan controlRequest

	// controlSlot admits one [Session.Control] at a time. A run has one
	// position, so two callers moving it at once is not a thing to arbitrate
	// between — it is a caller mistake, and serializing turns it into an
	// ordering rather than an interleaving of two commands into one prompt.
	//
	// A channel rather than a [sync.Mutex] because waiting for the slot is
	// part of the wait a caller's context is promised to bound. A mutex has no
	// cancellable acquire, so a second caller would block on it *before*
	// reaching either context-aware select — and if the first command is never
	// consumed, that second caller never returns at all, whatever its deadline
	// says (Codex, #1122).
	controlSlot chan struct{}

	// pauseGen counts changes to at, and pauseChanged is closed and replaced
	// on each one, so a caller can wait for the run to stop somewhere new
	// rather than spin on [Session.Paused].
	//
	// A generation as well as a signal, because "paused" is not enough to
	// wait on: a caller that asked the run to move is looking at a session
	// that is still paused where it was, and must not read that as the answer.
	pauseGen     uint64
	pauseChanged chan struct{}
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

	// step and kind are where the run is held, for a caller asking through
	// [Session.Paused] rather than reading the line the prompt printed. Empty
	// at an autopsy, where the run is over and there is no step to be at.
	step string
	kind string

	// workflow is which workflow's steps those are — see [Position.Workflow].
	// Empty at an autopsy, and empty on a run carrying no runtime position.
	workflow string

	// backtrace is the current step followed by the call sites that reached
	// it, from the engine's execution context. It is captured with the scope so
	// every front reads the same stop even if the run resumes concurrently.
	backtrace *v1.DebugBacktrace

	// redactText and redactValue are the withholding that was in force when
	// this pause began, captured with the scope rather than read when an
	// answer is returned.
	//
	// The two have to be taken together or a race opens between them. A caller
	// evaluating from its own goroutine snapshots the subject, and evaluation
	// takes time; `flow test` clears both redactors the moment [Session.Autopsy]
	// returns (`flowtest/run.go:768,790`), so a console that exits the autopsy
	// while an evaluation is in flight would leave that evaluation reading a
	// session with no redactors at all — and returning what they existed to
	// withhold (Codex, #1120).
	redactText  func(string) string
	redactValue func(any) any
}

// New returns a session configured by opts.
func New(opts Options) (*Session, error) {
	if len(opts.Breakpoints) > MaxBreakpoints {
		return nil, fmt.Errorf("a session may hold %d breakpoints, and %d were named", MaxBreakpoints, len(opts.Breakpoints))
	}

	// The inventory's declaration numbers are checked here rather than where
	// they are rendered, because here is the only place that can answer with an
	// error. `DebugStep.declaration` is a non-negative int32, so a negative or
	// unrepresentable number produces a message the schema rejects — and the
	// rejection would arrive at whoever asked for a window, about an inventory
	// somebody else supplied, which is a refusal nobody can act on. Rules
	// compile when configuration loads rather than when a request arrives
	// (CLAUDE.md); this is that rule applied to an inventory (Codex, #1194).
	for i, step := range opts.Steps {
		if !validDeclaration(step.Declaration) {
			return nil, fmt.Errorf(
				"step %d (%q) carries declaration %d; a declaration numbers a walk's descents from the root's 0 upward, so it must be between 0 and %d",
				i, step.ID, step.Declaration, math.MaxInt32)
		}
	}

	s := &Session{
		out:         opts.Out,
		emit:        opts.Emit,
		console:     opts.Console,
		clock:       opts.Clock,
		breakpoints: make(map[string]breakpoint, len(opts.Breakpoints)),
		done:        make(chan struct{}),
		steps:       slices.Clone(opts.Steps),
		seen:        map[string]StepState{},
		sharedIDs:   sharedStepIDs(opts.Steps),

		controlled:   opts.Controlled,
		control:      make(chan controlRequest),
		controlSlot:  make(chan struct{}, 1),
		pauseChanged: make(chan struct{}),
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

	for _, step := range s.steps {
		if _, shared := s.sharedIDs[step.ID]; shared {
			s.sharedCount++
		}
	}

	return s, nil
}

// sharedStepIDs are the ids more than one *workflow* in an inventory declares.
//
// By workflow rather than by row, which is the whole distinction. A workflow
// that declares one `build` inside a `for_each` body and another at the top
// level has two rows a run reaches separately, and a session can still
// attribute outcomes to them because both are that workflow's steps — the
// engine's own scope isolation is per call, not per row (`eval.go:1799`,
// `CallScope`). Two `build`s in two workflows are the pair
// [v1.RunObserver]'s bare ids cannot tell apart.
//
// A *name* is not a declaration, which is the correction this key carries: one
// callee invoked from two `call:` steps appears twice under one name, and so do
// two different embedded workflows that happen to share a `name:`. Grouping
// those by name says the session can attribute an outcome it cannot, which is
// the same defect this whole rule exists to prevent, one level down (Codex,
// #1186). [Step.Declaration] is what separates them, and the pair is the key so
// that an inventory built by hand — which numbers nothing and distinguishes its
// workflows by name — is served by the same function.
//
// An unnamed workflow at the same declaration groups with every other one, and
// that is a stated limit rather than an oversight. An inventory that names no
// workflow *and* numbers no declaration — a caller that said neither — reports
// nothing shared, which is right: there is no evidence of a second declaration
// and blanking every ordinary duplicate id on a guess would be worse than the
// disease.
//
// An earlier draft skipped unnamed entries entirely. That did nothing for the
// all-unnamed case it was written for (they group under one key either way) and
// took fail-closed *away* from the mixed case, where a named caller and an
// unnamed callee genuinely are two workflows.
func sharedStepIDs(steps []Step) map[string]struct{} {
	type declaration struct {
		number   int
		workflow string
	}

	declaring := make(map[string]map[declaration]struct{}, len(steps))
	for _, step := range steps {
		if declaring[step.ID] == nil {
			declaring[step.ID] = map[declaration]struct{}{}
		}
		declaring[step.ID][declaration{number: step.Declaration, workflow: step.Workflow}] = struct{}{}
	}

	shared := map[string]struct{}{}
	for id, declarations := range declaring {
		if len(declarations) > 1 {
			shared[id] = struct{}{}
		}
	}

	return shared
}

// Script returns the commands this session accepted, in order. Feeding them
// to a new session's In reproduces the same decisions against the same
// workflow — the replay half of #928's record-and-replay, which `flow debug
// replay` is the verb over (#1111, item 3). See script.go for what the file
// carrying them between the two is, and for the bounds it is read under.
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

	// Entered, said here because this is the only callback that means it — and
	// said on every arrival, so a loop body the run has come back to reads as
	// running rather than as whatever the last iteration left behind.
	s.noteStep(node.GetId(), StepRunning)

	stop, err := s.shouldStop(ctx, node.GetId(), scope)
	if err != nil {
		// Cancellation, and the only thing that reaches here as an error. It
		// unwinds the run exactly as a cancellation at the prompt does: a
		// person who interrupted a run while its condition was being evaluated
		// asked for the same thing as one who interrupted it at the prompt.
		return err
	}
	if !stop {
		return nil
	}

	// What this prompt is about, so that a completion arriving from a console's
	// own goroutine answers against the scope the run is actually held in.
	// Cleared on the way out: a session that kept the last scope alive would
	// answer questions about a position the run has left.
	// The workflow whose steps are running here, taken from where the engine
	// records it rather than from where the step was written: `runCall` moves
	// the position across a call so that a consumer cannot "confus[e] equal
	// step ids in two different workflow files" (`eval.go:1804-1812`), and a
	// debugger holding a run inside a callee is exactly that consumer.
	//
	// Empty only where the engine never ran — a session an embedder drives
	// through [v1.Debugger] itself — which a reader must treat as unsaid
	// rather than as a name. See [Position.Workflow].
	workflow, _ := v1.ExecutingWorkflowFromContext(ctx)

	kind := v1.NodeKind(node)
	s.prompting(promptSubject{
		scope: scope, step: node.GetId(), kind: kind, workflow: workflow,
		backtrace: v1.ExecutingBacktraceFromContext(ctx, node.GetId(), kind),
	})
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
	// not is danger, and everything else is account. The state a step list
	// carries is that same reading — one switch, so a pane and a printed line
	// cannot come to disagree about whether a step failed.
	tone, state := ToneInfo, StepDone
	switch {
	case err != nil && tolerated:
		tone, state = ToneWarning, StepTolerated
	case err != nil:
		tone, state = ToneDanger, StepFailed
	}
	s.noteStep(id, state)
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
	s.noteStep(id, StepSkipped)

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
func (s *Session) shouldStop(ctx context.Context, id string, scope *v1.Scope) (bool, error) {
	s.mu.Lock()
	at, isBreakpoint := s.breakpoints[id]
	mode, until, untilCondition := s.mode, s.until, s.untilCondition
	s.mu.Unlock()

	if isBreakpoint {
		holds, err := s.conditionHolds(ctx, declinedBreakpoint, id, at.condition, scope)
		if err != nil {
			return false, err
		}
		if holds {
			return true, nil
		}
	}

	switch mode {
	case modeStop:
		return true, nil
	case modeUntil:
		if until != id {
			return false, nil
		}
		// The same gate a breakpoint's condition is, through the same
		// function — `until x if e` and `break x if e` + `continue` cannot
		// disagree about when a run is held.
		return s.conditionHolds(ctx, declinedUntil, id, untilCondition, scope)
	default:
		return false, nil
	}
}

// The names the two condition-gated verbs go by in the declined-arrival
// notice, and the keys [Session.noteDeclined] files its once-only memory
// under — one per verb and id, because a breakpoint at `body` and an
// `until body if ...` are different questions.
const (
	declinedBreakpoint = "breakpoint at"
	declinedUntil      = "until"
)

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

// conditionHolds answers whether an arrival gated by a condition should stop —
// a breakpoint's arrival, or the one stop `until` names. One function for both
// verbs, because two evaluations of "does this condition hold here" would be
// two answers to one question. `what` is the verb's own name for itself in the
// declined-arrival notice.
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
func (s *Session) conditionHolds(ctx context.Context, what, id string, condition *v1.Value, scope *v1.Scope) (bool, error) {
	if condition == nil {
		return true, nil
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
	holds, err := v1.EvalConditionInScope(ctx, condition, scope)
	switch {
	case ctx.Err() != nil:
		// Not an unanswerable condition: the operator interrupted the run
		// while this was being evaluated, and a costly condition is exactly
		// when they would. Declining here would let `continue` walk into the
		// step *after* the cancel, which is the one thing an interrupt must
		// not do — and is the opposite of what cancellation at the prompt
		// does, three lines of call stack away (Codex, #1116).
		return false, ctx.Err()

	case err != nil:
		s.noteDeclined(what, id, err)

		return false, nil
	}

	return holds, nil
}

// resume sets what happens at the next boundary.
func (s *Session) resume(m mode, until string) {
	s.resumeUntil(m, until, nil)
}

// resumeUntil is resume carrying `until`'s optional condition. Every resume
// writes the condition — nil from every other verb — because `until` is
// one-shot: a condition that outlived its resume would turn some later
// `continue` into a conditional stop nobody asked for.
func (s *Session) resumeUntil(m mode, until string, condition *v1.Value) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mode = m
	s.until = until
	s.untilCondition = condition
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

	s.printfTone(ToneBreak, "break at %s (%s)%s\n", node.GetId(), v1.NodeKind(node), at)
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
	for {
		s.mu.Lock()
		closed, scanner, console, controlled := s.closed, s.in, s.console, s.controlled
		s.mu.Unlock()

		// Whether there is anything to read a *line* from, recomputed each
		// time round because a stream can end while this is waiting.
		reading := !closed && (scanner != nil || console != nil)

		if !reading && !controlled {
			return "", false, nil
		}

		// Nil unless a reader is running, and a receive on a nil channel
		// blocks forever — which is the right answer twice over: a session
		// that never had a stream has nothing to hear from, and one whose
		// stream ended has a *closed* channel that would otherwise fire
		// instantly, every time round, forever.
		var lines <-chan string

		if reading {
			s.readOnce.Do(func() {
				s.lines = make(chan string)
				// Buffered by one, so that asking for a line never blocks: at most one
				// request is outstanding, and a reader that has already exited leaves
				// the request sitting in the buffer where nothing waits on it.
				s.wants = make(chan struct{}, 1)
				go s.read(scanner, console)
			})
			lines = s.lines

			// Asked for only when the reader does not already owe one. The
			// channel holds one token, and a reader that has taken a token and
			// not yet answered still owes this session a line — see
			// [Session.wantOutstanding] for what queueing a second one does.
			s.mu.Lock()
			outstanding := s.wantOutstanding
			if !outstanding {
				s.wantOutstanding = true
			}
			s.mu.Unlock()

			// A reader that has already answered is heard before anything
			// else. A closed lines channel is a permanent state, and left in
			// the main select below it competes with the control arm at every
			// boundary — so a stream that broke could go unreported for
			// arbitrarily many stops, or forever if the run ends first. That
			// matters because the report is how a controller learns its
			// scripted setup was refused rather than finished.
			select {
			case text, open := <-lines:
				s.readerAnswered()

				if !open {
					if s.streamEnded(controlled) {
						continue
					}

					return "", false, nil
				}

				return text, true, nil
			default:
			}

			if !outstanding {
				// Cannot block: nothing is outstanding, so the one-token
				// channel is empty. The arm is kept for the torn-down case,
				// where the reader has gone and the token would sit unread.
				select {
				case s.wants <- struct{}{}:
				case <-s.done:
					return "", false, nil
				}

				// The prompt is the session's to draw only when it is reading a stream. A
				// console draws its own, because a line editor has to know the prompt's
				// width to put the cursor anywhere.
				//
				// Drawn with the request rather than with the boundary, for the
				// same reason: a second `debug> ` for a line already being
				// waited on is a prompt describing nothing.
				if console == nil {
					s.printfTone(TonePrompt, Prompt)
				}
			}
		}

		select {
		case text, open := <-lines:
			s.readerAnswered()

			if !open {
				if s.streamEnded(controlled) {
					continue
				}

				return "", false, nil
			}

			return text, true, nil

		case request := <-s.control:
			// Shutdown outranks admission. Both this arm and the done arm are
			// ready when [Session.Close] lands on a controller already parked
			// in its send, and a select picks between ready arms at random —
			// so half the time a pending `quit` turned the clean release Close
			// promises into an abandoned run, while its sender could still be
			// told [ErrRunOver] (Codex, #1122).
			//
			// Re-checked *after* the receive rather than before, because a
			// check before it settles nothing: the close can land in between.
			// What this makes true is the property worth having — a command is
			// either dispatched and acknowledged or neither, never dispatched
			// and reported as refused. It does not order a close against a
			// command admitted moments earlier, and nothing could: admission
			// happened first, and that is the honest answer.
			select {
			case <-s.done:
				s.mu.Lock()
				s.closed = true
				s.mu.Unlock()

				// Answered rather than dropped. A sender left to infer a
				// refusal from the session closing has to race its own
				// acknowledgement against that close, and can conclude
				// "refused" about a command another boundary dispatched. One
				// request, one answer.
				request.at <- controlTaken{refused: true}

				return "", false, nil
			default:
			}

			// The other way a line arrives, and it is deliberately the same
			// kind of thing: one command, delivered here, dispatched by the
			// loop above exactly as a typed one is. A programmatic front that
			// resumed the run by reaching into the session's fields would be a
			// second implementation of every verb, free to disagree with the
			// one people type — which is this repository's most-paid-for shape.
			//
			// Which pause this command is being delivered into goes back to
			// the sender, because only here is it knowable. A sender reading
			// the generation before the send has not been delivered yet and
			// would name the pause *before* the one it lands in; reading it
			// after gets whatever the run has since done. So the receiver
			// answers, on a channel of its own that is buffered and therefore
			// never blocks this boundary on a sender that has gone away. See
			// [Session.move].
			//
			// Whether it is an autopsy travels with it for the same reason and
			// decides the same question one step later: at an autopsy every
			// movement verb means "leave", so there is no next stop to wait
			// for and the sender has to be told rather than left waiting.
			s.mu.Lock()
			taken := controlTaken{generation: s.pauseGen, autopsy: s.at.autopsy}
			s.mu.Unlock()
			request.at <- taken

			return request.line, true, nil

		case <-s.done:
			// [Session.Close] while parked. A controlled session has no reader
			// to hear it from and would otherwise hold the run forever.
			//
			// Live for a *reading* session too, deliberately, and it is a
			// small trade rather than an oversight. A reader blocked inside
			// `console.Prompt` or `scanner.Scan` does not exit on Close until
			// its I/O returns, so before this a console session closed
			// mid-prompt stayed parked; this releases it. What it costs is
			// that when this arm wins a race with the reader's own exit, the
			// reason the stream ended (`readErr`, published just before lines
			// is closed) may not be set yet, so the boundary prints the plain
			// "no more commands" rather than naming a scanner error. A worse
			// sentence in a narrow race, against a hang that was certain.
			s.mu.Lock()
			s.closed = true
			s.mu.Unlock()

			return "", false, nil

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
}

// readerAnswered records that the reader has delivered what it was asked for,
// so the next boundary may ask again.
//
// Called wherever a value is taken from the reader's channel, and there are two
// such places — the check that runs before asking, and the wait that runs after.
// Forgetting it at one of them is not a subtle failure: the reader is never
// asked for another line, and the next boundary with no controller behind it
// waits forever (which is how it was found, by `flow run local --debug`).
func (s *Session) readerAnswered() {
	s.mu.Lock()
	s.wantOutstanding = false
	s.mu.Unlock()
}

// streamEnded records that the command stream is over and reports whether the
// session should keep holding the run and wait for programmatic commands.
//
// One place, because the two callers ask the same question at different moments
// — one before requesting a line, one while waiting for it — and a second copy
// of this reasoning is how the two would come to disagree about what the end of
// a stream means.
func (s *Session) streamEnded(controlled bool) (keepHolding bool) {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	// A stream running out is the end of *the stream*. On a controlled session
	// it is not the end of the debugging, and treating it as one resumed the
	// run to the end, unattended, while a controller was still holding it.
	//
	// An interrupt is different and must not keep holding: a person who pressed
	// ctrl-C asked for the run to stop, and waiting for a controller to say
	// otherwise would answer "stop" with "carry on".
	if !controlled || s.wasInterrupted() {
		return false
	}

	// A stream that *broke* says so before the session goes on without it.
	// [Session.consoleEnded] is where a refused command is told apart from a
	// finished one — an overlong line stops a [bufio.Scanner] exactly as EOF
	// does — and the boundary that normally asks it is the one this path
	// skips. Without it a scripted setup command refused for its length
	// vanished, and a controller carried on believing it had applied
	// (Codex, #1122).
	//
	// Said once, because `closed` is now set and no later boundary reaches
	// here.
	if why := s.consoleEnded(); why != "" {
		s.printfTone(ToneDanger,
			"(%s — the run is still held, on programmatic control)\n", why)
	}

	return true
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

	// The withholding in force is part of what a pause *is*, so it is captured
	// here with everything else rather than read later by whoever answers. See
	// [promptSubject.redactText].
	//
	// And so is the scope, for the same reason one step further out: the engine
	// owns `Scope.Outputs.StepValues` and resumes writing to it the moment the
	// pause ends (`eval.go:1494-1514`), while a caller admitted to this pause
	// may still be reading it from its own goroutine. A live map read there is
	// not a stale answer, it is a concurrent map read and write — which Go
	// answers with a fatal throw no recover reaches (Codex, #1120).
	if at.scope != nil {
		at.redactText, at.redactValue = s.redact, s.redactValue
		at.scope = frozen(at.scope)
	}

	s.at = at

	// Every change published, the clearing on the way out included. A caller
	// waiting for the run to stop somewhere new has to see it leave as well as
	// arrive, or a session that resumed and finished without stopping again
	// would look, forever, like one that had not moved. See
	// [Session.waitForPause].
	s.pauseGen++
	close(s.pauseChanged)
	s.pauseChanged = make(chan struct{})
}

// frozen is the scope as it was when a pause began, copied so that nothing the
// run does after resuming can be seen through it — or race with a reader.
//
// A copy rather than a lock held across the pause, and that direction is the
// decision. Holding the engine until every admitted query finishes would make a
// debug client able to delay a resume, which on the durable driver is an
// activity not heartbeating; and it would leave the reader looking at a live
// map that is merely *ordered* against the writer rather than separated from
// it. Copying makes "a pause is a snapshot" true in the same way the redactors
// above make it true of the withholding: one idea, applied to everything the
// pause hands out.
//
// The cost, stated rather than glossed: one deep copy per pause. It is paid
// only where the run actually stops — [Session.prompting] is reached from
// [Session.BeforeStep] only once `shouldStop` says so — and it copies state the
// run is already holding in memory, so it briefly doubles a bounded thing
// rather than introducing growth of its own. A stop is the slowest moment this
// system has; a copy is not what makes it slow.
//
// [promptSubject.extra] is deliberately not copied. It holds the autopsy's bare
// bindings and nothing at a breakpoint, and an autopsy runs after the engine has
// finished with the run — so there is no writer to be separated from.
func frozen(scope *v1.Scope) *v1.Scope {
	clone, ok := proto.Clone(scope).(*v1.Scope)
	if !ok {
		// Unreachable for a generated type, and the answer if it ever were
		// reached is the pause with nothing to hand out rather than a live map
		// handed to another goroutine.
		return nil
	}

	return clone
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
//
// It admits an id and says nothing about what the id did: every one of its
// three callers follows it with a [Session.noteStep] saying that. Splitting the
// two is what lets one map answer both questions — a state is only ever written
// for an id this has already admitted, so the bound here is the only bound
// either needs — and it is what keeps a step the run enters *again* from
// reading as whatever it did last time. A loop body is entered once per
// iteration, and a row saying `ok` while the run is held at that very step
// would be the list disagreeing with the prompt above it.
func (s *Session) sawStep(id string) {
	if id == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, held := s.seen[id]; held {
		return
	}

	if len(s.seen) >= celcomplete.MaxCandidates {
		s.seenShort = true

		return
	}
	s.seen[id] = StepPending
	// No workflow: this seam is handed a bare id and nothing else, which is
	// exactly why a caller that holds the file is asked for one.
	s.seenOrder = append(s.seenOrder, Step{ID: id})
}

// noteStep records what a step was last watched doing.
//
// Only for an id [Session.sawStep] already admitted, which every caller
// guarantees by calling that first. Past the bound the state is dropped exactly
// as the id was, and [Session.StepsTruncated] is what says so — a state written
// for an id the list does not carry would be an answer nothing can be asked
// about.
func (s *Session) noteStep(id string, state StepState) {
	if id == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, held := s.seen[id]; !held {
		return
	}
	s.seen[id] = state
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
// [Session.conditionHolds], which reversed a fail-closed rule on the strength
// of it.
// clearDeclined forgets a verb's declined-condition notice for id, so a newly
// accepted command carrying a fresh condition gets its own one notice.
func (s *Session) clearDeclined(what, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.notedUnbound, what+" "+id)
}

func (s *Session) noteDeclined(what, id string, err error) {
	// Keyed by verb and id together: a breakpoint at `body` and an
	// `until body if ...` are different questions, and one saying it could
	// not be asked must not spend the other's one notice.
	key := what + " " + id

	s.mu.Lock()
	if s.notedUnbound == nil {
		s.notedUnbound = map[string]struct{}{}
	}
	_, already := s.notedUnbound[key]
	s.notedUnbound[key] = struct{}{}
	s.mu.Unlock()

	if already {
		return
	}

	s.printfTone(ToneWarning, "%s %s: the condition could not be evaluated here, so the run was not held: %v\n", what, id, err)
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
	s.mu.Lock()
	redact := s.redactValue
	s.mu.Unlock()

	native, ok := redactedNative(out, redact)
	if !ok {
		return fmt.Sprint(out.Value())
	}

	return nativeText(native)
}

// redactedNative is out as the native Go value a renderer sees, with redact
// applied to it, and whether the conversion was possible.
//
// The one normalization, which is the point. CEL hands back its own backing
// representation from [ref.Val.Value] — `map[ref.Val]ref.Val` for a map,
// `[]ref.Val` for a list — and a redactor written against native Go walks
// neither, so it returns such a container unchanged. `flowtest`'s
// `redactSensitiveTree` switches on `map[string]any` and `[]any`
// (`flowtest/stub.go:940-957`).
//
// So a second path that reached for `Value()` directly would redact a scalar
// and hand a map straight through, which is exactly what happened when the
// structured answer was built beside this instead of from it (Codex, #1120).
// There is one conversion now, and both the text and the structured answer are
// taken from its result.
//
// A value the conversion cannot read is reported as such rather than redacted
// by guesswork: it is not one an equality can recognise either, and what covers
// it is the text redactor a caller applies to whatever comes back.
func redactedNative(out ref.Val, redact func(any) any) (any, bool) {
	lit, err := cel.RefValueToValue(out)
	if err != nil {
		return nil, false
	}
	native, err := v1.LiteralToGo(lit)
	if err != nil {
		return nil, false
	}
	if redact == nil {
		return native, true
	}

	return redact(native), true
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

		if IsComment(line) {
			// A comment here means what it means at a breakpoint — nothing —
			// and it has to, or a script carrying a sentence about itself
			// would leave the autopsy: an empty line and every movement verb
			// are `quit` in this loop, and an unknown command was a warning.
			// See [IsComment].
			continue
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

		case "complete":
			// Taken from the line rather than from `rest`, for the reason
			// `cutWord` exists: `split` trims, and a trailing space is what
			// says the current word is empty.
			_, text := cutWord(strings.TrimLeft(line, " \t"))
			s.record("complete " + text)
			s.showCompletion(text)

		case "scope":
			s.record("scope")
			s.showScopeWith(scope, extra)

		case "help", "h", "?":
			s.printf(`inspect <expr>              evaluate a CEL expression against the finished run
complete <partial-command>  list what could be written at the end of that text
scope                       list what the run can still name
help, h, ?                  this list
quit, q                     leave the autopsy (so do step/continue — the run is over)
`)

		default:
			s.printfTone(ToneWarning, "unknown command %q — try `help`\n", verb)
		}
	}
}
