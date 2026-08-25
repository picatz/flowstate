package flowdebug

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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

	// MaxInspectRunes bounds the rendered length of one inspection's answer.
	// The expression's own cost is bounded by the evaluator; this bounds the
	// *rendering* of a result that was cheap to compute and is large to print
	// — `${inputs.rows}` on a long list.
	MaxInspectRunes = 4096
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
}

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
	// in is nil when no console was given.
	in    *bufio.Scanner
	clock v1.Clock

	// lines carries what the reader goroutine scans, and closes when the
	// stream ends; readOnce starts that goroutine at the first prompt. A
	// goroutine rather than a synchronous Scan, because the run being held
	// is cancellable and a Scanner is not — see [Session.readCommand].
	lines    chan string
	readOnce sync.Once

	mu          sync.Mutex
	mode        mode
	until       string
	breakpoints map[string]struct{}
	// script records accepted commands, in order, for replay.
	script          []string
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
	// redact is what every printed line passes through — see [Session.SetRedactor].
	redact func(string) string
	// ended is set when the session itself abandoned the run (`quit` at a
	// breakpoint): the one command advertised as leaving must not be
	// answered with another prompt, so the autopsy checks it and stays shut
	// (Codex, #1107).
	ended bool
}

// New returns a session configured by opts.
func New(opts Options) (*Session, error) {
	if len(opts.Breakpoints) > MaxBreakpoints {
		return nil, fmt.Errorf("a session may hold %d breakpoints, and %d were named", MaxBreakpoints, len(opts.Breakpoints))
	}

	s := &Session{
		out:         opts.Out,
		emit:        opts.Emit,
		clock:       opts.Clock,
		breakpoints: make(map[string]struct{}, len(opts.Breakpoints)),
	}
	if s.out == nil {
		s.out = io.Discard
	}
	if opts.In != nil {
		s.in = bufio.NewScanner(opts.In)
		s.in.Buffer(make([]byte, 0, 4096), MaxCommandBytes)
	}
	for _, id := range opts.Breakpoints {
		if id = strings.TrimSpace(id); id != "" {
			s.breakpoints[id] = struct{}{}
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
var errQuit = errors.New("debug session ended by the `quit` command")

// BeforeStep implements [v1.Debugger]: the run is held here for as long as the
// session's reader takes to say otherwise.
func (s *Session) BeforeStep(ctx context.Context, node *v1.Node, scope *v1.Scope) error {
	if !s.shouldStop(node.GetId()) {
		return nil
	}

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
			// The console is gone: a replay script that ran out, or a
			// terminal that closed. The run resumes and finishes rather than
			// being held by a debugger that is not there — #928's own answer
			// to its question 4, that a run held paused by a vanished
			// debugger is an availability incident. Said out loud, because a
			// run that finished the rest of itself unattended is something
			// the reader has to know happened.
			s.printfTone(ToneWarning, "(no more commands — continuing to the end of the run)\n")
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
	text := stepOutcomeText(outputs, err, tolerated)

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
func (s *Session) shouldStop(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, isBreakpoint := s.breakpoints[id]; isBreakpoint {
		return true
	}

	switch s.mode {
	case modeStop:
		return true
	case modeUntil:
		return s.until == id
	default:
		return false
	}
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

// readCommand reads one line. ok is false once the stream has ended, and err
// is the context's own error when cancellation ended the wait instead.
//
// The scan runs on a goroutine of its own, because the run being held is
// cancellable and a Scanner is not: ctrl-C's first signal cancels the
// command's context for a graceful stop, and a session blocked synchronously
// in Scan would hold the process hostage for a second, harder signal (Codex,
// #1109). One reader goroutine per session, started at the first prompt;
// after a cancellation it may stay parked on the open stream, which costs a
// process that is already leaving nothing.
func (s *Session) readCommand(ctx context.Context) (line string, ok bool, err error) {
	s.mu.Lock()
	closed, scanner := s.closed, s.in
	s.mu.Unlock()

	if closed || scanner == nil {
		return "", false, nil
	}

	s.readOnce.Do(func() {
		s.lines = make(chan string)
		go func() {
			defer close(s.lines)
			for scanner.Scan() {
				s.lines <- scanner.Text()
			}
		}()
	})

	s.printfTone(TonePrompt, "debug> ")
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
		// boundary or autopsy asks a console the person already interrupted.
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()

		return "", false, ctx.Err()
	}
}

// record appends an accepted command to the replay script.
func (s *Session) record(line string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.script) >= MaxScriptCommands {
		s.scriptTruncated = true

		return
	}
	s.script = append(s.script, line)
}

// printfTone writes one classified fragment: through Emit when the caller
// installed one, to Out otherwise. Every write in this package goes through
// here, so a session is colourable without a second output path.
func (s *Session) printfTone(tone Tone, format string, args ...any) {
	text := s.redactText(fmt.Sprintf(format, args...))

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
func (s *Session) redactText(text string) string {
	s.mu.Lock()
	redact := s.redact
	s.mu.Unlock()

	if redact == nil {
		return text
	}

	return redact(text)
}

func (s *Session) printf(format string, args ...any) {
	s.printfTone(ToneInfo, format, args...)
}

// stepOutcomeText renders one step's recorded outcome for the console.
func stepOutcomeText(outputs *v1.Node_Outputs, err error, tolerated bool) string {
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
		parts = append(parts, name+": "+valueText(named[name]))
	}

	return "-> " + capRunes(strings.Join(parts, ", "), MaxInspectRunes)
}

// valueText renders one output value. A value that is not a resolved literal
// — a secret reference above all — renders as what it is rather than as what
// it points at.
func valueText(value *v1.Value) string {
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

	return nativeText(native)
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
func refValText(out ref.Val) string {
	lit, err := cel.RefValueToValue(out)
	if err != nil {
		return fmt.Sprint(out.Value())
	}
	native, err := v1.LiteralToGo(lit)
	if err != nil {
		return fmt.Sprint(out.Value())
	}

	return nativeText(native)
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
	// A session that quit at a breakpoint said it was done: the case fails
	// (abandoning a run is a verdict), and answering the command advertised
	// as leaving with another prompt would make `quit` a lie (Codex, #1107).
	s.mu.Lock()
	ended := s.ended
	s.mu.Unlock()
	if ended {
		return
	}

	s.printfTone(ToneBreak, "autopsy: the case failed %d expectation(s); the run is over, but its scope is still here\n", len(failures))
	for _, failure := range failures {
		s.printfTone(ToneDanger, "  %s\n", failure)
	}
	s.printf("(`inspect` questions the finished run; `quit` or `continue` leaves — the verdict is already in)\n")

	for {
		line, ok, readErr := s.readCommand(ctx)
		if readErr != nil || !ok {
			// A cancellation or a finished stream both end the autopsy the
			// same way: there is no run left for either to change.
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
