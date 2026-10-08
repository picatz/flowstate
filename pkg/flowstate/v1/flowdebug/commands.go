package flowdebug

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// A command is one verb a debugger front understands.
//
// The table is the vocabulary, in one place, because every front needs it and a
// verb known to some of them is a bug somebody meets rather than reads: the
// prompt's `dispatch` resolves an alias through it, `help` prints it, the
// completer offers it, [CheckScript] judges a script against it, the autopsy
// reads which verbs are movement from it, the [Driver] dispatches and documents
// itself from it, and the DEBUGGING.md command table is generated from it. Before
// it existed the aliases lived in `case` labels, the help text was a second
// hand-written copy of the same list, and the driver's help was a third — which
// is exactly the shape AGENTS.md names, one meaning written down several times,
// and a prompt that completed `breakpoints` while `help` had forgotten to
// mention it would have been nobody's fault in particular.
type command struct {
	// verb is the canonical spelling, and the only one the dispatch switches
	// have a case for.
	verb string

	// aliases are the short forms, in the order help shows them.
	aliases []string

	// argument names what follows the verb, for the help line and so the
	// completer knows there is a second word to complete at all.
	argument string

	// help is the sentence beside the verb.
	help string

	// driverArgument and driverHelp are what the [Driver] says in place of
	// argument and help, where the structured fronts read the verb a little
	// differently (`until` takes a step and no condition there). Empty means the
	// same words.
	driverArgument, driverHelp string

	// completes says what a surface should offer for this command's argument.
	completes completionSubject

	// fronts are where the verb is answered. A verb typed where it is not is
	// refused by name, with [command.elsewhere], never as an unknown command.
	fronts front

	// effect is what the verb does to the run, which is what a driver needs to
	// know to judge a stale revision and what the autopsy needs to know to
	// treat a movement as leaving.
	effect effect

	// rewinds marks a verb that steps the run back rather than forward. It moves
	// the run where there is one to move, so it is [effectMoves] to a driver's
	// stale-revision rule, but the autopsy has no run to rewind and does not
	// leave on it the way it leaves on `continue`.
	rewinds bool

	// elsewhere is the sentence a front that does not answer this verb gives,
	// naming what to type instead. Required for any verb not on every front.
	elsewhere string
}

// front is a place a command line is read.
type front uint8

const (
	// frontPrompt is a session read from its own prompt, script or stdin: the
	// console of `flow run local --debug` and `flow test --debug`, and
	// `flow debug replay`.
	frontPrompt front = 1 << iota
	// frontDriver is the structured fronts, which read the same lines through a
	// [Driver]: `flow debug attach` and `do`, the MCP session tools, and `embed`.
	frontDriver
	// frontAutopsy is the prompt after a failed case, where the run is over and
	// only questions are left.
	frontAutopsy

	frontsAll = frontPrompt | frontDriver | frontAutopsy
	// frontsLive is every front that holds a run.
	frontsLive = frontPrompt | frontDriver
)

// String names a front the way a refusal says it.
func (f front) String() string {
	switch f {
	case frontPrompt:
		return "prompt"
	case frontDriver:
		return "driver"
	case frontAutopsy:
		return "autopsy"
	default:
		return "front"
	}
}

// effect is what a verb does to the run.
type effect uint8

const (
	// effectRead only reads: it changes neither the breakpoints nor the run.
	effectRead effect = iota
	// effectChanges changes the session's breakpoints, logpoints or failure mode
	// without resuming.
	effectChanges
	// effectMoves resumes the run, or steps it back.
	effectMoves
)

// completionSubject is what the second word of a command names.
type completionSubject int

const (
	// completesNothing is a verb that takes no argument.
	completesNothing completionSubject = iota
	// completesStep takes a step id: one the run may still reach.
	completesStep
	// completesBreakpoint takes a step id the session already holds one at.
	completesBreakpoint
	// completesExpression takes CEL, completed against the paused run's scope.
	completesExpression
)

// commands is the whole vocabulary, in the order help lists it: movement first,
// because that is what a session does most, then breakpoints, then the
// questions, then leaving.
var commands = []command{
	{verb: "step", aliases: []string{"s"}, completes: completesNothing, fronts: frontsLive, effect: effectMoves,
		help:       "run this step and stop at the next (also: an empty line)",
		driverHelp: "run to the next step anywhere, including inside this one"},
	{verb: "next", aliases: []string{"n"}, completes: completesNothing, fronts: frontsLive, effect: effectMoves,
		help:       "run this step, including anything inside it, and stop at the next step at this level or above",
		driverHelp: "run this step whole; stop at the next step at this level or above"},
	{verb: "finish", aliases: []string{"fin", "out"}, completes: completesNothing, fronts: frontsLive, effect: effectMoves,
		help: "run until the loop, parallel, switch, or call around this step is left"},
	{verb: "continue", aliases: []string{"c"}, completes: completesNothing, fronts: frontsLive, effect: effectMoves,
		help:       "run until the next breakpoint, or to the end",
		driverHelp: "run to the next breakpoint, or the end"},
	{verb: "until", aliases: []string{"u"}, argument: "<step-id> [if <expr>]", completes: completesStep, fronts: frontsLive, effect: effectMoves,
		help:           "run until the step with that id, optionally only where the condition holds",
		driverArgument: "<step>",
		driverHelp:     "run to that step (an id or an address like pages[2]/page); a condition is `break <step> if <expr>` and `continue`"},
	{verb: "back", completes: completesNothing, fronts: frontsLive, effect: effectMoves, rewinds: true,
		help: "return to the previous stop (a session that can step back)"},
	{verb: "reverse-continue", aliases: []string{"rc"}, completes: completesNothing, fronts: frontsLive, effect: effectMoves, rewinds: true,
		help: "return to the nearest earlier breakpoint stop, or the first"},
	{verb: "pause", completes: completesNothing, fronts: frontDriver, effect: effectChanges,
		help:      "hold at the next step boundary",
		elsewhere: "`pause` is a driver command: a prompt already holds the run at every stop"},
	{verb: "break", aliases: []string{"b"}, argument: "<step-id> [hit <count>] [if <expr>]", completes: completesStep, fronts: frontsLive, effect: effectChanges,
		help:           "stop at that step, always, when the expression holds, or from the given arrival count",
		driverArgument: "<step> [hit <n>] [if <expr>]",
		driverHelp:     "stop there, when the count and condition allow"},
	{verb: "log", argument: "<step-id> <message>", completes: completesStep, fronts: frontsLive, effect: effectChanges,
		help:           "record the message at every arrival without stopping; {expr} holes are CEL",
		driverArgument: "<step> <message>"},
	{verb: "catch", argument: "none|uncaught|all", completes: completesNothing, fronts: frontsLive, effect: effectChanges,
		help:       "stop where a step fails: never, when its failure propagates, or always",
		driverHelp: "stop where a step fails"},
	{verb: "delete", aliases: []string{"d"}, argument: "<step-id>", completes: completesBreakpoint, fronts: frontsLive, effect: effectChanges,
		help:           "remove that breakpoint",
		driverArgument: "<step>|log <step>",
		driverHelp:     "remove that breakpoint, or that logpoint"},
	{verb: "clear", completes: completesNothing, fronts: frontDriver, effect: effectChanges,
		help:      "remove every breakpoint, whoever set it",
		elsewhere: "`clear` is a driver command: at a prompt `delete <step-id>` removes one breakpoint, and `detach` removes them all and lets the run go"},
	{verb: "breakpoints", completes: completesNothing, fronts: frontsLive, effect: effectRead,
		help:       "list them",
		driverHelp: "list breakpoints and their hit counts"},
	{verb: "inspect", aliases: []string{"p"}, argument: "<expr>", completes: completesExpression, fronts: frontsAll, effect: effectRead,
		help:       "evaluate a CEL expression against this run's scope",
		driverHelp: "evaluate a read-only CEL expression at this stop"},
	{verb: "expand", argument: "<expr>", completes: completesExpression, fronts: frontsLive, effect: effectRead,
		help: "list a map's or list's children"},
	{verb: "scope", completes: completesNothing, fronts: frontsAll, effect: effectRead,
		help:       "list what this run can name right now",
		driverHelp: "list what this stop can name"},
	{verb: "complete", argument: "<partial-command>", completes: completesNothing, fronts: frontsAll, effect: effectRead,
		help: "list what could be written at the end of that text"},
	{verb: "status", completes: completesNothing, fronts: frontsLive, effect: effectRead,
		help: "where the run is, and why"},
	{verb: "info", aliases: []string{"step-info"}, completes: completesNothing, fronts: frontPrompt, effect: effectRead,
		help:      "describe the step the run is stopped at",
		elsewhere: "`info` describes the step at a prompt; `status` says where the run is, and why"},
	{verb: "backtrace", aliases: []string{"bt"}, completes: completesNothing, fronts: frontsLive, effect: effectRead,
		help:       "list this step and each iteration, branch, arm and call around it",
		driverHelp: "the step and every container around it"},
	{verb: "detach", completes: completesNothing, fronts: frontsLive, effect: effectMoves,
		help:       "clear every breakpoint and let the run finish unattended",
		driverHelp: "clear breakpoints and let the run go on unattended"},
	{verb: "quit", aliases: []string{"q"}, completes: completesNothing, fronts: frontPrompt | frontAutopsy, effect: effectMoves,
		help:      "end the run here",
		elsewhere: "`quit` ends a local run at its prompt; a durable or attached run is released with `detach`"},
	{verb: "help", aliases: []string{"h", "?"}, completes: completesNothing, fronts: frontsAll, effect: effectRead,
		help:       "list these",
		driverHelp: "this list"},
}

// onFront reports whether the verb is answered on f.
func (c command) onFront(f front) bool { return c.fronts&f != 0 }

// argumentOn is the argument grammar f writes after the verb.
func (c command) argumentOn(f front) string {
	if f == frontDriver && c.driverArgument != "" {
		return c.driverArgument
	}

	return c.argument
}

// helpOn is the sentence f writes beside the verb.
func (c command) helpOn(f front) string {
	if f == frontDriver && c.driverHelp != "" {
		return c.driverHelp
	}

	return c.help
}

// commandsOn lists the commands f answers, in help's order.
func commandsOn(f front) []command {
	out := make([]command, 0, len(commands))
	for _, c := range commands {
		if c.onFront(f) {
			out = append(out, c)
		}
	}

	return out
}

// refuse is the sentence a front gives for a verb it does not answer, and
// whether verb is in the vocabulary at all. A verb that exists elsewhere is never
// "unknown": the author is looking for a misspelling that is not there.
func refuse(verb string, at front) (string, bool) {
	c, ok := resolve(verb)
	if !ok || c.onFront(at) {
		return "", false
	}

	return c.elsewhere, true
}

// The one-line usages three verbs print when their argument is missing.
//
// Constants rather than literals at the printf, because [CheckScript] reports
// the same three problems about a *file* before a session runs it, and the
// advice a script author reads must be the advice the prompt gives: one
// meaning, one place. See CLAUDE.md on a value with one meaning written down
// twice.
const (
	usageUntil   = "until needs a step id: until <step-id> [if <expr>]"
	usageBreak   = "break needs a step id: break <step-id> [if <expr>]"
	grammarBreak = "break <step-id> [if <expr>]"
	grammarUntil = "until <step-id> [if <expr>]"
	usageInspect = "inspect needs an expression: inspect steps.build.artifact"
	usageExpand  = "expand needs an expression: expand steps.build"
	// usageCondition is completed by the asking verb's grammar, so `break
	// body if ` and `until body if ` are each corrected in their own words.
	usageCondition = "`if` needs an expression: %s"
)

// IsComment reports whether a line is a comment rather than a command.
//
// `#` is the comment marker, and it is answered *here* — in the one dispatch
// every front goes through — rather than stripped by whoever reads a script
// file. That placement is the whole point: a recorded script is the command
// stream written down (see script.go), so `flow debug replay script wf` and
// `flow run local --debug wf < script` are the same session only for as long as
// nothing transforms the file on its way in. A comment understood at the prompt
// is a comment understood everywhere.
//
// It was previously an unknown command, answered with a warning, which is the
// worst of both: a reproduction pasted into an issue could not carry a sentence
// saying what it reproduces without the session complaining about it once per
// line.
//
// Not recorded either, for the same reason a mistyped command is not: the
// script is what the session *did*.
func IsComment(line string) bool {
	return strings.HasPrefix(strings.TrimLeft(line, " \t"), "#")
}

// resolveOn is [resolve] restricted to the verbs f answers: a verb that exists
// only on another front is not this front's, whatever it is called.
func resolveOn(typed string, f front) (command, bool) {
	if c, ok := resolve(typed); ok && c.onFront(f) {
		return c, true
	}

	return command{}, false
}

// resolve returns the canonical verb for what was typed, and whether it is one
// this session knows.
func resolve(typed string) (command, bool) {
	for _, c := range commands {
		if c.verb == typed || slices.Contains(c.aliases, typed) {
			return c, true
		}
	}

	return command{}, false
}

// dispatch runs one command line. It reports whether the run resumes, and
// returns an error only where the session is ending the run — a mistyped
// command is answered and asked again, never fatal, because ending someone's
// run over a typo is the worst possible reading of an ambiguous line.
func (s *Session) dispatch(ctx context.Context, line string, node *v1.Node, scope *v1.Scope) (resumed bool, err error) {
	if IsComment(line) {
		// Nothing to do and nothing to say: see [IsComment]. Checked before
		// the empty-line arm below, because a comment is not a step.
		return false, nil
	}

	typed, rest := split(line)
	if typed == "" {
		// A bare newline repeats the most useful thing: one step. It is what
		// every debugger a person has used already does, and a session where
		// return does nothing is one where they press it twice.
		typed = "step"
	}

	// Aliases resolve through the table rather than through the case labels,
	// so the vocabulary the completer offers and the vocabulary this
	// understands are one list. An unknown verb keeps the spelling that was
	// typed, because that is what the diagnostic has to quote back.
	verb := typed
	if known, ok := resolve(typed); ok {
		verb = known.verb
	}
	if why, refused := refuse(typed, frontPrompt); refused {
		s.printfTone(ToneWarning, "%s\n", why)

		return false, nil
	}

	// Recorded before the command runs and only for commands that were
	// understood, so a replay script holds a session's decisions and not its
	// typing mistakes.
	switch verb {
	case "step":
		s.record("step")
		s.resume(modeStop, v1.DebugTarget{})

		return true, nil

	case "next":
		s.record("next")
		s.resume(modeOver, v1.DebugTarget{})

		return true, nil

	case "finish":
		s.record("finish")
		s.resume(modeOut, v1.DebugTarget{})

		return true, nil

	case "continue":
		s.record("continue")
		s.resume(modeRun, v1.DebugTarget{})

		return true, nil

	case "detach":
		s.record("detach")
		s.mu.Lock()
		clear(s.breakpoints)
		s.contract.failureMode = v1.DebugFailureMode_DEBUG_FAILURE_MODE_NONE
		s.contract.detached = true
		s.mu.Unlock()
		s.resume(modeRun, v1.DebugTarget{})
		s.printfTone(ToneWarning, "(detached — the run finishes unattended)\n")

		return true, nil

	case "catch":
		s.setFailureMode(strings.TrimSpace(rest))

		return false, nil

	case "log":
		s.addLogpoint(strings.TrimSpace(rest))

		return false, nil

	case "until":
		// The same grammar, compiler and refusals as `break`, sharing its
		// helpers so the two condition-gated verbs cannot drift: an accepted
		// condition is compiled now, a malformed tail is a refusal rather
		// than a silent unconditional stop, and the evaluation at arrival is
		// [Session.conditionHolds] either way.
		id, condition, conditional, err := splitCondition(rest, grammarUntil)
		if err != nil {
			s.printfTone(ToneWarning, "until: %v\n", err)

			return false, nil
		}
		if id == "" {
			s.printfTone(ToneWarning, "%s\n", usageUntil)

			return false, nil
		}
		// Checked before the condition is compiled, where `break` checks it
		// too: an id the workflow does not declare is refused whether or not
		// a condition follows it, and refusing first spends nothing on
		// compiling a question about a step that will never be reached.
		if notice, unknown := s.unknownStepNotice(id); unknown {
			s.printfTone(ToneWarning, "until: %s\n", notice)

			return false, nil
		}
		target := v1.ParseDebugTargetOrStep(id)

		var (
			compiled *v1.Value
			note     string
		)
		if conditional {
			compiled, err = compileCondition(condition, scope, grammarUntil)
			if err == nil {
				note, err = s.conditionInScope(compiled, scope.GetProfile(), target.Resolve)
			}
			if err != nil {
				s.printfTone(ToneWarning, "until %s: %v\n", id, err)

				return false, nil
			}
		}
		// A newly accepted `until` is a new question, so it gets its own
		// chance to say it could not be asked — the same rule holdBreakpoint
		// applies when a breakpoint is replaced. Without this, a second
		// `until body if <broken>` after a declined first one is skipped in
		// silence, behind a prompt that said it was set (Copilot, #1274).
		s.clearDeclined(declinedUntil, id)
		s.record("until " + strings.TrimSpace(rest))
		if note != "" {
			s.printfTone(ToneWarning, "until %s: %s\n", id, note)
		}
		conditionText := ""
		if compiled != nil {
			conditionText = strings.TrimSpace(condition)
		}
		s.resumeUntil(modeUntil, target, compiled, conditionText)

		return true, nil

	case "break":
		s.addBreakpoint(ctx, strings.TrimSpace(rest), scope)

		return false, nil

	case "delete":
		s.deleteBreakpoint(strings.TrimSpace(rest))

		return false, nil

	case "breakpoints":
		s.record("breakpoints")
		s.listBreakpoints()

		return false, nil

	case "inspect":
		expression := strings.TrimSpace(rest)
		if expression == "" {
			s.printfTone(ToneWarning, "%s\n", usageInspect)

			return false, nil
		}
		s.record("inspect " + expression)
		s.inspect(ctx, expression, scope)

		return false, nil

	case "expand":
		expression := strings.TrimSpace(rest)
		if expression == "" {
			s.printfTone(ToneWarning, "%s\n", usageExpand)

			return false, nil
		}
		s.record("expand " + expression)
		s.expand(ctx, expression)

		return false, nil

	case "status":
		s.record("status")
		s.showStatus()

		return false, nil

	case "back", "reverse-continue":
		// Named at the prompt so a script written for a session that can step
		// back reads the same here, and answers the capability it lacks. Not
		// recorded: a refused command is not part of the session it would
		// replay.
		s.printfTone(ToneWarning, "%s\n", errCannotStepBack)

		return false, nil

	case "complete":
		// The text after the verb exactly as typed, taken from the line
		// rather than from `rest`, because `split` trims and a trailing space
		// is the thing that says the current word is empty — the same
		// distinction `cutWord` exists for.
		_, text := cutWord(strings.TrimLeft(line, " \t"))
		s.record("complete " + text)
		s.showCompletion(text)

		return false, nil

	case "scope":
		s.record("scope")
		s.showScope(scope)

		return false, nil

	case "info":
		s.record("info")
		s.showStep(node)

		return false, nil

	case "backtrace":
		s.record("backtrace")
		s.showBacktrace()

		return false, nil

	case "quit":
		s.record("quit")
		// Remembered, so the autopsy stays shut: quit is the one command
		// advertised as leaving, and it must not be answered with another
		// prompt (Codex, #1107).
		s.mu.Lock()
		s.ended = true
		s.mu.Unlock()

		return false, errQuit

	case "help":
		s.help()

		return false, nil

	default:
		// Named rather than ignored, the diagnostics rule this repo applies
		// to a misspelled key in a file: silently doing nothing gives the
		// author no reason to doubt what they typed.
		s.printfTone(ToneWarning, "unknown command %q — try `help`\n", verb)

		return false, nil
	}
}

// showBacktrace prints the held stop's frames: the step, then each iteration,
// branch, arm and call around it. They are the snapshot's own frames, rendered
// as the typed Driver renders them, so the prompt, `flow debug do`, MCP and an
// editor's call stack number and name one stop the same way; the call chain
// alone could not say which iteration a stop is in.
func (s *Session) showBacktrace() {
	trace, err := s.Backtrace()
	if err != nil {
		s.printfTone(ToneWarning, "%s\n", err)
		return
	}
	// The autopsy's run is over and has no frames to show.
	if len(trace.GetFrames()) == 0 {
		return
	}
	// Assembled unredacted from the held occurrence, then redacted once and
	// whole by the redactor this pause was taken under, as
	// [Session.BacktraceLabels] is: joined fields can recreate a protected
	// substring, and the session's live redactor may already have changed.
	redact := s.snapshotTextRedactor()
	s.mu.Lock()
	frames := Frames(s.contract.occurrence, func(site *v1.DebugSite) *v1.DebugSourceLocation {
		return s.contract.sources[v1.DebugSiteKey(site)]
	}, nil)
	s.mu.Unlock()
	s.printf("%s", applyText(redact, formatFrames(&v1.DebugSnapshot{Frames: frames})))
}

// split separates the first word of a line from the rest.
// cutWord is [split] without the trimming, for a line whose end is a cursor
// position rather than a command.
//
// The two are deliberately not one function. `split` reads a line somebody has
// finished typing, where trailing space is noise; this reads a line somebody is
// *still* typing, where trailing space is the thing that says the current word
// is empty. Trimming it told the completer the cursor sat three characters
// left of where it was, and the console — which replaces exactly the reported
// prefix — cut into the word before the space (Codex, #1116).
func cutWord(line string) (word, rest string) {
	if index := strings.IndexFunc(line, func(r rune) bool { return r == ' ' || r == '\t' }); index >= 0 {
		return line[:index], line[index+1:]
	}

	return line, ""
}

func split(line string) (verb, rest string) {
	line = strings.TrimSpace(line)
	if index := strings.IndexFunc(line, func(r rune) bool { return r == ' ' || r == '\t' }); index >= 0 {
		return line[:index], line[index+1:]
	}

	return line, ""
}

// inspect evaluates one expression against the paused run's own scope.
//
// The evaluation goes through the run's activation, evaluator and profile, so
// an inspection is bounded exactly as an expression in the file is
// ([v1.DefaultCostLimit]) and can reach exactly what the file could reach at
// this point — including reaching a `${secret(...)}` reference and getting
// back the refusal an activation always gives one.
func (s *Session) inspect(ctx context.Context, expression string, scope *v1.Scope) {
	s.inspectWith(ctx, expression, scope, nil)
}

// inspectWith is inspect with extra bare bindings layered over the scope —
// the autopsy's door, where the post-run `vars` and extended `run` root must
// answer exactly as the failing check read them (Codex, #1107).
func (s *Session) inspectWith(ctx context.Context, expression string, scope *v1.Scope, extra map[string]ref.Val) {
	libs, err := v1.ProfileLibraries(scope.GetProfile())
	if err != nil {
		s.printfTone(ToneWarning, "cannot inspect: %v\n", err)

		return
	}

	// The pause's own redactors, which withhold what the workflow held there
	// declares sensitive, a callee's included, as the typed contract's
	// inspection does (#2208, exact-head review): the session's alone know
	// only what its caller installed. The answer and the error alike, since an
	// evaluation error quotes the scope as readily as an answer shows it
	// (`no such key: <value>`).
	text, value := s.pauseRedactors()

	activation := scope.Activation(ctx)
	if len(extra) > 0 {
		activation = scope.ActivationWith(ctx, extra)
	}
	out, err := v1.DefaultEvaluator().EvalString(ctx, expression, libs, activation)
	if err != nil {
		// An author's expression failing is an ordinary event at a debugger
		// prompt, not a session-ending one: they are asking questions, and
		// some of them will not compile.
		s.emitTone(ToneWarning, applyText(text, err.Error())+"\n")

		return
	}

	// Redacted before the cap, for the reason [Session.stepOutcomeText] gives:
	// truncating first would leave the first MaxInspectRunes of a long secret
	// in a string no substring match can recognise (Codex, #1109).
	//
	// A value that does not fit a line is laid out as a tree, from the tree the
	// redactors already walked; the backstop pass over what it writes is
	// unchanged.
	var rendered string
	if native, ok := redactedTree(out, text, value); ok {
		rendered = RenderValue(native, Layout{})
	} else {
		rendered = unrenderedText(out, text != nil || value != nil)
	}
	s.printfTone(ToneValue, "%s\n", capRunes(applyText(text, rendered), MaxInspectRunes))
}

// showCompletion answers `complete`, which is tab made into a command.
//
// A terminal has a key for this and nothing else does, so without a verb the
// completion this package builds is reachable only by a person with a
// keyboard — the same capability-on-one-surface gap that the redaction seam
// and the step inventory each had to be given a second front for. A script is
// this session's other front, so a question about the current scope belongs in
// it beside `inspect`, which is the same shape: ask, get an answer, do not
// move the run.
func (s *Session) showCompletion(text string) {
	answer := s.Complete(text, len(text))
	if len(answer.Candidates) == 0 {
		if answer.Truncated {
			// Nothing matched *and* the list was cut, which is a different
			// answer from nothing matching: somewhere past the bound there
			// may be a name that does.
			s.printf("nothing matched, and the list was cut — try a longer prefix\n")

			return
		}
		s.printf("nothing to complete there\n")

		return
	}

	s.printf("%s", RenderCompletion(answer))
}

// showScope lists what the paused run can name, which is the question an
// author asks before they know what to inspect.
func (s *Session) showScope(scope *v1.Scope) {
	s.showScopeWith(scope, nil)
}

// showScopeWith is showScope with the extra bare bindings listed too — the
// autopsy's door, for the reason inspectWith exists: a listing that omits
// `vars` and `run` while `inspect vars.x` answers would be a scope command
// hiding exactly the names it is for discovering (Codex, #1109).
func (s *Session) showScopeWith(scope *v1.Scope, extra map[string]ref.Val) {
	groups := scopeNames(scope, extra)

	steps := false
	for _, group := range groups {
		if group.Group == scopeGroupSteps {
			steps = true
		}
		s.printf("%s: %s\n", group.Group, namesLine(group.Names, group.listing))
	}

	// The one group that says so when it is empty, because "no step has
	// produced an output yet" is a real answer at the first breakpoint of a
	// run — where the others simply do not appear, a workflow that declares no
	// `vars:` having no line to print about them.
	if !steps {
		s.printf("no steps have produced outputs yet\n")
	}
}

// The group names `scope` prints and [Session.Scope] returns, written once
// because a caller matching on them and a person reading them are looking at
// the same list.
const (
	scopeGroupBound        = "bound"
	scopeGroupSteps        = "steps"
	scopeGroupLocals       = "locals"
	scopeGroupWorkflowVars = "vars"
	scopeGroupInputs       = "inputs"
	scopeGroupRun          = "run"
	scopeGroupTrigger      = "trigger"
)

// scopeNames is what the paused run can reach, grouped as the prompt groups it.
//
// The values behind `scope`, so the printed lines are one rendering of this
// rather than the only way to reach it — see inspect.go for why a session needs
// answers a caller can hold as well as ones it can read.
//
// Complete, and bounded by nothing. [MaxScopeNames] is a property of a line
// somebody reads, not of what a run can name: a debug adapter filling a
// variables pane wants every name and does its own paging, and applying a
// display cap here would make the value surface quietly narrower than the run.
// The cap lives in [namesLine], which is the renderer.
func scopeNames(scope *v1.Scope, extra map[string]ref.Val) []Names {
	var groups []Names

	// The root is the parameter and the listing is derived from it, rather
	// than both being written at each call. They are one fact — `inspect
	// steps.` is the command that enumerates the names under `steps` — and
	// writing it twice per group is how the prompt's pointer and a renderer's
	// prefix come to disagree about a group somebody adds later. See
	// [Names.Root].
	add := func(name, root string, names []string) {
		if len(names) == 0 {
			return
		}
		listing := ""
		if root != "" {
			listing = "inspect " + root + "."
		}
		groups = append(groups, Names{Group: name, Names: names, Root: root, listing: listing})
	}

	// No namespace named for the autopsy's bindings: one is offered bare, and
	// one carrying members is a root under its *own* name rather than a shared
	// one, so there is no single spelling to point at.
	add(scopeGroupBound, "", sortedKeys(extra))

	if steps := scope.GetOutputs().GetStepValues(); len(steps) > 0 {
		names := make([]string, 0, len(steps))
		for name := range steps {
			names = append(names, name)
		}
		sort.Strings(names)
		add(scopeGroupSteps, "steps", names)
	}

	// Labelled by how the names are reached, which is the other way round
	// from the fields that hold them. `Scope.Vars` are the *bare* bindings — a
	// loop's `as:`, a step's own `vars:` — offered as
	// [celcomplete.Scope.Locals] under no root at all (complete.go:271), so
	// they are "locals". `Scope.AmbientVars` are the workflow's declared
	// `vars:`, and those are what `vars.` reaches (complete.go:280-282), so
	// they are "vars".
	add(scopeGroupLocals, "", sortedKeys(scope.GetVars()))
	add(scopeGroupWorkflowVars, "vars", sortedKeys(scope.GetAmbientVars()))

	// The arguments the run was started with, which completion has offered
	// since it learned the `inputs.` root (complete.go:305) and this collector
	// did not. A value surface narrower than what [Session.Evaluate] resolves
	// is the failure this function's own comment warns about, so leaving it out
	// made the warning describe the code (Codex, #1120).
	add(scopeGroupInputs, "inputs", sortedKeys(scope.GetInputs()))

	// The last two roots, and the ones that are not keyed by anything in the
	// scope: `run` and `trigger` are answered *whole* by the activation
	// (`eval.go:349-358`), so their members come from that answer rather than
	// from a list written here. A list would be a second spelling of
	// [v1.RunRoot]'s and [v1.TriggerRoot]'s own field sets, which is the thing
	// that drifts — and this collector has now been short a root twice, both
	// times because it enumerated what it thought a run could name instead of
	// asking (Codex, #1120).
	//
	// Always present, unlike the groups above, because these are facts about
	// the run rather than contents of it: `run.local` is a real answer when it
	// is false, and both roots resolve for every run there is.
	activation := scope.Activation(context.Background())
	add(scopeGroupRun, "run", rootNames(activation, v1.RunRoot))
	add(scopeGroupTrigger, "trigger", rootNames(activation, v1.TriggerRoot))

	return groups
}

// rootNames are the members of one whole-answered root, read out of the answer.
//
// [context.Background] is what builds the activation above, and it is correct
// rather than convenient: these two roots are plain values, so resolving one
// evaluates no stored expression and there is nothing for a context to bound.
// The roots that *can* evaluate are keyed by the scope and are collected from
// it directly, above.
func rootNames(activation cel.Activation, root string) []string {
	resolved, ok := activation.ResolveName(root)
	if !ok {
		return nil
	}
	value, ok := resolved.(ref.Val)
	if !ok {
		return nil
	}

	// The same conversion the answers themselves take, so a root's members are
	// named here exactly as `inspect run.` renders them.
	native, ok := redactedNative(value, nil)
	if !ok {
		return nil
	}
	members, ok := native.(map[string]any)
	if !ok {
		return nil
	}

	return sortedKeysOf(members)
}

// sortedKeysOf is [sortedKeys] for a map this package holds natively.
func sortedKeysOf(m map[string]any) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}

// namesLine renders one scope line's names, bounded by [MaxScopeNames].
//
// The remainder is counted rather than dropped, for the reason every other
// truncation in this package carries a notice: a list silently cut at twenty
// tells a reader their run has twenty steps.
//
// listing is the command that enumerates *these* names, or "" where they
// belong to no namespace one could name. A parameter rather than a constant
// because this renders four lines drawn from three different completion
// sources, and a suffix naming one of them pointed the other three at names it
// cannot reach — worse than no pointer at all, since after the cut the notice
// is the only thing left saying those names exist (Codex, #1115).
func namesLine(names []string, listing string) string {
	if len(names) <= MaxScopeNames {
		return strings.Join(names, ", ")
	}

	where := "tab completes them"
	if listing != "" {
		where += fmt.Sprintf("; `%s` lists them", listing)
	}

	return fmt.Sprintf("%s … and %d more (%s)",
		strings.Join(names[:MaxScopeNames], ", "), len(names)-MaxScopeNames, where)
}

// showStep prints what the run is stopped at.
func (s *Session) showStep(node *v1.Node) {
	s.printf("%s (%s)\n", node.GetId(), v1.NodeKind(node))
	if description := node.GetDescription(); description != "" {
		s.printf("  %s\n", description)
	}
	if node.GetCondition() != nil {
		// Worth saying precisely because the stop happened: the step has an
		// `if:`, and reaching this boundary is what tells the reader it
		// evaluated true.
		s.printf("  if: evaluated true (a false one would have skipped the step, not stopped here)\n")
	}
	if node.GetAsync() {
		s.printf("  async: the result is heard at its join, not here\n")
	}
}

// addBreakpoint takes `<step-id> [hit <count>] [if <expr>]`.
//
// The condition is compiled here rather than at each arrival, which is the
// difference between this and `inspect`. `inspect` parses at evaluation time
// ([v1.Evaluator.EvalString]) and that is right for a question asked once; a
// breakpoint condition is a rule that fires at every arrival, and this
// repository's own shape for a rule is that it compiles when it is *accepted*
// rather than when it is reached — see `auth.SecretAccessPolicy.Compile` and
// netpolicy's rule compiler. So a malformed expression is refused now, loudly,
// with nothing set: a breakpoint accepted broken is worse than one refused,
// because it looks armed and never fires.
//
// `if` over `when` because `if:` is already this language's word for a
// condition gating whether something happens, and the parse is positional — a
// step legally named `if` is still the id, since the first word always is.
func (s *Session) addBreakpoint(ctx context.Context, rest string, scope *v1.Scope) {
	rest, hitText, err := cutHitClause(rest)
	if err != nil {
		s.printfTone(ToneWarning, "break: %v\n", err)

		return
	}
	hit, err := v1.ParseDebugHitCondition(hitText)
	if err != nil {
		s.printfTone(ToneWarning, "break: hit condition: %v\n", err)

		return
	}

	id, condition, conditional, err := splitCondition(rest, grammarBreak)
	if err != nil {
		s.printfTone(ToneWarning, "break: %v\n", err)

		return
	}
	if id == "" {
		s.printfTone(ToneWarning, "%s\n", usageBreak)

		return
	}

	if notice, unknown := s.unknownStepNotice(id); unknown {
		s.printfTone(ToneWarning, "break: %s\n", notice)

		return
	}

	target := v1.ParseDebugTargetOrStep(id)

	at := breakpoint{source: rest, id: id, target: target, hit: hit,
		definition: &v1.DebugBreakpoint{Id: id, Step: id, HitCondition: hitText}}
	if hitText != "" {
		at.source = id + " hit " + hitText + strings.TrimPrefix(rest, id)
	}
	var note string
	if conditional {
		compiled, err := compileCondition(condition, scope, grammarBreak)
		if err == nil {
			note, err = s.conditionInScope(compiled, scope.GetProfile(), target.Resolve)
		}
		if err != nil {
			s.printfTone(ToneWarning, "break %s: %v\n", id, err)

			return
		}
		at.condition = compiled
		at.definition.Condition = strings.TrimSpace(condition)
	}

	full := !s.holdBreakpoint(id, at)

	if full {
		s.printfTone(ToneWarning, "a session holds at most %d breakpoints\n", MaxBreakpoints)

		return
	}
	s.record("break " + at.source)
	s.printf("breakpoint at %s\n", breakpointLabel(at.definition))
	if note != "" {
		s.printfTone(ToneWarning, "break %s: %s\n", id, note)
	}
}

// breakpointLabel is how every front echoes an armed breakpoint: its step or
// address, then what decides when it stops — the hit count and the condition
// — as they were set. An echo that drops either says the breakpoint is
// broader than it is. The prompt and the typed [Driver] both render through
// this, so one breakpoint reads the same on each.
func breakpointLabel(definition *v1.DebugBreakpoint) string {
	label := definition.GetStep()
	if hit := strings.TrimSpace(definition.GetHitCondition()); hit != "" {
		label += " hit " + hit
	}
	if condition := strings.TrimSpace(definition.GetCondition()); condition != "" {
		label += " if " + condition
	}

	return label
}

// maxStepSuggestionInput bounds the typed id a did-you-mean is computed for:
// the longest id the schema permits, plus the most edits [nearest] will call a
// near miss.
//
// The rule is cmd/flow's maxSuggestionInput (#428) — bound the typed side
// before scanning — but the *number* has to come from what a real step id can
// be, not from that constant, which sizes this CLI's own short command names.
// `Node.id` is `max_len: 128` in proto/flowstate/v1/workflow.proto, so a
// declared id of 128 characters mistyped once is 129 and is genuinely worth a
// suggestion; a threshold of 64 borrowed from the command surface would have
// skipped it at the prompt while `flow debug replay` still offered it, which
// is the prompt-versus-replay divergence this whole change exists to close
// (Codex, #1347).
//
// Derived rather than written down as 130: the two facts it is made of live
// where they are enforced, and a schema that widens the id should widen this
// with it.
const maxStepSuggestionInput = maxStepIDLength + nearest.MaxDistance

// maxStepIDLength is `Node.id`'s own `max_len` in
// proto/flowstate/v1/workflow.proto. Asserted against the descriptor by
// TestMaxStepIDLengthMatchesTheSchema, so it cannot drift from the constraint
// that decides what a step may actually be called.
const maxStepIDLength = 128

// UnknownStep reports whether a step id names nothing this session can reach,
// with the notice explaining it.
//
// It is `Session.unknownStepNotice` for callers outside this package, so that a
// front end which must answer per breakpoint — a DAP adapter, whose client sets
// them one edit at a time and expects a verdict for each — can ask before it
// sends, rather than losing a whole set to one typo. Programmatic callers that
// have nothing to answer per id need not call it: [New] and
// [Session.SetBreakpoints] apply the same check themselves.
//
// An empty inventory reports nothing unknown; see `Session.unknownStepNotice`.
func (s *Session) UnknownStep(id string) (string, bool) {
	return s.unknownStep(strings.TrimSpace(id), s.snapshotTextRedactor())
}

// StepNotice is one front end's answer about whether a requested step exists.
type StepNotice struct {
	Message string
	Unknown bool
}

// SetBreakpointsWithNotices validates a front end's whole requested set under
// one redaction snapshot, omitting unknown and empty ids from the installed set
// while returning one notice slot per request.
func (s *Session) SetBreakpointsWithNotices(ids []string) ([]StepNotice, error) {
	redact := s.snapshotTextRedactor()
	notices := make([]StepNotice, 0, len(ids))
	known := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			notices = append(notices, StepNotice{})
			continue
		}
		message, unknown := s.unknownStep(id, redact)
		notices = append(notices, StepNotice{Message: message, Unknown: unknown})
		if !unknown {
			known = append(known, id)
		}
	}

	return notices, s.setBreakpoints(known, redact, false)
}

func (s *Session) unknownStep(id string, redact func(string) string) (string, bool) {
	notice, unknown := s.unknownStepNotice(id)

	return applyText(redact, notice), unknown
}

// unknownStepNotice reports that a step id names nothing this run can reach,
// in the words `flow debug replay` already refuses the same line with.
//
// The prompt used to arm anything: `break nosuchstep` answered "breakpoint at
// nosuchstep", listed it, and never fired, while `until nosuchstep` printed
// nothing at all and ran the workflow to its end — one mistyped character
// forfeiting the session, with every queued command after it unanswered. The
// check that catches it already existed one door over, in [CheckScript], over
// the same inventory; this is that check where a person types rather than
// where a script is read, so the two fronts stop disagreeing about the same
// word.
//
// The inventory is [Options.Steps] and the ids this session has watched go
// past, which is exactly what completion offers ([Session.reachableSteps]) —
// so a name the prompt would complete is a name it accepts. Ids are bare and
// not qualified by workflow, deliberately: a `call:`'s callee declares its own
// steps and a breakpoint on one is a breakpoint the run genuinely stops at,
// which is why the inventory holds them too.
//
// An empty inventory refuses nothing. A caller that supplied no steps has said
// nothing about what exists, and [checkStepArgument] takes that same silence
// the same way: absence of evidence is not evidence a step is missing.
func (s *Session) unknownStepNotice(id string) (string, bool) {
	// A target in the address grammar: a site path is resolved against the
	// program when there is one, and otherwise judged by its step.
	target := v1.ParseDebugTargetOrStep(id)

	s.mu.Lock()
	sitesKnown, sites := s.contract.sitesKnown, s.contract.sites
	program, inProgram := s.contract.program, s.contract.declaredInProgram
	s.mu.Unlock()
	if sitesKnown {
		if len(target.Resolve(sites)) > 0 {
			return "", false
		}

		if !strings.ContainsRune(id, '/') {
			ids := make([]string, 0, len(sites))
			for _, site := range sites {
				path := site.Site.GetPath()
				ids = append(ids, path[len(path)-1])
			}
			slices.Sort(ids)
			ids = slices.Compact(ids)
			if utf8.RuneCountInString(id) <= maxStepSuggestionInput {
				if suggestion, found := nearest.Name(id, ids); found {
					return fmt.Sprintf("no step named %q: did you mean %q?", id, suggestion), true
				}
			}

			return fmt.Sprintf("no step named %q: this workflow declares %s", id, stepList(ids)), true
		}

		return noSiteMatches(id), true
	}
	address, qualified := id, strings.ContainsRune(id, '/')
	id = target.Step()

	// Built once at construction ([declaredStepIDs]); this is a lookup rather
	// than a walk, because a refused command is not recorded and so may be
	// repeated without bound. A program whose sites were cut short answers
	// from what it declares instead, as the durable driver does: its ids,
	// built once too, refuse a step it never declares at once, and only a
	// qualified target naming a declared step walks the program for the
	// containers it names ([v1.DebugTarget.DeclaredIn]).
	var known bool
	if program != nil {
		_, known = inProgram[id]
		if known && qualified {
			known = target.DeclaredIn(program)
		}
	} else {
		_, known = s.declaredIDs[id]
	}

	s.mu.Lock()
	// An id this session has watched go past is reachable whatever the
	// inventory said, so it is admitted — but it never *makes* an inventory:
	// what has run so far is not what the workflow declares, and reading it
	// that way would refuse every step the run has not reached yet, which on
	// an empty inventory is all of them. A program answers for itself: every
	// id it has run is one it declares, so the fallback could only admit a
	// qualified target the program has already refused.
	if !known && program == nil {
		_, known = s.seen[id]
	}
	s.mu.Unlock()

	names := s.declared
	if known || (len(names) == 0 && inProgram == nil) {
		return "", false
	}
	if qualified && program != nil {
		// No declared step answers to the address as written — whether its
		// step or the containers it names are what is missing — and a notice
		// about the bare step alone would misstate which.
		return noSiteMatches(address), true
	}
	if len(names) == 0 {
		return fmt.Sprintf("no step named %q is declared by this workflow or a workflow it calls", id), true
	}

	// The suggestion is skipped for input too long to have been a typo of
	// anything declared, which is the bound [nearest]'s own doc puts on every
	// caller and cmd/flow's argv suggestions already keep (maxSuggestionInput,
	// #428). A refused command is not recorded, so it can be repeated without
	// reaching [MaxScriptCommands], and each scan is one [nearest.Distance]
	// per declared id over a word this session will read up to
	// [MaxCommandBytes] of — work a redirected stdin would otherwise size
	// (Codex, #1347). Nothing within [nearest.MaxDistance] edits of a real id
	// can be longer than the longest declared one plus that many, so the
	// refusal below loses no suggestion anybody could have earned.
	if utf8.RuneCountInString(id) <= maxStepSuggestionInput {
		if suggestion, found := nearest.Name(id, names); found {
			return fmt.Sprintf("no step named %q: did you mean %q?", id, suggestion), true
		}
	}

	// names is [Session.declared], sorted once at construction, so the
	// rendering below takes it as it is: re-sorting an already-sorted
	// inventory on every refusal is work a redirected stdin chooses the
	// amount of, and refused commands are not recorded (Codex, #1347).
	return fmt.Sprintf("no step named %q: this workflow declares %s", id, stepList(names)), true
}

// noSiteMatches is the refusal of an address no site of the program matches,
// in the words the prompt and a script check share.
func noSiteMatches(id string) string {
	return fmt.Sprintf("no step matches %q: its last part names the step, and each part before it an enclosing loop, parallel, switch, or call", id)
}

// holdBreakpoint puts one breakpoint in the set, reporting whether there was
// room.
//
// The one place that knows what *adding* one costs: whether there is room, and
// that replacing an existing id is not an addition. [Session.SetBreakpoints]
// answers the same question over a whole set instead, before it touches
// anything, because a replacement that emptied the set and refilled it through
// here would take the lock per entry and leave a window with no breakpoints in
// it (#1124). Both read [MaxBreakpoints], which is the number, written once.
func (s *Session) holdBreakpoint(id string, at breakpoint) (held bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if at.id == "" {
		at.id = id
	}

	if _, replacing := s.breakpoints[id]; !replacing && len(s.breakpoints) >= MaxBreakpoints {
		return false
	}

	s.breakpoints[id] = at
	// A replacement is a different question, so it gets its own chance to say
	// it could not be asked. Carrying the old notice over would leave a second
	// unbound condition skipped in silence, after the prompt said it was set
	// (Codex, #1116).
	delete(s.notedUnbound, declinedBreakpoint+" "+id)

	return true
}

// splitCondition reads `<step-id>` or `<step-id> if <expr>`.
//
// One more [split] rather than a grammar: the vocabulary is one table parsed by
// taking the first word and handing the rest on ([Session.dispatch]), and
// `inspect` already treats its whole rest as an expression. Anything after the
// `if` is the expression, spaces and all.
// splitCondition reads `<step-id>` or `<step-id> if <expr>`, and refuses
// anything else.
//
// Refusing is the whole of it, and it took two review rounds to get there
// because the failure is silent and its shape is generous: every way of
// mistyping a condition used to fall back to an *unconditional* breakpoint.
// `break body if` did, because an empty condition and an absent one were one
// value; `break body iff n == 7` did, because a tail that was not `if` was
// discarded rather than rejected. Both arm exactly the stop-on-every-iteration
// behaviour somebody types a condition to escape, from a command that printed
// success (Codex, #1116).
//
// So the rule is that a tail is either nothing or a condition. Anything else
// is a typo, and a typo whose punishment is "your breakpoint means something
// else now" is one this prompt should not administer quietly.
func splitCondition(rest, grammar string) (id, condition string, conditional bool, err error) {
	id, tail := cutWord(strings.TrimLeft(rest, " \t"))
	tail = strings.TrimLeft(tail, " \t")
	if tail == "" {
		return id, "", false, nil
	}

	keyword, expression := cutWord(tail)
	expression = strings.TrimLeft(expression, " \t")
	if keyword != "if" {
		return "", "", false, fmt.Errorf("expected `if` after the step id, got %q: %s", keyword, grammar)
	}

	// Returned exactly as typed, trailing space included. The completer reads
	// this to find where the expression begins, and a trimmed answer told it
	// the cursor was three characters further left than it was: `break body if
	// inp ` reported the prefix `inp`, so the console cut `np ` from in front
	// of the cursor and wrote `iinputs.` (Codex, #1116). Whitespace before a
	// cursor is not nothing — it is what says the current word is empty.
	//
	// Nothing downstream minds: CEL's parser takes the surrounding space, and
	// the emptiness check below trims for its own question.
	return id, expression, true, nil
}

// compileCondition parses a condition-gated verb's condition against the run's
// own profile, returning it in the shape a step's `if:` travels in. `grammar`
// is the asking verb's own spelling, for the empty-condition refusal.
//
// A [v1.Value] holding a parsed expression, so that evaluating it is literally
// [v1.EvalConditionInScope] — the engine's own function — rather than a second
// implementation that could disagree with it.
func compileCondition(expression string, scope *v1.Scope, grammar string) (*v1.Value, error) {
	if strings.TrimSpace(expression) == "" {
		return nil, fmt.Errorf(usageCondition, grammar)
	}

	return v1.CompileDebugCondition(expression, scope.GetProfile())
}

func (s *Session) deleteBreakpoint(id string) {
	if id == "" {
		s.printfTone(ToneWarning, "delete needs a step id: delete <step-id>\n")

		return
	}

	s.mu.Lock()
	_, existed := s.breakpoints[id]
	delete(s.breakpoints, id)
	delete(s.notedUnbound, declinedBreakpoint+" "+id)
	s.mu.Unlock()

	s.record("delete " + id)
	if !existed {
		s.printf("no breakpoint at %s\n", id)

		return
	}
	s.printf("deleted breakpoint at %s\n", id)
}

func (s *Session) listBreakpoints() {
	s.mu.Lock()
	ids := make([]string, 0, len(s.breakpoints))
	for id, at := range s.breakpoints {
		// Printed as it was typed, so a reader can copy one back onto a
		// `break` line and get the breakpoint they are looking at.
		ids = append(ids, at.source)
		if at.source == "" {
			ids[len(ids)-1] = id
		}
	}
	s.mu.Unlock()

	if len(ids) == 0 {
		s.printf("no breakpoints\n")

		return
	}
	sort.Strings(ids)
	s.printf("breakpoints: %s\n", strings.Join(ids, ", "))
}

// help prints the vocabulary the prompt answers, rendered from [commands]
// rather than written out beside it: a hand-kept second copy is how a verb comes
// to be understood and undocumented, or documented and gone.
func (s *Session) help() {
	s.printf("%s\n", helpText(frontPrompt))
}

// helpText renders the commands f answers as the aligned list `help` prints.
//
// One pass to measure and one to print, so the sentences line up whatever the
// longest spelling turns out to be — a width constant would be a third place the
// vocabulary is written down.
func helpText(f front) string {
	list := commandsOn(f)
	width := 0
	for _, c := range list {
		width = max(width, len(c.spellingOn(f)))
	}
	lines := make([]string, 0, len(list))
	for _, c := range list {
		lines = append(lines, fmt.Sprintf("%-*s   %s", width, c.spellingOn(f), c.helpOn(f)))
	}

	return strings.Join(lines, "\n")
}

// spellingOn renders a command the way front f's help names it: the verb, the
// argument grammar f writes, and then the short forms.
func (c command) spellingOn(f front) string {
	out := c.verb
	if argument := c.argumentOn(f); argument != "" {
		out += " " + argument
	}
	for _, alias := range c.aliases {
		out += ", " + alias
	}

	return out
}

// sortedKeys returns a map's keys in order, for a stable listing.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
}

// cutHitClause removes a `hit <count>` clause written right after a break's
// target, returning the rest and the clause's text.
func cutHitClause(rest string) (string, string, error) {
	id, tail := cutWord(strings.TrimLeft(rest, " \t"))
	tail = strings.TrimLeft(tail, " \t")
	keyword, after := cutWord(tail)
	if keyword != "hit" {
		return rest, "", nil
	}

	after = strings.TrimLeft(after, " \t")
	clause, condition, found := strings.Cut(after, " if ")
	clause = strings.TrimSpace(clause)
	if clause == "" {
		return "", "", fmt.Errorf("`hit` needs a count: %s", grammarBreak)
	}
	if !found {
		return id, clause, nil
	}

	return id + " if " + condition, clause, nil
}

// setFailureMode is `catch`.
func (s *Session) setFailureMode(word string) {
	modes := map[string]v1.DebugFailureMode{
		"none":     v1.DebugFailureMode_DEBUG_FAILURE_MODE_NONE,
		"uncaught": v1.DebugFailureMode_DEBUG_FAILURE_MODE_UNCAUGHT,
		"all":      v1.DebugFailureMode_DEBUG_FAILURE_MODE_ALL,
	}
	if word == "" {
		word = "uncaught"
	}
	mode, ok := modes[word]
	if !ok {
		s.printfTone(ToneWarning, "catch takes none, uncaught, or all, not %q\n", word)

		return
	}

	s.mu.Lock()
	s.contract.failureMode = mode
	s.mu.Unlock()

	s.record("catch " + word)
	s.printf("failure stops: %s\n", word)
}

// addLogpoint is `log <step-id> <message>`.
func (s *Session) addLogpoint(rest string) {
	id, message := cutWord(rest)
	message = strings.TrimSpace(message)
	if id == "" || message == "" {
		s.printfTone(ToneWarning, "log needs a step id and a message: log <step-id> <message>\n")

		return
	}
	if notice, unknown := s.unknownStepNotice(id); unknown {
		s.printfTone(ToneWarning, "log: %s\n", notice)

		return
	}
	target := v1.ParseDebugTargetOrStep(id)
	template, err := parseLogTemplate(message)
	if err != nil {
		s.printfTone(ToneWarning, "log: %v\n", err)

		return
	}

	source := id + " " + message
	if !s.holdBreakpoint("log "+id, breakpoint{source: "log " + source, id: "log " + id, target: target, log: template,
		definition: &v1.DebugBreakpoint{Id: "log " + id, Step: id, LogMessage: message}}) {
		s.printfTone(ToneWarning, "a session holds at most %d breakpoints\n", MaxBreakpoints)

		return
	}
	s.record("log " + source)
	s.printf("logpoint at %s\n", id)
}

// expand lists an expression's children, through the same [Session.Inspect] a
// structured front reads, so the redactors, the page size and the wording are
// one thing on both.
func (s *Session) expand(ctx context.Context, expression string) {
	answer, err := s.Inspect(ctx, &v1.DebugInspectRequest{Expression: expression, Children: true})
	switch {
	case err != nil:
		s.printfTone(ToneWarning, "cannot expand: %v\n", err)
	case answer.GetError() != "":
		s.emitTone(ToneWarning, answer.GetError()+"\n")
	default:
		s.printf("%s", formatChildren(expression, answer))
	}
}

// showStatus prints where the run is, and why, as a driver's `status` does.
func (s *Session) showStatus() {
	snapshot, _ := s.Snapshot(context.Background())
	s.printf("%s", FormatSnapshot(snapshot))
}
