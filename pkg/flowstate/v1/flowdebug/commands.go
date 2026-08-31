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
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/nearest"
)

// A command is one verb the session understands.
//
// The table is the vocabulary, in one place, because three things need it and a
// verb known to two of them is a bug somebody meets rather than reads: `dispatch`
// resolves an alias through it, `help` prints it, and the completer offers it.
// Before it existed the aliases lived in the `case` labels and the help text was
// a second hand-written copy of the same list — which is exactly the shape
// CLAUDE.md names, one meaning written down twice, and a prompt that completed
// `breakpoints` while `help` had forgotten to mention it would have been nobody's
// fault in particular.
type command struct {
	// verb is the canonical spelling, and the only one the dispatch switch
	// below has a case for.
	verb string

	// aliases are the short forms, in the order help shows them.
	aliases []string

	// argument names what follows the verb, for the help line and so the
	// completer knows there is a second word to complete at all.
	argument string

	// help is the sentence beside the verb.
	help string

	// completes says what a surface should offer for this command's argument.
	completes completionSubject
}

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
// because that is what a session does most, then breakpoints, then the two
// questions, then leaving.
var commands = []command{
	{verb: "step", aliases: []string{"s"}, completes: completesNothing,
		help: "run this step and stop at the next (also: an empty line)"},
	{verb: "continue", aliases: []string{"c"}, completes: completesNothing,
		help: "run until the next breakpoint, or to the end"},
	{verb: "until", aliases: []string{"u"}, argument: "<step-id> [if <expr>]", completes: completesStep,
		help: "run until the step with that id, optionally only where the condition holds"},
	{verb: "break", aliases: []string{"b"}, argument: "<step-id> [if <expr>]", completes: completesStep,
		help: "stop at that step, always or when the expression holds"},
	{verb: "delete", aliases: []string{"d"}, argument: "<step-id>", completes: completesBreakpoint,
		help: "remove that breakpoint"},
	{verb: "breakpoints", completes: completesNothing,
		help: "list them"},
	{verb: "inspect", aliases: []string{"p"}, argument: "<expr>", completes: completesExpression,
		help: "evaluate a CEL expression against this run's scope"},
	{verb: "scope", completes: completesNothing,
		help: "list what this run can name right now"},
	{verb: "complete", argument: "<partial-command>", completes: completesNothing,
		help: "list what could be written at the end of that text"},
	{verb: "info", aliases: []string{"step-info"}, completes: completesNothing,
		help: "describe the step the run is stopped at"},
	{verb: "backtrace", aliases: []string{"bt"}, completes: completesNothing,
		help: "list this step and the call chain that reached it"},
	{verb: "quit", aliases: []string{"q"}, completes: completesNothing,
		help: "end the run here"},
	{verb: "help", aliases: []string{"h", "?"}, completes: completesNothing,
		help: "list these"},
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

	// Recorded before the command runs and only for commands that were
	// understood, so a replay script holds a session's decisions and not its
	// typing mistakes.
	switch verb {
	case "step":
		s.record("step")
		s.resume(modeStop, "")

		return true, nil

	case "continue":
		s.record("continue")
		s.resume(modeRun, "")

		return true, nil

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

		var compiled *v1.Value
		if conditional {
			compiled, err = compileCondition(condition, scope, grammarUntil)
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
		s.resumeUntil(modeUntil, id, compiled)

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

func (s *Session) showBacktrace() {
	trace, err := s.Backtrace()
	if err != nil {
		s.printfTone(ToneWarning, "%s\n", err)
		return
	}
	for i, frame := range trace.GetFrames() {
		name := frame.GetStepId()
		if frame.GetWorkflow() != "" {
			name = frame.GetWorkflow() + "." + name
		}
		s.printf("#%d %s (%s)\n", i, name, frame.GetKind())
	}
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

	activation := scope.Activation(ctx)
	if len(extra) > 0 {
		activation = scope.ActivationWith(ctx, extra)
	}
	out, err := v1.DefaultEvaluator().EvalString(ctx, expression, libs, activation)
	if err != nil {
		// An author's expression failing is an ordinary event at a debugger
		// prompt, not a session-ending one: they are asking questions, and
		// some of them will not compile.
		s.printfTone(ToneWarning, "%v\n", err)

		return
	}

	// Redacted before the cap, for the reason [Session.stepOutcomeText] gives:
	// truncating first would leave the first MaxInspectRunes of a long secret
	// in a string no substring match can recognise (Codex, #1109).
	s.printf("%s\n", capRunes(s.redactText(s.refValText(out)), MaxInspectRunes))
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
	groups := s.scopeNames(scope, extra)

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
	scopeGroupVars         = "vars"
	scopeGroupWorkflowVars = "workflow vars"
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
func (s *Session) scopeNames(scope *v1.Scope, extra map[string]ref.Val) []Names {
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

	// These two are the lines a namespace is easiest to get wrong on, because
	// the labels read the other way round from where the names live.
	// `Scope.Vars` are the *bare* bindings — a loop's `as:`, a step's own
	// `vars:` — offered as [celcomplete.Scope.Locals] under no root at all
	// (complete.go:271). `Scope.AmbientVars` are the workflow's declared
	// `vars:`, and those are what `vars.` reaches (complete.go:280-282).
	add(scopeGroupVars, "", sortedKeys(scope.GetVars()))
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

// addBreakpoint takes `<step-id>` or `<step-id> if <expr>`.
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

	at := breakpoint{source: rest}
	if conditional {
		compiled, err := compileCondition(condition, scope, grammarBreak)
		if err != nil {
			s.printfTone(ToneWarning, "break %s: %v\n", id, err)

			return
		}
		at.condition = compiled
	}

	full := !s.holdBreakpoint(id, at)

	if full {
		s.printfTone(ToneWarning, "a session holds at most %d breakpoints\n", MaxBreakpoints)

		return
	}
	s.record("break " + rest)
	if at.condition == nil {
		s.printf("breakpoint at %s\n", id)

		return
	}
	s.printf("breakpoint at %s if %s\n", id, strings.TrimSpace(condition))
}

// maxStepSuggestionInput bounds the typed id a did-you-mean is computed for,
// the same rule and the same reasoning cmd/flow's maxSuggestionInput states
// for argv. A step id is a CEL identifier, so a real one is short; this sits
// far above any an author would write and far below what a scan of the
// inventory can be made to cost.
const maxStepSuggestionInput = 64

// unknownStepNotice reports that a step id names nothing this run can reach,
// in the words `flow debug replay` already refuses the same line with.
//
// The prompt used to arm anything: `break nosuchstep` answered "breakpoint at
// nosuchstep", listed it, and never fired, while `until nosuchstep` printed
// nothing at all and ran the workflow to its end — one mistyped character
// forfeiting the session, with every queued command after it unanswered. The
// check that catches it already existed one door over, in [checkScript], over
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
	// Built once at construction ([declaredStepIDs]); this is a lookup rather
	// than a walk, because a refused command is not recorded and so may be
	// repeated without bound.
	_, known := s.declaredIDs[id]

	s.mu.Lock()
	// An id this session has watched go past is reachable whatever the
	// inventory said, so it is admitted — but it never *makes* an inventory:
	// what has run so far is not what the workflow declares, and reading it
	// that way would refuse every step the run has not reached yet, which on
	// an empty inventory is all of them.
	if !known {
		_, known = s.seen[id]
	}
	s.mu.Unlock()

	names := s.declared
	if known || len(names) == 0 {
		return "", false
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

	return fmt.Sprintf("no step named %q: this workflow declares %s", id, stepList(names)), true
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

	env, err := v1.DefaultEvaluator().ProfileEnv(scope.GetProfile())
	if err != nil {
		return nil, err
	}
	ast, issues := env.Parse(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("parse condition: %w", issues.Err())
	}

	// Parsing is syntax only, so `1 + true` and `missing_function(n)` both
	// parse — and an accepted condition that cannot be compiled fails at every
	// arrival, which with the stop-on-error rule above means stopping at every
	// iteration. That is the exact behaviour a condition is typed to escape,
	// reached by a typo the prompt reported as accepted (Codex, #1116).
	//
	// Checked against an environment declaring the names *the expression
	// itself mentions*, which is `flowfile`'s spelling for this same problem
	// (`celcheck.go:177`, `envDeclaring(referencedNames(...))`) and the only
	// one that works here. A breakpoint is usually set before the run reaches
	// the step it names, so the binding a condition reads — a loop's `as:` —
	// does not exist in the scope this is typed in. Declaring what is in scope
	// now would reject `n == 7` typed at the first step, which is a false
	// diagnostic about a condition that will be perfectly valid when it fires.
	checked, err := checkedInScope(env, ast)
	if err != nil {
		return nil, err
	}

	// And it has to be a boolean, refused here rather than at the first
	// arrival — the same shape `compileMustIn` uses for the other place this
	// repository compiles an author's boolean rule (`constraints.go:238-245`).
	if checked.OutputType() != cel.BoolType && checked.OutputType() != cel.DynType {
		return nil, fmt.Errorf("a condition must be a boolean, and this one is %s", checked.OutputType())
	}

	parsed, err := cel.AstToParsedExpr(ast)
	if err != nil {
		return nil, fmt.Errorf("parse condition: %w", err)
	}

	return &v1.Value{Kind: &v1.Value_Expr{Expr: parsed}}, nil
}

// checkedInScope type-checks an expression against an environment extended
// with every identifier it references, declared dynamically.
//
// Dynamic because nothing here knows the type: a step output's shape is the
// task's, and a loop binding's is the collection's. What the check is for is
// the errors that do not depend on those — an unknown function, an operator
// applied to types that can never combine.
func checkedInScope(env *cel.Env, ast *cel.Ast) (*cel.Ast, error) {
	parsed, err := cel.AstToParsedExpr(ast)
	if err != nil {
		return nil, fmt.Errorf("parse condition: %w", err)
	}

	names := map[string]struct{}{}
	collectIdentifiers(parsed.GetExpr(), names)

	declarations := make([]cel.EnvOption, 0, len(names))
	for name := range names {
		declarations = append(declarations, cel.Variable(name, cel.DynType))
	}

	declaring, err := env.Extend(declarations...)
	if err != nil {
		// Extending failed, which is this build's problem rather than the
		// author's, so the condition is accepted unchecked rather than
		// refused: leaving the failure to evaluation is where it was before
		// this check existed, and blaming an author for it is worse.
		return ast, nil
	}

	checked, issues := declaring.Check(ast)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("condition: %w", issues.Err())
	}

	return checked, nil
}

// collectIdentifiers gathers every bare name an expression reads, for the
// environment the type check declares.
//
// Only the *root* of a selection: `steps.build.ok` reads the identifier
// `steps`, and declaring `steps` dynamically is what makes the whole chain
// legal without claiming to know its shape. Macro bindings are included on
// purpose here — declaring one is harmless, and not declaring it would make
// `items.exists(i, i > 2)` fail a check over a name CEL itself provides.
//
// This is deliberately not the same question as [conditionNames]: declaring a
// name costs nothing, while *requiring* one to be bound at a step decides
// whether the run stops there.
func collectIdentifiers(e *expr.Expr, into map[string]struct{}) {
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		into[kind.IdentExpr.GetName()] = struct{}{}

	case *expr.Expr_SelectExpr:
		collectIdentifiers(kind.SelectExpr.GetOperand(), into)

	case *expr.Expr_CallExpr:
		collectIdentifiers(kind.CallExpr.GetTarget(), into)
		for _, arg := range kind.CallExpr.GetArgs() {
			collectIdentifiers(arg, into)
		}

	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.GetElements() {
			collectIdentifiers(element, into)
		}

	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			collectIdentifiers(entry.GetMapKey(), into)
			collectIdentifiers(entry.GetValue(), into)
		}

	case *expr.Expr_ComprehensionExpr:
		comprehension := kind.ComprehensionExpr
		into[comprehension.GetIterVar()] = struct{}{}
		into[comprehension.GetIterVar2()] = struct{}{}
		into[comprehension.GetAccuVar()] = struct{}{}
		collectIdentifiers(comprehension.GetIterRange(), into)
		collectIdentifiers(comprehension.GetAccuInit(), into)
		collectIdentifiers(comprehension.GetLoopCondition(), into)
		collectIdentifiers(comprehension.GetLoopStep(), into)
		collectIdentifiers(comprehension.GetResult(), into)
	}
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

// help prints the vocabulary, rendered from [commands] rather than written out
// beside it: a hand-kept second copy is how a verb comes to be understood and
// undocumented, or documented and gone.
func (s *Session) help() {
	// One pass to measure and one to print, so the sentences line up whatever
	// the longest spelling turns out to be — a width constant would be a third
	// place the vocabulary is written down.
	width := 0
	for _, c := range commands {
		width = max(width, len(c.spelling()))
	}
	for _, c := range commands {
		s.printf("%-*s   %s\n", width, c.spelling(), c.help)
	}
}

// spelling renders a command the way help names it: the verb, its argument, and
// then the short forms.
func (c command) spelling() string {
	out := c.verb
	if c.argument != "" {
		out += " " + c.argument
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
