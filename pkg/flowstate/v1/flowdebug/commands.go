package flowdebug

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/google/cel-go/common/types/ref"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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
	{verb: "until", aliases: []string{"u"}, argument: "<step-id>", completes: completesStep,
		help: "run until the step with that id"},
	{verb: "break", aliases: []string{"b"}, argument: "<step-id>", completes: completesStep,
		help: "stop at that step whenever it is reached"},
	{verb: "delete", aliases: []string{"d"}, argument: "<step-id>", completes: completesBreakpoint,
		help: "remove that breakpoint"},
	{verb: "breakpoints", completes: completesNothing,
		help: "list them"},
	{verb: "inspect", aliases: []string{"p"}, argument: "<expr>", completes: completesExpression,
		help: "evaluate a CEL expression against this run's scope"},
	{verb: "scope", completes: completesNothing,
		help: "list what this run can name right now"},
	{verb: "info", aliases: []string{"step-info"}, completes: completesNothing,
		help: "describe the step the run is stopped at"},
	{verb: "quit", aliases: []string{"q"}, completes: completesNothing,
		help: "end the run here"},
	{verb: "help", aliases: []string{"h", "?"}, completes: completesNothing,
		help: "list these"},
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
		target := strings.TrimSpace(rest)
		if target == "" {
			s.printfTone(ToneWarning, "until needs a step id: until <step-id>\n")

			return false, nil
		}
		s.record("until " + target)
		s.resume(modeUntil, target)

		return true, nil

	case "break":
		s.addBreakpoint(strings.TrimSpace(rest))

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
			s.printfTone(ToneWarning, "inspect needs an expression: inspect steps.build.artifact\n")

			return false, nil
		}
		s.record("inspect " + expression)
		s.inspect(ctx, expression, scope)

		return false, nil

	case "scope":
		s.record("scope")
		s.showScope(scope)

		return false, nil

	case "info":
		s.record("info")
		s.showStep(node)

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

// split separates the first word of a line from the rest.
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
	if len(extra) > 0 {
		s.printf("bound: %s\n", namesLine(sortedKeys(extra)))
	}
	steps := scope.GetOutputs().GetStepValues()
	if len(steps) == 0 {
		s.printf("no steps have produced outputs yet\n")
	} else {
		names := make([]string, 0, len(steps))
		for name := range steps {
			names = append(names, name)
		}
		sort.Strings(names)
		s.printf("steps: %s\n", namesLine(names))
	}

	if vars := scope.GetVars(); len(vars) > 0 {
		s.printf("vars: %s\n", namesLine(sortedKeys(vars)))
	}
	if ambient := scope.GetAmbientVars(); len(ambient) > 0 {
		s.printf("workflow vars: %s\n", namesLine(sortedKeys(ambient)))
	}
}

// namesLine renders one scope line's names, bounded by [MaxScopeNames].
//
// The remainder is counted rather than dropped, for the reason every other
// truncation in this package carries a notice: a list silently cut at twenty
// tells a reader their run has twenty steps.
func namesLine(names []string) string {
	if len(names) <= MaxScopeNames {
		return strings.Join(names, ", ")
	}

	return fmt.Sprintf("%s … and %d more (tab completes them; `inspect steps.` lists them)",
		strings.Join(names[:MaxScopeNames], ", "), len(names)-MaxScopeNames)
}

// showStep prints what the run is stopped at.
func (s *Session) showStep(node *v1.Node) {
	s.printf("%s (%s)\n", node.GetId(), NodeKind(node))
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

func (s *Session) addBreakpoint(id string) {
	if id == "" {
		s.printfTone(ToneWarning, "break needs a step id: break <step-id>\n")

		return
	}

	s.mu.Lock()
	full := len(s.breakpoints) >= MaxBreakpoints
	if !full {
		s.breakpoints[id] = struct{}{}
	}
	s.mu.Unlock()

	if full {
		s.printfTone(ToneWarning, "a session holds at most %d breakpoints\n", MaxBreakpoints)

		return
	}
	s.record("break " + id)
	s.printf("breakpoint at %s\n", id)
}

func (s *Session) deleteBreakpoint(id string) {
	if id == "" {
		s.printfTone(ToneWarning, "delete needs a step id: delete <step-id>\n")

		return
	}

	s.mu.Lock()
	_, existed := s.breakpoints[id]
	delete(s.breakpoints, id)
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
	for id := range s.breakpoints {
		ids = append(ids, id)
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

// NodeKind names a step's kind for a person reading a prompt: the word the
// file spells, plus the one detail that identifies which one it is.
//
// A new spelling rather than a shared one, deliberately and narrowly: nothing
// exported names a node's kind for a reader today (flowfile's describeNode
// names YAML AST nodes, a different thing), and the engine's own switches over
// [v1.Node] kinds exist to *run* them. If a second reader-facing namer ever
// appears, these two should become one — that is the rule, and this is the
// first of them rather than the second.
func NodeKind(node *v1.Node) string {
	switch kind := node.GetKind().(type) {
	case *v1.Node_Task:
		return fmt.Sprintf("task %q", kind.Task.GetName())
	case *v1.Node_Value:
		return "value"
	case *v1.Node_Wait:
		if signal := kind.Wait.GetSignal(); signal != nil {
			return fmt.Sprintf("wait_for_signal %q", signal.GetName())
		}

		return "wait"
	case *v1.Node_ForEach:
		return "for_each"
	case *v1.Node_Parallel:
		return "parallel"
	case *v1.Node_Switch:
		return "switch"
	case *v1.Node_Call:
		return fmt.Sprintf("call %q", kind.Call.GetWorkflow())
	default:
		return "step"
	}
}
