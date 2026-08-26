package flowdebug

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

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
		// No namespace named: an autopsy binding is offered bare, and one
		// carrying members is a root under its *own* name rather than a shared
		// one, so there is no single spelling to point at.
		s.printf("bound: %s\n", namesLine(sortedKeys(extra), ""))
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
		s.printf("steps: %s\n", namesLine(names, "inspect steps."))
	}

	// These two are the lines a namespace is easiest to get wrong on, because
	// the labels read the other way round from where the names live.
	// `Scope.Vars` are the *bare* bindings — a loop's `as:`, a step's own
	// `vars:` — offered as [celcomplete.Scope.Locals] under no root at all
	// (complete.go:271). `Scope.AmbientVars` are the workflow's declared
	// `vars:`, and those are what `vars.` reaches (complete.go:280-282).
	if vars := scope.GetVars(); len(vars) > 0 {
		s.printf("vars: %s\n", namesLine(sortedKeys(vars), ""))
	}
	if ambient := scope.GetAmbientVars(); len(ambient) > 0 {
		s.printf("workflow vars: %s\n", namesLine(sortedKeys(ambient), "inspect vars."))
	}
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
	id, condition, conditional, err := splitCondition(rest)
	if err != nil {
		s.printfTone(ToneWarning, "break: %v\n", err)

		return
	}
	if id == "" {
		s.printfTone(ToneWarning, "break needs a step id: break <step-id> [if <expr>]\n")

		return
	}

	at := breakpoint{source: rest}
	if conditional {
		compiled, names, err := compileCondition(condition, scope)
		if err != nil {
			s.printfTone(ToneWarning, "break %s: %v\n", id, err)

			return
		}
		at.condition, at.names = compiled, names
	}

	s.mu.Lock()
	_, replacing := s.breakpoints[id]
	full := !replacing && len(s.breakpoints) >= MaxBreakpoints
	if !full {
		s.breakpoints[id] = at
		// A replacement is a different question, so it gets its own chance to
		// say it could not be asked. Carrying the old notice over would leave
		// a second unbound condition skipped in silence, after the prompt said
		// it was set (Codex, #1116).
		delete(s.notedUnbound, id)
	}
	s.mu.Unlock()

	if full {
		s.printfTone(ToneWarning, "a session holds at most %d breakpoints\n", MaxBreakpoints)

		return
	}
	s.record("break " + rest)
	if at.condition == nil {
		s.printf("breakpoint at %s\n", id)

		return
	}
	s.printf("breakpoint at %s if %s\n", id, condition)
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
func splitCondition(rest string) (id, condition string, conditional bool, err error) {
	id, tail := split(strings.TrimSpace(rest))
	tail = strings.TrimSpace(tail)
	if tail == "" {
		return id, "", false, nil
	}

	keyword, expression := split(tail)
	if keyword != "if" {
		return "", "", false, fmt.Errorf("expected `if` after the step id, got %q: break <step-id> [if <expr>]", keyword)
	}

	return id, strings.TrimSpace(expression), true, nil
}

// compileCondition parses a breakpoint's condition against the run's own
// profile, returning it in the shape a step's `if:` travels in.
//
// A [v1.Value] holding a parsed expression, so that evaluating it is literally
// [v1.EvalConditionInScope] — the engine's own function — rather than a second
// implementation that could disagree with it.
func compileCondition(expression string, scope *v1.Scope) (*v1.Value, []string, error) {
	if expression == "" {
		return nil, nil, fmt.Errorf("`if` needs an expression: break <step-id> if <expr>")
	}

	env, err := v1.DefaultEvaluator().ProfileEnv(scope.GetProfile())
	if err != nil {
		return nil, nil, err
	}
	ast, issues := env.Parse(expression)
	if issues != nil && issues.Err() != nil {
		return nil, nil, fmt.Errorf("parse condition: %w", issues.Err())
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
		return nil, nil, err
	}

	// And it has to be a boolean, refused here rather than at the first
	// arrival — the same shape `compileMustIn` uses for the other place this
	// repository compiles an author's boolean rule (`constraints.go:238-245`).
	if checked.OutputType() != cel.BoolType && checked.OutputType() != cel.DynType {
		return nil, nil, fmt.Errorf("a condition must be a boolean, and this one is %s", checked.OutputType())
	}

	parsed, err := cel.AstToParsedExpr(ast)
	if err != nil {
		return nil, nil, fmt.Errorf("parse condition: %w", err)
	}

	return &v1.Value{Kind: &v1.Value_Expr{Expr: parsed}}, conditionNames(checked, parsed), nil
}

// conditionNames are the names a condition needs bound to be a question about
// a scope at all.
//
// Two independent facts decide it, and each was learned by getting it wrong.
//
// Which references are *values* comes from the checker, not from syntactic
// position: `total.startsWith("3")` reads `total` from the scope and
// `math.abs(n)` reads `math` from nowhere, and those are the same shape. A
// reference carrying overload ids is a function; one carrying only a name is a
// variable (Codex, #1116).
//
// Which references are *free* comes from the parse tree, per node rather than
// per name. `n == 3 && [1].exists(n, n == 1)` binds an `n` inside the macro
// and reads a different `n` outside it, so excluding the name globally dropped
// a genuinely free reference and reopened the sibling-loop stop this guard
// exists for (Codex, #1116). The reference map is keyed by expression id, and
// so is the scope walk, so the two are joined on the node rather than on the
// spelling.
func conditionNames(checked *cel.Ast, parsed *expr.ParsedExpr) []string {
	free := map[int64]struct{}{}
	collectFreeIdents(parsed.GetExpr(), map[string]int{}, free)

	required := map[string]struct{}{}
	for id, reference := range checked.NativeRep().ReferenceMap() {
		if len(reference.OverloadIDs) > 0 || reference.Name == "" {
			continue
		}
		if _, isFree := free[id]; !isFree {
			continue
		}
		required[reference.Name] = struct{}{}
	}

	out := make([]string, 0, len(required))
	for name := range required {
		out = append(out, name)
	}
	sort.Strings(out)

	return out
}

// collectFreeIdents records the id of every identifier an expression reads
// from outside itself.
//
// bound counts how many enclosing comprehensions bind each name, so a name
// shadowed at one depth is still free at another — a count rather than a set
// because macros nest, and `[1].exists(n, [2].exists(n, n == 2)) && n == 3`
// has to leave the last `n` free while the middle one is not.
func collectFreeIdents(e *expr.Expr, bound map[string]int, into map[int64]struct{}) {
	switch kind := e.GetExprKind().(type) {
	case *expr.Expr_IdentExpr:
		if bound[kind.IdentExpr.GetName()] == 0 {
			into[e.GetId()] = struct{}{}
		}

	case *expr.Expr_SelectExpr:
		collectFreeIdents(kind.SelectExpr.GetOperand(), bound, into)

	case *expr.Expr_CallExpr:
		collectFreeIdents(kind.CallExpr.GetTarget(), bound, into)
		for _, arg := range kind.CallExpr.GetArgs() {
			collectFreeIdents(arg, bound, into)
		}

	case *expr.Expr_ListExpr:
		for _, element := range kind.ListExpr.GetElements() {
			collectFreeIdents(element, bound, into)
		}

	case *expr.Expr_StructExpr:
		for _, entry := range kind.StructExpr.GetEntries() {
			collectFreeIdents(entry.GetMapKey(), bound, into)
			collectFreeIdents(entry.GetValue(), bound, into)
		}

	case *expr.Expr_ComprehensionExpr:
		comprehension := kind.ComprehensionExpr

		// The range and the accumulator's initial value are evaluated before
		// the loop binds anything, so they are the enclosing scope's.
		collectFreeIdents(comprehension.GetIterRange(), bound, into)
		collectFreeIdents(comprehension.GetAccuInit(), bound, into)

		// Every name the comprehension binds, which is a closed set of three:
		// cel-go's own ComprehensionExpr exposes IterVar, IterVar2 and
		// AccuVar and nothing else. Two-variable macros — `exists(i, v, …)`
		// over a map or an indexed list — bind the second, and omitting it
		// made a macro-local name look like a scope name the step had to
		// have, so a true condition never fired (Codex, #1116).
		//
		// [comprehensionBindings] is where that set is written down, with a
		// test that fails if the schema grows a fourth.
		bindings := comprehensionBindings(comprehension)
		for _, name := range bindings {
			bound[name]++
		}
		collectFreeIdents(comprehension.GetLoopCondition(), bound, into)
		collectFreeIdents(comprehension.GetLoopStep(), bound, into)
		collectFreeIdents(comprehension.GetResult(), bound, into)
		for _, name := range bindings {
			bound[name]--
		}
	}
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

// comprehensionBindings are the names a comprehension binds for its body.
//
// One place, because two walks need the same answer and a list written twice
// is how one of them comes to be missing a member — which is exactly how
// IterVar2 went missing from one of them. An empty name is skipped rather than
// bound: a one-variable macro leaves IterVar2 unset, and binding "" would
// shadow nothing while making the count bookkeeping lie.
func comprehensionBindings(comprehension *expr.Expr_Comprehension) []string {
	all := []string{comprehension.GetIterVar(), comprehension.GetIterVar2(), comprehension.GetAccuVar()}

	bindings := make([]string, 0, len(all))
	for _, name := range all {
		if name != "" {
			bindings = append(bindings, name)
		}
	}

	return bindings
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
		for _, name := range comprehensionBindings(comprehension) {
			into[name] = struct{}{}
		}
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
	delete(s.notedUnbound, id)
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
