package flowdebug

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/google/cel-go/common/types/ref"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// dispatch runs one command line. It reports whether the run resumes, and
// returns an error only where the session is ending the run — a mistyped
// command is answered and asked again, never fatal, because ending someone's
// run over a typo is the worst possible reading of an ambiguous line.
func (s *Session) dispatch(ctx context.Context, line string, node *v1.Node, scope *v1.Scope) (resumed bool, err error) {
	verb, rest := split(line)
	if verb == "" {
		// A bare newline repeats the most useful thing: one step. It is what
		// every debugger a person has used already does, and a session where
		// return does nothing is one where they press it twice.
		verb = "step"
	}

	// Recorded before the command runs and only for commands that were
	// understood, so a replay script holds a session's decisions and not its
	// typing mistakes.
	switch verb {
	case "step", "s":
		s.record("step")
		s.resume(modeStop, "")

		return true, nil

	case "continue", "c":
		s.record("continue")
		s.resume(modeRun, "")

		return true, nil

	case "until", "u":
		target := strings.TrimSpace(rest)
		if target == "" {
			s.printfTone(ToneWarning, "until needs a step id: until <step-id>\n")

			return false, nil
		}
		s.record("until " + target)
		s.resume(modeUntil, target)

		return true, nil

	case "break", "b":
		s.addBreakpoint(strings.TrimSpace(rest))

		return false, nil

	case "delete", "d":
		s.deleteBreakpoint(strings.TrimSpace(rest))

		return false, nil

	case "breakpoints":
		s.record("breakpoints")
		s.listBreakpoints()

		return false, nil

	case "inspect", "p":
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

	case "step-info", "info":
		s.record("info")
		s.showStep(node)

		return false, nil

	case "quit", "q":
		s.record("quit")
		// Remembered, so the autopsy stays shut: quit is the one command
		// advertised as leaving, and it must not be answered with another
		// prompt (Codex, #1107).
		s.mu.Lock()
		s.ended = true
		s.mu.Unlock()

		return false, errQuit

	case "help", "h", "?":
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

	s.printf("%s\n", capRunes(refValText(out), MaxInspectRunes))
}

// showScope lists what the paused run can name, which is the question an
// author asks before they know what to inspect.
func (s *Session) showScope(scope *v1.Scope) {
	steps := scope.GetOutputs().GetStepValues()
	if len(steps) == 0 {
		s.printf("no steps have produced outputs yet\n")
	} else {
		names := make([]string, 0, len(steps))
		for name := range steps {
			names = append(names, name)
		}
		sort.Strings(names)
		s.printf("steps: %s\n", strings.Join(names, ", "))
	}

	if vars := scope.GetVars(); len(vars) > 0 {
		s.printf("vars: %s\n", strings.Join(sortedKeys(vars), ", "))
	}
	if ambient := scope.GetAmbientVars(); len(ambient) > 0 {
		s.printf("workflow vars: %s\n", strings.Join(sortedKeys(ambient), ", "))
	}
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

func (s *Session) help() {
	s.printf(`step, s          run this step and stop at the next (also: an empty line)
continue, c      run until the next breakpoint, or to the end
until <id>, u    run until the step with that id
break <id>, b    stop at that step whenever it is reached
delete <id>, d   remove that breakpoint
breakpoints      list them
inspect <expr>   evaluate a CEL expression against this run's scope
scope            list what this run can name right now
info             describe the step the run is stopped at
quit, q          end the run here
`)
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
