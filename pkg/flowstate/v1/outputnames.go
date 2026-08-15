package flowstatev1

import (
	"fmt"
	"maps"
	"slices"
)

// "What names does `steps.<id>.` expose?" used to be answered independently in
// at least three places — the language server's hover and completion, and
// switchDomain in the flowfile package — each holding a partial, private copy
// of what a node kind produces. [ValueOutput]'s own doc comment names six
// hand-spelled copies of `"value"` as the failure it exists to prevent; this
// file treats the same disease at the level of the whole per-kind mapping
// rather than one string. One function, called by every surface that answers
// the question, so a kind added to the schema cannot land with three call
// sites silently disagreeing about what it exposes — see
// TestEveryNodeKindIsCovered.

// NamedOutput describes one name a step exposes under `steps.<id>.<name>`,
// suitable for an editor's hover popup or completion candidate list.
//
// Name is empty when the set of names a node instance actually produces is not
// knowable from the file alone — a task whose `outputs:` shaping is a computed
// expression, a plugin task with no descriptor, an unregistered task. Description
// still says what to expect in that case; a caller that wants only the concrete,
// offerable names should skip entries with an empty Name.
type NamedOutput struct {
	Name        string
	Description string

	// Source is the [Value] that computes this output, when the node holds it
	// directly as a written expression — a `value:` step's whole expression, or
	// one entry of a `wait_for_signal:`'s `outputs:` shaping. nil for every other
	// output: a task's declared fields are typed by a descriptor rather than an
	// expression, and `results`, `state`, `timed_out`, `payload`, and `sender`
	// are the engine's own record of what happened, not a value written in the
	// file. A domain-inference reader — walking a shaping expression's literal
	// leaves to bound a `switch:`'s discriminant — is the one consumer this
	// field exists for; everything else can ignore it.
	Source *Value
}

// OutputNames answers, for one node, every name `steps.<id>.` may expose and
// what each one is.
//
// tasks resolves a task step's declared or shaped outputs; nil means
// [DefaultRegistry], the convention every other registry-optional lookup in
// this codebase already uses.
//
// ok reports whether this node kind can ever expose a name under its own step
// id at all. It is false for exactly one kind today: a `parallel:` block's
// branches merge their outputs into the enclosing scope when it joins, so
// nothing is reachable under the parallel step's own id — an empty names slice
// there is the true answer, not an omission, and ok says so explicitly rather
// than leaving a caller to guess whether zero means "nothing to offer" or "this
// case was never handled". Every other kind returns ok true with at least one
// entry, even when the concrete name set is not statically knowable — that
// entry's Name is empty and its Description says why.
func OutputNames(node *Node, tasks *Registry) (names []NamedOutput, ok bool) {
	switch kind := node.GetKind().(type) {
	case *Node_Task:
		return taskOutputNames(kind.Task, tasks), true

	case *Node_Wait:
		return waitOutputNames(kind.Wait), true

	case *Node_Value:
		return []NamedOutput{{
			Name:        ValueOutput,
			Description: "What the step's expression evaluated to. A `value:` step produces exactly this one output.",
			Source:      kind.Value,
		}}, true

	case *Node_Switch:
		return []NamedOutput{{
			Name:        SwitchValueOutput,
			Description: "What the switch's `value:` evaluated to, recorded whether or not any case matched.",
		}, {
			Name:        SwitchCaseOutput,
			Description: "The case literal that matched, and `null` when none did — whether the `default:` body ran or nothing did.",
		}}, true

	case *Node_Loop:
		out := []NamedOutput{{
			Name:        LoopResultsField,
			Description: "One entry per iteration, each a map of body step id to that step's named outputs. Body outputs do not escape the loop.",
		}}
		if LoopCarriesState(kind.Loop) {
			out = append(out, NamedOutput{
				Name:        LoopStateField,
				Description: "The value the loop carried between iterations, after its last update — the loop's answer for an accumulate-until shape.",
			})
		}
		return out, true

	case *Node_ForEach:
		return []NamedOutput{{
			Name:        LoopResultsField,
			Description: "One entry per iteration, each a map of body step id to that step's named outputs. Body outputs do not escape the loop.",
		}}, true

	case *Node_Parallel:
		// Deliberately nothing: see ok's doc above.
		return nil, false

	case *Node_Call:
		decls := kind.Call.GetWorkflow().GetDeclaredOutputs()
		if len(decls) == 0 {
			return []NamedOutput{{
				Description: "the called workflow declares no outputs",
			}}, true
		}
		out := make([]NamedOutput, 0, len(decls))
		for _, d := range decls {
			desc := fmt.Sprintf("Declared output %q of the called workflow.", d.GetName())
			if p := d.GetDescription(); p != "" {
				desc = p
			}
			out = append(out, NamedOutput{Name: d.GetName(), Description: desc})
		}
		return out, true

	default:
		// A node kind the schema defines and this switch has not learned about
		// yet. Reported as unknowable rather than empty, which is the honest
		// answer and the one that cannot be mistaken for "this kind produces
		// nothing" — see [Node_Parallel]'s case above for that one.
		return []NamedOutput{{
			Description: "this node kind is not yet covered by OutputNames",
		}}, true
	}
}

// waitOutputNames answers [OutputNames] for a wait, whose shape depends on
// which of the three kinds it is and, for a signal, on whether it shapes its
// own result.
func waitOutputNames(wait *Wait) []NamedOutput {
	signal := wait.GetSignal()
	if signal == nil {
		// sleep, wait_until: a timer with no sender, so only timed_out — see
		// [TimerOutputs].
		return []NamedOutput{{
			Name:        TimedOutOutput,
			Description: "Whether the wait ended because the duration or moment was reached rather than being cancelled.",
		}}
	}

	if shaped := signal.GetOutputs(); len(shaped) > 0 {
		// `outputs:` replaces the wait's own outputs entirely (see
		// [ShapeSignalOutputs]): the shaped names are the whole answer, not an
		// addition to timed_out/payload/sender.
		names := make([]NamedOutput, 0, len(shaped))
		for _, name := range slices.Sorted(maps.Keys(shaped)) {
			names = append(names, NamedOutput{
				Name:        name,
				Description: fmt.Sprintf("Shaped output of the wait's `outputs:`, replacing what it would otherwise produce (%s, %s, %s).", TimedOutOutput, PayloadOutput, SenderOutput),
				Source:      shaped[name],
			})
		}
		return names
	}

	// An unshaped `wait_for_signal:` — see [SignalOutputs].
	return []NamedOutput{{
		Name:        TimedOutOutput,
		Description: "Whether the wait ended because nobody answered in time. A lapsed gate is an ordinary outcome, not a failure.",
	}, {
		Name:        PayloadOutput,
		Description: "What the sender's signal carried, under this root so a sender can never write outside it. Empty on a gate that timed out.",
	}, {
		Name:        SenderOutput,
		Description: "The server-attested sender: `identity.subject`, `identity.issuer`, `accepted_at`, `local`. Never anything the payload claims.",
	}}
}

// taskOutputNames answers [OutputNames] for a task step: its declared outputs,
// or — for a task that shapes — the names its `outputs:` input defines.
func taskOutputNames(task *Task, tasks *Registry) []NamedOutput {
	def, found := lookupTaskDef(tasks, task.GetName())
	if !found {
		return []NamedOutput{{
			Description: fmt.Sprintf("task %q is not registered; its outputs cannot be described", task.GetName()),
		}}
	}

	if def.ShapesOutputs {
		if shaping, has := task.GetInputs()[ShapingInput]; has && shaping != nil {
			if shapedNames, ok := ShapedOutputNames(shaping); ok {
				out := make([]NamedOutput, 0, len(shapedNames))
				for _, name := range shapedNames {
					out = append(out, NamedOutput{
						Name: name,
						Description: fmt.Sprintf(
							"Shaped output of step: its `%s:` replaces what the %s task declares, and names this output itself.",
							ShapingInput, def.Name),
					})
				}
				return out
			}
			return []NamedOutput{{
				Description: fmt.Sprintf(
					"its `%s:` replaces the %s task's declared outputs with names that expression computes, not knowable from the file",
					ShapingInput, def.Name),
			}}
		}
	}

	fields := Outputs(def)
	if len(fields) == 0 {
		return []NamedOutput{{
			Description: fmt.Sprintf("the %s task declares no outputs", def.Name),
		}}
	}
	out := make([]NamedOutput, 0, len(fields))
	for _, f := range fields {
		out = append(out, NamedOutput{
			Name:        f.Name,
			Description: fmt.Sprintf("%s output of the %s task, of type %s.", f.Name, def.Name, f.Type),
		})
	}
	return out
}

// lookupTaskDef resolves a task name against tasks, or [DefaultRegistry] when
// tasks is nil — the same nil-means-default convention [TaskShapesOutputs] and
// the language server's registry-optional lookups already use.
func lookupTaskDef(tasks *Registry, name string) (TaskDef, bool) {
	if tasks == nil {
		return LookupTask(name)
	}
	return tasks.Lookup(name)
}
