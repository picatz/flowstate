package flowstatev1

import (
	"maps"
	"slices"
	"strconv"
)

// This file holds the one traversal of a workflow document.
//
// # Why there is exactly one
//
// A workflow document has a fixed set of places a [Value] can be written, and
// several questions have to be asked at every one of them: which step outputs an
// expression still needs (Continue-As-New compaction, async joins, loop-result
// suppression), whether an expression type-checks, how often the file states the
// same expression, whether a `log:` surfaces a `sensitive:` input, whether sibling
// conditions have drifted apart. Each of those used to keep its own list of
// places to look, and each list was written on the day its question was asked.
//
// A branch added to the schema afterwards then had to be added to every list by
// hand, and the ones that were missed kept answering confidently about a document
// they had only partly seen. Five of those were found in one week (#508):
//
//   - [CollectNodeRefs] did not walk `Node.undo`, so a completed effect registered
//     no compensation (#492)
//   - the flowfile type checker did not walk `Workflow.triggers`, so a trigger
//     expression that deterministically fails validated clean (#502)
//   - the flowfile expression audit did not walk `Workflow.triggers` either (#506)
//   - the `sensitive:` log lint did not walk `Node.undo`, so a compensation could
//     print a value the declaration exists to keep out of the clear (#509)
//
// Note the shape: two missed `Node.undo`, two missed `Workflow.triggers`. Both are
// branches that landed *after* the walks were written, which is when this defect
// gets introduced and why it would have happened again with the next branch.
//
// So the positions are enumerated here, once, and every walk asks this. Adding a
// branch to the schema means teaching this file about it; a caller that does not
// care about the new position is unaffected, and a caller that does cannot miss
// it, because a position it has said nothing about is delivered rather than
// skipped.
//
// # What the guarantee actually is, stated precisely
//
// It is a schema-derived completeness check, not a compile error, and the
// difference is worth being exact about because the issue asked for the stronger
// one.
//
// Go has no exhaustiveness check for a type switch, and a protobuf `oneof` is
// modelled as an interface with generated wrapper types, so there is no
// enumeration a `switch` can be checked against at compile time: adding an arm to
// `Node.kind` produces a new type that every existing switch silently declines to
// match. Nothing in the repo's toolchain (`go vet`, `staticcheck`) reports a
// non-exhaustive type switch either. A generated visitor would move the problem
// rather than solve it — the generator's own mapping from schema to callback would
// be the new hand-maintained list — unless it generated the callback *set* too,
// which would make every caller a generated file.
//
// What is achievable, and what is built, is that the enumeration below is checked
// against the schema itself rather than against another list somebody wrote.
// `TestEveryValuePositionInTheSchemaIsWalked` reflects over the descriptors,
// finds every field of type [Value] reachable from [Workflow], and requires each
// one to be named by exactly one [ValueSlot] here or to carry a written reason for
// being left out. `TestEveryNodeRecursionEdgeIsDeclared` does the same for the
// message-graph edges that recurse. So a new [Value] position, or a new place a
// document nests steps, fails a test that names the schema path it added — a
// build-time failure in CI rather than a silent gap, one place to fix, and no
// second list to forget.
//
// The nearest thing to a compile error that does exist: [NodeContainerKinds] is
// derived from the schema too, and the walker guards in
// walkers_guard_test.go fail the moment a container kind has no test builder.

// A ValueSlot names one place in a workflow document that holds a [Value].
//
// Every slot corresponds to exactly one field in the schema; the mapping is
// [ValueSlotSchemaPath], and a test checks it covers the schema. A caller
// switches on the slot to decide what a position means to it, and anything it
// does not name is still delivered — the safe direction, since a walk that
// receives a position it has no opinion about costs nothing, and one that never
// receives it is the defect this file exists to prevent.
type ValueSlot int

const (
	// SlotUnknown is the zero value and is never emitted.
	SlotUnknown ValueSlot = iota

	// SlotInputDefault is an `inputs:` declaration's `default:`.
	SlotInputDefault
	// SlotInputExample is an `inputs:` declaration's `example:`.
	SlotInputExample
	// SlotWorkflowVar is an entry of the workflow's own `vars:`.
	SlotWorkflowVar
	// SlotDeclaredOutput is a declared `outputs:` entry's value.
	SlotDeclaredOutput
	// SlotSignalSubject is a signal policy rule's computed `subject:`.
	SlotSignalSubject
	// SlotWebhookIdempotencyKey is a webhook trigger's `idempotency_key:`.
	SlotWebhookIdempotencyKey
	// SlotWebhookArgument is one entry of a webhook trigger's `with:`.
	SlotWebhookArgument
	// SlotWebhookVerify is one entry of a webhook trigger's `verify:`.
	SlotWebhookVerify

	// SlotCondition is a step's `if:`.
	SlotCondition
	// SlotStepVar is one entry of a step's own `vars:`.
	SlotStepVar
	// SlotTaskInput is one input of the step's own task.
	SlotTaskInput
	// SlotUndoInput is one input of a step's `undo:` compensation.
	SlotUndoInput
	// SlotForEachItems is a `for_each:`'s item list.
	SlotForEachItems
	// SlotLoopUntil is a `loop:`'s `until:`.
	SlotLoopUntil
	// SlotLoopInitial is a `loop:`'s `init:`.
	SlotLoopInitial
	// SlotLoopUpdate is a `loop:`'s `update:`.
	SlotLoopUpdate
	// SlotSwitchValue is a `switch:`'s discriminant.
	SlotSwitchValue
	// SlotSwitchCaseValue is one literal a `switch:` case matches.
	SlotSwitchCaseValue
	// SlotWaitUntil is a wait's `wait_until:`.
	SlotWaitUntil
	// SlotWaitSleep is a wait's computed `sleep:`.
	SlotWaitSleep
	// SlotWaitTimeout is a wait's computed `timeout:`.
	SlotWaitTimeout
	// SlotWaitPrompt is a `wait_for_signal:`'s `prompt:`.
	SlotWaitPrompt
	// SlotWaitSignalOutput is one entry of a `wait_for_signal:`'s `outputs:`.
	SlotWaitSignalOutput
	// SlotStepValue is a `value:` step's whole content.
	SlotStepValue
	// SlotCallArgument is one entry of a `call:`'s `with:`.
	SlotCallArgument
	// SlotWaitBatchPrompt is a `wait_for_signals:`'s `prompt:`.
	//
	// Its own slot rather than sharing [SlotWaitPrompt], because
	// `TestEveryValuePositionInTheSchemaIsWalked` holds one rule that is worth
	// more than the tidiness of sharing: one schema position is one slot. A slot
	// covering two positions would let a *third* be added later and land on an
	// already-claimed name, which is the blindness that test exists to prevent.
	SlotWaitBatchPrompt
	// SlotWaitBatchOutput is one entry of a `wait_for_signals:`'s `outputs:`.
	SlotWaitBatchOutput
)

// ValueSlotSchemaPath maps each slot to the schema field it names.
//
// This is the table `TestEveryValuePositionInTheSchemaIsWalked` checks against the
// descriptors, so it is the authority on what "every position" means and the one
// place a new position is recorded. The spelling is the reflected one: `{}` marks
// a map's values and `[]` marks a repeated field.
func ValueSlotSchemaPath() map[ValueSlot]string {
	return map[ValueSlot]string{
		SlotInputDefault:          "Workflow.declared_inputs[].default",
		SlotInputExample:          "Workflow.declared_inputs[].example",
		SlotWorkflowVar:           "Workflow.vars{}",
		SlotDeclaredOutput:        "Workflow.declared_outputs[].value",
		SlotSignalSubject:         "Workflow.signals{}.allow[].subject_from",
		SlotWebhookIdempotencyKey: "Workflow.triggers.webhooks[].idempotency_key",
		SlotWebhookArgument:       "Workflow.triggers.webhooks[].arguments{}",
		SlotWebhookVerify:         "Workflow.triggers.webhooks[].verify{}",

		SlotCondition:        "Workflow.steps[].condition",
		SlotStepVar:          "Workflow.steps[].vars{}",
		SlotTaskInput:        "Workflow.steps[].task.inputs{}",
		SlotUndoInput:        "Workflow.steps[].undo.task.inputs{}",
		SlotForEachItems:     "Workflow.steps[].for_each.items",
		SlotLoopUntil:        "Workflow.steps[].loop.until",
		SlotLoopInitial:      "Workflow.steps[].loop.initial",
		SlotLoopUpdate:       "Workflow.steps[].loop.update",
		SlotSwitchValue:      "Workflow.steps[].switch.value",
		SlotSwitchCaseValue:  "Workflow.steps[].switch.cases[].values[]",
		SlotWaitUntil:        "Workflow.steps[].wait.until",
		SlotWaitSleep:        "Workflow.steps[].wait.duration_expr",
		SlotWaitTimeout:      "Workflow.steps[].wait.timeout_expr",
		SlotWaitPrompt:       "Workflow.steps[].wait.signal.prompt",
		SlotWaitSignalOutput: "Workflow.steps[].wait.signal.outputs{}",
		SlotWaitBatchPrompt:  "Workflow.steps[].wait.signal_batch.prompt",
		SlotWaitBatchOutput:  "Workflow.steps[].wait.signal_batch.outputs{}",
		SlotStepValue:        "Workflow.steps[].value",
		SlotCallArgument:     "Workflow.steps[].call.arguments{}",
	}
}

// NodeRecursionEdges are the message-graph edges along which a document nests
// further steps, each mapped to whether this traversal follows it.
//
// The value is the reason, and an edge this walk does not follow has to say why —
// there is exactly one, and it is the callee of a `call:`. A callee is a different
// workflow, embedded whole at compile time, with its own isolated scope
// ([CallScope]) and its own author: following it here would count a library
// workflow's expressions against every caller, resolve its references against the
// caller's steps, and report its diagnostics on the caller's lines.
//
// Checked against the descriptors by `TestEveryNodeRecursionEdgeIsDeclared`, so a
// new place the schema nests steps in fails a test rather than being quietly left
// unvisited.
func NodeRecursionEdges() map[string]bool {
	return map[string]bool{
		"Workflow.steps[]":                             true,
		"Workflow.steps[].for_each.body[]":             true,
		"Workflow.steps[].loop.body[]":                 true,
		"Workflow.steps[].parallel.branches[].steps[]": true,
		"Workflow.steps[].switch.cases[].steps[]":      true,
		"Workflow.steps[].switch.default.steps[]":      true,

		// The one edge deliberately not followed. See this function's doc.
		"Workflow.steps[].call.workflow": false,
	}
}

// A ValueSite is one position in a document, delivered to a walk.
//
// Step, [ValueSite.Field] and Name are what a caller reports against. Owner,
// Parent and Index address a position inside a repeated or keyed parent — a
// webhook by name and by its index among webhooks, a signal policy by its name, a
// case literal by its case — for the callers that place a diagnostic by path
// rather than by field.
type ValueSite struct {
	// Slot names the position in the schema.
	Slot ValueSlot

	// Step is the id of the step the position belongs to, empty for a
	// workflow-level one.
	Step string

	// Name is the bare name within a keyed slot: a var, a task input, a webhook
	// argument, a declared output, a shaped signal output, an input declaration.
	// Empty otherwise.
	Name string

	// Owner names the parent a position hangs off where that parent is itself
	// named: a webhook's name, or a signal policy's name.
	Owner string

	// Parent is the index of the position's parent within *its* repeated parent,
	// and is used only where two levels of index are needed to address one
	// position: a `switch:` case's literal is the Index'th value of the Parent'th
	// case.
	Parent int

	// Index is the position's index within its repeated parent, and is zero where
	// the parent is not repeated.
	Index int

	// Value is what was written.
	Value *Value
}

// Field is the key an author writes, qualified where the position needs it —
// `if`, `until`, `vars.count`, `outputs.decision`, `triggers[0].with.order`.
//
// It is the spelling the walks used before they shared this traversal, so a
// diagnostic reported against it lands exactly where it did.
//
// Computed on demand rather than carried, because most positions are never
// reported: [CollectNodeRefs] visits every position in a document and wants a label
// for none of them. A caller that reports pays for the string; a caller that only
// counts references does not.
func (s ValueSite) Field() string {
	switch s.Slot {
	case SlotInputDefault:
		return "inputs." + s.Name + ".default"
	case SlotInputExample:
		return "inputs." + s.Name + ".example"
	case SlotWorkflowVar, SlotStepVar:
		return VarsRoot + "." + s.Name
	case SlotDeclaredOutput:
		return "outputs." + s.Name
	case SlotSignalSubject:
		return "signals." + s.Owner + ".allow[" + strconv.Itoa(s.Index) + "].subject"
	case SlotWebhookIdempotencyKey:
		return s.triggerPath() + ".idempotency_key"
	case SlotWebhookArgument:
		return s.triggerPath() + ".with." + s.Name
	case SlotWebhookVerify:
		return s.triggerPath() + ".verify." + s.Name
	case SlotCondition:
		return "if"
	case SlotTaskInput:
		return s.Name
	case SlotUndoInput:
		// The `undo:` key rather than the input's name, for the reason
		// validateUndoInputs gives: an input name here would be looked up among the
		// *step's* inputs, and a plugin task may declare one of any name.
		return "undo"
	case SlotForEachItems:
		return "items"
	case SlotLoopUntil:
		return "until"
	case SlotLoopInitial:
		return "init"
	case SlotLoopUpdate:
		return "update"
	case SlotSwitchValue, SlotStepValue:
		return "value"
	case SlotSwitchCaseValue:
		return "cases[" + strconv.Itoa(s.Parent) + "].values[" + strconv.Itoa(s.Index) + "]"
	case SlotWaitUntil:
		return "wait_until"
	case SlotWaitSleep:
		return "sleep"
	case SlotWaitTimeout:
		return "timeout"
	case SlotWaitPrompt, SlotWaitBatchPrompt:
		// One path for both, unlike the slots: this is the key an author wrote
		// in their file, and both spellings write `prompt:`.
		return "prompt"
	case SlotWaitSignalOutput, SlotWaitBatchOutput:
		return "outputs." + s.Name
	case SlotCallArgument:
		return "with." + s.Name
	default:
		return ""
	}
}

// triggerPath addresses a webhook by its index among webhooks.
func (s ValueSite) triggerPath() string {
	return "triggers[" + strconv.Itoa(s.Index) + "]"
}

// A Walk is the set of callbacks one traversal delivers to.
//
// Every callback is optional; a nil one is not called and costs nothing. A walk
// that wires only Value never sees the tree, and one that wires only Steps sees
// only the sibling groups — which is what makes five different questions share one
// enumeration of positions.
type Walk struct {
	// Steps is called once for each list of sibling steps, before those steps are
	// visited: the workflow's own top level, then each `for_each` body, `loop:`
	// body, `parallel` branch and `switch` body, in document order.
	//
	// A sibling group is a real unit rather than a convenience: two steps are only
	// comparable when they branch on the same run, which is why the negation-drift
	// lint asks for the groups rather than for the nodes.
	Steps func(nodes []*Node)

	// Node is called once for each step, before its own value positions and before
	// its body is descended.
	Node func(node *Node)

	// Value is called once for each value position, in document order.
	Value func(site ValueSite)

	// Truncated is called when a structure nests past [MaxStructureDepth] and
	// [Walk.nested] stops descending into it, naming the top-level site whose
	// contents it could not finish walking.
	//
	// Optional, and its absence is not the same question as Value being nil.
	// A caller that only wants positions can leave this unset and get exactly
	// what it asked for; a caller for whom "I stopped looking" and "there was
	// nothing to find" are different answers — [CollectNodeRefs] is the one
	// today — wires this to react rather than accept a walk that silently
	// under-reports past its own bound. See [MaxStructureDepth]'s doc for why
	// the bound exists and what reaching it means for the value underneath.
	Truncated func(site ValueSite)
}

// WalkWorkflow visits every position in a workflow document.
//
// Document order, which is the order the file is written in: declared inputs,
// `vars:`, `steps:` (depth first, each step before its body), declared `outputs:`,
// `signals:`, then `triggers:`. Map keys are visited in sorted order so two runs
// over one document report in the same order — a walk that produces diagnostics
// cannot have its output depend on Go's map iteration.
func WalkWorkflow(wf *Workflow, w Walk) {
	if wf == nil {
		return
	}

	for _, declaration := range wf.GetDeclaredInputs() {
		name := declaration.GetName()
		w.value(ValueSite{Slot: SlotInputDefault, Name: name, Value: declaration.GetDefault()})
		w.value(ValueSite{Slot: SlotInputExample, Name: name, Value: declaration.GetExample()})
	}

	for _, name := range slices.Sorted(maps.Keys(wf.GetVars())) {
		w.value(ValueSite{Slot: SlotWorkflowVar, Name: name, Value: wf.GetVars()[name]})
	}

	WalkNodes(wf.GetSteps(), w)

	for _, declaration := range wf.GetDeclaredOutputs() {
		name := declaration.GetName()
		w.value(ValueSite{Slot: SlotDeclaredOutput, Name: name, Value: declaration.GetValue()})
	}

	for _, policy := range slices.Sorted(maps.Keys(wf.GetSignals())) {
		for i, rule := range wf.GetSignals()[policy].GetAllow() {
			// A computed subject is written under `subject:` and routed to
			// subject_from by the parser when it interpolates, so the field is the
			// one the author wrote and the value read is where it landed.
			w.value(ValueSite{
				Slot:  SlotSignalSubject,
				Owner: policy,
				Index: i,
				Value: rule.GetSubjectFrom(),
			})
		}
	}

	for i, webhook := range wf.GetTriggers().GetWebhooks() {
		name := webhook.GetName()

		w.value(ValueSite{
			Slot:  SlotWebhookIdempotencyKey,
			Owner: name,
			Index: i,
			Value: webhook.GetIdempotencyKey(),
		})

		for _, argument := range slices.Sorted(maps.Keys(webhook.GetArguments())) {
			w.value(ValueSite{
				Slot:  SlotWebhookArgument,
				Name:  argument,
				Owner: name,
				Index: i,
				Value: webhook.GetArguments()[argument],
			})
		}

		for _, key := range slices.Sorted(maps.Keys(webhook.GetVerify())) {
			w.value(ValueSite{
				Slot:  SlotWebhookVerify,
				Name:  key,
				Owner: name,
				Index: i,
				Value: webhook.GetVerify()[key],
			})
		}
	}
}

// WalkNodes visits a list of sibling steps and everything under them.
func WalkNodes(nodes []*Node, w Walk) {
	if w.Steps != nil {
		w.Steps(nodes)
	}

	for _, node := range nodes {
		WalkNode(node, w)
	}
}

// WalkNode visits one step: the step itself, every value position it holds, and
// every step nested under it.
//
// A `call:`'s arguments are visited and its callee's steps are not; see
// [NodeRecursionEdges].
func WalkNode(node *Node, w Walk) {
	if node == nil {
		return
	}

	if w.Node != nil {
		w.Node(node)
	}

	id := node.GetId()

	// The condition, the step's own `vars:` and an `undo:`'s inputs sit outside the
	// kind switch because they are step *properties*: a `for_each` and a `wait`
	// carry each of them exactly as a task step does.
	w.value(ValueSite{Slot: SlotCondition, Step: id, Value: node.GetCondition()})

	for _, name := range slices.Sorted(maps.Keys(node.GetVars())) {
		w.value(ValueSite{Slot: SlotStepVar, Step: id, Name: name, Value: node.GetVars()[name]})
	}

	if task := node.GetTask(); task != nil {
		for _, name := range slices.Sorted(maps.Keys(task.GetInputs())) {
			w.value(ValueSite{Slot: SlotTaskInput, Step: id, Name: name, Value: task.GetInputs()[name]})
		}
	}

	if undo := node.GetUndo(); undo != nil {
		inputs := undo.GetTask().GetInputs()
		for _, name := range slices.Sorted(maps.Keys(inputs)) {
			w.value(ValueSite{Slot: SlotUndoInput, Step: id, Name: name, Value: inputs[name]})
		}
	}

	switch kind := node.GetKind().(type) {
	case *Node_ForEach:
		w.value(ValueSite{Slot: SlotForEachItems, Step: id, Value: kind.ForEach.GetItems()})
		WalkNodes(kind.ForEach.GetBody(), w)

	case *Node_Loop:
		w.value(ValueSite{Slot: SlotLoopUntil, Step: id, Value: kind.Loop.GetUntil()})
		w.value(ValueSite{Slot: SlotLoopInitial, Step: id, Value: kind.Loop.GetInitial()})
		w.value(ValueSite{Slot: SlotLoopUpdate, Step: id, Value: kind.Loop.GetUpdate()})
		WalkNodes(kind.Loop.GetBody(), w)

	case *Node_Parallel:
		for _, branch := range kind.Parallel.GetBranches() {
			WalkNodes(branch.GetSteps(), w)
		}

	case *Node_Switch:
		w.value(ValueSite{Slot: SlotSwitchValue, Step: id, Value: kind.Switch.GetValue()})
		for i, c := range kind.Switch.GetCases() {
			for j, literal := range c.GetValues() {
				w.value(ValueSite{
					Slot:   SlotSwitchCaseValue,
					Step:   id,
					Parent: i,
					Index:  j,
					Value:  literal,
				})
			}
		}
		for _, body := range SwitchBodies(kind.Switch) {
			WalkNodes(body, w)
		}

	case *Node_Wait:
		w.value(ValueSite{Slot: SlotWaitUntil, Step: id, Value: kind.Wait.GetUntil()})
		w.value(ValueSite{Slot: SlotWaitSleep, Step: id, Value: kind.Wait.GetDurationExpr()})
		w.value(ValueSite{Slot: SlotWaitTimeout, Step: id, Value: kind.Wait.GetTimeoutExpr()})
		w.value(ValueSite{Slot: SlotWaitPrompt, Step: id, Value: kind.Wait.GetSignal().GetPrompt()})
		// The batch spelling's own two expression positions, under the same
		// slots as the single wait's. A walker's whole job is to see every
		// expression a specification holds — `CollectNodeRefs` reads this to
		// decide what compaction may drop — so an arm missing here is not a
		// cosmetic gap: it is a live reference nothing knows is live. The two
		// arms are mutually exclusive by the oneof, so reading both is reading
		// whichever one is set.
		w.value(ValueSite{Slot: SlotWaitBatchPrompt, Step: id, Value: kind.Wait.GetSignalBatch().GetPrompt()})

		shaped := kind.Wait.GetSignal().GetOutputs()
		for _, name := range slices.Sorted(maps.Keys(shaped)) {
			w.value(ValueSite{Slot: SlotWaitSignalOutput, Step: id, Name: name, Value: shaped[name]})
		}

		batchShaped := kind.Wait.GetSignalBatch().GetOutputs()
		for _, name := range slices.Sorted(maps.Keys(batchShaped)) {
			w.value(ValueSite{Slot: SlotWaitBatchOutput, Step: id, Name: name, Value: batchShaped[name]})
		}

	case *Node_Value:
		w.value(ValueSite{Slot: SlotStepValue, Step: id, Value: kind.Value})

	case *Node_Call:
		for _, name := range slices.Sorted(maps.Keys(kind.Call.GetArguments())) {
			w.value(ValueSite{Slot: SlotCallArgument, Step: id, Name: name, Value: kind.Call.GetArguments()[name]})
		}
	}
}

// value delivers one position, skipping one that was never written.
//
// An absent value is not a position an author can be told anything about, and
// every walk sharing this traversal used to make the same check itself.
func (w Walk) value(site ValueSite) {
	if w.Value == nil || site.Value == nil {
		return
	}
	w.Value(site)
	w.nested(site, 0)
}

// nested delivers the values held *inside* one value.
//
// A [Value_Structure] is a value made of values, and its entries are written by
// the same author in the same file: a shaped `outputs:` mapping is compiled entry
// by entry precisely so its names survive, and each entry is an ordinary
// expression. A traversal that stopped at the top of a value would hand every
// caller a position whose contents it cannot see — the same blindness #508 is
// about, one level further in, and the one place it costs correctness rather than
// coverage is compaction: a `${steps.a.b}` inside a shaped entry is a reference
// that has to keep that output alive across a Continue-As-New.
//
// The entry keeps its parent's slot and gains its name, so `outputs` holding
// `reference` arrives with a field of `outputs.reference` — the spelling a
// diagnostic and a measurement both already use for a shaped name. A list's
// elements keep the parent's name outright, because an index is not a name and
// inventing one would put a spelling in a diagnostic that appears nowhere in the
// file.
//
// Bounded by [MaxStructureDepth] for that constant's reason: a specification does
// not have to have come from a Flowfile, and this recursion's depth would
// otherwise be a number the sender picks.
//
// Reaching the bound with more structure still beneath it calls
// [Walk.Truncated], naming the position this traversal stopped inside of,
// rather than returning as though there were nothing left — the same
// fail-closed shape [CollectValueRefs] uses for the identical bound, applied
// here so any caller of this shared traversal inherits it rather than having
// to reimplement it.
func (w Walk) nested(site ValueSite, depth int) {
	if depth >= MaxStructureDepth {
		if site.Value.GetStructure() != nil && w.Truncated != nil {
			w.Truncated(site)
		}
		return
	}

	switch structure := site.Value.GetStructure().GetKind().(type) {
	case *Value_Structure_Map_:
		entries := structure.Map.GetEntries()
		for _, name := range slices.Sorted(maps.Keys(entries)) {
			entry := site
			entry.Name = name
			if site.Name != "" {
				entry.Name = site.Name + "." + name
			}
			entry.Value = entries[name]
			if entry.Value == nil {
				continue
			}
			w.Value(entry)
			w.nested(entry, depth+1)
		}

	case *Value_Structure_List_:
		for _, element := range structure.List.GetValues() {
			if element == nil {
				continue
			}
			entry := site
			entry.Value = element
			w.Value(entry)
			w.nested(entry, depth+1)
		}
	}
}
