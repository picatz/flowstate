package flowstatev1

import (
	"fmt"
)

// RequiredSecretInputMessage is the sentence every admission-time refusal of a
// non-reference value in an input a task declares in
// [TaskDef.RequiredSecretInputs] says.
//
// One function rather than one string per caller because two paths reach this
// refusal — compiling a Flowfile (`pkg/flowstate/v1/flowfile`'s task schema
// check) and [CheckRequiredSecretInputs] on the RPC path that has no compiler —
// and an author who moved from one to the other should meet the same sentence
// rather than discover that the two mechanisms disagree about what they want.
// It names the spelling that works, because the next thing the author needs is
// what to write.
func RequiredSecretInputMessage(taskName, input string) string {
	return fmt.Sprintf(
		"task %q requires input %q to be a whole secret reference such as ${secret('env:NAME')}, never a literal",
		taskName, input)
}

// CheckRequiredSecretInputs refuses a workflow that writes anything but a whole
// secret reference into an input its task declares in
// [TaskDef.RequiredSecretInputs] — for a task step, for an `undo:` step, and
// inside a `call:`'s inlined callee.
//
// This is the RPC-path half of the bound the Flowfile compiler enforces while
// compiling, and it is not parity for its own sake. The other half of the
// mechanism, the plugin host's own refusal at dispatch
// (`pkg/flowstate/v1/plugin`'s resolvePluginSecretInputs), runs inside an
// activity — which is to say after the specification carrying the literal is
// durable in workflow history and readable by anyone with substrate access to
// the namespace. The sentence that refusal produces names the harm as already
// done. A specification built by hand and submitted straight to [Run],
// [SignalWithStart] or [CreateSchedule] arrives with no compiler in front of
// it, so without this check that is the only refusal it meets, and by then
// invariant 7 has already been broken.
//
// registry is the deployment's task registry, read the way
// [ResolveTaskCapabilities] reads it. This check runs after that resolution
// because an unknown task is already refused there, so it only has to rely on
// [TaskDef.RequiredSecretInputs] for the tasks the registry knows.
// A task the registry does not have is skipped: that specification is already
// refused by [ResolveTaskCapabilities], and answering the question a second
// time here could only add a second sentence about the same missing task. A nil
// registry decides nothing, and a check that cannot decide must not admit.
//
// An input the workflow does not supply at all is not this check's business —
// a required input left unset is a different mistake with its own diagnostic —
// so only supplied inputs are examined, and only the ones the task named. The
// walk is the one [RequiredTaskNames] performs, so a position that can reach a
// task is a position this reaches: [WalkWorkflow] for nested control flow and
// compensations, walkPluginWorkflows for the bounded callee edge. It is bounded
// the same way, and runs behind [CheckSpecSize] and [CheckStructureDepth].
//
// The refusal names the step and the input and never the value: the value is
// the credential this exists to keep out of durable state, and an error message
// is a place values are read from.
func CheckRequiredSecretInputs(wf *Workflow, registry *Registry) error {
	if registry == nil {
		return fmt.Errorf("no task registry: cannot decide which task inputs must be whole secret references")
	}

	var refusal error
	walkErr := walkPluginWorkflows(wf, 0, func(current *Workflow) error {
		WalkWorkflow(current, Walk{Node: func(node *Node) {
			if refusal != nil {
				return
			}
			if err := checkNodeRequiredSecretInputs(node.GetId(), "", node.GetTask(), registry); err != nil {
				refusal = err
				return
			}
			if err := checkNodeRequiredSecretInputs(node.GetId(), "undo", node.GetUndo().GetTask(), registry); err != nil {
				refusal = err
			}
		}})
		return nil
	})
	if refusal != nil {
		return refusal
	}
	if walkErr != nil {
		return fmt.Errorf("checking which task inputs must be whole secret references: %w", walkErr)
	}

	return nil
}

// checkNodeRequiredSecretInputs applies the rule to one task position. position
// is the step key the task sits under, empty for the step's own task, so a
// compensation's input is not reported as if it were the step's — the same
// distinction the compiler makes by refiling an `undo:` diagnostic onto that
// key.
func checkNodeRequiredSecretInputs(stepID, position string, task *Task, registry *Registry) error {
	if task == nil {
		return nil
	}
	def, found := registry.Lookup(task.GetName())
	if !found {
		return nil
	}

	// Iterated over what the task declared rather than over what the caller
	// supplied: the declaration is this deployment's own and short, the input
	// map is the untrusted half.
	for _, name := range def.RequiredSecretInputs {
		value, supplied := task.GetInputs()[name]
		if !supplied {
			continue
		}
		if value.GetSecretRef() != nil {
			continue
		}
		step := fmt.Sprintf("step %q", stepID)
		if position != "" {
			step = fmt.Sprintf("step %q %s", stepID, position)
		}
		return fmt.Errorf("%s: %s", step, RequiredSecretInputMessage(def.Name, name))
	}

	return nil
}
