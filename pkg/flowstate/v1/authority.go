package flowstatev1

import (
	"fmt"
	"maps"
	"slices"
)

// ValidateCredentialTargets checks every literal federation target before a run
// can perform any side effect. The target catalog is deployment configuration,
// so this complements schema validation rather than living in the Flowfile
// grammar itself.
//
// Two spellings name a target. A task's declared credential inputs
// ([TaskDef.CredentialInputs]) take it as a literal string, and any task input
// may hold a [CredentialRef], which is what `${credential('target')}` compiles
// to. Both are refused here, naming the target and listing what is configured,
// so a typo is a diagnostic before the run starts rather than a denial at the
// step that reaches it. A reference too deep to inspect is refused too: this is
// a preflight that must not pass what it could not read.
func ValidateCredentialTargets(workflow *Workflow, targets []string) error {
	sortedTargets := slices.Clone(targets)
	slices.Sort(sortedTargets)
	available := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		available[target] = struct{}{}
	}
	return validateCredentialNodes(workflow.GetSteps(), available, sortedTargets)
}

func validateCredentialNodes(nodes []*Node, available map[string]struct{}, targets []string) error {
	for _, node := range nodes {
		for _, task := range []*Task{node.GetTask(), node.GetUndo().GetTask()} {
			if task == nil {
				continue
			}
			if err := validateCredentialRefs(node.GetId(), task, available, targets); err != nil {
				return err
			}
			def, found := LookupTask(task.GetName())
			if found {
				for _, input := range def.CredentialInputs {
					value := task.GetInputs()[input]
					if value == nil {
						continue
					}
					target := value.GetLiteral().GetStringValue()
					if target == "" {
						continue // expressions remain fail-closed at broker authorization
					}
					if _, ok := available[target]; !ok {
						return fmt.Errorf("step %q: %s target %q is not configured on this deployment (configured: %v)",
							node.GetId(), input, target, targets)
					}
				}
			}
		}
		if loop := node.GetForEach(); loop != nil {
			if err := validateCredentialNodes(loop.GetBody(), available, targets); err != nil {
				return err
			}
		}
		// A callee is inlined in the caller's own specification and runs its
		// tasks like any other, so a target it names is as much this run's as one
		// written at the top level.
		if callee := node.GetCall().GetWorkflow(); callee != nil {
			if err := validateCredentialNodes(callee.GetSteps(), available, targets); err != nil {
				return err
			}
		}
		if loop := node.GetLoop(); loop != nil {
			// A literal credential target inside a loop body must be caught by the
			// same server- and schedule-side preflight every other one is, or it is
			// only denied after the run starts — possibly after earlier iterations'
			// side effects have already happened.
			if err := validateCredentialNodes(loop.GetBody(), available, targets); err != nil {
				return err
			}
		}
		if parallel := node.GetParallel(); parallel != nil {
			for _, branch := range parallel.GetBranches() {
				if err := validateCredentialNodes(branch.GetSteps(), available, targets); err != nil {
					return err
				}
			}
		}
		if sw := node.GetSwitch(); sw != nil {
			// Every case body and the default: only one of them will run, but a
			// literal credential target in any of them is a file property this
			// preflight exists to refuse before side effects, whichever branch a
			// run would take.
			for _, body := range SwitchBodies(sw) {
				if err := validateCredentialNodes(body, available, targets); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// validateCredentialRefs refuses a [CredentialRef] naming a target the
// deployment does not federate, or one that is malformed, in any input of task.
//
// Inputs are walked in sorted order so a task holding two bad references names
// the same one every time.
func validateCredentialRefs(stepID string, task *Task, available map[string]struct{}, targets []string) error {
	for _, input := range slices.Sorted(maps.Keys(task.GetInputs())) {
		for ref := range credentialRefs(task.GetInputs()[input]) {
			if ref == nil {
				return fmt.Errorf("step %q: input %q nests a value deeper than %d levels, "+
					"too deep to confirm it names no credential target the deployment lacks",
					stepID, input, MaxStructureDepth)
			}
			if err := ValidateCredentialTarget(ref.GetTarget()); err != nil {
				return fmt.Errorf("step %q: input %q: %w", stepID, input, err)
			}
			if _, ok := available[ref.GetTarget()]; !ok {
				return fmt.Errorf("step %q: input %q: credential target %q is not configured on this deployment (configured: %v)",
					stepID, input, ref.GetTarget(), targets)
			}
		}
	}
	return nil
}
