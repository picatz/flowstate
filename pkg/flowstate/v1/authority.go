package flowstatev1

import (
	"fmt"
	"slices"
)

// ValidateCredentialTargets checks every literal federation target before a run
// can perform any side effect. The target catalog is deployment configuration,
// so this complements schema validation rather than living in the Flowfile
// grammar itself.
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
		if task := node.GetTask(); task != nil {
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
	}
	return nil
}
