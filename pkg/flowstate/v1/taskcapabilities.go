package flowstatev1

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// CurrentTaskCapabilitySchemaVersion is the requirement-walk semantics a
// current control plane writes into [ResolvedTaskCapabilities].
const CurrentTaskCapabilitySchemaVersion uint32 = 1

// RequiredTaskNames returns the sorted, unique task capabilities a workflow can
// reach, including compensations, nested control flow, and inlined callees.
//
// [WalkWorkflow] is the one enumeration of a workflow's node positions and
// walkPluginWorkflows supplies its bounded callee edge. Keeping the two pieces
// together here makes task availability one requirement walk rather than a list
// maintained separately by the compiler, local evaluator, and durable engine.
func RequiredTaskNames(wf *Workflow) ([]string, error) {
	required := map[string]struct{}{}
	err := walkPluginWorkflows(wf, 0, func(current *Workflow) error {
		WalkWorkflow(current, Walk{Node: func(node *Node) {
			if task := node.GetTask(); task != nil {
				required[task.GetName()] = struct{}{}
			}
			if task := node.GetUndo().GetTask(); task != nil {
				required[task.GetName()] = struct{}{}
			}
		}})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("collecting task requirements: %w", err)
	}

	names := make([]string, 0, len(required))
	for name := range required {
		names = append(names, name)
	}
	slices.Sort(names)
	return names, nil
}

// ResolveTaskCapabilities records the admitting registry's decision on wf.
// Caller-supplied state is discarded: only this control-plane operation may
// attest that a deployment had every required task when it accepted the run.
func ResolveTaskCapabilities(wf *Workflow, registry *Registry) error {
	if wf == nil {
		return fmt.Errorf("workflow cannot be nil")
	}
	wf.ResolvedTaskCapabilities = nil

	required, err := RequiredTaskNames(wf)
	if err != nil {
		return err
	}
	if err := CheckTaskCapabilitiesAvailable(required, namesOf(registry)); err != nil {
		return err
	}

	wf.ResolvedTaskCapabilities = &ResolvedTaskCapabilities{
		SchemaVersion: CurrentTaskCapabilitySchemaVersion,
		TaskNames:     required,
	}
	return nil
}

// PinnedTaskCapabilities reads and validates the control plane's durable task
// decision. The bool distinguishes an old workflow carrying no decision from a
// current workflow that affirmatively requires no tasks.
func PinnedTaskCapabilities(wf *Workflow) ([]string, bool, error) {
	if wf == nil || wf.GetResolvedTaskCapabilities() == nil {
		return nil, false, nil
	}

	pin := wf.GetResolvedTaskCapabilities()
	if pin.GetSchemaVersion() != CurrentTaskCapabilitySchemaVersion {
		return nil, true, fmt.Errorf("task capability snapshot uses schema version %d; this worker understands only %d",
			pin.GetSchemaVersion(), CurrentTaskCapabilitySchemaVersion)
	}

	required, err := RequiredTaskNames(wf)
	if err != nil {
		return nil, true, err
	}
	if !slices.Equal(pin.GetTaskNames(), required) {
		return nil, true, fmt.Errorf("task capability snapshot does not match the workflow requirements: snapshot has [%s], workflow requires [%s]",
			strings.Join(pin.GetTaskNames(), ", "), strings.Join(required, ", "))
	}

	return required, true, nil
}

// CheckTaskCapabilitiesIn is the local driver's admission check. It reads the
// same context-scoped registry task dispatch will use, so a rehearsal never
// borrows capabilities from the process-wide registry it cannot execute.
func CheckTaskCapabilitiesIn(ctx context.Context, wf *Workflow) error {
	required, err := RequiredTaskNames(wf)
	if err != nil {
		return err
	}
	return CheckTaskCapabilitiesAvailable(required, TaskNamesIn(ctx))
}

// CheckTaskCapabilitiesAvailable reports every required name missing from an
// availability snapshot. Both inputs are names only: executable TaskDefs remain
// owned by Registry, so this replay contract cannot become a second registry.
func CheckTaskCapabilitiesAvailable(required, available []string) error {
	has := make(map[string]struct{}, len(available))
	for _, name := range available {
		has[name] = struct{}{}
	}

	missing := make([]string, 0)
	for _, name := range required {
		if _, ok := has[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 {
		return nil
	}

	return NewTaskError(missing[0], ErrorKindUnknownTask, fmt.Errorf(
		"required task capabilities are unavailable: %s", strings.Join(missing, ", ")))
}

func namesOf(registry *Registry) []string {
	if registry == nil {
		return nil
	}
	return registry.Names()
}
