package flowstatev1

import (
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
)

// CapabilityBindingRevision returns the revision a binding must carry. A
// revision is the deterministic digest of the complete binding with only its
// revision field cleared. BindingId remains in the digest: editing identity is
// a new binding as well as a new revision, never a silent rename.
func CapabilityBindingRevision(binding *CapabilityBinding) string {
	if binding == nil {
		return ContentDigest(nil)
	}
	canonical := proto.Clone(binding).(*CapabilityBinding)
	canonical.Revision = ""
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(canonical)
	if err != nil {
		// CapabilityBinding has no field whose protobuf encoding can fail.
		panic(fmt.Sprintf("marshal capability binding: %v", err))
	}
	return ContentDigest(encoded)
}

// CapabilityContractDigest returns the identity of a capability's complete task
// contract. Provider qualifiers are removed from task names before hashing, so
// two providers implementing the same methods produce the same digest. Every
// descriptor and security-claim field remains in the digest.
func CapabilityContractDigest(tasks []*TaskDescription) (string, error) {
	canonical := make([]*TaskDescription, 0, len(tasks))
	seen := make(map[string]bool, len(tasks))
	for _, task := range tasks {
		if task == nil {
			return "", fmt.Errorf("capability contract contains an empty task description")
		}
		name := task.GetName()
		if _, bare, qualified := strings.Cut(name, "."); qualified {
			name = bare
		}
		if name == "" || strings.Contains(name, ".") {
			return "", fmt.Errorf("capability contract task %q is not a bare task name or one qualifier followed by a task name", task.GetName())
		}
		if seen[name] {
			return "", fmt.Errorf("capability contract declares task %q more than once", name)
		}
		seen[name] = true
		copy := proto.Clone(task).(*TaskDescription)
		copy.Name = name
		canonical = append(canonical, copy)
	}
	slices.SortFunc(canonical, func(a, b *TaskDescription) int {
		return strings.Compare(a.GetName(), b.GetName())
	})
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&TaskCatalog{Tasks: canonical})
	if err != nil {
		return "", fmt.Errorf("encode capability contract: %w", err)
	}
	return ContentDigest(encoded), nil
}

// ResolveCapabilityBindings binds the root workflow's capability parameters to
// deployment qualifiers, verifies each provider's schema-owned task contract,
// and normalizes logical task names to concrete provider task names throughout
// the call tree.
//
// selections maps root parameter name to a stable deployment binding ID. Calls
// do not carry deployment names: Call.capability_arguments maps callee parameter
// names to caller parameter names, making authority forwarding explicit at each
// boundary. The operation is atomic; wf is changed only after the entire graph
// has been checked.
func ResolveCapabilityBindings(wf *Workflow, selections map[string]string, catalog *PluginCatalog) error {
	if wf == nil {
		return fmt.Errorf("cannot resolve capability bindings for an empty workflow")
	}
	bindings, plugins, err := indexCapabilityCatalog(catalog)
	if err != nil {
		return err
	}

	resolved := proto.Clone(wf).(*Workflow)
	if err := resolveWorkflowCapabilities(resolved, selections, bindings, plugins, 0, false); err != nil {
		return err
	}
	proto.Reset(wf)
	proto.Merge(wf, resolved)
	return nil
}

func indexCapabilityCatalog(catalog *PluginCatalog) (map[string]*CapabilityBinding, map[string]*PluginDescription, error) {
	bindings := make(map[string]*CapabilityBinding, len(catalog.GetCapabilityBindings()))
	qualifiers := make(map[string]bool, len(catalog.GetCapabilityBindings()))
	for _, binding := range catalog.GetCapabilityBindings() {
		if binding == nil {
			return nil, nil, fmt.Errorf("capability catalog contains an empty binding")
		}
		if err := Validate(binding); err != nil {
			return nil, nil, fmt.Errorf("capability binding %q is invalid: %w", binding.GetQualifier(), err)
		}
		if binding.GetPlugin() == nil {
			return nil, nil, fmt.Errorf("capability binding %q names no provider", binding.GetQualifier())
		}
		if binding.GetPlugin().GetPluginName() != binding.GetQualifier() {
			return nil, nil, fmt.Errorf("capability binding %q selects plugin %q; the first executable binding path requires the qualifier and plugin name to match", binding.GetQualifier(), binding.GetPlugin().GetPluginName())
		}
		if want := CapabilityBindingRevision(binding); binding.GetRevision() != want {
			return nil, nil, fmt.Errorf("capability binding %q revision is %q, want %q for its current contents", binding.GetQualifier(), binding.GetRevision(), want)
		}
		if qualifiers[binding.GetQualifier()] {
			return nil, nil, fmt.Errorf("capability qualifier %q is bound more than once", binding.GetQualifier())
		}
		if _, exists := bindings[binding.GetBindingId()]; exists {
			return nil, nil, fmt.Errorf("capability binding id %q is used more than once", binding.GetBindingId())
		}
		bindings[binding.GetBindingId()] = binding
		qualifiers[binding.GetQualifier()] = true
	}

	plugins := make(map[string]*PluginDescription, len(catalog.GetPlugins()))
	for _, plugin := range catalog.GetPlugins() {
		if plugin == nil {
			continue
		}
		if _, exists := plugins[plugin.GetName()]; exists {
			return nil, nil, fmt.Errorf("plugin %q is described more than once", plugin.GetName())
		}
		plugins[plugin.GetName()] = plugin
	}
	return bindings, plugins, nil
}

type selectedCapability struct {
	parameter *CapabilityParameter
	binding   *CapabilityBinding
}

func resolveWorkflowCapabilities(wf *Workflow, selections map[string]string, bindings map[string]*CapabilityBinding, plugins map[string]*PluginDescription, depth int, explicitlyForwarded bool) error {
	if depth > maxWorkflowScanDepth {
		return fmt.Errorf("capability forwarding nests more than %d deep, past what a specification is checked to", maxWorkflowScanDepth)
	}
	if explicitlyForwarded && len(wf.GetPluginRequirements()) > 0 {
		return fmt.Errorf("workflow %q receives explicit capabilities and also declares `plugins:` requirements; a called library must receive every effectful dependency through its capability parameters", wf.GetName())
	}

	parameters := make(map[string]*CapabilityParameter, len(wf.GetCapabilityParameters()))
	for _, parameter := range wf.GetCapabilityParameters() {
		if parameter == nil {
			return fmt.Errorf("workflow %q has an empty capability parameter", wf.GetName())
		}
		if err := Validate(parameter); err != nil {
			return fmt.Errorf("workflow %q capability parameter %q is invalid: %w", wf.GetName(), parameter.GetName(), err)
		}
		if _, exists := parameters[parameter.GetName()]; exists {
			return fmt.Errorf("workflow %q declares capability parameter %q more than once", wf.GetName(), parameter.GetName())
		}
		parameters[parameter.GetName()] = parameter
	}
	for name := range selections {
		if parameters[name] == nil {
			return fmt.Errorf("workflow %q declares no capability parameter %q", wf.GetName(), name)
		}
	}

	selected := make(map[string]selectedCapability, len(parameters))
	wf.ResolvedCapabilityBindings = nil
	for _, parameter := range wf.GetCapabilityParameters() {
		bindingID, ok := selections[parameter.GetName()]
		if !ok {
			return fmt.Errorf("workflow %q requires capability %q, which was not bound", wf.GetName(), parameter.GetName())
		}
		binding := bindings[bindingID]
		if binding == nil {
			return fmt.Errorf("workflow %q capability %q selects binding id %q, which this deployment does not have", wf.GetName(), parameter.GetName(), bindingID)
		}
		qualifier := binding.GetQualifier()
		if binding.GetContract() != parameter.GetContract() {
			return fmt.Errorf("workflow %q capability %q requires contract %q, but binding %q provides %q", wf.GetName(), parameter.GetName(), parameter.GetContract(), qualifier, binding.GetContract())
		}
		if binding.GetContractDigest() != parameter.GetContractDigest() {
			return fmt.Errorf("workflow %q capability %q requires contract digest %s, but binding %q provides %s", wf.GetName(), parameter.GetName(), parameter.GetContractDigest(), qualifier, binding.GetContractDigest())
		}
		plugin := plugins[binding.GetPlugin().GetPluginName()]
		if plugin == nil {
			return fmt.Errorf("capability binding %q selects plugin %q, which this deployment does not have", qualifier, binding.GetPlugin().GetPluginName())
		}
		contractTasks, err := projectCapabilityTasks(parameter.GetTasks(), qualifier, plugin)
		if err != nil {
			return fmt.Errorf("workflow %q capability %q: %w", wf.GetName(), parameter.GetName(), err)
		}
		digest, err := CapabilityContractDigest(contractTasks)
		if err != nil {
			return fmt.Errorf("workflow %q capability %q: %w", wf.GetName(), parameter.GetName(), err)
		}
		if digest != parameter.GetContractDigest() {
			return fmt.Errorf("workflow %q capability %q selects binding %q whose provider contract digest is %s, want %s", wf.GetName(), parameter.GetName(), qualifier, digest, parameter.GetContractDigest())
		}

		selected[parameter.GetName()] = selectedCapability{parameter: parameter, binding: binding}
		wf.ResolvedCapabilityBindings = append(wf.ResolvedCapabilityBindings, &ResolvedCapabilityBinding{
			Parameter: parameter.GetName(), BindingId: binding.GetBindingId(), Revision: binding.GetRevision(),
			Qualifier: qualifier, Contract: binding.GetContract(), ContractDigest: binding.GetContractDigest(),
		})
		ensurePluginRequirement(wf, plugin)
	}

	if err := rewriteCapabilityNodes(wf.GetSteps(), selected, bindings, plugins, depth); err != nil {
		return fmt.Errorf("workflow %q: %w", wf.GetName(), err)
	}
	return nil
}

func projectCapabilityTasks(methods []string, qualifier string, plugin *PluginDescription) ([]*TaskDescription, error) {
	byName := make(map[string]*TaskDescription, len(plugin.GetTasks()))
	for _, task := range plugin.GetTasks() {
		byName[task.GetName()] = task
	}
	out := make([]*TaskDescription, 0, len(methods))
	for _, method := range methods {
		name := qualifier + "." + method
		task := byName[name]
		if task == nil {
			return nil, fmt.Errorf("binding %q provider has no task %q", qualifier, name)
		}
		out = append(out, task)
	}
	return out, nil
}

func ensurePluginRequirement(wf *Workflow, plugin *PluginDescription) {
	for _, requirement := range wf.GetPluginRequirements() {
		if requirement.GetName() == plugin.GetName() {
			return
		}
	}
	version := plugin.GetVersion()
	if !strings.HasPrefix(version, "v") {
		version = "v" + version
	}
	wf.PluginRequirements = append(wf.PluginRequirements, &PluginRequirement{Name: plugin.GetName(), MinimumVersion: version})
}

func rewriteCapabilityNodes(nodes []*Node, selected map[string]selectedCapability, bindings map[string]*CapabilityBinding, plugins map[string]*PluginDescription, depth int) error {
	if depth > maxWorkflowScanDepth {
		return fmt.Errorf("steps nest more than %d deep, past what a specification is checked to", maxWorkflowScanDepth)
	}
	for _, node := range nodes {
		if node == nil {
			continue
		}
		for _, task := range []*Task{node.GetTask(), node.GetUndo().GetTask()} {
			if task == nil {
				continue
			}
			qualifier, method, dotted := strings.Cut(task.GetName(), ".")
			if !dotted {
				continue
			}
			selection, ok := selected[qualifier]
			if !ok {
				return fmt.Errorf("step %q uses undeclared capability %q; explicit-capability workflows may use dotted tasks only through a declared parameter", node.GetId(), qualifier)
			}
			if !slices.Contains(selection.parameter.GetTasks(), method) {
				return fmt.Errorf("step %q uses task %q, which capability %q's contract does not declare", node.GetId(), task.GetName(), qualifier)
			}
			task.Name = selection.binding.GetQualifier() + "." + method
		}

		if loop := node.GetForEach(); loop != nil {
			if err := rewriteCapabilityNodes(loop.GetBody(), selected, bindings, plugins, depth+1); err != nil {
				return err
			}
		}
		if loop := node.GetLoop(); loop != nil {
			if err := rewriteCapabilityNodes(loop.GetBody(), selected, bindings, plugins, depth+1); err != nil {
				return err
			}
		}
		if parallel := node.GetParallel(); parallel != nil {
			for _, branch := range parallel.GetBranches() {
				if err := rewriteCapabilityNodes(branch.GetSteps(), selected, bindings, plugins, depth+1); err != nil {
					return err
				}
			}
		}
		if sw := node.GetSwitch(); sw != nil {
			for _, body := range SwitchBodies(sw) {
				if err := rewriteCapabilityNodes(body, selected, bindings, plugins, depth+1); err != nil {
					return err
				}
			}
		}
		if call := node.GetCall(); call != nil {
			callee := call.GetWorkflow()
			if callee == nil {
				return fmt.Errorf("step %q calls an empty workflow", node.GetId())
			}
			calleeParams := make(map[string]bool, len(callee.GetCapabilityParameters()))
			for _, parameter := range callee.GetCapabilityParameters() {
				calleeParams[parameter.GetName()] = true
			}
			for name := range call.GetCapabilityArguments() {
				if !calleeParams[name] {
					return fmt.Errorf("step %q binds capability %q, which workflow %q does not declare", node.GetId(), name, callee.GetName())
				}
			}
			childSelections := make(map[string]string, len(calleeParams))
			for _, parameter := range callee.GetCapabilityParameters() {
				parentName, ok := call.GetCapabilityArguments()[parameter.GetName()]
				if !ok {
					return fmt.Errorf("step %q calls workflow %q, whose capability %q is not explicitly forwarded", node.GetId(), callee.GetName(), parameter.GetName())
				}
				parent, ok := selected[parentName]
				if !ok {
					return fmt.Errorf("step %q forwards undeclared caller capability %q into workflow %q", node.GetId(), parentName, callee.GetName())
				}
				childSelections[parameter.GetName()] = parent.binding.GetBindingId()
			}
			if err := resolveWorkflowCapabilities(callee, childSelections, bindings, plugins, depth+1, len(calleeParams) > 0); err != nil {
				return fmt.Errorf("step %q calls workflow %q: %w", node.GetId(), callee.GetName(), err)
			}
		}
	}
	return nil
}
