package flowstatev1

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/picatz/flowstate/internal/textbound"
)

// MaxPluginRequirements bounds the plugins one workflow requires. It matches the
// max_items rule on Workflow.plugin_requirements, which a message read off the
// wire is held to by [Validate] but a specification built in process is not.
const MaxPluginRequirements = 64

// maxBoundCredentialBytes bounds what [BindPluginCredentials] writes into one
// workflow: the sum of the encoded sizes of every reference it copies into a
// step. A binding is at most a few kilobytes and a step is one of many, so
// without this a short binding spread over a long step list multiplies into a
// specification the admission size check only sees afterwards.
const maxBoundCredentialBytes = 1 << 20

// TaskCredentialInputs maps each input of def that claims a plugin credential
// to the credential's name, or is nil when none does. It is the registry-side
// reading of the `credential` input claim, from the definition's input
// descriptor ([InputClaims]); an unreadable descriptor is an error because a
// task whose claims cannot be read cannot be held to them.
func TaskCredentialInputs(def TaskDef) (map[string]string, error) {
	claims, err := InputClaims(def.Inputs)
	if err != nil {
		return nil, fmt.Errorf("reading the credential claims of task %q: %w", def.Name, err)
	}

	return CredentialInputs(claims), nil
}

// credentialClaimCache reads each distinct task's credential claims once per
// walk, so a specification of many steps of the same few tasks walks each task's
// descriptor once.
type credentialClaimCache struct {
	registry *Registry
	tasks    map[string]map[string]string
}

func newCredentialClaimCache(registry *Registry) *credentialClaimCache {
	return &credentialClaimCache{registry: registry, tasks: make(map[string]map[string]string)}
}

// of returns the credential inputs of the named task, nil for a task the
// registry does not have (an unknown task is refused by
// [ResolveTaskCapabilities], so this answers nothing a second time).
func (c *credentialClaimCache) of(name string) (map[string]string, error) {
	if inputs, ok := c.tasks[name]; ok {
		return inputs, nil
	}

	var inputs map[string]string
	if def, found := c.registry.Lookup(name); found {
		var err error
		if inputs, err = TaskCredentialInputs(def); err != nil {
			return nil, err
		}
	}
	c.tasks[name] = inputs

	return inputs, nil
}

// pluginOfTask is the plugin a task name belongs to: the segment before the first
// dot, which a plugin name cannot contain. A task with no dot is not a plugin's.
func pluginOfTask(task string) (string, bool) {
	plugin, _, qualified := strings.Cut(task, ".")

	return plugin, qualified && plugin != ""
}

// pluginBindings returns the credential bindings of one workflow's `plugins:`,
// plugin name to credential name to reference, after holding them to what a
// binding is: at most [MaxPluginCredentials] per plugin, a [ValidCredentialName]
// each, and a whole secret reference, never anything else. Plugins that bind
// nothing are absent.
//
// A reference is the only thing a binding may be because it is what crosses the
// boundary in place of a value; a `${credential()}` reference is refused until a
// plugin's declaration of the credential as federated is something this
// enforcement point can read, which fails closed. The refusal names the plugin
// and credential and never the binding.
func pluginBindings(wf *Workflow) (map[string]map[string]*Value, error) {
	requirements := wf.GetPluginRequirements()
	if len(requirements) > MaxPluginRequirements {
		return nil, fmt.Errorf("the workflow requires %d plugins, more than the %d a workflow may", len(requirements), MaxPluginRequirements)
	}

	var (
		bindings map[string]map[string]*Value
		bound    = make(map[string]bool, len(requirements))
	)
	for _, requirement := range requirements {
		plugin := requirement.GetName()
		credentials := requirement.GetCredentials()

		// One name twice would let a later requirement's bindings or a later
		// requirement's silence stand for the earlier one's, so a name that binds
		// anything is written once.
		if previous, seen := bound[plugin]; seen && (previous || len(credentials) > 0) {
			return nil, fmt.Errorf("plugin %q is required twice and binds credentials; write it once", textbound.Truncate(plugin, 64))
		}
		bound[plugin] = len(credentials) > 0

		if len(credentials) == 0 {
			continue
		}
		if len(credentials) > MaxPluginCredentials {
			return nil, fmt.Errorf("plugin %q binds %d credentials, more than the %d a plugin may declare", textbound.Truncate(plugin, 64), len(credentials), MaxPluginCredentials)
		}
		for _, name := range slices.Sorted(maps.Keys(credentials)) {
			if !ValidCredentialName(name) {
				return nil, fmt.Errorf("plugin %q binds credential %q, which is not a name matching %s", textbound.Truncate(plugin, 64), textbound.Truncate(name, 64), credentialName)
			}
			value := credentials[name]
			if value.GetSecretRef() == nil {
				return nil, fmt.Errorf("plugin %q credential %q must be bound to a whole secret reference such as ${secret('env:NAME')}, never a literal, an expression or a credential reference",
					textbound.Truncate(plugin, 64), name)
			}
		}
		if bindings == nil {
			bindings = make(map[string]map[string]*Value)
		}
		bindings[plugin] = credentials
	}

	return bindings, nil
}

// declaredCredentials reads which credentials each of plugins declares, from the
// registry: the union of the credential claims of every task registered under
// the plugin's prefix. Declaring a credential no task claims is refused when a
// plugin loads ([CheckPluginCredentials]), so a claim is a declaration. A plugin
// with no task in the registry is absent from the result; nothing can be said of
// it here and a deployment without it is refused at plugin resolution.
func declaredCredentials(registry *Registry, bindings map[string]map[string]*Value) (map[string]map[string]bool, error) {
	declared := make(map[string]map[string]bool, len(bindings))
	for _, def := range registry.All() {
		plugin, qualified := pluginOfTask(def.Name)
		if !qualified || bindings[plugin] == nil {
			continue
		}
		inputs, err := TaskCredentialInputs(def)
		if err != nil {
			return nil, err
		}
		if declared[plugin] == nil {
			declared[plugin] = make(map[string]bool)
		}
		for _, credential := range inputs {
			declared[plugin][credential] = true
		}
	}

	return declared, nil
}

// BindPluginCredentials expands the credential bindings of `plugins:` into the
// per-step secret references the engine already enforces, for wf and every
// workflow it calls, each against its own requirements (a caller's binding
// never crosses a `call:`).
//
// For each task step, `undo:` task included, of a plugin that binds a credential,
// every input the task claims for that credential which the step does not write
// receives a copy of the binding. An input the step writes is left exactly as it
// is: a step's own reference overrides the binding, and a literal there is
// refused by [CheckInputClaims] as it would be with no binding at all. A
// credential input with neither stays absent, which [CheckInputClaims] refuses;
// this binds and does not require.
//
// Nothing here resolves anything. A binding is a reference and expansion copies
// references, so the specification a driver executes is the one that wrote the
// reference on every step, and both drivers read that one specification: the
// Flowfile compiler and the server's admission both run this, before anything
// durable exists, so a hand-built specification is expanded exactly as a compiled
// one is.
//
// It is idempotent. It fails closed on a binding that is not a whole secret
// reference, on one naming a credential none of the plugin's registered tasks
// claim (when the registry holds any of the plugin's tasks to say), on a plugin
// required twice with bindings, and on an expansion past
// [maxBoundCredentialBytes]. registry is the deployment's task registry; a nil
// one decides nothing, and a check that cannot decide must not admit.
func BindPluginCredentials(wf *Workflow, registry *Registry) error {
	if registry == nil {
		return errors.New("no task registry: cannot bind plugin credentials")
	}

	budget := maxBoundCredentialBytes
	for current, err := range specWorkflows(wf) {
		if err != nil {
			return fmt.Errorf("binding plugin credentials: %w", err)
		}
		if err := bindWorkflowCredentials(current, registry, &budget); err != nil {
			if current != wf {
				return fmt.Errorf("workflow %q: %w", current.GetName(), err)
			}

			return err
		}
	}

	return nil
}

func bindWorkflowCredentials(wf *Workflow, registry *Registry, budget *int) error {
	bindings, err := pluginBindings(wf)
	if err != nil {
		return err
	}
	if len(bindings) == 0 {
		return nil
	}

	declared, err := declaredCredentials(registry, bindings)
	if err != nil {
		return err
	}
	for _, plugin := range slices.Sorted(maps.Keys(bindings)) {
		known, ok := declared[plugin]
		if !ok {
			continue
		}
		for _, name := range slices.Sorted(maps.Keys(bindings[plugin])) {
			if !known[name] {
				return fmt.Errorf("plugin %q has no credential %q to bind; it declares %s",
					textbound.Truncate(plugin, 64), name, listNames(slices.Sorted(maps.Keys(known))))
			}
		}
	}

	var (
		claims  = newCredentialClaimCache(registry)
		failure error
	)
	bind := func(task *Task) {
		if failure != nil || task == nil {
			return
		}
		plugin, qualified := pluginOfTask(task.GetName())
		if !qualified || bindings[plugin] == nil {
			return
		}
		credentialInputs, err := claims.of(task.GetName())
		if err != nil {
			failure = err

			return
		}
		for _, input := range slices.Sorted(maps.Keys(credentialInputs)) {
			binding, bound := bindings[plugin][credentialInputs[input]]
			if !bound {
				continue
			}
			if _, written := task.GetInputs()[input]; written {
				continue
			}
			if *budget -= proto.Size(binding); *budget < 0 {
				failure = fmt.Errorf("binding plugin credentials would add more than %d bytes of references to the workflow; bind fewer credentials or use fewer steps", maxBoundCredentialBytes)

				return
			}
			if task.Inputs == nil {
				task.Inputs = make(map[string]*Value)
			}
			task.Inputs[input] = proto.Clone(binding).(*Value)
		}
	}

	WalkNodes(wf.GetSteps(), Walk{Node: func(node *Node) {
		bind(node.GetTask())
		bind(node.GetUndo().GetTask())
	}})

	return failure
}

// ElideBoundCredentials is the inverse of [BindPluginCredentials] for writing a
// workflow back out: it removes, from wf's own steps, every input that equals
// the binding `plugins:` makes for the credential the input claims, so the
// Flowfile states the reference once. Expanding the result again gives wf back.
// A callee is a separate document and is not touched. It mutates wf; a caller
// that keeps wf clones it first.
//
// A registry that does not hold the plugin's tasks elides nothing, which is the
// safe direction: the file then carries the reference on each step, and reading
// it back still gives the same workflow.
func ElideBoundCredentials(wf *Workflow, registry *Registry) error {
	if registry == nil {
		return errors.New("no task registry: cannot elide plugin credentials")
	}
	bindings, err := pluginBindings(wf)
	if err != nil || len(bindings) == 0 {
		return err
	}

	claims := newCredentialClaimCache(registry)
	var failure error
	elide := func(task *Task) {
		if failure != nil || task == nil {
			return
		}
		plugin, qualified := pluginOfTask(task.GetName())
		if !qualified || bindings[plugin] == nil {
			return
		}
		credentialInputs, err := claims.of(task.GetName())
		if err != nil {
			failure = err

			return
		}
		for input, credential := range credentialInputs {
			binding, bound := bindings[plugin][credential]
			if bound && proto.Equal(task.GetInputs()[input], binding) {
				delete(task.Inputs, input)
			}
		}
	}

	WalkNodes(wf.GetSteps(), Walk{Node: func(node *Node) {
		elide(node.GetTask())
		elide(node.GetUndo().GetTask())
	}})

	return failure
}

// listNames renders at most [MaxPluginCredentials] names for a message.
func listNames(names []string) string {
	if len(names) == 0 {
		return "none"
	}
	if len(names) > MaxPluginCredentials {
		names = names[:MaxPluginCredentials]
	}

	return strings.Join(names, ", ")
}

// copyIfBindsCredentials returns wf, or a deep copy of it when wf or a workflow
// it calls binds any credential under `plugins:`, so a caller that must not
// change its argument copies only when [BindPluginCredentials] would write. A
// specification that cannot be walked is copied: the binding is what refuses it.
func copyIfBindsCredentials(wf *Workflow) *Workflow {
	if bindsPluginCredentials(wf) {
		return proto.Clone(wf).(*Workflow)
	}

	return wf
}

func bindsPluginCredentials(wf *Workflow) bool {
	for current, err := range specWorkflows(wf) {
		if err != nil {
			return true
		}
		for _, requirement := range current.GetPluginRequirements() {
			if len(requirement.GetCredentials()) > 0 {
				return true
			}
		}
	}

	return false
}
