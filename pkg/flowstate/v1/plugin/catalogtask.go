package plugin

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Rebuilding a task definition from a described one, with no process launched.
//
// [Plugin.taskDef] rebuilds a [flowstatev1.TaskDef] from the manifest a plugin
// sent over a socket the host opened. This rebuilds the same thing from a
// [flowstatev1.TaskDescription] — the catalog form — which is the half of #710
// that a browser authoring surface, a server-side Validate RPC and a
// checked-in catalog document all need, because none of them can exec.
//
// One rebuild would have been better than two, and the shapes are close enough
// that it is worth saying why there are two. A manifest is what a plugin says
// about itself and carries a bare task name the host qualifies; a description
// is what a host says about a task it has already admitted, qualified and
// canonicalized, and it describes a built-in task as readily as a plugin's.
// What they share — turning descriptor bytes and a message name back into a
// live descriptor — is [messageDescriptor], called by both.
//
// What is deliberately *not* shared is execution. A def rebuilt here has no
// plugin process behind it and never will, so its [flowstatev1.TaskDef.Fn]
// refuses rather than doing something surprising; see [ErrCatalogOnly].

// ErrCatalogOnly reports that a task rebuilt from a catalog was asked to run.
//
// A catalog says what a task's shape and claims are, which is what validation,
// completion and documentation need. It carries no way to execute anything —
// the plugin binary may not exist on this machine, and on a browser authoring
// surface there is no such thing as a binary. Registering a def with a nil Fn
// is refused by [flowstatev1.Registry.Register], and a def that executed
// something else would be worse, so the function fails closed and says which
// of the two situations the caller is in.
var ErrCatalogOnly = errors.New("plugin: task was loaded from a catalog and cannot be executed")

// ErrCatalogClaims reports that a catalog's claim fields cannot be read by this
// build, so nothing may be rebuilt from it.
//
// The five claim fields with security weight (#712) are proto3 scalars with no
// presence, so a catalog written by a build that predates them decodes as a
// task that needs no scope, takes no secret and defers nothing — the *weakest*
// possible reading of every one of them, arrived at by silence. That is the
// fail-open direction, and it is why the catalog carries
// ClaimsSchemaVersion as a presence signal for the whole set. A rebuild is
// refused unless the version is exactly the one this build understands, on the
// identical reasoning [flowstatev1.TaskDescriptionClaimsKnown] and
// ResolvePlugins already apply to it: neither a newer version nor an older one
// is safe to guess at.
var ErrCatalogClaims = errors.New("plugin: catalog claims schema version is not this build's")

// ErrCatalogTaskName reports that a catalog names a task its plugin could not
// have provided.
//
// A launched plugin never chooses the name a task is registered under: the host
// qualifies every manifest's bare name with the plugin's own
// (`p.name + "." + name`, task.go:36), so `example.greet` is the only shape a
// launch can produce and a bare `http` is not reachable that way at all. A
// catalog is a *document*, and a document says whatever its author typed — so a
// catalog naming a task `http` would, once rebuilt and registered, replace the
// built-in of that name ([flowstatev1.Registry.Register] replaces rather than
// refuses) with a definition that carries the document's descriptors and cannot
// execute.
//
// That is a reader stronger than the thing it stands in for, in the one
// direction that matters: a catalog could change what `flow validate` checks a
// built-in step against. So the qualification the launcher applies is checked
// here rather than assumed, and a catalog no host could have written is refused
// whole (#710).
var ErrCatalogTaskName = errors.New("plugin: catalog names a task its plugin could not have provided")

// ErrCatalogDuplicateTask reports that a catalog gives one task name two
// definitions.
//
// Its own sentinel rather than [ErrCatalogTaskName] because the two are
// different mistakes with different fixes: that one is a name no plugin could
// have declared, and this one is two declarations of a name that is perfectly
// well formed. See [checkOneDefinitionPer] for why a live host cannot be in
// this state and a document can.
var ErrCatalogDuplicateTask = errors.New("plugin: catalog defines one task twice")

// ErrCatalogTooLarge reports that a catalog is over one of the bounds on the
// document as a whole — how many plugins it names, how many tasks across all of
// them, or how many descriptor bytes it carries in total.
//
// Distinct from the per-descriptor refusal ([ErrDescriptor]) because it is a
// different attack and a different answer: that one means "this descriptor is
// too big to parse", and this one means "each of these is small enough and
// there are far too many of them". An operator whose honest catalog hits one of
// these raises the matching Config bound; an operator seeing it on a document
// they did not produce has been handed a decompression bomb's cousin. Both need
// to know which bound, so every message names the field (#854 review).
var ErrCatalogTooLarge = errors.New("plugin: catalog is over a bound on the whole document")

// TaskDefsFromCatalog rebuilds every task in a plugin catalog, with no plugin
// process launched.
//
// The catalog is one `flow plugins --output json` document, or the
// [flowstatev1.PluginCatalog] a GetCatalog-style RPC answers with. Every task
// in it is rebuilt or none is: a caller validating a workflow against a partial
// catalog would report an unknown task for a name the catalog was carrying, and
// a diagnostic drawn from a document this failed to read is a false one.
//
// cfg bounds the document, and bounds two different resources with it. Each
// descriptor is bounded exactly as launching a plugin bounds one, by
// MaxDescriptorBytes and MaxDescriptorFiles — a catalog naming a
// hundred-megabyte descriptor is the same attack as a plugin sending one. The
// catalog as a whole is bounded by MaxCatalogPlugins, MaxCatalogTasks and
// MaxCatalogDescriptorBytes, because a bound on the size of one item is not a
// bound on how many items arrive, and the peer chooses how many (#854 review).
// A zero Config takes the defaults.
func TaskDefsFromCatalog(catalog *flowstatev1.PluginCatalog, cfg Config) ([]flowstatev1.TaskDef, error) {
	if catalog.GetClaimsSchemaVersion() != flowstatev1.CurrentClaimsSchemaVersion {
		return nil, fmt.Errorf(
			"%w: the catalog reports %d and this build reads %d, so what its tasks claim "+
				"cannot be told from what a build predating those fields left unsaid",
			ErrCatalogClaims, catalog.GetClaimsSchemaVersion(), flowstatev1.CurrentClaimsSchemaVersion)
	}

	cfg = cfg.withDefaults()

	total, err := boundCatalog(catalog, cfg)
	if err != nil {
		return nil, err
	}

	// Before a single definition is built, for the same reason the bounds are
	// checked in a pre-pass: a refusal that arrives halfway has already paid for
	// half the work, and a caller that registered the first half of a catalog
	// this then refused is exactly the partial state the all-or-nothing rule
	// above exists to prevent.
	if err := checkOneDefinitionPer(catalog); err != nil {
		return nil, err
	}

	defs := make([]flowstatev1.TaskDef, 0, total)
	for _, described := range catalog.GetPlugins() {
		for _, task := range described.GetTasks() {
			if err := checkQualified(described.GetName(), task.GetName()); err != nil {
				return nil, err
			}

			def, err := TaskDefFromDescription(task, cfg)
			if err != nil {
				return nil, fmt.Errorf("plugin %q: %w", truncate(described.GetName(), 64), err)
			}
			defs = append(defs, def)
		}
	}

	return defs, nil
}

// checkQualified refuses a task name the plugin it is listed under could not
// have produced. See [ErrCatalogTaskName] for why a document gets asked this
// and a launched plugin does not.
//
// The prefix is this function's own business — it is the *host* that joins the
// two segments, and nothing in the schema describes the joined form. Both
// segments, though, are already spelled out exactly once, as the protovalidate
// rules on PluginManifest.name (`^[a-z0-9][a-z0-9-]*$`, 1–64) and
// TaskManifest.name (`^[a-z][a-z0-9_]*$`, 1–64), which
// [Plugin.checkManifest] applies through [flowstatev1.Validate] at launch. So
// the segments are checked by handing those rules the manifest a plugin would
// have had to describe itself with to produce this name, rather than by
// transcribing the two patterns into a second regex here (#863 review). A
// pattern that changes in the schema changes here on the same day, by
// construction.
//
// The capability is set because the manifest has to be a *valid* one for the
// rules on its name fields to be the thing that fails; nothing about this call
// launches, dispatches or trusts anything.
func checkQualified(plugin, task string) error {
	if plugin == "" {
		return fmt.Errorf("%w: a plugin in the catalog has no name, so nothing it lists can be attributed to it",
			ErrCatalogTaskName)
	}

	prefix := plugin + "."

	bare, qualified := strings.CutPrefix(task, prefix)
	if !qualified || bare == "" {
		return fmt.Errorf(
			"%w: plugin %q lists a task named %q, and a host names every one of a plugin's tasks "+
				"%s<task>; a catalog naming a task any other way would register that name over "+
				"whatever already holds it",
			ErrCatalogTaskName, truncate(plugin, 64), truncate(task, 64), prefix)
	}

	if err := flowstatev1.Validate(&pluginv1.PluginManifest{
		Name:         plugin,
		Capabilities: []pluginv1.Capability{pluginv1.Capability_CAPABILITY_TASKS},
		Tasks:        []*pluginv1.TaskManifest{{Name: bare}},
	}); err != nil {
		return fmt.Errorf(
			"%w: plugin %q lists a task named %q, and no plugin could have declared that — the "+
				"host builds a task's name from a manifest whose own rules refuse it: %w",
			ErrCatalogTaskName, truncate(plugin, 64), truncate(task, 64), err)
	}

	return nil
}

// checkOneDefinitionPer refuses a catalog that gives one task name two
// definitions.
//
// A live host cannot be in this state. Within a plugin, [Plugin.checkManifest]
// refuses a manifest that "provides task %q twice"; across plugins, the host
// keys by the binary's name and qualifies every task with it, so two plugins
// cannot reach the same qualified name at all. A document can do both — two
// entries under one plugin name, or one plugin listing a task twice — and
// [flowstatev1.Registry.Register] *replaces*, so whichever definition came last
// in the file would silently win. That makes what a validator says a property
// of the order lines appear in, which is not a property a file should have
// (#863 review).
//
// Refused for the whole catalog before anything is rebuilt or registered, on
// the all-or-nothing rule [TaskDefsFromCatalog] already states: a caller left
// holding half a catalog reports an unknown task for a name the document was
// carrying. Both plugin entries are named, because with duplicate plugin names
// in a document the task name alone does not say where to look.
func checkOneDefinitionPer(catalog *flowstatev1.PluginCatalog) error {
	// Index rather than name alone: a document may list two plugin entries with
	// the same name, and "declared by example and by example" is not an answer
	// somebody can act on.
	type source struct {
		plugin string
		index  int
	}

	seen := make(map[string]source)
	for i, described := range catalog.GetPlugins() {
		for _, task := range described.GetTasks() {
			name := task.GetName()
			if first, dup := seen[name]; dup {
				return fmt.Errorf(
					"%w: task %q is defined twice — by plugin entry %d (%q) and again by entry %d (%q); "+
						"a registry keeps one definition per name, so which of them a file were checked "+
						"against would depend on the order they appear in",
					ErrCatalogDuplicateTask, truncate(name, 64),
					first.index, truncate(first.plugin, 64), i, truncate(described.GetName(), 64))
			}
			seen[name] = source{plugin: described.GetName(), index: i}
		}
	}

	return nil
}

// boundCatalog refuses a catalog that is over one of the whole-document bounds,
// and returns how many tasks it holds so the caller can size its result once.
//
// A pre-pass rather than a running total inside the rebuild loop, because the
// point is to refuse *before* doing the expensive thing: counting walks a
// message that is already decoded, while rebuilding parses, links and retains a
// type registry per task. A bound that only stops the work halfway through has
// already paid for half of it.
//
// Three bounds because the peer controls three resources independently, and any
// two of them leave the third open: ten plugins declaring a hundred thousand
// tasks between them, a hundred thousand plugins declaring one task each, and a
// thousand tasks whose descriptors are each comfortably inside
// MaxDescriptorBytes and sum to gigabytes. This is CLAUDE.md's "ask which
// resource the attacker controls, then bound that resource", applied to a
// document whose author is not this process (#854 review).
func boundCatalog(catalog *flowstatev1.PluginCatalog, cfg Config) (int, error) {
	plugins := catalog.GetPlugins()
	if len(plugins) > cfg.MaxCatalogPlugins {
		return 0, fmt.Errorf(
			"%w: the catalog names %d plugins, over the %d this reader will rebuild "+
				"(Config.MaxCatalogPlugins)",
			ErrCatalogTooLarge, len(plugins), cfg.MaxCatalogPlugins)
	}

	var tasks, descriptorBytes int
	for _, described := range plugins {
		tasks += len(described.GetTasks())
		if tasks > cfg.MaxCatalogTasks {
			return 0, fmt.Errorf(
				"%w: the catalog names more than %d tasks across its plugins, which is all "+
					"this reader will rebuild (Config.MaxCatalogTasks)",
				ErrCatalogTooLarge, cfg.MaxCatalogTasks)
		}

		for _, task := range described.GetTasks() {
			descriptorBytes += len(task.GetInputDescriptor()) + len(task.GetOutputDescriptor())
			if descriptorBytes > cfg.MaxCatalogDescriptorBytes {
				return 0, fmt.Errorf(
					"%w: the catalog carries more than %d bytes of task descriptors in total, "+
						"which is all this reader will parse (Config.MaxCatalogDescriptorBytes); "+
						"each descriptor is inside Config.MaxDescriptorBytes and there are too many of them",
					ErrCatalogTooLarge, cfg.MaxCatalogDescriptorBytes)
			}
		}
	}

	return tasks, nil
}

// TaskDefFromDescription rebuilds one task from its catalog description.
//
// The name is taken as written. A description is produced by
// [flowstatev1.DescribeTask] from a def that was already registered, so the
// name in it is the qualified one an author writes — unlike a manifest's, which
// is bare and gets the plugin's name prefixed onto it by [Plugin.taskDef].
//
// The claim fields are carried straight across, which is the point of the
// exercise: a def rebuilt here has to refuse everything the launched def
// refuses, and a validator built on a def that quietly claims less is weaker
// than the one it stands in for.
//
// Callers with a whole catalog should use [TaskDefsFromCatalog], which checks
// the claims schema version the claim fields' meaning depends on. This function
// cannot: that version is a property of the catalog, not of one task in it.
func TaskDefFromDescription(described *flowstatev1.TaskDescription, cfg Config) (flowstatev1.TaskDef, error) {
	name := described.GetName()
	if name == "" {
		return flowstatev1.TaskDef{}, fmt.Errorf("%w: a described task has no name", ErrDescriptor)
	}

	cfg = cfg.withDefaults()

	inputs, err := messageDescriptor(described.GetInputDescriptor(), described.GetInputMessage(), cfg)
	if err != nil {
		return flowstatev1.TaskDef{}, fmt.Errorf("task %q inputs: %w", truncate(name, 64), err)
	}

	outputs, err := messageDescriptor(described.GetOutputDescriptor(), described.GetOutputMessage(), cfg)
	if err != nil {
		return flowstatev1.TaskDef{}, fmt.Errorf("task %q outputs: %w", truncate(name, 64), err)
	}

	return flowstatev1.TaskDef{
		Name:    name,
		Summary: described.GetSummary(),
		Inputs:  inputs,
		Outputs: outputs,

		// The claims with security weight (#712), each read from the
		// field DescribeTask wrote it from. A field added to that mapping
		// belongs here the same day, or a catalog carries a fact this drops.
		NeedsPrevOutputs:     described.GetNeedsScope(),
		SecretInputs:         slices.Clone(described.GetSecretInputs()),
		RequiredSecretInputs: slices.Clone(described.GetRequiredSecretInputs()),
		ShapesOutputs:        described.GetShapesOutputs(),
		DeferredInputs:       slices.Clone(described.GetDeferredInputs()),
		ExpressionInputs:     slices.Clone(described.GetExpressionInputs()),

		Fn: catalogTaskFunc(name),
	}, nil
}

// catalogTaskFunc is what a task rebuilt from a catalog does when something
// tries to run it, which is refuse and say why. See [ErrCatalogOnly].
func catalogTaskFunc(name string) flowstatev1.TaskFunc {
	return func(context.Context, map[string]*flowstatev1.Value, *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error) {
		// UnknownTask rather than Upstream, which is what a launched plugin
		// task returns when its process is not there: that one is retryable
		// because a restart can make it true, and this one is not — no retry
		// makes a plugin appear in a process that was never going to launch
		// one. It is the same fact the kind's own doc comment names, that the
		// specification or the worker has to change first.
		return nil, flowstatev1.NewTaskError(name, flowstatev1.ErrorKindUnknownTask, ErrCatalogOnly)
	}
}
