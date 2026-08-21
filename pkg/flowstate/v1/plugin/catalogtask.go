package plugin

import (
	"context"
	"errors"
	"fmt"
	"slices"

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

// TaskDefsFromCatalog rebuilds every task in a plugin catalog, with no plugin
// process launched.
//
// The catalog is one `flow plugins --output json` document, or the
// [flowstatev1.PluginCatalog] a GetCatalog-style RPC answers with. Every task
// in it is rebuilt or none is: a caller validating a workflow against a partial
// catalog would report an unknown task for a name the catalog was carrying, and
// a diagnostic drawn from a document this failed to read is a false one.
//
// cfg bounds the descriptors exactly as launching a plugin does — the document
// is untrusted input, and a catalog naming a hundred-megabyte descriptor is the
// same attack as a plugin sending one. A zero Config takes the defaults.
func TaskDefsFromCatalog(catalog *flowstatev1.PluginCatalog, cfg Config) ([]flowstatev1.TaskDef, error) {
	if catalog.GetClaimsSchemaVersion() != flowstatev1.CurrentClaimsSchemaVersion {
		return nil, fmt.Errorf(
			"%w: the catalog reports %d and this build reads %d, so what its tasks claim "+
				"cannot be told from what a build predating those fields left unsaid",
			ErrCatalogClaims, catalog.GetClaimsSchemaVersion(), flowstatev1.CurrentClaimsSchemaVersion)
	}

	cfg = cfg.withDefaults()

	var defs []flowstatev1.TaskDef
	for _, described := range catalog.GetPlugins() {
		for _, task := range described.GetTasks() {
			def, err := TaskDefFromDescription(task, cfg)
			if err != nil {
				return nil, fmt.Errorf("plugin %q: %w", truncate(described.GetName(), 64), err)
			}
			defs = append(defs, def)
		}
	}

	return defs, nil
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

		// The five claims with security weight (#712), each read from the
		// field DescribeTask wrote it from. A field added to that mapping
		// belongs here the same day, or a catalog carries a fact this drops.
		NeedsPrevOutputs: described.GetNeedsScope(),
		SecretInputs:     slices.Clone(described.GetSecretInputs()),
		ShapesOutputs:    described.GetShapesOutputs(),
		DeferredInputs:   slices.Clone(described.GetDeferredInputs()),
		ExpressionInputs: slices.Clone(described.GetExpressionInputs()),

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
