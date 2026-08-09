package plugin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// Host discovers, launches, supervises, and talks to plugin processes.
//
// A Host is built once at worker startup, opened, and closed at shutdown. It
// owns every plugin process it starts, and [Host.Close] is the only thing that
// ends them — a Host that is dropped without being closed leaves processes
// running, which is why Close belongs in a defer next to the construction.
//
// A Host is safe for concurrent use.
type Host struct {
	cfg       Config
	log       *slog.Logger
	telemetry telemetry

	// procCtx bounds every plugin process. Close cancels it, which is what makes
	// shutdown reach processes that are mid-launch as well as ones already
	// serving.
	procCtx context.Context
	cancel  context.CancelFunc

	mu       sync.RWMutex
	opened   bool
	closed   bool
	plugins  map[string]*Plugin
	schemes  map[string]*Plugin
	taskDefs map[string]taskBinding
}

// taskBinding is a plugin task, reconstructed into the same shape a built-in
// task has.
type taskBinding struct {
	plugin *Plugin
	def    flowstatev1.TaskDef
}

// NewHost returns a Host for the given configuration, without launching
// anything. It reports an error for a configuration that cannot be used, so that
// a misconfigured deployment fails at startup rather than when the first
// workflow needs a plugin.
func NewHost(cfg Config) (*Host, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	cfg = cfg.withDefaults()
	procCtx, cancel := context.WithCancel(context.Background())

	return &Host{
		cfg:       cfg,
		log:       cfg.logger(),
		telemetry: newTelemetry(cfg),
		procCtx:   procCtx,
		cancel:    cancel,
		plugins:   make(map[string]*Plugin),
		schemes:   make(map[string]*Plugin),
		taskDefs:  make(map[string]taskBinding),
	}, nil
}

// Open discovers the plugins on the search path, launches each of them, and
// takes what they advertise.
//
// It is strict on purpose: if any discovered plugin cannot be brought up — it
// does not handshake, its manifest does not validate, it advertises nothing, it
// claims a scheme another plugin claims or the deployment does not permit, or
// its task descriptors cannot be reconstructed — Open terminates everything it
// started and returns the reasons. A worker that came up with a plugin silently
// missing would fail later, further from the cause, in a workflow that had no
// way to know its task was not going to be there.
//
// The context bounds the work of opening. It does not bound the plugins' lives:
// they run until [Host.Close].
func (h *Host) Open(ctx context.Context) error {
	ctx, span := h.telemetry.tracer.Start(ctx, "flowstate.plugin.host.open")
	defer span.End()
	h.mu.Lock()
	switch {
	case h.closed:
		h.mu.Unlock()
		return ErrClosed
	case h.opened:
		h.mu.Unlock()
		return fmt.Errorf("plugin: host is already open")
	}
	h.opened = true
	h.mu.Unlock()

	found, err := Discover(h.cfg)
	if err != nil {
		return err
	}

	var (
		launched []*Plugin
		problems []error
	)

	// A deployment that pinned its plugin set expects that set. A name in Only
	// with no binary behind it is a missing installation or a typo, and either
	// one silently yields a worker without a capability someone thought it had.
	for _, name := range h.cfg.Only {
		if !slices.ContainsFunc(found, func(f Found) bool { return f.Name == name }) {
			problems = append(problems, fmt.Errorf(
				"%w: no %s%s on the search path %v",
				ErrLaunch, BinaryPrefix, name, h.cfg.SearchPath,
			))
		}
	}

	for _, f := range found {
		if !h.cfg.wanted(f.Name) {
			h.log.Debug("plugin not in the configured set, so it was not launched", "plugin", f.Name)
			continue
		}

		if err := ctx.Err(); err != nil {
			problems = append(problems, err)
			break
		}

		p, err := newPlugin(h.procCtx, h.cfg, f)
		if err != nil {
			problems = append(problems, err)
			continue
		}

		launched = append(launched, p)
	}

	if len(problems) == 0 {
		problems = h.bind(launched)
	}

	if len(problems) > 0 {
		// Nothing is left running. A half-open host would be a worker with an
		// unpredictable subset of its plugins, which is worse than none.
		h.cancel()

		// Bounded, because this runs on the path that reports a configuration
		// error: a plugin ignoring its signals must not turn a clear refusal
		// into a hang.
		stopCtx, stopCancel := context.WithTimeout(context.WithoutCancel(ctx), h.cfg.ShutdownGrace*3)
		for _, p := range launched {
			p.close(stopCtx)
		}
		stopCancel()

		h.mu.Lock()
		h.closed = true
		h.plugins = make(map[string]*Plugin)
		h.schemes = make(map[string]*Plugin)
		h.taskDefs = make(map[string]taskBinding)
		h.mu.Unlock()

		return errors.Join(problems...)
	}

	h.log.Info("plugins ready", "count", len(launched), "plugins", h.Names())

	return nil
}

// bind records what each plugin advertises, refusing the conflicts that would
// otherwise be resolved by load order.
func (h *Host) bind(launched []*Plugin) []error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Close may have run while these were being launched, in which case it found
	// an empty map and stopped nothing. Publishing them now would hand the
	// engine adapters for plugins nothing will ever stop, so they are reported
	// as a problem instead and Open's cleanup path stops them.
	if h.closed {
		return []error{ErrClosed}
	}

	var problems []error

	for _, p := range launched {
		h.plugins[p.Name()] = p

		for _, scheme := range p.Schemes() {
			if other, taken := h.schemes[scheme]; taken {
				problems = append(problems, fmt.Errorf(
					"%w: %q is claimed by both %q and %q; one of them has to stop claiming it, because whichever loaded last would silently answer every reference for it",
					ErrDuplicateScheme, scheme, other.Name(), p.Name(),
				))
				continue
			}
			h.schemes[scheme] = p
		}

		for _, manifest := range p.Tasks() {
			// The registered name is `<plugin>.<task>`, and the dot is doing three
			// jobs at once. It is provenance — a reviewer reading `slack.post:`
			// knows the step leaves the engine's code for code somebody installed,
			// at the line where that fact matters. It makes shadowing a built-in
			// unrepresentable, because every plugin task carries a dot and no
			// built-in does — a rule this loop used to enforce with a check that
			// once misfired, telling a second host its task collided with "a
			// built-in" the first host had registered. And it makes two plugins
			// wanting the same task name a non-event rather than a race decided by
			// installation order, because the first segment is the name discovery
			// established from the binary, which no manifest gets to choose.
			//
			// What is still checked is the one collision the prefix cannot rule
			// out: one plugin declaring the same task twice.
			name := p.Name() + "." + manifest.GetName()

			if other, taken := h.taskDefs[name]; taken && other.plugin == p {
				problems = append(problems, pluginError(p.Name(), p.Path(), fmt.Errorf(
					"%w: declares task %q twice",
					ErrManifest, truncate(manifest.GetName(), 64),
				)))
				continue
			}

			def, err := p.taskDef(manifest, h.cfg)
			if err != nil {
				problems = append(problems, err)
				continue
			}

			h.taskDefs[name] = taskBinding{plugin: p, def: def}
		}
	}

	return problems
}

// Close terminates every plugin and waits for them to be gone.
//
// It is what stops the processes. Nothing else does, and a Host that is
// garbage-collected without being closed leaves them running — so this belongs
// in a defer beside [NewHost]. The context bounds the wait, not the
// termination: a plugin that has not exited when it expires has already been
// killed and is being waited on, so a Close whose context expires reports that
// it stopped waiting rather than that anything was left running deliberately.
func (h *Host) Close(ctx context.Context) error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	plugins := slices.Collect(maps.Values(h.plugins))
	h.mu.Unlock()

	// Cancelling first tells every supervisor to stop before any of them can
	// react to the exits below by relaunching.
	h.cancel()

	var wg sync.WaitGroup
	for _, p := range plugins {
		wg.Go(func() { p.close(ctx) })
	}
	wg.Wait()

	h.mu.Lock()
	// Adapters already handed to the engine keep working — they report the
	// plugin closed — but nothing new should be handed out from a closed host.
	h.plugins = make(map[string]*Plugin)
	h.schemes = make(map[string]*Plugin)
	h.taskDefs = make(map[string]taskBinding)
	h.mu.Unlock()

	h.log.Info("plugins stopped", "count", len(plugins))

	// Only report a failure if waiting actually gave up. A Close that stopped
	// everything within its deadline succeeded, and returning the context's
	// error because some unrelated deadline passed afterwards would say
	// otherwise.
	if err := ctx.Err(); err != nil {
		for _, p := range plugins {
			select {
			case <-p.supervisorDone:
			default:
				return fmt.Errorf("plugin: stopped waiting for plugins to finish: %w", err)
			}
		}
	}

	return nil
}

// Names returns the names of the running plugins, sorted.
func (h *Host) Names() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return slices.Sorted(maps.Keys(h.plugins))
}

// Plugins returns the running plugins, sorted by name. It is what `flow plugins`
// reports: each one's state, health, and what it advertises.
func (h *Host) Plugins() []*Plugin {
	h.mu.RLock()
	defer h.mu.RUnlock()

	plugins := make([]*Plugin, 0, len(h.plugins))
	for _, name := range slices.Sorted(maps.Keys(h.plugins)) {
		plugins = append(plugins, h.plugins[name])
	}

	return plugins
}

// Lookup returns the plugin of the given name.
func (h *Host) Lookup(name string) (*Plugin, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	p, ok := h.plugins[name]
	return p, ok
}

// SecretProviders returns a [secrets.Provider] for every scheme the running
// plugins resolve, ordered by scheme.
//
// Register them with a [secrets.Registry] and the rest of the engine dispatches
// to a plugin exactly as it does to the built-in environment and file providers,
// because that is the whole of what a secrets backend is.
func (h *Host) SecretProviders() []secrets.Provider {
	h.mu.RLock()
	defer h.mu.RUnlock()

	providers := make([]secrets.Provider, 0, len(h.schemes))
	for _, scheme := range slices.Sorted(maps.Keys(h.schemes)) {
		providers = append(providers, newSecretProvider(h.schemes[scheme], scheme, h.cfg))
	}

	return providers
}

// TaskDefs returns a [flowstatev1.TaskDef] for every task the running plugins
// provide, ordered by name.
//
// They are the same shape a built-in task has, descriptors included, so
// validation, editor completion, and generated documentation read a plugin task
// exactly as they read a built-in one.
func (h *Host) TaskDefs() []flowstatev1.TaskDef {
	h.mu.RLock()
	defer h.mu.RUnlock()

	defs := make([]flowstatev1.TaskDef, 0, len(h.taskDefs))
	for _, name := range slices.Sorted(maps.Keys(h.taskDefs)) {
		defs = append(defs, h.taskDefs[name].def)
	}

	return defs
}

// Catalog describes what the running plugins add to this build.
//
// It is the answer `flow plugins` prints and the shape a worker-introspection
// RPC would return, built here rather than in the CLI because the host is what
// knows: a manifest is what a plugin said about itself, and everything beside it
// — which executable it came from, which of its tasks survived descriptor
// reconstruction — is a fact only the process that launched it has.
func (h *Host) Catalog() *flowstatev1.PluginCatalog {
	h.mu.RLock()
	plugins := make([]*Plugin, 0, len(h.plugins))
	for _, name := range slices.Sorted(maps.Keys(h.plugins)) {
		plugins = append(plugins, h.plugins[name])
	}
	byPlugin := make(map[string][]*flowstatev1.TaskDescription, len(h.plugins))
	for _, name := range slices.Sorted(maps.Keys(h.taskDefs)) {
		binding := h.taskDefs[name]
		owner := binding.plugin.Name()
		byPlugin[owner] = append(byPlugin[owner], flowstatev1.DescribeTask(binding.def))
	}
	h.mu.RUnlock()

	catalog := &flowstatev1.PluginCatalog{
		Plugins:    make([]*flowstatev1.PluginDescription, 0, len(plugins)),
		SearchPath: slices.Clone(h.cfg.SearchPath),
	}

	for _, p := range plugins {
		manifest := p.Manifest()

		catalog.Plugins = append(catalog.Plugins, &flowstatev1.PluginDescription{
			Name:        p.Name(),
			Version:     manifest.GetVersion(),
			Description: manifest.GetDescription(),
			Path:        p.Path(),
			// From the host's own scheme table rather than from the manifest,
			// because the two can differ: a scheme the deployment does not permit
			// is refused at bind time, and reporting what the plugin asked for
			// would tell an operator it resolves something it will not be asked
			// to resolve.
			SecretSchemes: p.Schemes(),
			Tasks:         byPlugin[p.Name()],
		})
	}

	return catalog
}

// Register adds every plugin-provided task to a task registry and every
// plugin-provided secret scheme to a secrets registry.
//
// Either may be nil, for a worker that takes only one of the two. It reports the
// first registration that failed, which for the secrets registry includes a
// scheme a non-plugin provider already claims — a conflict this host cannot see
// on its own, because it knows only about plugins.
//
// # Which registry to hand it
//
// [flowstatev1.DefaultRegistry], in a worker. That is not one choice among two:
// dispatch, input partitioning, scope shipping and validation all reach the
// registry through package-level functions that read the default one, so a host
// registered into a fresh [flowstatev1.NewRegistry] holds tasks nothing will ever
// look up. This paragraph exists because the example above used to show exactly
// that, and a deployment following it would have got a worker that discovered its
// plugins, launched them, health-checked them, and answered `unknown task`.
//
// Registering into the process-global default is a one-way door — there is no
// Unregister — so a process opens one host and keeps it until it exits. That is
// what a worker does; it is not what a long-lived process reopening a host would
// want, and such a process does not exist yet.
func (h *Host) Register(tasks *flowstatev1.Registry, providers *secrets.Registry) error {
	if tasks != nil {
		for _, def := range h.TaskDefs() {
			if err := tasks.Register(def); err != nil {
				return fmt.Errorf("plugin: registering task %q: %w", def.Name, err)
			}
		}
	}

	if providers != nil {
		for _, provider := range h.SecretProviders() {
			if err := providers.Register(provider); err != nil {
				return fmt.Errorf("plugin: registering secret provider: %w", err)
			}
		}
	}

	return nil
}

// CheckHealth polls every plugin and returns the results by plugin name. It is
// what a worker's own health endpoint reports, so that a plugin whose backend is
// unreachable is visible without reading logs.
func (h *Host) CheckHealth(ctx context.Context) map[string]Health {
	plugins := h.Plugins()

	var (
		mu      sync.Mutex
		results = make(map[string]Health, len(plugins))
		wg      sync.WaitGroup
	)

	for _, p := range plugins {
		wg.Add(1)
		go func() {
			defer wg.Done()
			health := p.CheckHealth(ctx)
			mu.Lock()
			results[p.Name()] = health
			mu.Unlock()
		}()
	}
	wg.Wait()

	return results
}
