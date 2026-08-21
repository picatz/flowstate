package plugin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
)

// State is what a plugin is currently doing.
type State int

const (
	// StateStarting means the plugin is being launched or is handshaking.
	StateStarting State = iota

	// StateReady means the plugin has described itself and is serving.
	StateReady

	// StateRestarting means the process exited and a relaunch is pending.
	StateRestarting

	// StateFailed means the plugin will not be relaunched again: it exhausted
	// its restart budget, or it came back describing itself differently. It is
	// reported rather than retried, because a plugin that fails every launch is
	// something an operator has to fix.
	StateFailed

	// StateStopped means the host closed and the plugin was terminated.
	StateStopped
)

// String returns the state's name.
func (s State) String() string {
	switch s {
	case StateStarting:
		return "starting"
	case StateReady:
		return "ready"
	case StateRestarting:
		return "restarting"
	case StateFailed:
		return "failed"
	case StateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// HealthStatus is what the last health poll found.
//
// The distinction between [HealthNotServing] and [HealthUnreachable] is the
// point of polling at all: one says the plugin is working and something it
// depends on is not, which restarting cannot fix and an operator has to hear
// about; the other says the plugin itself has stopped answering, which
// restarting can fix and usually does.
type HealthStatus int

const (
	// HealthUnknown means no poll has completed yet, or polling is disabled.
	HealthUnknown HealthStatus = iota

	// HealthServing means the plugin reported that it can serve.
	HealthServing

	// HealthNotServing means the plugin answered, and answered that it cannot
	// serve. The plugin is not restarted: it is not the broken thing.
	HealthNotServing

	// HealthUnreachable means the plugin did not answer at all.
	HealthUnreachable
)

// String returns the status's name.
func (s HealthStatus) String() string {
	switch s {
	case HealthServing:
		return "serving"
	case HealthNotServing:
		return "not serving"
	case HealthUnreachable:
		return "unreachable"
	default:
		return "unknown"
	}
}

// Health is the result of a health poll.
type Health struct {
	// Status is what the poll found.
	Status HealthStatus

	// Message is the plugin's explanation of a non-serving status, which the
	// schema requires to be free of credential material because it is logged.
	Message string

	// CheckedAt is when the poll completed. Zero means no poll has completed.
	CheckedAt time.Time

	// Err is why the plugin could not be reached, when Status is
	// [HealthUnreachable].
	Err error
}

// stableRun is how long a plugin has to stay up before its restart budget is
// considered spent on a problem that has passed.
//
// Without it, a plugin that runs for a week and then crashes shares a budget
// with one that crashed at startup six days ago, and a long-lived worker
// eventually exhausts the budget of every plugin it has. The budget is meant to
// stop a crash loop, and a plugin that ran this long was not in one.
const stableRun = time.Minute

// Plugin is one plugin, kept running.
//
// It owns the process's whole life: launching it, watching it, relaunching it
// with backoff when it exits, polling its health, and terminating it when the
// host closes. Callers hold a *Plugin and make calls through it without knowing
// which process is currently serving them, because across a restart that
// changes.
//
// A Plugin is safe for concurrent use.
type Plugin struct {
	name string
	path string
	cfg  Config
	log  *slog.Logger

	// procCtx bounds every process this supervises, and cancel ends it.
	//
	// It is derived from the host's context but cancelled independently, so that
	// closing one plugin does not depend on the host having cancelled first.
	// Without that, the order of "stop the process" and "stop supervising it"
	// would matter, and getting it wrong deadlocks: the supervisor waits on a
	// process that has been stopped while the closer waits on a supervisor that
	// has not been told to stop.
	procCtx context.Context
	cancel  context.CancelFunc

	mu       sync.RWMutex
	inst     *instance
	manifest *pluginv1.PluginManifest

	// distribution is the digest of the executable this plugin was launched
	// from, taken from the handle it was launched through rather than read back
	// later from the path.
	//
	// The two are not the same fact. A path is a name, and an atomic replacement
	// rebinds it: a digest read after the fact identifies whatever is at the name
	// now, which is exactly the binary that did not run. What a run is pinned to
	// has to be the bytes that served it, so it is captured beside the launch and
	// retained, and a relaunch that produces a different one is refused (see
	// [ErrDistribution]).
	//
	// Beside the launch is not close enough on its own, either: hashing the path
	// and then executing the path is two opens with a window between them, and an
	// atomic rename is what an in-place upgrade does. The digest is therefore
	// taken from the same open descriptor the process is executed through, which
	// closes the window where the platform permits it — see [execImage] for what
	// each platform can promise.
	distribution string

	state     State
	lastErr   error
	restarts  int
	health    Health
	telemetry telemetry

	supervisorDone chan struct{}
	closeOnce      sync.Once
}

// newPlugin launches a discovered plugin and completes its first handshake and
// Describe. It returns an error rather than a Plugin if either fails: a plugin
// that cannot start is refused at startup rather than kept around in a broken
// state, so a worker's plugin set is what it says it is.
func newPlugin(hostCtx context.Context, cfg Config, found Found) (*Plugin, error) {
	procCtx, cancel := context.WithCancel(hostCtx)

	p := &Plugin{
		name:           found.Name,
		path:           found.Path,
		cfg:            cfg,
		log:            cfg.logger().With("plugin", found.Name),
		procCtx:        procCtx,
		cancel:         cancel,
		state:          StateStarting,
		telemetry:      newTelemetry(cfg),
		supervisorDone: make(chan struct{}),
	}

	inst, manifest, distribution, err := p.start()
	if err != nil {
		cancel()
		close(p.supervisorDone)
		return nil, err
	}

	p.inst = inst
	p.manifest = manifest
	p.distribution = distribution
	p.state = StateReady

	go p.supervise()

	return p, nil
}

// Name returns the plugin's name, taken from its binary rather than from
// anything it said about itself.
func (p *Plugin) Name() string { return p.name }

// Path returns the binary the plugin was launched from.
func (p *Plugin) Path() string { return p.path }

// ProtocolVersion is the negotiated wire protocol of the current instance.
func (p *Plugin) ProtocolVersion() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.inst == nil {
		return 0
	}
	return p.inst.protocolVersion
}

// Manifest returns what the plugin said about itself. The result is a shared
// message and must not be modified; clone it if it needs to be.
func (p *Plugin) Manifest() *pluginv1.PluginManifest {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.manifest
}

// State returns what the plugin is currently doing.
func (p *Plugin) State() State {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

// LastError returns why the plugin last failed, or nil.
func (p *Plugin) LastError() error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.lastErr
}

// Health returns the result of the most recent health poll.
func (p *Plugin) Health() Health {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.health
}

// Restarts returns how many times the plugin has been relaunched.
func (p *Plugin) Restarts() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.restarts
}

// PID returns the process id currently serving, or 0 when none is.
func (p *Plugin) PID() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.inst == nil {
		return 0
	}
	return p.inst.pid
}

// HasCapability reports whether the plugin advertised a capability.
func (p *Plugin) HasCapability(c pluginv1.Capability) bool {
	return slices.Contains(p.Manifest().GetCapabilities(), c)
}

// Schemes returns the secret schemes the plugin resolves, or nothing if it did
// not advertise the secrets capability. A plugin serves what it advertised: a
// scheme listed without the capability is not a scheme this host will dispatch
// to it.
func (p *Plugin) Schemes() []string {
	if !p.HasCapability(pluginv1.Capability_CAPABILITY_SECRETS) {
		return nil
	}
	return slices.Clone(p.Manifest().GetSchemes())
}

// Tasks returns the task manifests the plugin provides, or nothing if it did not
// advertise the tasks capability.
func (p *Plugin) Tasks() []*pluginv1.TaskManifest {
	if !p.HasCapability(pluginv1.Capability_CAPABILITY_TASKS) {
		return nil
	}
	return slices.Clone(p.Manifest().GetTasks())
}

// ready returns the instance currently able to serve.
//
// It fails fast rather than waiting for a restart in progress. A caller is
// inside a step with its own retry policy and its own deadline, and blocking it
// on a relaunch spends that deadline on something the caller can neither see nor
// bound. [ErrUnavailable] is retryable, so a step's policy gets to decide.
func (p *Plugin) ready() (*instance, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	switch {
	case p.state == StateReady && p.inst != nil:
		return p.inst, nil
	case p.state == StateFailed:
		return nil, pluginError(p.name, p.path, fmt.Errorf(
			"%w: gave up after %d restarts: %w", ErrUnavailable, p.restarts, p.lastErr))
	case p.state == StateStopped:
		return nil, pluginError(p.name, p.path, ErrClosed)
	default:
		return nil, pluginError(p.name, p.path, fmt.Errorf("%w: %s", ErrUnavailable, p.state))
	}
}

// callContext bounds one call to a plugin.
//
// The host's own timeout is applied on top of whatever deadline the caller
// already carries, so the shorter of the two wins. That is the intended
// relationship: a step with a five second timeout must not wait thirty for a
// plugin, and a plugin must not be able to hold a request open past the host's
// bound just because the caller passed a context with no deadline at all.
func (p *Plugin) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, p.cfg.CallTimeout)
}

// CheckHealth polls the plugin now, rather than waiting for the next scheduled
// poll, and records the result.
func (p *Plugin) CheckHealth(ctx context.Context) Health {
	ctx, _, finish := p.telemetry.start(ctx, "health", p.name, "")
	inst, err := p.ready()
	if err != nil {
		health := Health{Status: HealthUnreachable, CheckedAt: time.Now(), Err: err}
		p.recordHealth(health)
		p.telemetry.health.Add(ctx, 1, metricschema.WithAttributes(attribute.String(metricschema.PluginName, p.name), attribute.String(metricschema.PluginHealthStatus, health.Status.String())))
		finish(err)
		return health
	}

	ctx, cancel := context.WithTimeout(ctx, p.cfg.HealthTimeout)
	defer cancel()

	resp, err := inst.clients.plugin.Health(ctx, connect.NewRequest(&pluginv1.HealthRequest{}))

	var health Health
	switch {
	case err != nil:
		health = Health{
			Status:    HealthUnreachable,
			CheckedAt: time.Now(),
			Err:       pluginError(p.name, p.path, err),
		}
	case resp.Msg.GetStatus() == pluginv1.HealthResponse_STATUS_SERVING:
		health = Health{Status: HealthServing, CheckedAt: time.Now()}
	default:
		// Anything that is not explicitly serving is treated as not serving,
		// including STATUS_UNSPECIFIED. A plugin that does not say it can serve
		// has not said it can serve.
		health = Health{
			Status:    HealthNotServing,
			CheckedAt: time.Now(),
			Message:   truncate(resp.Msg.GetMessage(), 1024),
		}
	}

	p.recordHealth(health)
	p.telemetry.health.Add(ctx, 1, metricschema.WithAttributes(attribute.String(metricschema.PluginName, p.name), attribute.String(metricschema.PluginHealthStatus, health.Status.String())))
	finish(health.Err)
	return health
}

// recordHealth stores a poll result.
func (p *Plugin) recordHealth(h Health) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.health = h
}

// start launches the process, handshakes, and asks the plugin to describe
// itself. It is the whole of what has to succeed for a plugin to be usable, and
// it is the same on the first launch and on every relaunch.
//
// The executable is opened once, hashed, and executed through that same open
// handle, so the digest returned beside the instance is the digest of the image
// this process is running rather than of whatever answered to its path a moment
// earlier or a moment later. See [execImage].
func (p *Plugin) start() (*instance, *pluginv1.PluginManifest, string, error) {
	ctx, _, finish := p.telemetry.start(p.procCtx, "start", p.name, "")
	var startErr error
	defer func() { finish(startErr) }()

	image, err := openExecImage(p.path, p.log)
	if err != nil {
		startErr = pluginError(p.name, p.path, fmt.Errorf("%w: %w", ErrLaunch, err))
		return nil, nil, "", startErr
	}
	// Held open across the launch and no longer: Start returns after the child's
	// execve has been attempted, so this cannot race the exec it is holding the
	// image open for.
	defer image.close()

	distribution, err := image.digest()
	if err != nil {
		startErr = pluginError(p.name, p.path, fmt.Errorf("%w: %w", ErrLaunch, err))
		return nil, nil, "", startErr
	}

	// The seam the time-of-check-to-time-of-use test replaces the binary
	// through. It is nil in every configuration outside this package's tests —
	// the field is unexported — and it sits exactly where the old window was, so
	// a launch that went back to executing the path would fail that test rather
	// than pass it by timing.
	if p.cfg.beforeExec != nil {
		p.cfg.beforeExec(p.path)
	}

	inst, err := launch(ctx, p.cfg, Found{Name: p.name, Path: p.path}, image)
	if err != nil {
		startErr = err
		return nil, nil, "", err
	}

	manifest, err := p.describe(ctx, inst)
	if err != nil {
		startErr = err
		inst.stop(context.Background(), p.cfg.ShutdownGrace)
		return nil, nil, "", err
	}

	p.log.Info("plugin ready",
		"pid", inst.pid,
		"version", manifest.GetVersion(),
		"protocol", inst.protocolVersion,
		"distribution", distribution,
		"distribution_pinned", image.pinned,
		"capabilities", capabilityNames(manifest.GetCapabilities()),
		"schemes", manifest.GetSchemes(),
		"tasks", taskNames(manifest.GetTasks()),
	)

	return inst, manifest, distribution, nil
}

// DistributionDigest is the digest of the executable currently serving.
//
// Where the host can execute an already-open descriptor — Linux, through
// /proc/self/fd — this is the digest of the image the kernel ran: the file is
// opened once, hashed, and executed through that same handle, so the two cannot
// name different inodes. Where it cannot, it is the digest of the file that was
// at the plugin's path immediately before the launch, which a replacement
// landing between the hash and the exec can still falsify. [openExecImage] logs
// which of the two a given launch got, so the weaker answer is never handed over
// as though it were the stronger one.
func (p *Plugin) DistributionDigest() string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.distribution
}

// describe calls Describe and refuses anything the host cannot accept.
// describe asks the plugin who it is, on the context its caller's span lives
// in, so the mandatory first RPC is a child of the start trace rather than a
// root of its own.
func (p *Plugin) describe(ctx context.Context, inst *instance) (*pluginv1.PluginManifest, error) {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.DescribeTimeout)
	defer cancel()

	resp, err := inst.clients.plugin.Describe(ctx, connect.NewRequest(&pluginv1.DescribeRequest{
		HostVersion: p.cfg.HostVersion,
	}))
	if err != nil {
		return nil, pluginError(p.name, p.path, fmt.Errorf("%w: describing: %w", ErrLaunch, err))
	}

	manifest := resp.Msg.GetManifest()
	if err := p.checkManifest(manifest); err != nil {
		return nil, pluginError(p.name, p.path, err)
	}

	return manifest, nil
}

// checkManifest applies every rule a manifest has to satisfy before the host
// will use anything the plugin offers.
func (p *Plugin) checkManifest(manifest *pluginv1.PluginManifest) error {
	// The schema's own rules first, so that a field this code goes on to read is
	// known to be within its declared bounds.
	if err := flowstatev1.Validate(manifest); err != nil {
		return fmt.Errorf("%w: %w", ErrManifest, err)
	}

	if manifest.GetName() != p.name {
		// Not fatal: the schema calls the match a convention, and the host keys
		// everything by the binary's name regardless, so a plugin cannot claim
		// another's identity by describing itself as it. It is still worth
		// saying, because the mismatch will confuse whoever reads the logs.
		p.log.Warn("plugin manifest name does not match its binary",
			"manifest_name", truncate(manifest.GetName(), 64), "binary_name", p.name)
	}

	// A capability the host does not know is ignored rather than refused, which
	// is what the schema's additive rule requires: an old engine must keep
	// working against a plugin that also serves something newer. Nothing is
	// dispatched for it, so ignoring it is still fail-closed — the host acts
	// only on capabilities it understands.
	var known []pluginv1.Capability
	for _, c := range manifest.GetCapabilities() {
		switch c {
		case pluginv1.Capability_CAPABILITY_SECRETS, pluginv1.Capability_CAPABILITY_TASKS, pluginv1.Capability_CAPABILITY_TASK_PROGRESS:
			if !slices.Contains(known, c) {
				known = append(known, c)
			}
		case pluginv1.Capability_CAPABILITY_UNSPECIFIED:
			// Not a future capability: the zero value where a real one was
			// required, which is a malformed manifest rather than a newer plugin.
			return fmt.Errorf("%w: advertises CAPABILITY_UNSPECIFIED", ErrManifest)
		default:
			p.log.Warn("plugin advertises a capability this host does not know, which it will not use",
				"capability", int32(c))
		}
	}

	if len(known) == 0 {
		return fmt.Errorf(
			"%w: advertises no capability this host can use, so there would be nothing for it to do",
			ErrManifest,
		)
	}

	secrets := slices.Contains(known, pluginv1.Capability_CAPABILITY_SECRETS)
	tasks := slices.Contains(known, pluginv1.Capability_CAPABILITY_TASKS)
	taskProgress := slices.Contains(known, pluginv1.Capability_CAPABILITY_TASK_PROGRESS)

	switch {
	case secrets && len(manifest.GetSchemes()) == 0:
		return fmt.Errorf("%w: advertises CAPABILITY_SECRETS but claims no schemes, so no reference would ever reach it", ErrManifest)
	case tasks && len(manifest.GetTasks()) == 0:
		return fmt.Errorf("%w: advertises CAPABILITY_TASKS but provides no tasks", ErrManifest)
	case !secrets && len(manifest.GetSchemes()) > 0:
		p.log.Warn("plugin lists secret schemes without advertising CAPABILITY_SECRETS; they will not be registered",
			"schemes", manifest.GetSchemes())
	case !tasks && len(manifest.GetTasks()) > 0:
		p.log.Warn("plugin lists tasks without advertising CAPABILITY_TASKS; they will not be registered",
			"tasks", taskNames(manifest.GetTasks()))
	case taskProgress && !tasks:
		// Harmless rather than refused — nothing dispatches ExecuteStream for a
		// plugin that has no tasks to run it against either way — but worth a
		// line, since a manifest built by hand rather than by this package's
		// own [sdk.Plugin.manifest] could set this without meaning to.
		p.log.Warn("plugin advertises CAPABILITY_TASK_PROGRESS without CAPABILITY_TASKS, so it will never be asked")
	}

	if secrets {
		for _, scheme := range manifest.GetSchemes() {
			if !p.cfg.schemePermitted(scheme) {
				return fmt.Errorf(
					"%w: claims scheme %q, which this deployment does not permit (permitted: %s)",
					ErrSchemeNotPermitted, truncate(scheme, 32), strings.Join(p.cfg.PermittedSchemes, ", "),
				)
			}
		}
	}

	if tasks {
		seen := make(map[string]struct{}, len(manifest.GetTasks()))
		for _, task := range manifest.GetTasks() {
			if _, dup := seen[task.GetName()]; dup {
				return fmt.Errorf("%w: provides task %q twice", ErrManifest, truncate(task.GetName(), 64))
			}
			seen[task.GetName()] = struct{}{}
		}
	}

	return nil
}

// supervise watches the running process and decides what happens when it ends.
//
// It is the only goroutine that changes which instance is current, which is what
// makes a restart a swap rather than a sequence of partial states other
// goroutines could observe.
func (p *Plugin) supervise() {
	defer close(p.supervisorDone)

	var healthTick <-chan time.Time
	if !p.cfg.DisableHealthChecks && p.cfg.HealthInterval > 0 {
		ticker := time.NewTicker(p.cfg.HealthInterval)
		defer ticker.Stop()
		healthTick = ticker.C
	}

	var consecutiveUnreachable int
	startedAt := time.Now()

	for {
		if p.procCtx.Err() != nil {
			return
		}

		p.mu.RLock()
		inst, state := p.inst, p.state
		p.mu.RUnlock()

		switch {
		case state == StateFailed || state == StateStopped:
			return
		case inst == nil:
			// No process is running, so one is due. restart applies the budget
			// and the backoff, and reports false when the plugin has been given
			// up on.
			if !p.restart() {
				return
			}
			startedAt = time.Now()
			consecutiveUnreachable = 0
			continue
		}

		select {
		case <-p.procCtx.Done():
			return

		case <-inst.exited:
			consecutiveUnreachable = 0
			p.noteExit(inst, time.Since(startedAt))

		case <-healthTick:
			health := p.CheckHealth(p.procCtx)
			switch health.Status {
			case HealthServing:
				consecutiveUnreachable = 0
			case HealthNotServing:
				consecutiveUnreachable = 0
				// The plugin is working and something it depends on is not.
				// Restarting it would replace a process that is answering
				// correctly with an identical one that will answer the same
				// way, so this is reported and left alone.
				p.log.Warn("plugin reports it cannot serve; its backend is the thing to look at, not the plugin",
					"message", health.Message)
			case HealthUnreachable:
				consecutiveUnreachable++
				p.log.Warn("plugin did not answer a health check",
					"consecutive", consecutiveUnreachable,
					"threshold", p.cfg.HealthFailureThreshold,
					"error", health.Err)

				if consecutiveUnreachable >= p.cfg.HealthFailureThreshold {
					consecutiveUnreachable = 0
					p.log.Warn("plugin has stopped answering; restarting it")
					// Ending the process routes this through the same restart
					// path a crash takes, rather than being a second way to
					// replace an instance.
					inst.stop(p.procCtx, p.cfg.ShutdownGrace)
				}
			}
		}
	}
}

// noteExit records that the process ended and releases what it held, leaving no
// current instance so that the supervisor's next pass relaunches.
func (p *Plugin) noteExit(inst *instance, ranFor time.Duration) {
	exitErr := inst.waitErr
	inst.stop(p.procCtx, p.cfg.ShutdownGrace)

	p.mu.Lock()
	if p.state == StateStopped {
		// close ran while this was waking up. It has already said what the
		// plugin's state is, and it is not restarting.
		p.mu.Unlock()
		return
	}
	p.inst = nil
	p.state = StateRestarting
	if exitErr != nil {
		p.lastErr = pluginError(p.name, p.path, fmt.Errorf("%w: %w", ErrExited, exitErr))
	} else {
		p.lastErr = pluginError(p.name, p.path, ErrExited)
	}

	// A plugin that ran for a while and then died is not in a crash loop, so it
	// does not inherit the budget of one that was.
	if ranFor >= stableRun {
		p.restarts = 0
	}
	p.mu.Unlock()

	p.log.Warn("plugin exited", "ran_for", ranFor.Round(time.Millisecond), "error", exitErr)
}

// restart makes one relaunch attempt, and reports whether supervision should
// continue.
//
// It returns false only when the plugin has been given up on for good: the
// budget is spent, the host is shutting down, or the plugin came back describing
// itself differently. Every other outcome, including a failed launch, leaves the
// supervisor to come back here for the next attempt.
func (p *Plugin) restart() bool {
	p.mu.Lock()
	p.restarts++
	attempt := p.restarts
	p.mu.Unlock()

	budget := p.cfg.MaxRestarts
	if budget < 0 || attempt > budget {
		p.fail(fmt.Errorf(
			"%w: has been relaunched %d times, which is its whole budget; the last failure was: %w",
			ErrUnavailable, attempt-1, p.LastError(),
		))
		return false
	}

	if !p.sleep(backoffFor(attempt, p.cfg.RestartBackoff, p.cfg.MaxRestartBackoff)) {
		return false
	}

	// Counted here, after the budget said yes and the backoff was not cancelled
	// by shutdown, so the metric reports relaunches actually attempted rather
	// than intentions: a plugin with MaxRestarts zero records none.
	//
	// Labelled with the plugin's name for the same reason the calls and health
	// counters are: a restart rate summed over every plugin a deployment runs
	// cannot answer the only question anyone asks of it, which is *which* one
	// is flapping.
	p.telemetry.restarts.Add(p.procCtx, 1, metricschema.WithAttributes(
		attribute.String(metricschema.PluginName, p.name),
	))

	inst, manifest, distribution, err := p.start()
	if err != nil {
		p.log.Warn("plugin relaunch failed", "attempt", attempt, "error", err)

		p.mu.Lock()
		p.lastErr = err
		p.mu.Unlock()

		// Still under budget: the supervisor finds no instance and comes back.
		return true
	}

	// Everything the engine already holds — a registered provider, a registered
	// task — was built from the first manifest. A plugin that comes back
	// claiming something else has changed under the engine, and the honest
	// response is to refuse it rather than to serve two different contracts
	// through one registration.
	if err := manifestUnchanged(p.Manifest(), manifest); err != nil {
		inst.stop(p.procCtx, p.cfg.ShutdownGrace)
		p.fail(fmt.Errorf("%w: %w", ErrManifest, err))
		return false
	}

	// And the bytes, which the manifest does not cover. A binary swapped under a
	// running worker can describe itself identically and behave differently, which
	// is what a build with a change in it looks like from here, so an unchanged
	// manifest is not evidence that this is the same plugin. Runs in flight are
	// pinned to the digest captured at the first launch (see
	// [flowstatev1.ResolvedPlugin]), and serving them from other bytes under that
	// pin would make the contract a decoration. Refused rather than repinned: the
	// operator replaced a plugin under a live worker, and the honest response is
	// to stop serving it until the worker is restarted deliberately.
	if was := p.DistributionDigest(); was != distribution {
		inst.stop(p.procCtx, p.cfg.ShutdownGrace)
		p.fail(fmt.Errorf("%w: launched from %s and is now %s, though its manifest is unchanged",
			ErrDistribution, was, distribution))
		return false
	}

	p.mu.Lock()
	if p.state == StateStopped {
		// close ran while this relaunch was in flight. It found no instance to
		// stop, because there was none yet, so this one has to be stopped here
		// or it is a process nothing owns — with a socket directory, three
		// pipes, and an open connection behind it.
		p.mu.Unlock()

		// Not procCtx: it is already cancelled, so waiting on it would skip the
		// grace period and go straight to killing.
		inst.stop(context.Background(), p.cfg.ShutdownGrace)
		return false
	}
	p.inst = inst
	p.manifest = manifest
	p.state = StateReady
	p.mu.Unlock()

	p.log.Info("plugin relaunched", "attempt", attempt, "pid", inst.pid)

	return true
}

// fail marks the plugin as one that will not be relaunched.
func (p *Plugin) fail(err error) {
	wrapped := pluginError(p.name, p.path, err)

	p.mu.Lock()
	if p.state == StateStopped {
		// close has already said what this plugin's state is, and "stopped" is a
		// truer answer for a caller than "gave up after N restarts".
		p.mu.Unlock()
		return
	}
	p.inst = nil
	p.state = StateFailed
	p.lastErr = wrapped
	p.mu.Unlock()

	p.log.Error("plugin will not be relaunched; an operator has to look at it", "error", wrapped)
}

// sleep waits for d, reporting false if the host shut down first.
func (p *Plugin) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-p.procCtx.Done():
		return false
	}
}

// close terminates the plugin and waits for supervision to finish.
func (p *Plugin) close(ctx context.Context) {
	p.closeOnce.Do(func() {
		// First, so that nothing relaunches the process that is about to be
		// stopped, and so that a supervisor blocked anywhere — waiting out a
		// backoff, mid-launch, mid-health-check — is already on its way out
		// before anything waits for it.
		p.cancel()

		p.mu.Lock()
		inst := p.inst
		p.inst = nil
		p.state = StateStopped
		p.mu.Unlock()

		if inst != nil {
			inst.stop(ctx, p.cfg.ShutdownGrace)
		}

		// Waiting for the supervisor is what makes this mean the plugin is gone
		// rather than going.
		select {
		case <-p.supervisorDone:
		case <-ctx.Done():
			p.log.Warn("gave up waiting for plugin supervision to finish", "error", ctx.Err())
		}
	})
}

// backoffFor returns the delay before relaunch attempt n, doubling from base and
// capped at max, with jitter.
//
// The jitter is not decoration. Several plugins whose shared backend went away
// crash at the same moment and would otherwise relaunch in lockstep, hitting
// that backend together on every attempt.
func backoffFor(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if base <= 0 {
		return 0
	}

	delay := base
	for range attempt - 1 {
		// Tested before doubling rather than after, so a large base cannot
		// overflow into a negative duration — which a timer treats as elapsed,
		// turning the backoff into no backoff at all.
		if delay >= max/2 {
			delay = max
			break
		}
		delay *= 2
	}
	if delay > max {
		delay = max
	}

	// Equal jitter: half the interval, plus a random amount up to the other
	// half, so the result stays within [delay/2, delay] and never exceeds the
	// configured cap. Full jitter would spread a group of restarts more evenly
	// but permits a delay of nearly zero, and for one plugin in a crash loop
	// that is the thing the delay exists to prevent.
	if half := int64(delay / 2); half > 0 {
		delay = delay/2 + time.Duration(rand.Int64N(half+1))
	}

	return delay
}

// manifestUnchanged reports whether a relaunched plugin still offers exactly
// what it offered before, naming the first difference it finds.
func manifestUnchanged(before, after *pluginv1.PluginManifest) error {
	if before == nil || after == nil {
		return nil
	}

	if !sameSet(before.GetCapabilities(), after.GetCapabilities()) {
		return fmt.Errorf(
			"came back advertising %v rather than %v",
			capabilityNames(after.GetCapabilities()), capabilityNames(before.GetCapabilities()),
		)
	}

	if !sameSet(before.GetSchemes(), after.GetSchemes()) {
		return fmt.Errorf(
			"came back claiming schemes %v rather than %v",
			after.GetSchemes(), before.GetSchemes(),
		)
	}

	beforeTasks, afterTasks := before.GetTasks(), after.GetTasks()
	if len(beforeTasks) != len(afterTasks) {
		return fmt.Errorf(
			"came back providing tasks %v rather than %v",
			taskNames(afterTasks), taskNames(beforeTasks),
		)
	}

	byName := make(map[string]*pluginv1.TaskManifest, len(beforeTasks))
	for _, task := range beforeTasks {
		byName[task.GetName()] = task
	}

	for _, task := range afterTasks {
		previous, ok := byName[task.GetName()]
		if !ok {
			return fmt.Errorf("came back providing task %q, which it did not provide before", truncate(task.GetName(), 64))
		}
		if !proto.Equal(previous, task) {
			// The engine validates workflows against the descriptors from the
			// first manifest, so a task whose schema changed would be validated
			// against one shape and executed against another.
			return fmt.Errorf("came back defining task %q differently", truncate(task.GetName(), 64))
		}
	}

	return nil
}

// sameSet reports whether two slices hold the same elements, ignoring order and
// repetition.
func sameSet[T comparable](a, b []T) bool {
	inA := make(map[T]struct{}, len(a))
	for _, v := range a {
		inA[v] = struct{}{}
	}

	inB := make(map[T]struct{}, len(b))
	for _, v := range b {
		if _, ok := inA[v]; !ok {
			return false
		}
		inB[v] = struct{}{}
	}

	// Compared as sets on both sides, so that a repeated element on one side
	// alone does not read as a difference — and, more importantly, so that an
	// element present only in a is caught. Deleting from one map as b is walked
	// gets the second case wrong whenever b repeats something.
	return len(inA) == len(inB)
}

// capabilityNames renders capabilities for a log line or an error.
func capabilityNames(caps []pluginv1.Capability) []string {
	names := make([]string, 0, len(caps))
	for _, c := range caps {
		names = append(names, c.String())
	}
	slices.Sort(names)
	return names
}

// taskNames renders task names for a log line or an error.
func taskNames(tasks []*pluginv1.TaskManifest) []string {
	names := make([]string, 0, len(tasks))
	for _, t := range tasks {
		names = append(names, truncate(t.GetName(), 64))
	}
	slices.Sort(names)
	return names
}

// connectError reports whether err came back from a plugin as a Connect error,
// and with which code.
func connectError(err error) (connect.Code, bool) {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return connectErr.Code(), true
	}
	return 0, false
}
