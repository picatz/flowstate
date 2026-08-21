package plugin

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/internal/protocol"
)

// Defaults applied to a zero-valued [Config] field. They are exported so that a
// deployment tuning one of them can see what it is departing from, and so that
// documentation of the bounds has one place to be right.
const (
	// DefaultHandshakeTimeout bounds the wait for a launched process to
	// announce itself. A plugin that has not printed its line by then is killed
	// rather than waited on: the common cause is a binary that is not a plugin
	// at all, and waiting longer does not make it one.
	DefaultHandshakeTimeout = 10 * time.Second

	// DefaultDescribeTimeout bounds the Describe call made immediately after the
	// handshake.
	DefaultDescribeTimeout = 10 * time.Second

	// DefaultCallTimeout bounds a secret resolution or a task execution. It is
	// the outer bound on a plugin's own work, applied on top of whatever
	// deadline the caller already carries — the shorter of the two wins.
	DefaultCallTimeout = 30 * time.Second

	// DefaultHealthTimeout bounds one health poll. It is short because a health
	// check that needs a long time to answer has already answered.
	DefaultHealthTimeout = 5 * time.Second

	// DefaultHealthInterval is how often a plugin is polled. Zero in a [Config]
	// selects this; [Config.DisableHealthChecks] turns polling off.
	DefaultHealthInterval = 30 * time.Second

	// DefaultHealthFailureThreshold is how many consecutive health polls must
	// fail at the RPC level before the plugin is considered broken and
	// restarted. More than one, so that a single blip does not restart a working
	// plugin.
	//
	// This counts transport failures only. A plugin answering NOT_SERVING is
	// working and saying its backend is not, which restarting cannot fix, so it
	// is reported to the operator instead.
	DefaultHealthFailureThreshold = 3

	// DefaultShutdownGrace is how long a plugin gets between the signal asking
	// it to stop and the one that does not ask.
	DefaultShutdownGrace = 5 * time.Second

	// DefaultMaxResponseBytes bounds a plugin's response before it is read into
	// memory, via connect.WithReadMaxBytes. A plugin is not trusted because an
	// operator installed it, and a response is the one thing it fully controls
	// the size of.
	//
	// Deliberately larger than flowstatev1.MaxTaskOutputBytes, the bound on
	// what a task's *outputs* may weigh (#787). This cap is about memory — how
	// much of a response the host will read at all — and the output cap is
	// about storage — what Temporal will hold as the activity's result. The
	// gap between them exists for a plugin that reads a large response and
	// reduces it before returning, the same pattern as the http task's
	// `outputs:` selection: reading 4 MiB and emitting 10 KiB of outputs is
	// legitimate, while returning the 4 MiB whole is refused at
	// Task.EvalInScope with a diagnosis naming both numbers.
	DefaultMaxResponseBytes = 4 << 20 // 4 MiB

	// DefaultMaxProgressFrames bounds how many TaskProgress frames one
	// ExecuteStream call relays to the caller's progress reporter before the
	// rest are dropped rather than forwarded.
	//
	// Issue #804: MaxResponseBytes bounds the *aggregate* ExecuteStream
	// response — every TaskProgress frame and the terminal ExecuteResponse all
	// share one byte budget, enforced once over the whole HTTP response body
	// (see boundedTransport in transport.go). A task that reports progress
	// frequently, especially inside a loop, could exhaust that shared budget
	// on its own progress before its terminal response ever arrived, failing
	// an otherwise-successful task on the strength of its own reporting
	// frequency rather than anything wrong with its result.
	//
	// The fix is a separate, additive allowance reserved for progress frames
	// — see maxProgressFrameWireBytes in transport.go — so the terminal
	// response keeps its own full share of MaxResponseBytes regardless of how
	// much a task reports. That reserve is sized by a frame *count* rather
	// than by an arbitrary byte figure because TaskPhase is a closed
	// vocabulary (progress.go): every legitimate frame's wire size is fixed
	// and small, which makes a count of frames — not a count of bytes — the
	// resource actually worth bounding here. This constant is that count.
	// Once one ExecuteStream call has relayed this many, further progress
	// frames in the same call are dropped rather than forwarded or treated as
	// a failure: a task's own reporting frequency must never be able to fail
	// the call, only crowd out how much of its own progress the caller gets
	// to see. The overall aggregate bound plugin output is still capped by
	// MaxResponseBytes plus that same fixed reserve, so a plugin streaming
	// unbounded garbage — progress-shaped or not — is still refused.
	DefaultMaxProgressFrames = 4096

	// DefaultMaxRestarts is how many times a plugin that exits on its own is
	// relaunched before it is given up on. A plugin that crashes on every launch
	// must stop being relaunched and be reported, or a broken binary becomes an
	// infinite fork loop.
	DefaultMaxRestarts = 5

	// DefaultRestartBackoff is the delay before the first relaunch, doubled on
	// each subsequent one up to DefaultMaxRestartBackoff.
	DefaultRestartBackoff = 250 * time.Millisecond

	// DefaultMaxRestartBackoff caps the relaunch delay.
	DefaultMaxRestartBackoff = 30 * time.Second

	// DefaultMaxDescriptorBytes bounds one serialized descriptor in a task
	// manifest. Descriptors are attacker-chosen input that the host parses and
	// links, so their size is bounded before any of that happens.
	DefaultMaxDescriptorBytes = 1 << 20 // 1 MiB

	// DefaultMaxDescriptorFiles bounds how many files one descriptor may carry
	// when it is a FileDescriptorSet. Depth bounds do not stop breadth
	// explosions, so this bounds breadth and [maxDescriptorDepth] bounds depth.
	DefaultMaxDescriptorFiles = 256

	// DefaultMaxStderrLine bounds one line of a plugin's stderr. Logs are
	// streamed a line at a time, so this is the whole memory bound on a plugin
	// that writes without ever printing a newline.
	DefaultMaxStderrLine = 64 << 10 // 64 KiB

	// DefaultMaxStderrLinesPerMinute bounds how many stderr lines per minute the
	// host will relay into its own log stream. It bounds volume, not size —
	// MaxStderrLine already bounds one line, and the two are independent
	// resources: a plugin can flood by rate without ever writing an overlong
	// line. Generous enough that no honest plugin's normal logging, including a
	// noisy startup, ever reaches it.
	DefaultMaxStderrLinesPerMinute = 2000

	// stderrRateWindow is the interval [Config.MaxStderrLinesPerMinute] is
	// measured over. Fixed rather than configurable: the field name promises a
	// per-minute budget, and a window a deployment could change out from under
	// that name would make the field lie about its own units.
	stderrRateWindow = time.Minute
)

// Config describes which plugins a deployment will run and how far it will let
// them go.
//
// The zero Config runs nothing: [Config.SearchPath] is empty, and a deployment
// that configures no plugin directories gets no plugins. That is the correct
// configuration for one that should not run third-party code, and it is the
// default so that it takes a decision to change.
type Config struct {
	// TracerProvider and MeterProvider receive host-side plugin telemetry. Nil
	// uses OpenTelemetry's global provider, which is a no-op unless the host has
	// configured a backend.
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	// SearchPath is the list of directories to discover plugins in, in
	// precedence order: a plugin found in an earlier directory shadows one of
	// the same name in a later one.
	//
	// It is explicit configuration rather than $PATH because a plugin is
	// arbitrary code this process executes, and $PATH is a list of directories a
	// shell assembled for other reasons. Entries must be absolute — a relative
	// one resolves against a working directory that nothing here controls.
	SearchPath []string

	// AllowInsecureSearchPath permits a search path directory that other users
	// can write to.
	//
	// What that means is that any user who can write to that directory chooses
	// what code this worker runs, with the worker's credentials and network
	// reach. There is a legitimate use — a container image where the whole
	// filesystem is 0777 and the only user is root — and no other one. Setting
	// it because discovery refused a directory is the wrong fix; fixing the
	// directory's mode is the right one.
	AllowInsecureSearchPath bool

	// Only, when non-empty, restricts launching to plugins with these names. It
	// is how a deployment pins exactly which plugins run without curating the
	// directory, and how a test launches one of several.
	Only []string

	// PermittedSchemes, when non-empty, lists the secret schemes plugins may
	// claim. A plugin claiming anything else is refused rather than partially
	// accepted, since a plugin whose reason for existing was refused is a live
	// process serving nothing.
	//
	// Empty means any scheme is permitted, which is the right default for a
	// deployment that curates its plugin directory and the wrong one for a
	// deployment that does not.
	PermittedSchemes []string

	// SocketDir is where the per-launch socket directories are created. Empty
	// uses the operating system's temporary directory.
	//
	// It exists because a Unix socket path has a hard length limit — 104 bytes
	// on Darwin, 108 on Linux — and the default temporary directory on macOS is
	// long enough that this is a real failure rather than a theoretical one.
	// Pointing this at something short, such as /run/flowstate, is the fix.
	SocketDir string

	// HostVersion is what the host tells a plugin it is, in DescribeRequest,
	// so a plugin can refuse an engine it knows it cannot serve. It is
	// informational: compatibility is decided by the protocol version.
	HostVersion string

	// Env is extra environment for plugin processes, as "KEY=VALUE" entries.
	//
	// A plugin is launched with a deliberately minimal environment rather than a
	// copy of the worker's, because the worker's environment is where its own
	// credentials live and a plugin should receive only what it was configured
	// with. Anything a plugin needs — a vault address, a region, a path to a
	// credential file — belongs here, named by the operator.
	Env []string

	// Logger receives host events and plugin stderr. Nil discards them, which
	// silences the one diagnostic channel a plugin has; prefer passing one.
	Logger *slog.Logger

	// Timeouts and bounds. Zero selects the corresponding Default constant, so
	// that a partially-filled Config is bounded everywhere rather than unbounded
	// wherever it was not filled in.
	HandshakeTimeout time.Duration
	DescribeTimeout  time.Duration
	CallTimeout      time.Duration
	HealthTimeout    time.Duration
	HealthInterval   time.Duration
	ShutdownGrace    time.Duration

	// DisableHealthChecks turns off health polling. Zero HealthInterval means
	// the default rather than off, so that turning polling off is something a
	// deployment says rather than something it forgets to set.
	DisableHealthChecks bool

	// HealthFailureThreshold is how many consecutive failed health polls mark a
	// plugin broken. Zero selects [DefaultHealthFailureThreshold].
	HealthFailureThreshold int

	// MaxResponseBytes bounds a plugin response. Zero selects
	// [DefaultMaxResponseBytes].
	MaxResponseBytes int

	// MaxProgressFrames bounds how many TaskProgress frames one ExecuteStream
	// call relays to the caller's progress reporter before later ones in the
	// same call are dropped. Zero selects [DefaultMaxProgressFrames].
	MaxProgressFrames int

	// MaxRestarts caps relaunches of a plugin that exits on its own. Zero
	// selects [DefaultMaxRestarts]; a negative value disables restarting, so a
	// plugin that exits stays exited.
	MaxRestarts int

	// RestartBackoff and MaxRestartBackoff bound the relaunch delay. Zero
	// selects the corresponding defaults.
	RestartBackoff    time.Duration
	MaxRestartBackoff time.Duration

	// MaxDescriptorBytes and MaxDescriptorFiles bound the serialized descriptors
	// in a task manifest. Zero selects the corresponding defaults.
	MaxDescriptorBytes int
	MaxDescriptorFiles int

	// MaxStderrLine bounds one captured stderr line. Zero selects
	// [DefaultMaxStderrLine].
	MaxStderrLine int

	// MaxStderrLinesPerMinute bounds how many stderr lines per minute are
	// relayed into the host's log. Zero selects
	// [DefaultMaxStderrLinesPerMinute]; a negative value disables the bound,
	// for a deployment that has decided some other layer owns log volume.
	//
	// Lines suppressed past the budget are still drained from the pipe — the
	// plugin never blocks on a full one — and counted, and the count is
	// reported in one summary line logged the moment the next window opens, so
	// a flood is visible at a rate the host chooses rather than the plugin's.
	MaxStderrLinesPerMinute int

	// stderrClock is the time source a plugin's stderr rate limiter measures
	// its window against. Nil selects [time.Now]; a test seam so a rate
	// bounded in wall-clock minutes can be verified without waiting one. See
	// beforeExec below for why a seam that a deployment must not reach is
	// unexported rather than a field.
	stderrClock func() time.Time

	// beforeExec runs after a plugin's executable has been opened and hashed and
	// before it is executed, with the plugin's path.
	//
	// It is the seam the time-of-check-to-time-of-use test replaces a binary
	// through, and it is unexported so that no deployment can reach it: what it
	// exists to make deterministic is a window that a test would otherwise have
	// to hope for, and a sleep-shaped race test proves nothing on a loaded
	// machine. Nothing outside this package sets it, so it is nil everywhere a
	// plugin actually runs.
	beforeExec func(path string)
}

// withDefaults returns a copy with every zero bound replaced by its default, so
// that nothing downstream has to remember which fields were set.
func (c Config) withDefaults() Config {
	setDuration(&c.HandshakeTimeout, DefaultHandshakeTimeout)
	setDuration(&c.DescribeTimeout, DefaultDescribeTimeout)
	setDuration(&c.CallTimeout, DefaultCallTimeout)
	setDuration(&c.HealthTimeout, DefaultHealthTimeout)
	setDuration(&c.HealthInterval, DefaultHealthInterval)
	setDuration(&c.ShutdownGrace, DefaultShutdownGrace)
	setDuration(&c.RestartBackoff, DefaultRestartBackoff)
	setDuration(&c.MaxRestartBackoff, DefaultMaxRestartBackoff)

	setInt(&c.HealthFailureThreshold, DefaultHealthFailureThreshold)
	setInt(&c.MaxResponseBytes, DefaultMaxResponseBytes)
	setInt(&c.MaxProgressFrames, DefaultMaxProgressFrames)
	setInt(&c.MaxRestarts, DefaultMaxRestarts)
	setInt(&c.MaxDescriptorBytes, DefaultMaxDescriptorBytes)
	setInt(&c.MaxDescriptorFiles, DefaultMaxDescriptorFiles)
	setInt(&c.MaxStderrLine, DefaultMaxStderrLine)
	setInt(&c.MaxStderrLinesPerMinute, DefaultMaxStderrLinesPerMinute)

	if c.MaxRestartBackoff < c.RestartBackoff {
		c.MaxRestartBackoff = c.RestartBackoff
	}

	c.SearchPath = slices.Clone(c.SearchPath)
	c.Only = slices.Clone(c.Only)
	c.PermittedSchemes = slices.Clone(c.PermittedSchemes)
	c.Env = slices.Clone(c.Env)

	return c
}

// setDuration replaces a zero duration with a default. A negative value is left
// alone, since some fields give it a meaning of its own.
func setDuration(field *time.Duration, def time.Duration) {
	if *field == 0 {
		*field = def
	}
}

// setInt replaces a zero int with a default, leaving a negative value alone.
func setInt(field *int, def int) {
	if *field == 0 {
		*field = def
	}
}

// validate reports whether the configuration can be used at all. It runs when a
// host is constructed, so that a deployment with a malformed configuration fails
// at startup rather than when the first workflow needs a plugin.
func (c Config) validate() error {
	for _, dir := range c.SearchPath {
		if !filepath.IsAbs(dir) {
			return fmt.Errorf(
				"%w: %q is relative; plugin directories must be absolute, since a relative one resolves against a working directory this process does not control",
				ErrSearchPath, dir,
			)
		}
	}

	if c.SocketDir != "" && !filepath.IsAbs(c.SocketDir) {
		return fmt.Errorf("%w: socket directory %q is relative", ErrSearchPath, c.SocketDir)
	}

	for _, scheme := range c.PermittedSchemes {
		if scheme == "" {
			return fmt.Errorf("plugin: PermittedSchemes contains an empty scheme")
		}
	}

	for _, entry := range c.Env {
		if !isEnvEntry(entry) {
			return fmt.Errorf("plugin: Env entry %q is not of the form KEY=VALUE", truncate(entry, 64))
		}
	}

	return nil
}

// logger returns the configured logger, or one that discards.
func (c Config) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.New(slog.DiscardHandler)
}

// schemePermitted reports whether a plugin may claim a scheme.
func (c Config) schemePermitted(scheme string) bool {
	if len(c.PermittedSchemes) == 0 {
		return true
	}
	return slices.Contains(c.PermittedSchemes, scheme)
}

// wanted reports whether a discovered plugin name should be launched.
func (c Config) wanted(name string) bool {
	if len(c.Only) == 0 {
		return true
	}
	return slices.Contains(c.Only, name)
}

// protocolVersions returns the protocol versions this host offers a plugin.
func (c Config) protocolVersions() []int {
	return protocol.HostVersions()
}

// isEnvEntry reports whether s is a KEY=VALUE entry with a non-empty key.
func isEnvEntry(s string) bool {
	for i := range len(s) {
		if s[i] == '=' {
			return i > 0
		}
	}
	return false
}

// truncate bounds text bound for an error message or a log line.
//
// It cuts on a rune boundary. Everything this bounds was chosen by another
// process, so cutting mid-rune is not hypothetical, and a broken rune in a log
// line is a log line some consumer will refuse to parse.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "..."
}
