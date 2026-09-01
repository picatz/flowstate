package plugin

import (
	"bufio"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/picatz/flowstate/pkg/flowstate/v1/metricschema"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/internal/protocol"
)

// hostFD and tokenFD are the descriptors a plugin inherits the host-liveness
// pipe and the per-launch token on.
//
// Descriptors 0, 1 and 2 are stdin, stdout and stderr, and os/exec assigns
// Cmd.ExtraFiles from 3 upwards, so these follow the order of the ExtraFiles
// slice built in launch.
const (
	hostFD  = 3
	tokenFD = 4
)

// instance is one running plugin process: handshaked, connected, and alive until
// something stops it.
//
// It holds no state that survives the process. Everything about a plugin that
// outlives one process — its name, its manifest, its restart budget — belongs to
// the [Plugin] that supervises it, so that a restart replaces this wholesale
// rather than mutating it.
type instance struct {
	name string
	path string

	cmd  *exec.Cmd
	pid  int
	proc *os.Process

	socketDir  string
	socketPath string

	protocolVersion int

	clients *clients

	// hostPipe is the write end of a pipe the plugin holds the read end of. The
	// operating system closes it when this process dies however it dies, which
	// is what tells a plugin its host is gone even when the host got no chance
	// to say so.
	hostPipe *os.File

	// stdout and stderr are the read ends of the plugin's output. They are held
	// so that closing them can unblock the pumps reading them.
	stdout *os.File
	stderr *os.File

	// exited is closed once the process has been reaped, after which waitErr
	// holds why it ended.
	exited  chan struct{}
	waitErr error

	// pumps completes when the stdout and stderr readers have finished.
	pumps sync.WaitGroup

	stopOnce sync.Once
}

// launch starts a plugin binary and completes the handshake with it.
//
// image is the open handle on the executable, already hashed by the caller. What
// is executed is that handle where the platform allows naming one, and the path
// otherwise; either way the plugin's own argv[0] stays the path an operator
// installed it at, because that is the name everything else here reports. A nil
// image executes found.Path, which is what a caller with no digest to protect
// wants.
//
// procCtx bounds the process's whole life: cancelling it terminates the plugin.
// It is deliberately not the context of whatever call triggered the launch — a
// plugin must outlive the request that first needed it.
//
// Everything this function creates is cleaned up on any failure: the process is
// killed, the socket directory removed, and the pipes closed. A launch either
// yields a usable instance or leaves nothing behind, because the failure paths
// here are the ones that leak child processes.
func launch(procCtx context.Context, cfg Config, found Found, image *execImage) (inst *instance, err error) {
	tel := newTelemetry(cfg)
	procCtx, _, finish := tel.start(procCtx, "launch", found.Name, "")
	defer func() {
		finish(err)
		if err == nil {
			return
		}
		// Named, like every other instrument in this package. A deployment runs
		// several plugins, so "something failed to launch" with no plugin name
		// is a number an operator cannot act on: it says a launch failed and
		// refuses to say whose. The name is [metricschema.ClassConfiguration]
		// — bounded by which plugins the deployment installs — and is already
		// the label on the calls counter and the health counter beside this
		// one, so this is the schema's existing key rather than a new one.
		named := metricschema.WithAttributes(attribute.String(metricschema.PluginName, found.Name))
		tel.launchFailures.Add(procCtx, 1, named)
		if errors.Is(err, ErrHandshake) || errors.Is(err, ErrHandshakeTimeout) {
			tel.protocolErrors.Add(procCtx, 1, named)
		}
	}()
	log := cfg.logger().With("plugin", found.Name)

	// Admission comes first, before a socket directory exists and before any
	// process does. A pin the deployment declared decides whether these bytes
	// may run at all, so it is answered with nothing of the plugin's own in
	// evidence — it never gets to speak, let alone hand shake. See [admit].
	if err := admit(cfg, found.Name, found.Path, image); err != nil {
		return nil, err
	}

	socketDir, socketPath, err := makeSocketDir(cfg.SocketDir)
	if err != nil {
		return nil, pluginError(found.Name, found.Path, fmt.Errorf("%w: %w", ErrLaunch, err))
	}

	// Anything that fails from here on has to undo what has been done so far,
	// including a process that may already be running.
	defer func() {
		if err != nil && inst != nil {
			inst.stop(context.Background(), cfg.ShutdownGrace)
			return
		}
		if err != nil {
			os.RemoveAll(socketDir)
		}
	}()

	hostPipeR, hostPipeW, err := os.Pipe()
	if err != nil {
		return nil, pluginError(found.Name, found.Path, fmt.Errorf("%w: host pipe: %w", ErrLaunch, err))
	}
	defer hostPipeR.Close()

	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		hostPipeW.Close()
		return nil, pluginError(found.Name, found.Path, fmt.Errorf("%w: stdout pipe: %w", ErrLaunch, err))
	}
	defer stdoutW.Close()

	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		hostPipeW.Close()
		stdoutR.Close()
		return nil, pluginError(found.Name, found.Path, fmt.Errorf("%w: stderr pipe: %w", ErrLaunch, err))
	}
	defer stderrW.Close()

	// Open stdin ourselves so every descriptor os/exec will rebuild in the
	// child exists before a pinned image is moved above that scratch range. A
	// nil Stdin makes os/exec open /dev/null later, after the bound is chosen.
	stdin, err := os.Open(os.DevNull)
	if err != nil {
		hostPipeW.Close()
		stdoutR.Close()
		stderrR.Close()
		return nil, pluginError(found.Name, found.Path, fmt.Errorf("%w: stdin: %w", ErrLaunch, err))
	}
	defer stdin.Close()

	// The token exists for this launch only. Nothing persists it, and a restart
	// mints a new one, so a token that leaked stops being useful the moment the
	// process it belonged to ends.
	token := rand.Text()

	// It is handed over on its own inherited descriptor rather than in the
	// environment, because an environment variable cannot be withdrawn. On Linux
	// /proc/<pid>/environ shows the block the kernel copied at execve(2), so a
	// token placed there stays readable for as long as the plugin runs, however
	// promptly the plugin unsets it, and it is swept up by anything that
	// collects environments — a diagnostic bundle, a core dump. A pipe leaves
	// nothing behind: the token sits in kernel buffer space until the plugin
	// reads it, and after that it exists only in the plugin's memory.
	tokenPipeR, err := tokenPipe(token)
	if err != nil {
		hostPipeW.Close()
		stdoutR.Close()
		stderrR.Close()
		return nil, pluginError(found.Name, found.Path, fmt.Errorf("%w: %w", ErrLaunch, err))
	}
	defer tokenPipeR.Close()

	// An explicit argv with exactly one element: the executable. No shell, so
	// nothing in the path or the environment is interpreted, and no arguments,
	// so there is nothing for a plugin to be configured with that an operator
	// did not put in Config.Env.
	//
	// What is executed and what argv[0] says are deliberately allowed to differ:
	// the first is the already-hashed descriptor wherever the platform can name
	// one, which is the only way to be sure the digest describes this process,
	// and the second is the installed path, which is what an operator, the
	// catalog and every log line here call this plugin. Both contain a slash, so
	// neither is looked up on $PATH.
	execPath := found.Path
	if image != nil {
		execPath = image.execPath
	}

	cmd := exec.CommandContext(procCtx, execPath)
	cmd.Args = []string{found.Path}
	cmd.Dir = socketDir
	cmd.Env = pluginEnv(cfg, socketPath)
	cmd.Stdin = stdin
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW
	cmd.ExtraFiles = []*os.File{hostPipeR, tokenPipeR}

	isolateProcessGroup(cmd)

	// Cancelling procCtx asks the whole process group to stop, and WaitDelay is
	// how long "asks" lasts before os/exec stops asking.
	cmd.Cancel = func() error { return terminateProcess(cmd.Process, false) }
	cmd.WaitDelay = cfg.ShutdownGrace

	if image != nil && image.pinned {
		if err := image.prepareForExec([]*os.File{stdin, stdoutW, stderrW, hostPipeR, tokenPipeR}); err != nil {
			hostPipeW.Close()
			stdoutR.Close()
			stderrR.Close()
			return nil, pluginError(found.Name, found.Path, fmt.Errorf("%w: preparing pinned image: %w", ErrLaunch, err))
		}
		cmd.Path = image.execPath
	}

	if err := cmd.Start(); err != nil {
		hostPipeW.Close()
		stdoutR.Close()
		stderrR.Close()
		return nil, pluginError(found.Name, found.Path, fmt.Errorf("%w: %w", ErrLaunch, err))
	}

	inst = &instance{
		name:       found.Name,
		path:       found.Path,
		cmd:        cmd,
		pid:        cmd.Process.Pid,
		proc:       cmd.Process,
		socketDir:  socketDir,
		socketPath: socketPath,
		hostPipe:   hostPipeW,
		stdout:     stdoutR,
		stderr:     stderrR,
		exited:     make(chan struct{}),
	}

	log = log.With("pid", inst.pid)
	log.Debug("plugin launched", "path", found.Path, "socket", socketPath)

	// The child holds its own copies of the write ends and of the inherited read
	// ends; ours must go, or nothing here ever sees EOF.
	hostPipeR.Close()
	tokenPipeR.Close()
	stdoutW.Close()
	stderrW.Close()

	go func() {
		inst.waitErr = cmd.Wait()
		close(inst.exited)
	}()

	// stderr is a plugin's only diagnostic channel, so it is captured from
	// before the handshake: the most valuable thing a plugin ever writes there
	// is the reason it is about to fail to hand shake.
	//
	// The pump drains every line regardless of the limiter below: a full pipe
	// looks like a hung plugin, and only what gets *relayed* into the host's
	// own log is bounded.
	inst.pumps.Add(1)
	go func() {
		defer inst.pumps.Done()
		relay, flush := stderrRelayFunc(cfg, log)
		pumpPluginLog(stderrR, cfg.MaxStderrLine, relay)
		if summary := flush(); summary != "" {
			log.Warn(summary)
		}
	}()

	handshake, err := inst.handshake(cfg, stdoutR, log)
	if err != nil {
		return inst, pluginError(found.Name, found.Path, err)
	}

	inst.protocolVersion = handshake.ProtocolVersion
	inst.clients = newClients(socketPath, token, cfg.MaxResponseBytes, cfg.MaxProgressFrames, found.Name)

	return inst, nil
}

// handshake reads and verifies the single line a plugin prints once it is
// listening, then leaves a pump draining whatever it writes afterwards.
func (i *instance) handshake(cfg Config, stdout io.Reader, log *slog.Logger) (protocol.Handshake, error) {
	// A buffer exactly the size of the longest permitted line turns "no newline
	// ever arrives" into a bounded read rather than an unbounded one: bufio
	// reports a full buffer instead of growing it.
	reader := bufio.NewReaderSize(stdout, protocol.MaxHandshakeLine)

	type result struct {
		line string
		err  error
	}

	// Buffered, so that the goroutine can finish and be collected even when
	// nothing is left waiting for it.
	lines := make(chan result, 1)
	go func() {
		// ReadSlice rather than ReadString: ReadString grows a buffer until the
		// delimiter arrives, which is the unbounded allocation this is supposed
		// to prevent. ReadSlice reports a full buffer instead, and the buffer is
		// the bound. Its result aliases that buffer, so it is copied before it
		// leaves this goroutine.
		line, err := reader.ReadSlice('\n')
		if err != nil && len(line) == 0 {
			lines <- result{err: err}
			return
		}
		if err != nil {
			lines <- result{line: string(line), err: err}
			return
		}
		lines <- result{line: string(line)}
	}()

	timer := time.NewTimer(cfg.HandshakeTimeout)
	defer timer.Stop()

	var line string
	select {
	case res := <-lines:
		switch {
		case errors.Is(res.err, bufio.ErrBufferFull):
			return protocol.Handshake{}, fmt.Errorf(
				"%w: wrote more than %d bytes to stdout without a newline",
				ErrHandshake, protocol.MaxHandshakeLine,
			)
		case errors.Is(res.err, io.EOF), errors.Is(res.err, os.ErrClosed):
			if res.line != "" {
				// It wrote something and then stopped without ending the line,
				// which is a different failure from writing nothing at all and
				// worth saying so — the text is usually the reason.
				return protocol.Handshake{}, fmt.Errorf(
					"%w: stdout ended mid-line after %q; %w",
					ErrHandshake, truncate(res.line, 128), i.exitReason(cfg.ShutdownGrace),
				)
			}
			return protocol.Handshake{}, fmt.Errorf("%w: %w", ErrHandshake, i.exitReason(cfg.ShutdownGrace))
		case res.err != nil:
			return protocol.Handshake{}, fmt.Errorf("%w: reading stdout: %w", ErrHandshake, res.err)
		}
		line = res.line

	case <-timer.C:
		// The read is still blocked in that goroutine. Killing the process
		// closes the pipe, which is what lets the goroutine finish; waiting on
		// it here instead would be waiting forever on exactly the plugin that
		// has proven it will not answer.
		return protocol.Handshake{}, fmt.Errorf(
			"%w: no handshake line within %s", ErrHandshakeTimeout, cfg.HandshakeTimeout,
		)

	case <-i.exited:
		return protocol.Handshake{}, fmt.Errorf("%w: %w", ErrHandshake, i.exitReason(0))
	}

	handshake, err := protocol.ParseHandshake(line)
	if err != nil {
		return protocol.Handshake{}, fmt.Errorf("%w: %w", ErrHandshake, err)
	}

	if err := verifyHandshake(handshake, cfg, i.socketPath); err != nil {
		return protocol.Handshake{}, err
	}

	// From here the plugin has promised to leave stdout alone. Draining it
	// anyway costs one goroutine and prevents a plugin that breaks the promise
	// from blocking on a full pipe, which would look like a hung plugin rather
	// than a noisy one.
	i.pumps.Add(1)
	go func() {
		defer i.pumps.Done()
		var reported int
		pumpPluginLog(reader, cfg.MaxStderrLine, func(line string, truncated bool) {
			if reported++; reported <= 10 {
				log.Warn("plugin wrote to stdout after the handshake, which the protocol reserves",
					"line", line, "truncated", truncated)
			}
		})
	}()

	return handshake, nil
}

// verifyHandshake checks what a plugin announced against what the host offered
// and assigned. Every mismatch here is a refusal rather than an adjustment: the
// host cannot speak a version it did not offer, and a plugin serving somewhere
// other than where it was told is not a plugin whose socket the host should
// trust.
func verifyHandshake(h protocol.Handshake, cfg Config, socketPath string) error {
	if h.HandshakeVersion != protocol.HandshakeVersion {
		return fmt.Errorf(
			"%w: announced itself in handshake format %d, which this host does not understand (it speaks %d)",
			ErrHandshake, h.HandshakeVersion, protocol.HandshakeVersion,
		)
	}

	offered := cfg.protocolVersions()
	if _, ok := protocol.Negotiate(offered, []int{h.ProtocolVersion}); !ok {
		return fmt.Errorf(
			"%w: chose protocol version %d, which this host did not offer (it offered %s)",
			ErrHandshake, h.ProtocolVersion, protocol.FormatVersions(offered),
		)
	}

	if h.Network != protocol.NetworkUnix {
		return fmt.Errorf(
			"%w: serving on network %q; only %q is permitted",
			ErrHandshake, truncate(h.Network, 32), protocol.NetworkUnix,
		)
	}

	if h.Address != socketPath {
		return fmt.Errorf(
			"%w: serving on %q rather than the socket it was assigned, %q",
			ErrHandshake, truncate(h.Address, 128), socketPath,
		)
	}

	return nil
}

// exitReason describes how the process ended, waiting briefly for it to be
// reaped so that an exit status can be reported rather than guessed at.
func (i *instance) exitReason(grace time.Duration) error {
	if grace > 0 {
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-i.exited:
		case <-timer.C:
			return fmt.Errorf("%w: closed stdout without a handshake line and is still running", ErrExited)
		}
	} else {
		select {
		case <-i.exited:
		default:
			return fmt.Errorf("%w: closed stdout without a handshake line and is still running", ErrExited)
		}
	}

	var exit *exec.ExitError
	if errors.As(i.waitErr, &exit) {
		return fmt.Errorf("%w: %s, printing no handshake line", ErrExited, exit.ProcessState)
	}
	if i.waitErr != nil {
		return fmt.Errorf("%w: %w", ErrExited, i.waitErr)
	}

	return fmt.Errorf("%w: exited successfully without printing a handshake line", ErrExited)
}

// stop ends the plugin process and releases everything the launch created.
//
// It escalates: the host-liveness pipe closes first, which a well-behaved plugin
// notices; then the process group is asked to terminate; then, after the grace
// period, it is killed. Signalling the group rather than the process is what
// stops a plugin's own children from being orphaned.
//
// It is safe to call more than once and safe to call on a partially constructed
// instance, because the launch path calls it on failures that can happen at any
// point.
func (i *instance) stop(ctx context.Context, grace time.Duration) {
	i.stopOnce.Do(func() {
		if i.hostPipe != nil {
			i.hostPipe.Close()
		}

		// Only signal a process that has not been reaped. Termination addresses
		// the process *group*, which os.Process cannot guard the way it guards a
		// signal to a reaped process: once the child has been waited on its pid
		// is free, and on a busy machine with a small pid_max it is reused
		// within minutes. Signalling it then would send SIGTERM to whatever
		// group now holds that number.
		//
		// This is the common path, not an edge: the supervisor reaches here from
		// the exit it observed, so the process is already gone.
		if i.proc != nil && !i.reaped() {
			terminateProcess(i.proc, false)

			if !i.waitExit(ctx, grace) {
				terminateProcess(i.proc, true)
				// After SIGKILL the process cannot ignore anything, so this
				// wait is bounded by the kernel rather than by the plugin.
				i.waitExit(ctx, grace)
			}
		}

		// Closing the read ends unblocks the pumps, which may still be waiting
		// on output from a process whose children hold the write ends open.
		if i.stdout != nil {
			i.stdout.Close()
		}
		if i.stderr != nil {
			i.stderr.Close()
		}
		i.pumps.Wait()

		i.clients.close()

		if i.socketDir != "" {
			os.RemoveAll(i.socketDir)
		}
	})
}

// reaped reports whether the process has already been waited on, after which its
// pid means nothing.
func (i *instance) reaped() bool {
	if i.exited == nil {
		return true
	}

	select {
	case <-i.exited:
		return true
	default:
		return false
	}
}

// waitExit reports whether the process was reaped within the grace period.
func (i *instance) waitExit(ctx context.Context, grace time.Duration) bool {
	if i.exited == nil {
		return true
	}

	timer := time.NewTimer(grace)
	defer timer.Stop()

	select {
	case <-i.exited:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}

// makeSocketDir creates the private directory a plugin's socket lives in and
// returns the directory and the socket path within it.
//
// The directory is the security boundary. os.MkdirTemp creates it 0700, and a
// umask can only remove permission bits, never add them, so it cannot come out
// more permissive than that. That matters more than the socket's own mode:
// Linux checks write permission on a socket file, but several BSD-derived
// systems — macOS among them — do not, and enforce only the containing
// directory's traversal permission. The directory is therefore what keeps other
// users out everywhere, and the 0600 the plugin sets on the socket is the second
// line for the platforms that honor it.
//
// The name is kept short on purpose: a Unix socket address holds around a
// hundred bytes, and the temporary directory alone is most of that on macOS.
func makeSocketDir(base string) (dir, socket string, err error) {
	dir, err = os.MkdirTemp(base, "fsplug")
	if err != nil {
		return "", "", fmt.Errorf("creating the plugin socket directory: %w", err)
	}

	socket = filepath.Join(dir, "s")
	if err := checkSocketPath(socket); err != nil {
		os.RemoveAll(dir)
		return "", "", err
	}

	return dir, socket, nil
}

// tokenPipe returns the read end of a pipe already holding the per-launch token,
// for the plugin to inherit on tokenFD.
//
// The token is written and the write end closed before the process starts, so
// the plugin reads one line and then EOF without the host having to stay and
// feed it — and a plugin that never reads costs nothing, because a token is
// orders of magnitude smaller than a pipe's buffer and the bytes are discarded
// when both ends close.
func tokenPipe(token string) (*os.File, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("token pipe: %w", err)
	}

	if err := protocol.WriteToken(w, token); err != nil {
		w.Close()
		r.Close()
		return nil, err
	}

	// Closed here rather than deferred: the plugin's read must reach EOF, and
	// nothing else the host does depends on this end staying open.
	if err := w.Close(); err != nil {
		r.Close()
		return nil, fmt.Errorf("token pipe: %w", err)
	}

	return r, nil
}

// pluginEnv builds the environment a plugin is launched with.
//
// It starts empty rather than from the worker's own environment. The worker's
// environment is where its credentials live — a Temporal API key, a cloud role,
// whatever a deployment set — and a plugin has no claim on any of it. What a
// plugin needs is named by an operator in Config.Env, which makes the set of
// things a plugin can read a reviewable list rather than an accident of how the
// worker was started.
func pluginEnv(cfg Config, socketPath string) []string {
	env := []string{
		protocol.MagicCookieEnv + "=" + protocol.MagicCookieValue,
		protocol.VersionsEnv + "=" + protocol.FormatVersions(cfg.protocolVersions()),
		protocol.SocketEnv + "=" + socketPath,
		protocol.TokenFDEnv + "=" + strconv.Itoa(tokenFD),
		protocol.HostFDEnv + "=" + strconv.Itoa(hostFD),
	}

	// Operator-supplied entries come last, but cannot override the protocol's
	// own: a Config.Env that redefined the socket path or a token descriptor
	// would break the handshake in a way that looks like a plugin bug.
	for _, entry := range cfg.Env {
		if isProtocolEnv(entry) {
			continue
		}
		env = append(env, entry)
	}

	return env
}

// isProtocolEnv reports whether an operator-supplied entry would collide with
// one the protocol owns.
//
// protocol.TokenEnv is in the list although the host no longer sets it. The name
// is retired, not free: an operator entry spelling it would put something a
// plugin might read as the per-launch secret into the environment block, which
// is exactly the place this protocol stopped keeping secrets.
func isProtocolEnv(entry string) bool {
	for _, name := range []string{
		protocol.MagicCookieEnv,
		protocol.VersionsEnv,
		protocol.SocketEnv,
		protocol.TokenEnv,
		protocol.TokenFDEnv,
		protocol.HostFDEnv,
	} {
		if len(entry) > len(name) && entry[len(name)] == '=' && entry[:len(name)] == name {
			return true
		}
	}
	return false
}

// stderrRelayFunc returns the callback pumpPluginLog invokes per stderr line,
// rate-limited to cfg.MaxStderrLinesPerMinute so that the volume the host
// relays into its own log is bounded independently of how long any one line
// is, and a flush to call once the pump sees EOF. A negative
// MaxStderrLinesPerMinute disables the bound: every line is relayed, as
// before this existed, and flush is a no-op.
//
// allow only reports a window's suppressed count when another line arrives to
// roll the window over, so a plugin that floods and then goes quiet — a crash
// is a common reason, and often the one this limiter's summary would explain
// — leaves its last window's count unreported. flush recovers it once, after
// the pump can no longer call allow.
func stderrRelayFunc(cfg Config, log *slog.Logger) (relay func(line string, truncated bool), flush func() string) {
	if cfg.MaxStderrLinesPerMinute < 0 {
		return func(line string, truncated bool) {
			log.Info("plugin log", "line", line, "truncated", truncated)
		}, func() string { return "" }
	}

	limiter := newStderrLimiter(cfg.MaxStderrLinesPerMinute, stderrRateWindow, cfg.stderrClock)
	return func(line string, truncated bool) {
			ok, summary := limiter.allow()
			if summary != "" {
				log.Warn(summary)
			}
			if ok {
				log.Info("plugin log", "line", line, "truncated", truncated)
			}
		}, func() string {
			return limiter.flush()
		}
}

// pumpPluginLog reads lines from a plugin and hands each to fn.
//
// A line longer than maxLine is truncated and the rest of it discarded, rather
// than being buffered until it ends or aborting the pump. Both alternatives are
// worse: buffering makes a plugin that never writes a newline a memory
// exhaustion, and aborting means one overlong line costs every subsequent log
// line from that plugin.
func pumpPluginLog(r io.Reader, maxLine int, fn func(line string, truncated bool)) {
	// bufio refuses to make a buffer smaller than its own minimum, and silently
	// rounds up if asked to, so the floor is applied here where it is visible.
	const minBuffer = 512
	if maxLine < minBuffer {
		maxLine = minBuffer
	}

	// A *bufio.Reader may already wrap this reader — the handshake leaves one
	// holding buffered bytes — and re-wrapping would strand them.
	reader, ok := r.(*bufio.Reader)
	if !ok {
		reader = bufio.NewReaderSize(r, maxLine)
	}

	var discarding bool
	for {
		line, err := reader.ReadSlice('\n')

		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			if !discarding {
				fn(string(trimEOL(line)), true)
				discarding = true
			}
			continue
		case err != nil:
			if len(line) > 0 && !discarding {
				fn(string(trimEOL(line)), false)
			}
			return
		}

		if discarding {
			// The tail of a line already reported as truncated.
			discarding = false
			continue
		}

		if trimmed := trimEOL(line); len(trimmed) > 0 {
			fn(string(trimmed), false)
		}
	}
}

// trimEOL removes a trailing newline and carriage return.
func trimEOL(b []byte) []byte {
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
	}
	if n := len(b); n > 0 && b[n-1] == '\r' {
		b = b[:n-1]
	}
	return b
}
