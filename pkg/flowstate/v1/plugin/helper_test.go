package plugin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/internal/protocol"
)

// TestMain lets this test binary act as a plugin when a host launches it.
//
// Testing a host that launches processes needs processes to launch, and building
// a separate binary for each way a plugin can misbehave would mean a compiler in
// the test path and a set of fixtures to keep in step with the protocol. Instead
// this binary is every fake plugin: the host launches it through a symlink whose
// name selects which one, exactly as it would launch a real plugin.
//
// The magic cookie is the discriminator, which is a nice demonstration of what it
// is for — this binary run any other way runs its tests.
func TestMain(m *testing.M) {
	if os.Getenv(protocol.MagicCookieEnv) == protocol.MagicCookieValue {
		os.Exit(runFakePlugin())
	}
	os.Exit(m.Run())
}

// fakeMode is which fake plugin this process is, taken from the name it was
// launched as.
func fakeMode() string {
	name := filepath.Base(os.Args[0])
	mode, _ := strings.CutPrefix(name, BinaryPrefix)
	return mode
}

// runFakePlugin behaves as the plugin its name selects, and returns an exit code.
func runFakePlugin() int {
	mode := fakeMode()

	// Every fake exits when the host goes away, which is what a real plugin's
	// SDK does and what the orphan tests rely on.
	go exitWhenHostExits()

	switch mode {
	case "exit-now", "crash-loop":
		fmt.Fprintln(os.Stderr, "this plugin exits immediately")
		return 3

	case "garbage":
		fmt.Println("hello! i am a normal program, not a plugin at all")
		time.Sleep(10 * time.Second)
		return 0

	case "silent":
		// Never handshakes. The host must kill it rather than wait forever.
		time.Sleep(10 * time.Second)
		return 0

	case "long-line":
		// More than the handshake bound, with no newline in sight.
		os.Stdout.WriteString(strings.Repeat("x", protocol.MaxHandshakeLine+512))
		time.Sleep(10 * time.Second)
		return 0

	case "bad-sentinel":
		fmt.Printf("NOT-A-FLOWSTATE-PLUGIN|1|1|unix|%s\n", os.Getenv(protocol.SocketEnv))
		time.Sleep(10 * time.Second)
		return 0

	case "bad-version":
		fmt.Printf("%s|1|99|unix|%s\n", protocol.Sentinel, os.Getenv(protocol.SocketEnv))
		time.Sleep(10 * time.Second)
		return 0

	case "bad-address":
		fmt.Printf("%s|1|1|unix|/tmp/somewhere-else.sock\n", protocol.Sentinel)
		time.Sleep(10 * time.Second)
		return 0

	case "die-after":
		// Handshakes correctly, then exits before it can answer anything.
		if _, err := fakeListen(); err != nil {
			return 1
		}
		fakeAnnounce()
		return 0
	}

	if mode == "short-lived" {
		// Comes up correctly and then dies, over and over: the crash loop the
		// restart budget exists to stop.
		go func() {
			time.Sleep(150 * time.Millisecond)
			os.Exit(7)
		}()
	}

	// Everything else serves.
	listener, err := fakeListen()
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen: %v\n", err)
		return 1
	}

	handler, err := fakeHandler(mode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}

	fakeAnnounce()

	if mode == "stdout-noise" {
		// Breaks the protocol's promise by continuing to write to stdout. The
		// host must tolerate it rather than block or fail.
		go func() {
			for range 100 {
				fmt.Println("i was told not to do this")
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	fmt.Fprintf(os.Stderr, "fake plugin %q serving\n", mode)

	server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return 1
	}

	return 0
}

// exitWhenHostExits ends this process when the host's pipe closes.
func exitWhenHostExits() {
	raw := os.Getenv(protocol.HostFDEnv)
	fd, err := strconv.Atoi(raw)
	if err != nil {
		return
	}

	file := os.NewFile(uintptr(fd), "host")
	if file == nil {
		return
	}

	io.Copy(io.Discard, file)
	os.Exit(0)
}

// fakeListen listens on the socket the host assigned.
func fakeListen() (net.Listener, error) {
	path := os.Getenv(protocol.SocketEnv)
	listener, err := net.Listen(protocol.NetworkUnix, path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

// fakeAnnounce prints the handshake line.
func fakeAnnounce() {
	fmt.Printf("%s|%d|%d|%s|%s\n",
		protocol.Sentinel, protocol.HandshakeVersion, protocol.Version1,
		protocol.NetworkUnix, os.Getenv(protocol.SocketEnv))
}

// fakeHandler builds the services this fake serves.
func fakeHandler(mode string) (http.Handler, error) {
	manifest, err := fakeManifest(mode)
	if err != nil {
		return nil, err
	}

	// Requests are refused without the per-launch token, so every test that
	// succeeds is also a test that the host presents it.
	opts := []connect.HandlerOption{
		connect.WithInterceptors(connect.UnaryInterceptorFunc(
			func(next connect.UnaryFunc) connect.UnaryFunc {
				return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
					if req.Header().Get(protocol.TokenHeader) != os.Getenv(protocol.TokenEnv) {
						return nil, connect.NewError(connect.CodePermissionDenied,
							errors.New("missing or wrong plugin token"))
					}
					return next(ctx, req)
				}
			})),
	}

	mux := http.NewServeMux()

	path, handler := flowstatev1connect.NewPluginServiceHandler(&fakePluginService{
		manifest: manifest,
		mode:     mode,
	}, opts...)
	mux.Handle(path, handler)

	path, handler = flowstatev1connect.NewSecretServiceHandler(&fakeSecretService{mode: mode}, opts...)
	mux.Handle(path, handler)

	path, handler = flowstatev1connect.NewTaskServiceHandler(&fakeTaskService{mode: mode}, opts...)
	mux.Handle(path, handler)

	return mux, nil
}

// fakeManifest is what each fake says about itself.
func fakeManifest(mode string) (*flowstatev1.PluginManifest, error) {
	base := &flowstatev1.PluginManifest{
		Name:        mode,
		Version:     "0.0.1",
		Description: "a fake plugin for tests",
	}

	switch mode {
	case "no-caps":
		return base, nil

	case "bad-manifest":
		// An empty name violates the schema's own rules.
		base.Name = ""
		base.Capabilities = []flowstatev1.Capability{flowstatev1.Capability_CAPABILITY_SECRETS}
		base.Schemes = []string{"bad"}
		return base, nil

	case "unspecified-cap":
		base.Capabilities = []flowstatev1.Capability{flowstatev1.Capability_CAPABILITY_UNSPECIFIED}
		return base, nil

	case "unknown-caps":
		// Only capabilities from some newer engine. Each one is ignored, which
		// leaves nothing this host can dispatch to it.
		base.Capabilities = []flowstatev1.Capability{
			flowstatev1.Capability(9998),
			flowstatev1.Capability(9999),
		}
		return base, nil

	case "future-cap":
		// A capability from a newer engine, alongside one this host knows. The
		// unknown one must be ignored rather than refused.
		base.Capabilities = []flowstatev1.Capability{
			flowstatev1.Capability_CAPABILITY_SECRETS,
			flowstatev1.Capability(9999),
		}
		base.Schemes = []string{"future"}
		return base, nil

	case "secrets-no-schemes":
		base.Capabilities = []flowstatev1.Capability{flowstatev1.Capability_CAPABILITY_SECRETS}
		return base, nil

	case "tasks-no-tasks":
		base.Capabilities = []flowstatev1.Capability{flowstatev1.Capability_CAPABILITY_TASKS}
		return base, nil

	case "builtin-task":
		base.Capabilities = []flowstatev1.Capability{flowstatev1.Capability_CAPABILITY_TASKS}
		base.Tasks = []*flowstatev1.TaskManifest{{Name: "http", Summary: "shadows a built-in"}}
		return base, nil

	case "dup-one", "dup-two":
		base.Capabilities = []flowstatev1.Capability{flowstatev1.Capability_CAPABILITY_SECRETS}
		base.Schemes = []string{"shared"}
		return base, nil

	case "not-permitted":
		base.Capabilities = []flowstatev1.Capability{flowstatev1.Capability_CAPABILITY_SECRETS}
		base.Schemes = []string{"forbidden"}
		return base, nil

	case "bad-descriptor":
		base.Capabilities = []flowstatev1.Capability{flowstatev1.Capability_CAPABILITY_TASKS}
		base.Tasks = []*flowstatev1.TaskManifest{{
			Name:            "broken_task",
			InputDescriptor: []byte("this is not a descriptor"),
			InputMessage:    "nope.Nope",
		}}
		return base, nil

	default:
		// The general-purpose fake: both capabilities, a scheme named after
		// itself, and one task.
		base.Capabilities = []flowstatev1.Capability{
			flowstatev1.Capability_CAPABILITY_SECRETS,
			flowstatev1.Capability_CAPABILITY_TASKS,
		}
		base.Schemes = []string{mode}
		base.Tasks = []*flowstatev1.TaskManifest{{
			Name:    strings.ReplaceAll(mode, "-", "_") + "_task",
			Summary: "a fake task",
			// Reuses a message the engine already has, which exercises the
			// no-descriptor path; descriptor reconstruction is tested directly.
			InputMessage:  "flowstate.v1.Task.Echo.Inputs",
			OutputMessage: "flowstate.v1.Task.Echo.Outputs",
		}}
		return base, nil
	}
}

// fakePluginService answers the capability handshake.
type fakePluginService struct {
	flowstatev1connect.UnimplementedPluginServiceHandler

	manifest *flowstatev1.PluginManifest
	mode     string
}

func (s *fakePluginService) Describe(context.Context, *connect.Request[flowstatev1.DescribePluginRequest]) (*connect.Response[flowstatev1.DescribePluginResponse], error) {
	if s.mode == "describe-fails" {
		return nil, connect.NewError(connect.CodeInternal, errors.New("describe is broken"))
	}
	return connect.NewResponse(&flowstatev1.DescribePluginResponse{Manifest: s.manifest}), nil
}

func (s *fakePluginService) Health(context.Context, *connect.Request[flowstatev1.HealthRequest]) (*connect.Response[flowstatev1.HealthResponse], error) {
	switch s.mode {
	case "sick":
		return connect.NewResponse(&flowstatev1.HealthResponse{
			Status:  flowstatev1.HealthResponse_STATUS_NOT_SERVING,
			Message: "the backend this plugin needs is unreachable",
		}), nil
	case "health-fails":
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("cannot answer"))
	default:
		return connect.NewResponse(&flowstatev1.HealthResponse{
			Status: flowstatev1.HealthResponse_STATUS_SERVING,
		}), nil
	}
}

// fakeSecretService answers secret resolutions.
type fakeSecretService struct {
	flowstatev1connect.UnimplementedSecretServiceHandler

	mode string
}

func (s *fakeSecretService) Resolve(ctx context.Context, req *connect.Request[flowstatev1.ResolveSecretRequest]) (*connect.Response[flowstatev1.ResolveSecretResponse], error) {
	name := req.Msg.GetRef().GetName()

	switch {
	case s.mode == "huge":
		return connect.NewResponse(&flowstatev1.ResolveSecretResponse{
			Value: make([]byte, 256<<10),
		}), nil

	case s.mode == "slow":
		<-ctx.Done()
		return nil, connect.NewError(connect.CodeDeadlineExceeded, ctx.Err())

	case name == "missing":
		return nil, connect.NewError(connect.CodeNotFound, errors.New("no such secret"))

	case name == "refused":
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("refused"))

	case name == "down":
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("backend unreachable"))

	case name == "empty":
		return connect.NewResponse(&flowstatev1.ResolveSecretResponse{}), nil
	}

	// The namespace is part of the answer, so a test can prove it was carried
	// across the boundary rather than dropped.
	value := "value-for-" + name
	if ns := req.Msg.GetNamespace(); ns != "" {
		value += "-in-" + ns
	}
	if identity := req.Msg.GetIdentity(); identity.GetSubject() != "" {
		value += "-as-" + identity.GetSubject()
	}

	return connect.NewResponse(&flowstatev1.ResolveSecretResponse{Value: []byte(value)}), nil
}

// fakeTaskService answers task executions.
type fakeTaskService struct {
	flowstatev1connect.UnimplementedTaskServiceHandler

	mode string
}

func (s *fakeTaskService) Execute(ctx context.Context, req *connect.Request[flowstatev1.ExecuteTaskRequest]) (*connect.Response[flowstatev1.ExecuteTaskResponse], error) {
	switch s.mode {
	case "huge":
		big := make([]byte, 256<<10)
		for i := range big {
			big[i] = 'a'
		}
		return connect.NewResponse(&flowstatev1.ExecuteTaskResponse{
			Outputs: &flowstatev1.Node_Outputs{
				NamedValues: map[string]*flowstatev1.Value{
					"result": flowstatev1.NewLiteral(string(big)),
				},
			},
		}), nil

	case "slow":
		<-ctx.Done()
		return nil, connect.NewError(connect.CodeDeadlineExceeded, ctx.Err())

	case "retryable":
		err := connect.NewError(connect.CodeInternal, errors.New("transient trouble"))
		if detail, dErr := connect.NewErrorDetail(&flowstatev1.ExecuteTaskResponse{Retryable: true}); dErr == nil {
			err.AddDetail(detail)
		}
		return nil, err

	case "permanent":
		err := connect.NewError(connect.CodeInternal, errors.New("permanent trouble"))
		if detail, dErr := connect.NewErrorDetail(&flowstatev1.ExecuteTaskResponse{Retryable: false}); dErr == nil {
			err.AddDetail(detail)
		}
		return nil, err
	}

	// Echo back what came in, plus what the request carried about the workload,
	// so a test can prove those crossed the boundary.
	message := req.Msg.GetTask().GetInputs()["message"].GetLiteral().GetStringValue()

	return connect.NewResponse(&flowstatev1.ExecuteTaskResponse{
		Outputs: &flowstatev1.Node_Outputs{
			NamedValues: map[string]*flowstatev1.Value{
				"result":    flowstatev1.NewLiteral(message),
				"namespace": flowstatev1.NewLiteral(req.Msg.GetNamespace()),
				"subject":   flowstatev1.NewLiteral(req.Msg.GetIdentity().GetSubject()),
				"has_scope": flowstatev1.NewLiteral(req.Msg.GetScope() != nil),
			},
		},
	}), nil
}

// --- test-side helpers ---

// pluginDir creates a search path directory holding fake plugins with the given
// names, each a symlink to this test binary.
func pluginDir(t *testing.T, names ...string) string {
	t.Helper()

	self, err := os.Executable()
	if err != nil {
		t.Fatalf("finding the test binary: %v", err)
	}

	// Kept short: a Unix socket path has about a hundred bytes to work with, and
	// the temporary directory is most of that on macOS.
	dir := t.TempDir()

	for _, name := range names {
		link := filepath.Join(dir, BinaryPrefix+name)
		if err := os.Symlink(self, link); err != nil {
			t.Fatalf("linking fake plugin %q: %v", name, err)
		}
	}

	return dir
}

// testConfig returns a Config with the bounds turned down to test speed.
func testConfig(t *testing.T, dir string) Config {
	t.Helper()

	return Config{
		SearchPath:       []string{dir},
		HandshakeTimeout: 3 * time.Second,
		DescribeTimeout:  3 * time.Second,
		CallTimeout:      3 * time.Second,
		HealthTimeout:    time.Second,
		ShutdownGrace:    2 * time.Second,
		RestartBackoff:   10 * time.Millisecond,

		// Off by default: a test that wants polling turns it on, and one that
		// does not should not have a ticker racing its assertions.
		DisableHealthChecks: true,

		Logger: testLogger(t),
	}
}

// testLogger sends host and plugin logs to the test's own output, so a failure
// comes with the plugin's stderr rather than without it.
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testWriter adapts *testing.T to io.Writer.
//
// Writes are dropped once the test has finished, because a plugin's stderr pump
// can outlive the test that started it by a moment and logging from a finished
// test panics.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	defer func() { _ = recover() }()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// newCapturingLogger logs both to the test's output and into a buffer, so a
// test can assert on what was logged as well as read it on a failure.
func newCapturingLogger(t *testing.T, into *strings.Builder) *slog.Logger {
	t.Helper()

	var mu sync.Mutex
	capture := writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		into.Write(p)
		mu.Unlock()
		return testWriter{t}.Write(p)
	})

	return slog.New(slog.NewTextHandler(capture, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// writerFunc adapts a function to io.Writer.
type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// fmtSprint formats a value, for the containment checks that have to try every
// verb rather than assume one.
func fmtSprint(verb string, v any) string { return fmt.Sprintf(verb, v) }

// openHost builds and opens a host over a directory of fake plugins, and closes
// it when the test ends.
func openHost(t *testing.T, cfg Config) *Host {
	t.Helper()

	host, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		host.Close(ctx)
	})

	if err := host.Open(t.Context()); err != nil {
		t.Fatalf("Open: %v", err)
	}

	return host
}

// waitForProcessGone reports whether a pid stops existing within the deadline.
//
// Polling rather than asserting once, because a signalled process takes a moment
// to die and a test that asserts immediately tests the scheduler.
func waitForProcessGone(t *testing.T, pid int, within time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}

	return !processAlive(pid)
}

// waitFor polls until a condition holds or the deadline passes.
func waitFor(t *testing.T, within time.Duration, condition func() bool) bool {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}

	return condition()
}
