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
	"google.golang.org/protobuf/types/known/durationpb"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	pluginv1connect "github.com/picatz/flowstate/pkg/flowstate/plugin/v1/pluginv1connect"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
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

	code := m.Run()
	removeBuiltExample()

	// Checked after the suite rather than inside a test, because what it checks is
	// the absence of a test having run. See [requirePluginExampleRan].
	if err := requirePluginExampleRan(); err != nil && code == 0 {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}

	os.Exit(code)
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

	// The error-pipeline conformance fixture is a *real* SDK plugin rather than a
	// hand-rolled handler, because it exists to prove the SDK's own error
	// serialization survives the wire — see [TestPluginErrorPipelineRoundTrip].
	// [sdk.Run] installs its own host-exit watch and serving, so this returns
	// before the by-hand fake path below.
	if mode == "errors" {
		return runErrorsPlugin()
	}
	if mode == "deadline" {
		return runDeadlinePlugin()
	}
	if mode == "caller-mode" {
		return runCallerModePlugin()
	}

	// The progress-relay conformance fixture is likewise a real SDK plugin
	// rather than a hand-rolled handler, for the identical reason: it exists
	// to prove ReportProgress crosses the wire the SDK's own ExecuteStream
	// handler serves, not a hand-built one — see
	// [TestPluginProgressCrossesTheSubprocessBoundary].
	if mode == "progress" {
		return runProgressPlugin()
	}

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
		// 99 is the point of this fixture, so it stays a literal. The handshake
		// version beside it is not, and is derived so a change to the line format
		// does not quietly turn this into a test of something else.
		fmt.Printf("%s|%d|99|unix|%s\n",
			protocol.Sentinel, protocol.HandshakeVersion, os.Getenv(protocol.SocketEnv))
		time.Sleep(10 * time.Second)
		return 0

	case "bad-address":
		// The address is what this fixture gets wrong, so everything else about the
		// line has to be right — including the protocol version. Announcing a
		// retired one makes the host refuse on the version and never reach the
		// address, which passes the test for the wrong reason.
		fmt.Printf("%s|%d|%d|unix|/tmp/somewhere-else.sock\n",
			protocol.Sentinel, protocol.HandshakeVersion, protocol.Version3)
		time.Sleep(10 * time.Second)
		return 0

	case "die-after":
		// Handshakes correctly, then exits before it can answer anything.
		if _, err := fakeListen(); err != nil {
			return 1
		}
		fakeAnnounce()
		return 0

	case "record-run":
		// Touches the file its env names the instant it runs, then exits
		// without handshaking. It is the ELF — and so, on Linux, the
		// descriptor-pinnable — analogue of markerPlugin's shell script: a
		// launch that reaches exec leaves the marker, and admission that
		// refuses before exec does not. It never serves, so a launch that does
		// get here still fails, at the handshake, which is a different failure
		// from a pin refusal. See TestPinnedDigestMismatchIsRefusedBeforeAnythingRuns.
		if p := os.Getenv("FLOWSTATE_TEST_MARKER"); p != "" {
			_ = os.WriteFile(p, nil, 0o600)
		}
		return 0
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

	if mode == "short-lived" {
		// Comes up correctly and then dies, over and over: the crash loop the
		// restart budget exists to stop.
		//
		// The countdown starts *here* rather than at process start, and that
		// placement is the whole point. Started earlier it also runs while the
		// process is finding its socket path, listening and announcing, so what
		// is left for the host's first Describe is 150ms minus however long
		// this machine took to get that far — and on a loaded CI runner that
		// remainder can be nothing. The plugin then dies before answering the
		// call every test using it makes during Open, and the test fails at
		// setup with `connect: connection refused`, for a reason that has
		// nothing to do with what it asserts.
		//
		// TestARelaunchFromDifferentBytesIsRefused hit exactly this and worked
		// around it by killing the process instead of using this fixture, and
		// the three tests that do use it kept the hazard. Anchoring the clock
		// to the moment the plugin is reachable makes the window a real
		// second, not a leftover.
		go func() {
			time.Sleep(shortLivedLifetime)
			os.Exit(7)
		}()
	}

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

	if mode == "stderr-flood" {
		// Writes far more lines than any reasonable per-minute budget, as fast
		// as it can, on the channel a plugin is *supposed* to use — a buggy
		// dependency logging in a hot loop needs no attacker. The host must
		// keep draining the pipe (so this never blocks) while relaying only a
		// bounded prefix of it into its own log.
		go func() {
			for range 20_000 {
				fmt.Fprintln(os.Stderr, "i will not stop logging")
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

// shortLivedLifetime is how long the "short-lived" fake serves before exiting.
//
// Long enough that a Describe on a contended machine finishes first, short
// enough that a test waiting fifteen seconds sees several restarts: three lives
// exhaust a MaxRestarts of 2, and one is all TestRestartRecovers and
// TestServiceSurvivesARestart need.
const shortLivedLifetime = time.Second

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
		protocol.Sentinel, protocol.HandshakeVersion, protocol.Version3,
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

	path, handler := pluginv1connect.NewPluginServiceHandler(&fakePluginService{
		manifest: manifest,
		mode:     mode,
	}, opts...)
	mux.Handle(path, handler)

	path, handler = pluginv1connect.NewSecretServiceHandler(&fakeSecretService{mode: mode}, opts...)
	mux.Handle(path, handler)

	path, handler = pluginv1connect.NewTaskServiceHandler(&fakeTaskService{mode: mode}, opts...)
	mux.Handle(path, handler)

	return mux, nil
}

// fakeManifest is what each fake says about itself.
func fakeManifest(mode string) (*pluginv1.PluginManifest, error) {
	base := &pluginv1.PluginManifest{
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
		base.Capabilities = []pluginv1.Capability{pluginv1.Capability_CAPABILITY_SECRETS}
		base.Schemes = []string{"bad"}
		return base, nil

	case "unspecified-cap":
		base.Capabilities = []pluginv1.Capability{pluginv1.Capability_CAPABILITY_UNSPECIFIED}
		return base, nil

	case "unknown-caps":
		// Only capabilities from some newer engine. Each one is ignored, which
		// leaves nothing this host can dispatch to it.
		base.Capabilities = []pluginv1.Capability{
			pluginv1.Capability(9998),
			pluginv1.Capability(9999),
		}
		return base, nil

	case "future-cap":
		// A capability from a newer engine, alongside one this host knows. The
		// unknown one must be ignored rather than refused.
		base.Capabilities = []pluginv1.Capability{
			pluginv1.Capability_CAPABILITY_SECRETS,
			pluginv1.Capability(9999),
		}
		base.Schemes = []string{"future"}
		return base, nil

	case "self-digest":
		// Reports the digest of the image this process is *running*, read from
		// the running inode rather than from the path it was launched by, so a
		// test can compare what the host recorded against what actually ran.
		// See [runningImageDigest] and
		// TestTheDigestIsOfTheImageThatRanWhenTheBinaryIsSwappedAtExec.
		digest, err := runningImageDigest()
		if err != nil {
			return nil, err
		}
		base.Capabilities = []pluginv1.Capability{pluginv1.Capability_CAPABILITY_SECRETS}
		base.Schemes = []string{mode}
		base.Description = digest
		return base, nil

	case "secrets-no-schemes":
		base.Capabilities = []pluginv1.Capability{pluginv1.Capability_CAPABILITY_SECRETS}
		return base, nil

	case "tasks-no-tasks":
		base.Capabilities = []pluginv1.Capability{pluginv1.Capability_CAPABILITY_TASKS}
		return base, nil

	case "builtin-task":
		base.Capabilities = []pluginv1.Capability{pluginv1.Capability_CAPABILITY_TASKS}
		base.Tasks = []*pluginv1.TaskManifest{{Name: "http", Summary: "shadows a built-in"}}
		return base, nil

	case "dup-one", "dup-two":
		base.Capabilities = []pluginv1.Capability{pluginv1.Capability_CAPABILITY_SECRETS}
		base.Schemes = []string{"shared"}
		return base, nil

	case "not-permitted":
		base.Capabilities = []pluginv1.Capability{pluginv1.Capability_CAPABILITY_SECRETS}
		base.Schemes = []string{"forbidden"}
		return base, nil

	case "bad-descriptor":
		base.Capabilities = []pluginv1.Capability{pluginv1.Capability_CAPABILITY_TASKS}
		base.Tasks = []*pluginv1.TaskManifest{{
			Name:            "broken_task",
			InputDescriptor: []byte("this is not a descriptor"),
			InputMessage:    "nope.Nope",
		}}
		return base, nil

	case "hang-stream":
		// Advertises tasks but no progress-streaming capability, since this
		// fixture is reached only through [Plugin.TaskService]'s
		// ExecuteStream directly (service.go), never through the manifest-
		// driven dispatch in task.go that CAPABILITY_TASK_PROGRESS gates.
		base.Capabilities = []pluginv1.Capability{pluginv1.Capability_CAPABILITY_TASKS}
		base.Tasks = []*pluginv1.TaskManifest{{
			Name:          "hang_task",
			Summary:       "a fake task whose stream never ends on its own",
			InputMessage:  "flowstate.v1.Task.Log.Inputs",
			OutputMessage: "flowstate.v1.Task.Log.Outputs",
		}}
		return base, nil

	case "scoped":
		// Otherwise identical to the default fake below, except that it
		// declares NeedsScope — which is what routes the durable driver to
		// TaskInScope rather than the legacy Task activity, exercising the
		// path [engine.TaskInScope] carries an identity on independent of
		// [flowstatev1.TaskNeedsAuthority].
		base.Capabilities = []pluginv1.Capability{
			pluginv1.Capability_CAPABILITY_SECRETS,
			pluginv1.Capability_CAPABILITY_TASKS,
		}
		base.Schemes = []string{mode}
		base.Tasks = []*pluginv1.TaskManifest{{
			Name:          "scoped_task",
			Summary:       "a fake task that declares it needs a scope",
			InputMessage:  "flowstate.v1.Task.Log.Inputs",
			OutputMessage: "flowstate.v1.Task.Log.Outputs",
			NeedsScope:    true,
		}}
		return base, nil

	case "identity-stream":
		// Tasks *and* progress streaming, which is what routes
		// [Plugin.executeTask] (task.go) down its ExecuteStream branch rather
		// than unary Execute. Its task echoes back the namespace and subject
		// the *stream* request carried, so
		// TestExecuteStreamCarriesTheCallersOwnIdentity can compare the two
		// paths' tenancy against each other.
		base.Capabilities = []pluginv1.Capability{
			pluginv1.Capability_CAPABILITY_TASKS,
			pluginv1.Capability_CAPABILITY_TASK_PROGRESS,
		}
		base.Tasks = []*pluginv1.TaskManifest{{
			Name:          "identity_task",
			Summary:       "echoes the identity its ExecuteStream request carried",
			InputMessage:  "flowstate.v1.Task.Log.Inputs",
			OutputMessage: "flowstate.v1.Task.Log.Outputs",
		}}
		return base, nil

	case "secret-task", "secret-task-error":
		// Declares one input, "message", as accepting a host secret reference —
		// the manifest field TestResolvePluginSecretInputs* and
		// TestPluginTaskResolvesAndScrubsHostSecret exist to exercise.
		base.Capabilities = []pluginv1.Capability{pluginv1.Capability_CAPABILITY_TASKS}
		base.Tasks = []*pluginv1.TaskManifest{{
			Name:          "task",
			Summary:       "echoes what it received, to prove what crossed the boundary",
			InputMessage:  "flowstate.v1.Task.Log.Inputs",
			OutputMessage: "flowstate.v1.Task.Log.Outputs",
			SecretInputs:  []string{"message"},
		}}
		return base, nil

	default:
		// The general-purpose fake: both capabilities, a scheme named after
		// itself, and one task.
		base.Capabilities = []pluginv1.Capability{
			pluginv1.Capability_CAPABILITY_SECRETS,
			pluginv1.Capability_CAPABILITY_TASKS,
		}
		base.Schemes = []string{mode}
		base.Tasks = []*pluginv1.TaskManifest{{
			Name:    strings.ReplaceAll(mode, "-", "_") + "_task",
			Summary: "a fake task",
			// Reuses a message the engine already has, which exercises the
			// no-descriptor path; descriptor reconstruction is tested directly.
			InputMessage:  "flowstate.v1.Task.Log.Inputs",
			OutputMessage: "flowstate.v1.Task.Log.Outputs",
		}}
		return base, nil
	}
}

// fakePluginService answers the capability handshake.
type fakePluginService struct {
	pluginv1connect.UnimplementedPluginServiceHandler

	manifest *pluginv1.PluginManifest
	mode     string
}

func (s *fakePluginService) Describe(context.Context, *connect.Request[pluginv1.DescribeRequest]) (*connect.Response[pluginv1.DescribeResponse], error) {
	if s.mode == "describe-fails" {
		return nil, connect.NewError(connect.CodeInternal, errors.New("describe is broken"))
	}
	return connect.NewResponse(&pluginv1.DescribeResponse{Manifest: s.manifest}), nil
}

func (s *fakePluginService) Health(context.Context, *connect.Request[pluginv1.HealthRequest]) (*connect.Response[pluginv1.HealthResponse], error) {
	switch s.mode {
	case "sick":
		return connect.NewResponse(&pluginv1.HealthResponse{
			Status:  pluginv1.HealthResponse_STATUS_NOT_SERVING,
			Message: "the backend this plugin needs is unreachable",
		}), nil
	case "health-fails":
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("cannot answer"))
	default:
		return connect.NewResponse(&pluginv1.HealthResponse{
			Status: pluginv1.HealthResponse_STATUS_SERVING,
		}), nil
	}
}

// fakeSecretService answers secret resolutions.
type fakeSecretService struct {
	pluginv1connect.UnimplementedSecretServiceHandler

	mode string
}

func (s *fakeSecretService) Resolve(ctx context.Context, req *connect.Request[pluginv1.ResolveRequest]) (*connect.Response[pluginv1.ResolveResponse], error) {
	name := req.Msg.GetRef().GetName()

	switch {
	case s.mode == "huge":
		return connect.NewResponse(&pluginv1.ResolveResponse{
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
		return connect.NewResponse(&pluginv1.ResolveResponse{}), nil

	case name == "leased":
		// A backend that issues short-lived credentials and says so, which is the
		// only party that knows.
		return connect.NewResponse(&pluginv1.ResolveResponse{
			Value:     []byte("a credential that expires soon"),
			ExpiresIn: durationpb.New(30 * time.Second),
		}), nil
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

	return connect.NewResponse(&pluginv1.ResolveResponse{Value: []byte(value)}), nil
}

// sleepyTaskDuration is how long the "sleepy" fake below works before it
// answers. It has to be comfortably longer than the CallTimeout its tests
// configure — a fixture that finished within the host's backstop would pass
// whether the backstop had been skipped or not — and short enough that three
// tests can afford to wait it out.
const sleepyTaskDuration = time.Second

// fakeTaskService answers task executions.
type fakeTaskService struct {
	pluginv1connect.UnimplementedTaskServiceHandler

	mode string
}

func (s *fakeTaskService) Execute(ctx context.Context, req *connect.Request[pluginv1.ExecuteRequest]) (*connect.Response[pluginv1.ExecuteResponse], error) {
	switch s.mode {
	case "huge":
		big := make([]byte, 256<<10)
		for i := range big {
			big[i] = 'a'
		}
		return connect.NewResponse(&pluginv1.ExecuteResponse{
			Outputs: &flowstatev1.Node_Outputs{
				NamedValues: map[string]*flowstatev1.Value{
					"result": flowstatev1.NewLiteral(string(big)),
				},
			},
		}), nil

	case "slow":
		<-ctx.Done()
		return nil, connect.NewError(connect.CodeDeadlineExceeded, ctx.Err())

	case "sleepy":
		// Finishes on its own after a wait deliberately longer than the
		// CallTimeout its tests configure, which is the shape #1130 was
		// about: a task that legitimately takes longer than the host's
		// backstop, under a caller that allowed it the time. "slow" above
		// cannot answer that question, because it never finishes — a bound
		// biting and a task completing look the same from a fixture that
		// only ever ends when its context does.
		//
		// It still watches ctx, so a test that wants a bound to bite gets a
		// prompt answer rather than a wait.
		select {
		case <-time.After(sleepyTaskDuration):
		case <-ctx.Done():
			return nil, connect.NewError(connect.CodeDeadlineExceeded, ctx.Err())
		}

		return connect.NewResponse(&pluginv1.ExecuteResponse{
			Outputs: &flowstatev1.Node_Outputs{
				NamedValues: map[string]*flowstatev1.Value{
					"result": flowstatev1.NewLiteral("awake"),
				},
			},
		}), nil

	case "retryable":
		err := connect.NewError(connect.CodeInternal, errors.New("transient trouble"))
		if detail, dErr := connect.NewErrorDetail(&pluginv1.ExecuteResponse{Retryable: true}); dErr == nil {
			err.AddDetail(detail)
		}
		return nil, err

	case "permanent":
		err := connect.NewError(connect.CodeInternal, errors.New("permanent trouble"))
		if detail, dErr := connect.NewErrorDetail(&pluginv1.ExecuteResponse{Retryable: false}); dErr == nil {
			err.AddDetail(detail)
		}
		return nil, err

	case "denied":
		// A classified permanent failure whose cause is not bad inputs. The kind
		// the host reports has to say permission rather than blaming the inputs.
		err := connect.NewError(connect.CodePermissionDenied, errors.New("the backend refused"))
		if detail, dErr := connect.NewErrorDetail(&pluginv1.ExecuteResponse{Retryable: false}); dErr == nil {
			err.AddDetail(detail)
		}
		return nil, err

	case "huge-error":
		// A failure whose *error* body is enormous. Connect's read limit does
		// not cover the error path, so this is what proves the host bounds it
		// anyway.
		return nil, connect.NewError(connect.CodeInternal, errors.New(strings.Repeat("z", 512<<10)))

	case "secret-task":
		// By the time this runs, the host has already resolved `message` from
		// whatever reference it named into the value below — this fake never
		// sees a [flowstatev1.SecretRef]. It reports whether it received one at
		// all, and echoes it back the way a careless or hostile plugin might,
		// which is what proves the host scrubs its own response.
		received := req.Msg.GetTask().GetInputs()["message"].GetLiteral().GetStringValue()
		return connect.NewResponse(&pluginv1.ExecuteResponse{
			Outputs: &flowstatev1.Node_Outputs{
				NamedValues: map[string]*flowstatev1.Value{
					"received": flowstatev1.NewLiteral(received != ""),
					"echo":     flowstatev1.NewLiteral(received),
				},
			},
		}), nil

	case "secret-task-error":
		// Reflects the resolved value into an RPC failure, the way a backend's
		// own error message might quote a request back — the hazard the host's
		// scrubbing of the *error* path exists for, mirroring what the http
		// task's own scrubber protects against a reflecting server.
		received := req.Msg.GetTask().GetInputs()["message"].GetLiteral().GetStringValue()
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("backend said: %s", received))
	}

	// Echo back what came in, plus what the request carried about the workload,
	// so a test can prove those crossed the boundary.
	message := req.Msg.GetTask().GetInputs()["message"].GetLiteral().GetStringValue()

	return connect.NewResponse(&pluginv1.ExecuteResponse{
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

// ExecuteStream never sends anything and never returns on its own for the
// "hang-stream" mode: it blocks until its context ends, the way a plugin
// stuck mid-task — or one deliberately holding the call open — would. It
// exists for [TestTaskServiceExecuteStreamIsBoundedByCallTimeout], which
// proves [taskService.ExecuteStream] (service.go) cannot be held open past
// Config.CallTimeout the way unary Execute's "slow" case above proves for
// Execute.
func (s *fakeTaskService) ExecuteStream(ctx context.Context, req *connect.Request[pluginv1.ExecuteStreamRequest], stream *connect.ServerStream[pluginv1.ExecuteStreamResponse]) error {
	switch s.mode {
	case "hang-stream":
		<-ctx.Done()
		return ctx.Err()

	case "identity-stream":
		// One progress frame first, so this is a real stream and not a unary
		// call wearing a stream's clothes, and then the terminal response
		// echoing what the *streaming* request said about the workload. A host
		// that dropped or mixed up either field on the way from ExecuteRequest
		// to ExecuteStreamRequest shows up here as a pair that does not match
		// the caller's own.
		if err := stream.Send(&pluginv1.ExecuteStreamResponse{
			Message: &pluginv1.ExecuteStreamResponse_Progress{
				Progress: &pluginv1.TaskProgress{Phase: pluginv1.TaskPhase_TASK_PHASE_REQUESTING},
			},
		}); err != nil {
			return err
		}

		return stream.Send(&pluginv1.ExecuteStreamResponse{
			Message: &pluginv1.ExecuteStreamResponse_Response{
				Response: &pluginv1.ExecuteResponse{
					Outputs: &flowstatev1.Node_Outputs{
						NamedValues: map[string]*flowstatev1.Value{
							"result":    flowstatev1.NewLiteral(req.Msg.GetTask().GetInputs()["message"].GetLiteral().GetStringValue()),
							"namespace": flowstatev1.NewLiteral(req.Msg.GetNamespace()),
							"subject":   flowstatev1.NewLiteral(req.Msg.GetIdentity().GetSubject()),
							"identity_namespace": flowstatev1.NewLiteral(
								req.Msg.GetIdentity().GetNamespace()),
						},
					},
				},
			},
		})
	}

	return connect.NewError(connect.CodeUnimplemented, errors.New("this fake does not stream"))
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
func newCapturingLogger(t *testing.T, into *capturedLogs) *slog.Logger {
	t.Helper()

	return slog.New(slog.NewTextHandler(
		io.MultiWriter(into, testWriter{t}),
		&slog.HandlerOptions{Level: slog.LevelDebug},
	))
}

// capturedLogs synchronizes writes from asynchronous log pumps with test
// assertions that inspect the captured text.
type capturedLogs struct {
	mu      sync.Mutex
	builder strings.Builder
}

func (c *capturedLogs) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.builder.Write(p)
}

func (c *capturedLogs) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.builder.String()
}

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
