// Package sdk turns a manifest and a few functions into a Flowstate plugin.
//
// A plugin is a separate process the engine launches, hands a socket, and talks
// to over Connect RPC. Everything about that — checking the magic cookie,
// negotiating the protocol version, listening on the assigned socket with the
// right permissions, authenticating the host on every request, printing the one
// handshake line and then keeping off stdout, exiting when the host goes away —
// is the same for every plugin, so it is here rather than in every plugin.
//
// What is left for an author is what their plugin actually does:
//
//	func main() {
//		sdk.Main(sdk.Plugin{
//			Name:        "example",
//			Version:     "0.1.0",
//			Description: "An example plugin.",
//			Secrets: &sdk.Secrets{
//				Schemes: []string{"example"},
//				Resolve: resolve,
//			},
//			Tasks: []sdk.Task{{
//				Name:    "example_greet",
//				Summary: "Greet someone by name.",
//				Input:   &examplev1.GreetInputs{},
//				Output:  &examplev1.GreetOutputs{},
//				Fn:      greet,
//			}},
//		})
//	}
//
// The manifest the engine sees is derived from that value, which is what keeps a
// plugin from advertising something it did not implement: capabilities come from
// which fields are set, schemes from the resolver, and each task's input and
// output descriptors from the messages themselves. There is no second place to
// keep in sync.
//
// # Classifying failures
//
// Whether a step is retried is decided by the error a plugin returns, and only
// the plugin knows whether its backend's failure was transient. Return errors
// through [NotFound], [PermissionDenied], [InvalidInput], [Unavailable], and
// [Failed] rather than as bare errors, so that the engine retries what is worth
// retrying and gives up on what is not.
//
// # Values a plugin must not leak
//
// A resolved secret crosses the process boundary in a response, and the schema
// requires that a plugin not log it and not put it in an error. This package
// cannot enforce that inside a resolver, so it holds to it on the paths it does
// control: nothing here logs a response, and the errors it constructs quote only
// what a caller sent.
//
// # Writing a plugin in another language
//
// Nothing about the protocol requires this package or Go. It is documented in
// the host package, [github.com/picatz/flowstate/pkg/flowstate/v1/plugin], and a
// plugin that implements it is a plugin whether or not it was written with this.
package sdk

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/durationpb"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowstatev1connect"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/internal/protocol"
)

// ErrNotLaunchedByHost reports that the process was not started by a Flowstate
// worker: the magic cookie was absent or wrong.
//
// It is the error behind the explanation [Main] prints when someone runs a
// plugin binary from a shell. That case is worth handling deliberately — without
// the check, a curious human gets a handshake line and then a process speaking a
// binary protocol at their terminal, with nothing to say what it is.
var ErrNotLaunchedByHost = errors.New("sdk: not launched by a Flowstate worker")

// ErrProtocolVersion reports that the host speaks no protocol version this
// plugin implements. It is refused at startup, as one clear message, rather than
// discovered later as an unexplained failure on some request.
var ErrProtocolVersion = errors.New("sdk: no protocol version in common with the host")

// Plugin is what a plugin is and what it can do.
//
// The capabilities the engine is told about are derived from it: setting
// [Plugin.Secrets] advertises secret resolution, and a non-empty [Plugin.Tasks]
// advertises tasks. A plugin therefore cannot advertise something it did not
// implement, which is one fewer thing to get wrong than a manifest written by
// hand beside the code it describes.
type Plugin struct {
	// Name identifies the plugin, and by convention matches the binary's
	// suffix, so flowstate-plugin-vault names itself "vault". It must be
	// lowercase letters, digits, and dashes.
	Name string

	// Version is the plugin's own version, for operator diagnostics. It is not
	// used for compatibility decisions.
	Version string

	// Description is one line about what the plugin does.
	Description string

	// Secrets, when set, makes this a plugin that resolves secret references.
	Secrets *Secrets

	// Tasks are the tasks this plugin provides.
	Tasks []Task

	// Health reports whether the plugin can serve. Leaving it nil reports
	// serving always, which is right for a plugin with nothing to be unable to
	// reach.
	//
	// A plugin that depends on something — a vault, an API — should implement
	// it and report [NotServing] when that dependency is unreachable, rather
	// than failing every request. The engine restarts a plugin that stops
	// answering and reports one that answers "not serving", because restarting
	// does not fix a dependency, and telling the two apart is the whole reason
	// the engine polls.
	Health HealthFunc
}

// Secrets is a plugin's secret resolution.
type Secrets struct {
	// Schemes are the reference schemes this plugin answers, such as "vault".
	// The engine refuses a plugin claiming a scheme another plugin claims.
	Schemes []string

	// Resolve returns the value a reference names.
	Resolve ResolveFunc
}

// SecretRequest is a reference to resolve.
type SecretRequest struct {
	// Scheme and Name are the reference. Scheme is always one of the plugin's
	// own; Name means whatever this plugin decides it means, including how it
	// expresses versions.
	Scheme string
	Name   string

	// Namespace is the tenant the requesting workload belongs to, established
	// from the authenticated caller rather than declared by the workload.
	//
	// A plugin serving several tenants must scope resolution by it. Ignoring it
	// is not a missing feature; it lets one tenant read another's secrets.
	Namespace string

	// Identity is who the requesting workload acts as, for a plugin that applies
	// its own authorization on top of the engine's. It carries no credentials
	// and may be nil.
	Identity *flowstatev1.WorkloadIdentity
}

// SecretResponse is a resolved secret.
type SecretResponse struct {
	// Value is the secret. It must not be logged and must not appear in an
	// error.
	Value []byte

	// ExpiresIn, when set, is how long the value stays valid, so the engine
	// caches it no longer than this plugin considers safe. Zero lets the engine
	// apply its own default.
	ExpiresIn time.Duration
}

// ResolveFunc resolves one secret reference.
type ResolveFunc func(ctx context.Context, req SecretRequest) (SecretResponse, error)

// Task is a task a plugin provides.
//
// It mirrors the engine's own task definition deliberately: a plugin task is
// meant to be indistinguishable from a built-in one to everything else in the
// system, and it is easier to keep that true when it is written the same way.
type Task struct {
	// Name is how a Flowfile refers to the task. It must be lowercase letters,
	// digits, and underscores, starting with a letter. Prefixing it with the
	// plugin's name — "example_greet" — avoids colliding with another plugin's
	// task, which the engine refuses.
	Name string

	// Summary is one line, shown by `flow tasks` and in editor completion.
	Summary string

	// Input and Output are zero values of the messages describing the task's
	// schema, such as &examplev1.GreetInputs{}.
	//
	// Their descriptors travel to the engine, which is what lets it validate a
	// workflow that uses this task, complete its fields in an editor, and
	// document it — all without compiling anything of this plugin's. Either may
	// be nil for a side that has no message.
	Input  proto.Message
	Output proto.Message

	// DeferredInputs names inputs whose expressions this task evaluates itself,
	// in a scope the workflow does not have.
	//
	// The engine resolves expression inputs before scheduling a step. That is
	// wrong for an input whose expression references something that exists only
	// once the task has run — a response body, say — since the workflow has no
	// such variable in scope and resolution would fail. Inputs named here are
	// passed through untouched.
	DeferredInputs []string

	// NeedsScope reports whether the task must receive prior step outputs and
	// the variables bound by enclosing control flow. A task that evaluates its
	// own expressions does; most tasks do not, and asking for it puts data on
	// the wire for nothing.
	NeedsScope bool

	// Fn executes the task.
	Fn TaskFunc
}

// TaskFunc executes one task, given its resolved inputs and the scope its own
// expressions are evaluated against. It has the same shape as the engine's own
// task functions.
type TaskFunc func(ctx context.Context, inputs map[string]*flowstatev1.Value, scope *flowstatev1.Scope) (*flowstatev1.Node_Outputs, error)

// HealthFunc reports whether the plugin can serve. Returning an error is
// reported to the operator as a non-serving status rather than failing the poll.
type HealthFunc func(ctx context.Context) error

// Main serves the plugin and never returns.
//
// It is the whole of a plugin's func main. A process started by a Flowstate
// worker serves until the worker stops it; one started any other way explains
// what it is and exits without serving.
func Main(p Plugin, opts ...Option) {
	err := Run(context.Background(), p, opts...)
	if err == nil {
		os.Exit(0)
	}

	if errors.Is(err, ErrNotLaunchedByHost) {
		explain(p)
		os.Exit(2)
	}

	fmt.Fprintf(os.Stderr, "%s: %v\n", p.Name, err)
	os.Exit(1)
}

// explain tells a human who ran the binary what it is.
//
// It goes to stderr, and nothing goes to stdout, because stdout is the
// handshake channel and a process that has not handshaked has nothing to say on
// it. The point of the magic cookie is to reach this instead of speaking a
// binary protocol into someone's terminal.
func explain(p Plugin) {
	name := p.Name
	if name == "" {
		name = "this program"
	}

	fmt.Fprintf(os.Stderr, `%[1]s is a Flowstate plugin, not a command.

Plugins are separate processes that a Flowstate worker launches, hands a socket
to serve on, and talks to over RPC. Run from a shell there is no worker, no
socket, and nothing to serve, so it stops here rather than doing something
confusing.

To use it, put it in a directory on a worker's plugin search path — the file has
to be named flowstate-plugin-%[1]s — and configure the worker to look there.

`, name)

	if p.Description != "" {
		fmt.Fprintf(os.Stderr, "What it does: %s\n", p.Description)
	}
	if p.Version != "" {
		fmt.Fprintf(os.Stderr, "Version: %s\n", p.Version)
	}
	if p.Secrets != nil && len(p.Secrets.Schemes) > 0 {
		fmt.Fprintf(os.Stderr, "Secret schemes: %v\n", p.Secrets.Schemes)
	}
	if len(p.Tasks) > 0 {
		names := make([]string, 0, len(p.Tasks))
		for _, t := range p.Tasks {
			names = append(names, t.Name)
		}
		fmt.Fprintf(os.Stderr, "Tasks: %v\n", names)
	}
}

// Option configures serving.
type Option func(*options)

type options struct {
	logger          *slog.Logger
	maxRequestBytes int
	shutdownTimeout time.Duration
}

// WithLogger sets where the plugin's own diagnostics go.
//
// They go to stderr regardless of what is passed here, because stderr is what
// the host captures and attributes to this plugin; a logger writing anywhere
// else writes where nothing is reading. What this changes is the format and the
// level.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) { o.logger = logger }
}

// WithMaxRequestBytes bounds a request from the host.
func WithMaxRequestBytes(n int) Option {
	return func(o *options) { o.maxRequestBytes = n }
}

// WithShutdownTimeout bounds how long in-flight requests get to finish once the
// plugin has been asked to stop.
func WithShutdownTimeout(d time.Duration) Option {
	return func(o *options) { o.shutdownTimeout = d }
}

// Default bounds applied to a plugin's own server.
const (
	// DefaultMaxRequestBytes bounds a request from the host, so that a plugin is
	// not made to allocate without limit by whatever is on the other end of its
	// socket.
	DefaultMaxRequestBytes = 4 << 20 // 4 MiB

	// DefaultShutdownTimeout is how long in-flight requests get to finish.
	DefaultShutdownTimeout = 10 * time.Second

	// readHeaderTimeout bounds the time between a connection opening and its
	// request headers arriving. There is no equivalent bound on the body or the
	// response, because a task may legitimately take a long time and the host's
	// own per-call deadline is what bounds that.
	readHeaderTimeout = 30 * time.Second
)

// Run serves the plugin until the host stops it, the process is signalled, or
// ctx is cancelled.
//
// It returns [ErrNotLaunchedByHost] when the magic cookie is absent, which is
// what [Main] turns into an explanation rather than a stack trace.
func Run(ctx context.Context, p Plugin, opts ...Option) error {
	cfg := options{
		maxRequestBytes: DefaultMaxRequestBytes,
		shutdownTimeout: DefaultShutdownTimeout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.logger == nil {
		cfg.logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	env, err := readEnvironment()
	if err != nil {
		return err
	}

	manifest, err := p.manifest()
	if err != nil {
		return err
	}

	// A plugin that would be refused by the engine should fail here, where the
	// message reaches whoever is building it, rather than at a worker's startup
	// where it reaches an operator who cannot fix it.
	if err := flowstatev1.Validate(manifest); err != nil {
		return fmt.Errorf("sdk: this plugin's own manifest is invalid: %w", err)
	}

	handler, err := p.handler(manifest, env.token, cfg)
	if err != nil {
		return err
	}

	listener, err := listen(env.socketPath)
	if err != nil {
		return err
	}
	defer listener.Close()

	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ErrorLog:          nil,
	}

	// Announce only once the listener exists, so that the host's next act —
	// dialing the socket — cannot race the socket's creation. This is the last
	// thing written to stdout, ever.
	if err := announce(env); err != nil {
		return err
	}

	// From here a stray fmt.Println in a plugin's own code would corrupt the
	// protocol, so stdout is pointed at stderr and stops being a hazard. This
	// covers Go code writing through os.Stdout; anything writing to file
	// descriptor 1 directly, such as linked C code, would still get through.
	os.Stdout = os.Stderr

	log := cfg.logger.With("plugin", p.Name)
	log.Info("serving", "socket", env.socketPath, "protocol", env.protocolVersion)

	return serve(ctx, server, listener, cfg.shutdownTimeout, log)
}

// serve runs the server until something asks it to stop, then shuts it down.
//
// Three things can ask, and all three have to work: the host closing the pipe it
// gave this process, which happens even when the host crashes without cleaning
// up; a signal, which is how the host asks politely; and the caller's context.
// A plugin that outlives its host is the failure this is here to prevent.
func serve(ctx context.Context, server *http.Server, listener net.Listener, timeout time.Duration, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	hostGone := watchHost()

	errs := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errs <- err
	}()

	var reason string
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		reason = "signalled"
	case <-hostGone:
		reason = "host exited"
	}

	log.Info("shutting down", "reason", reason)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		// In-flight requests did not finish in time. Closing is what makes this
		// process exit rather than wait on them; the host has already stopped
		// caring about their results.
		server.Close()
		log.Warn("shut down without waiting for in-flight requests", "error", err)
	}

	return <-errs
}

// watchHost returns a channel closed when the host process goes away.
//
// The host passes an inherited pipe whose write end only it holds. The operating
// system closes that end when the host exits, however it exits, so a read here
// returning EOF means the host is gone — including the case where it crashed and
// ran no cleanup at all. Nothing is ever written to the pipe; the read is only
// there to notice it close.
func watchHost() <-chan struct{} {
	gone := make(chan struct{})

	raw, ok := os.LookupEnv(protocol.HostFDEnv)
	if !ok {
		return gone
	}

	fd, err := strconv.Atoi(raw)
	if err != nil || fd < 0 {
		return gone
	}

	file := os.NewFile(uintptr(fd), "flowstate-host")
	if file == nil {
		return gone
	}

	go func() {
		defer close(gone)
		defer file.Close()
		io.Copy(io.Discard, file)
	}()

	return gone
}

// environment is what the host told this process at launch.
type environment struct {
	socketPath      string
	protocolVersion int

	// token is held in a closure rather than a field, so that printing this
	// struct cannot print the secret: fmt reaches a struct's fields by
	// reflection, and cannot reach a captured variable.
	token func() string
}

// readEnvironment reads and checks the launch environment.
func readEnvironment() (environment, error) {
	if os.Getenv(protocol.MagicCookieEnv) != protocol.MagicCookieValue {
		return environment{}, ErrNotLaunchedByHost
	}

	offered, err := protocol.ParseVersions(os.Getenv(protocol.VersionsEnv))
	if err != nil {
		return environment{}, fmt.Errorf("sdk: %s: %w", protocol.VersionsEnv, err)
	}

	version, ok := protocol.Negotiate(offered, []int{protocol.Version1})
	if !ok {
		return environment{}, fmt.Errorf(
			"%w: it offered %s, this plugin speaks %d",
			ErrProtocolVersion, protocol.FormatVersions(offered), protocol.Version1,
		)
	}

	socketPath := os.Getenv(protocol.SocketEnv)
	if socketPath == "" {
		return environment{}, fmt.Errorf("sdk: %s is not set", protocol.SocketEnv)
	}

	token := os.Getenv(protocol.TokenEnv)
	if token == "" {
		return environment{}, fmt.Errorf("sdk: %s is not set", protocol.TokenEnv)
	}

	// The environment is this process's own, but it is also readable by anything
	// that can read this process, so the token does not stay there any longer
	// than it has to.
	os.Unsetenv(protocol.TokenEnv)

	return environment{
		socketPath:      socketPath,
		protocolVersion: version,
		token:           func() string { return token },
	}, nil
}

// listen creates the plugin's socket.
//
// The host chose the path, inside a directory only it can enter, which is what
// actually keeps other users out: Linux checks a socket file's permissions but
// several BSD-derived systems do not, and enforce only the containing
// directory's. The 0600 set here is the second line for the platforms that
// honor it, and it is set explicitly because a umask would otherwise decide it.
func listen(socketPath string) (net.Listener, error) {
	// A leftover socket from a previous launch would make Listen fail with
	// "address already in use". The path is inside a directory the host just
	// created for this launch, so anything there is stale by construction.
	os.Remove(socketPath)

	listener, err := net.Listen(protocol.NetworkUnix, socketPath)
	if err != nil {
		return nil, fmt.Errorf("sdk: listening on %s: %w", socketPath, err)
	}

	if err := os.Chmod(socketPath, 0o600); err != nil {
		listener.Close()
		return nil, fmt.Errorf("sdk: securing %s: %w", socketPath, err)
	}

	return listener, nil
}

// announce prints the one line the host is waiting for.
func announce(env environment) error {
	handshake := protocol.Handshake{
		HandshakeVersion: protocol.HandshakeVersion,
		ProtocolVersion:  env.protocolVersion,
		Network:          protocol.NetworkUnix,
		Address:          env.socketPath,
	}

	if _, err := fmt.Fprintln(os.Stdout, handshake.String()); err != nil {
		return fmt.Errorf("sdk: announcing on stdout: %w", err)
	}

	return nil
}

// manifest builds what the engine is told about this plugin.
func (p Plugin) manifest() (*flowstatev1.PluginManifest, error) {
	manifest := &flowstatev1.PluginManifest{
		Name:        p.Name,
		Version:     p.Version,
		Description: p.Description,
	}

	if p.Secrets != nil {
		if p.Secrets.Resolve == nil {
			return nil, fmt.Errorf("sdk: Secrets is set but has no Resolve function")
		}
		if len(p.Secrets.Schemes) == 0 {
			return nil, fmt.Errorf("sdk: Secrets is set but claims no schemes, so no reference would ever reach it")
		}
		manifest.Capabilities = append(manifest.Capabilities, flowstatev1.Capability_CAPABILITY_SECRETS)
		manifest.Schemes = slices.Clone(p.Secrets.Schemes)
	}

	if len(p.Tasks) > 0 {
		manifest.Capabilities = append(manifest.Capabilities, flowstatev1.Capability_CAPABILITY_TASKS)
		for _, task := range p.Tasks {
			entry, err := task.manifest()
			if err != nil {
				return nil, err
			}
			manifest.Tasks = append(manifest.Tasks, entry)
		}
	}

	if len(manifest.Capabilities) == 0 {
		return nil, fmt.Errorf("sdk: this plugin implements nothing: set Secrets, Tasks, or both")
	}

	return manifest, nil
}

// manifest builds the engine's description of one task, including the serialized
// descriptors that let it validate a workflow using the task.
func (t Task) manifest() (*flowstatev1.TaskManifest, error) {
	if t.Fn == nil {
		return nil, fmt.Errorf("sdk: task %q has no Fn", t.Name)
	}

	inputDescriptor, inputMessage, err := describeMessage(t.Input)
	if err != nil {
		return nil, fmt.Errorf("sdk: task %q input: %w", t.Name, err)
	}

	outputDescriptor, outputMessage, err := describeMessage(t.Output)
	if err != nil {
		return nil, fmt.Errorf("sdk: task %q output: %w", t.Name, err)
	}

	return &flowstatev1.TaskManifest{
		Name:             t.Name,
		Summary:          t.Summary,
		InputDescriptor:  inputDescriptor,
		InputMessage:     inputMessage,
		OutputDescriptor: outputDescriptor,
		OutputMessage:    outputMessage,
		DeferredInputs:   slices.Clone(t.DeferredInputs),
		NeedsScope:       t.NeedsScope,
	}, nil
}

// describeMessage serializes a message's file descriptor and everything it
// imports that the engine does not already have.
//
// Dependencies the engine is known to have are left out deliberately, and the
// set of those is derived rather than listed: it is the transitive imports of
// flowstate's own schema files, which any engine that can talk to a plugin has
// compiled in. That keeps a descriptor small — a task whose input references a
// flowstate type would otherwise carry protobuf's, protovalidate's, and CEL's
// descriptors along with it — without hardcoding an assumption about the engine
// that could quietly stop being true.
func describeMessage(msg proto.Message) ([]byte, string, error) {
	if msg == nil {
		return nil, "", nil
	}

	descriptor := msg.ProtoReflect().Descriptor()
	fullName := string(descriptor.FullName())

	set := &descriptorpb.FileDescriptorSet{}
	seen := make(map[string]struct{})
	provided := hostProvidedFiles()

	var collect func(file protoreflect.FileDescriptor)
	collect = func(file protoreflect.FileDescriptor) {
		path := file.Path()
		if _, done := seen[path]; done {
			return
		}
		seen[path] = struct{}{}

		if _, known := provided[path]; known {
			return
		}

		imports := file.Imports()
		for i := range imports.Len() {
			collect(imports.Get(i).FileDescriptor)
		}

		set.File = append(set.File, protodesc.ToFileDescriptorProto(file))
	}

	collect(descriptor.ParentFile())

	if len(set.File) == 0 {
		// Every file this message needs is one the engine already has, so there
		// is nothing to send but the name.
		return nil, fullName, nil
	}

	raw, err := proto.Marshal(set)
	if err != nil {
		return nil, "", fmt.Errorf("serializing the descriptor of %s: %w", fullName, err)
	}

	return raw, fullName, nil
}

// hostProvidedFiles returns the descriptor paths any Flowstate engine has,
// computed as the transitive imports of flowstate's own schema.
var hostProvidedFiles = sync.OnceValue(func() map[string]struct{} {
	provided := make(map[string]struct{})

	var walk func(file protoreflect.FileDescriptor)
	walk = func(file protoreflect.FileDescriptor) {
		if _, done := provided[file.Path()]; done {
			return
		}
		provided[file.Path()] = struct{}{}

		imports := file.Imports()
		for i := range imports.Len() {
			walk(imports.Get(i).FileDescriptor)
		}
	}

	walk(flowstatev1.File_flowstate_v1_flowstate_proto)
	walk(flowstatev1.File_flowstate_v1_plugin_proto)

	return provided
})

// handler builds the HTTP handler serving this plugin's services.
func (p Plugin) handler(manifest *flowstatev1.PluginManifest, token func() string, cfg options) (http.Handler, error) {
	opts := []connect.HandlerOption{
		connect.WithReadMaxBytes(cfg.maxRequestBytes),
		connect.WithInterceptors(requireToken(token)),
	}

	mux := http.NewServeMux()

	path, handler := flowstatev1connect.NewPluginServiceHandler(&pluginService{
		manifest: manifest,
		health:   p.Health,
	}, opts...)
	mux.Handle(path, handler)

	if p.Secrets != nil {
		path, handler := flowstatev1connect.NewSecretServiceHandler(&secretService{
			schemes: p.Secrets.Schemes,
			resolve: p.Secrets.Resolve,
		}, opts...)
		mux.Handle(path, handler)
	}

	if len(p.Tasks) > 0 {
		byName := make(map[string]Task, len(p.Tasks))
		for _, task := range p.Tasks {
			if _, dup := byName[task.Name]; dup {
				return nil, fmt.Errorf("sdk: task %q is defined twice", task.Name)
			}
			byName[task.Name] = task
		}

		path, handler := flowstatev1connect.NewTaskServiceHandler(&taskService{tasks: byName}, opts...)
		mux.Handle(path, handler)
	}

	return mux, nil
}

// requireToken refuses a request that does not carry the per-launch secret.
//
// The socket's directory is what keeps other users out; this is what keeps
// anything that reaches the socket anyway from acting as the host. The
// comparison is constant time because the alternative leaks the token one byte
// at a time to whatever can retry.
func requireToken(token func() string) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			presented := req.Header().Get(protocol.TokenHeader)
			if subtle.ConstantTimeCompare([]byte(presented), []byte(token())) != 1 {
				return nil, connect.NewError(connect.CodePermissionDenied, errors.New(
					"this plugin serves only the worker that launched it"))
			}
			return next(ctx, req)
		}
	})
}

// pluginService implements the capability handshake every plugin must serve.
type pluginService struct {
	flowstatev1connect.UnimplementedPluginServiceHandler

	manifest *flowstatev1.PluginManifest
	health   HealthFunc
}

// Describe reports what the plugin is and can do.
func (s *pluginService) Describe(context.Context, *connect.Request[flowstatev1.DescribePluginRequest]) (*connect.Response[flowstatev1.DescribePluginResponse], error) {
	return connect.NewResponse(&flowstatev1.DescribePluginResponse{
		Manifest: s.manifest,
	}), nil
}

// Health reports whether the plugin can serve.
func (s *pluginService) Health(ctx context.Context, _ *connect.Request[flowstatev1.HealthRequest]) (*connect.Response[flowstatev1.HealthResponse], error) {
	if s.health == nil {
		return connect.NewResponse(&flowstatev1.HealthResponse{
			Status: flowstatev1.HealthResponse_STATUS_SERVING,
		}), nil
	}

	if err := s.health(ctx); err != nil {
		// Reported rather than returned as an RPC failure: a plugin that cannot
		// reach its backend is working correctly and saying so, and the engine
		// tells that apart from a plugin that has stopped answering. The message
		// is logged by the engine, so it must not carry credential material —
		// which is the plugin author's responsibility, since only they know what
		// their backend puts in an error.
		return connect.NewResponse(&flowstatev1.HealthResponse{
			Status:  flowstatev1.HealthResponse_STATUS_NOT_SERVING,
			Message: truncate(err.Error(), 1024),
		}), nil
	}

	return connect.NewResponse(&flowstatev1.HealthResponse{
		Status: flowstatev1.HealthResponse_STATUS_SERVING,
	}), nil
}

// secretService implements secret resolution.
type secretService struct {
	flowstatev1connect.UnimplementedSecretServiceHandler

	schemes []string
	resolve ResolveFunc
}

// Resolve returns the value a reference names.
func (s *secretService) Resolve(ctx context.Context, req *connect.Request[flowstatev1.ResolveSecretRequest]) (*connect.Response[flowstatev1.ResolveSecretResponse], error) {
	ref := req.Msg.GetRef()
	if ref == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("no reference"))
	}

	// A plugin answers for the schemes it advertised and no others, so that a
	// host bug or a confused caller cannot make it resolve something it never
	// claimed.
	if !slices.Contains(s.schemes, ref.GetScheme()) {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(
			"this plugin does not resolve %q", truncate(ref.GetScheme(), 32)))
	}

	resp, err := s.resolve(ctx, SecretRequest{
		Scheme:    ref.GetScheme(),
		Name:      ref.GetName(),
		Namespace: req.Msg.GetNamespace(),
		Identity:  req.Msg.GetIdentity(),
	})
	if err != nil {
		return nil, asConnectError(err)
	}

	if len(resp.Value) == 0 {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("no value"))
	}

	out := &flowstatev1.ResolveSecretResponse{Value: resp.Value}
	if resp.ExpiresIn > 0 {
		out.ExpiresIn = durationpb.New(resp.ExpiresIn)
	}

	return connect.NewResponse(out), nil
}

// taskService implements task execution.
type taskService struct {
	flowstatev1connect.UnimplementedTaskServiceHandler

	tasks map[string]Task
}

// Execute runs a task.
func (s *taskService) Execute(ctx context.Context, req *connect.Request[flowstatev1.ExecuteTaskRequest]) (*connect.Response[flowstatev1.ExecuteTaskResponse], error) {
	name := req.Msg.GetTask().GetName()

	task, ok := s.tasks[name]
	if !ok {
		return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf(
			"this plugin does not provide task %q", truncate(name, 64)))
	}

	outputs, err := task.Fn(ctx, req.Msg.GetTask().GetInputs(), req.Msg.GetScope())
	if err != nil {
		return nil, asConnectError(err)
	}

	return connect.NewResponse(&flowstatev1.ExecuteTaskResponse{Outputs: outputs}), nil
}

// truncate bounds text on its way into a response or an error.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
