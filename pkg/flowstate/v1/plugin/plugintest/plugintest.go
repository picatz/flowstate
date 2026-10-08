package plugintest

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/cel-go/cel"
	"google.golang.org/protobuf/types/known/structpb"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// issuer is the issuer every identity the kit installs names, so the plugin and
// the host-side secret policy read the same caller.
const issuer = "https://plugintest.invalid"

// buildTimeout bounds compiling a plugin. A cold module cache on a slow runner
// is the case it is sized for.
const buildTimeout = 4 * time.Minute

// Build compiles the plugin main package pkg into a fresh directory and returns
// the directory, ready to be handed to [Launch].
//
// pkg is whatever `go build` accepts, resolved from the test's working
// directory, which `go test` sets to the package under test: "." builds the
// plugin whose tests are running, and "../.." or a full import path builds
// another. name is the plugin's name; the binary is written as
// `flowstate-plugin-<name>`, because discovery reads the plugin's name off that
// suffix and ignores anything else (see [plugin.BinaryPrefix]).
//
// The directory is removed when the test ends. A failed build fails the test
// with the compiler's output.
func Build(t testing.TB, pkg, name string) string {
	t.Helper()

	dir := t.TempDir()
	output := filepath.Join(dir, plugin.BinaryPrefix+name)

	ctx, cancel := context.WithTimeout(context.Background(), buildTimeout)
	defer cancel()

	args := []string{"build", "-o", output, pkg}
	if out, err := exec.CommandContext(ctx, "go", args...).CombinedOutput(); err != nil {
		t.Fatalf("plugintest: building plugin %q: %v\n%s", pkg, err, out)
	}

	return dir
}

// Option configures [Launch].
type Option func(*options)

type options struct {
	config    plugin.Config
	secrets   map[string]string
	subject   string
	namespace string
}

// WithConfig adjusts the [plugin.Config] the host is opened with, after the
// kit's own defaults are applied. Use it for what a test of a particular
// deployment needs — pinned digests, a permitted-scheme list, an environment
// variable the plugin reads.
func WithConfig(adjust func(*plugin.Config)) Option {
	return func(o *options) { adjust(&o.config) }
}

// WithEnv adds KEY=VALUE entries to the plugin's environment. A plugin's
// environment is built from scratch, so nothing reaches it unless named here
// or forwarded by the operator's own configuration.
func WithEnv(env ...string) Option {
	return func(o *options) { o.config.Env = append(o.config.Env, env...) }
}

// WithSecrets installs the host-side secret store a worker would: each key is a
// reference such as "env:API_TOKEN" and each value what it resolves to, for a
// task input that declares it accepts a host secret reference. A reference not
// listed here does not resolve, which is what the task sees in production when
// the store has no such secret.
//
// Policy is permissive — every listed reference is allowed — because a plugin
// test is about the plugin. Whether an operator's policy would deny a
// reference is the host's test to write, not the plugin's.
func WithSecrets(refs map[string]string) Option {
	return func(o *options) { o.secrets = refs }
}

// WithIdentity sets the workload identity the calls run as, so a plugin that
// scopes by caller ([github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk.CallerFromContext]) or by tenant sees them. The
// default is an unnamespaced test workload.
func WithIdentity(subject, namespace string) Option {
	return func(o *options) { o.subject, o.namespace = subject, namespace }
}

// Session is a launched plugin host and the means to call what it serves.
type Session struct {
	host    *plugin.Host
	runtime flowstatev1.TaskRuntime
	subject string
	ns      string
	dir     string
	opts    []Option
}

// Launch starts every plugin found in dir through a real [plugin.Host] and
// registers its cleanup with the test.
//
// The host is configured for a test: health polling off (a test asks for
// health itself, see [Session.Health]), bounded timeouts so a hung plugin fails
// the test instead of the run, and the plugin's log output routed to t.Log. The
// search path is treated as trusted, since the kit just built it.
func Launch(t testing.TB, dir string, opts ...Option) *Session {
	t.Helper()

	o := options{config: plugin.Config{
		SearchPath:          []string{dir},
		HandshakeTimeout:    10 * time.Second,
		DescribeTimeout:     10 * time.Second,
		CallTimeout:         30 * time.Second,
		HealthTimeout:       5 * time.Second,
		ShutdownGrace:       5 * time.Second,
		DisableHealthChecks: true,
		Logger:              slog.New(slog.NewTextHandler(logWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}}
	for _, opt := range opts {
		opt(&o)
	}

	host, err := plugin.NewHost(o.config)
	if err != nil {
		t.Fatalf("plugintest: NewHost: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := host.Close(ctx); err != nil {
			t.Errorf("plugintest: closing the plugin host: %v", err)
		}
	})
	if err := host.Open(t.Context()); err != nil {
		t.Fatalf("plugintest: opening the plugin host: %v", err)
	}
	if len(host.Names()) == 0 {
		t.Fatalf("plugintest: no plugin was found in %s; discovery reads the name off the binary's "+
			"%q prefix, so a binary built under another name is invisible", dir, plugin.BinaryPrefix)
	}

	s := &Session{
		host:    host,
		subject: cmp.Or(o.subject, "plugintest"),
		ns:      o.namespace,
		dir:     dir,
		opts:    opts,
	}
	s.runtime = taskRuntime(t, o.secrets, s.subject, s.ns)

	return s
}

// Host is the underlying host, for what this kit does not wrap: the
// [plugin.Host.Catalog] a `flow plugins` would print, each plugin's state and
// restarts, or registering its tasks into a registry.
func (s *Session) Host() *plugin.Host { return s.host }

// Tasks lists the qualified name of every task the launched plugins serve,
// sorted.
func (s *Session) Tasks() []string {
	defs := s.host.TaskDefs()
	names := make([]string, len(defs))
	for i, def := range defs {
		names[i] = def.Name
	}
	slices.Sort(names)
	return names
}

// Call runs one task the way a workflow step would and returns its outputs or
// the classified error a step would fail with.
//
// name is qualified, `plugin.task`. Inputs are Go values converted the way a
// Flowfile literal is ([flowstatev1.NewValue]); a value that is already a
// [*flowstatev1.Value] passes through, which is how to supply a
// [SecretRef]. The error, when there is one, is a [*flowstatev1.TaskError]
// whose kind [ErrorKind] reads — the same classification a retry policy or a
// `flow run` failure message is built from.
func (s *Session) Call(ctx context.Context, name string, inputs map[string]any) (Outputs, error) {
	def, ok := s.task(name)
	if !ok {
		return Outputs{}, fmt.Errorf("plugintest: no task %q; serving %s", name, strings.Join(s.Tasks(), ", "))
	}

	ctx = flowstatev1.ContextWithTaskRuntime(ctx, s.runtime)
	identity := &flowstatev1.WorkloadIdentity{Principal: &flowstatev1.Principal{Subject: s.subject, Issuer: issuer, Namespace: s.ns}}
	ctx = plugin.NewContextWithIdentity(ctx, identity)
	out, err := def.Fn(ctx, flowstatev1.NewNamedValues(inputs), &flowstatev1.Scope{Identity: identity})
	if err != nil {
		return Outputs{}, err
	}

	return Outputs{node: out}, nil
}

// Run is [Session.Call] for a call that must succeed.
func (s *Session) Run(t testing.TB, name string, inputs map[string]any) Outputs {
	t.Helper()

	out, err := s.Call(t.Context(), name, inputs)
	if err != nil {
		t.Fatalf("plugintest: %s failed: %v", name, err)
	}
	return out
}

// Health polls every launched plugin once and returns the answers by plugin
// name.
func (s *Session) Health(ctx context.Context) map[string]plugin.Health {
	return s.host.CheckHealth(ctx)
}

// Resolve resolves a reference through the secret provider a launched plugin
// serves, as the engine would for a `${secret('scheme:name')}` whose scheme the
// plugin claims. It is how to test a plugin that *is* a secret provider; a
// task's own secret inputs are resolved by the host from [WithSecrets].
func (s *Session) Resolve(ctx context.Context, ref, namespace string) (string, error) {
	parsed, err := secrets.ParseRef(ref)
	if err != nil {
		return "", fmt.Errorf("plugintest: %w", err)
	}
	// The host derives the request's identity from the context, so the session's
	// is installed there; it still drops one whose namespace is not the request's.
	ctx = plugin.NewContextWithIdentity(ctx, &flowstatev1.WorkloadIdentity{Principal: &flowstatev1.Principal{Subject: s.subject, Issuer: issuer, Namespace: s.ns}})
	for _, provider := range s.host.SecretProviders() {
		if provider.Scheme() != parsed.GetScheme() {
			continue
		}
		secret, err := provider.Resolve(ctx, secrets.Request{Namespace: namespace, Ref: parsed})
		if err != nil {
			return "", err
		}
		return secret.Reveal(), nil
	}
	return "", fmt.Errorf("plugintest: no launched plugin serves the %q scheme", parsed.GetScheme())
}

func (s *Session) task(name string) (flowstatev1.TaskDef, bool) {
	for _, def := range s.host.TaskDefs() {
		if def.Name == name {
			return def, true
		}
	}
	return flowstatev1.TaskDef{}, false
}

// SecretRef builds an input value holding a whole host secret reference, such
// as SecretRef("env", "API_TOKEN"). The host resolves it from [WithSecrets]
// before the plugin runs, so the plugin sees a value and never a reference.
func SecretRef(scheme, name string) *flowstatev1.Value {
	return &flowstatev1.Value{Kind: &flowstatev1.Value_SecretRef{SecretRef: &flowstatev1.SecretRef{Scheme: scheme, Name: name}}}
}

// ErrorKind is the classification of a task failure, or the empty kind for a
// nil error or one that is not a task error — which a plugin task's failure
// never is, so an empty kind on a non-nil error is itself a finding.
func ErrorKind(err error) flowstatev1.ErrorKind {
	var taskErr *flowstatev1.TaskError
	if errors.As(err, &taskErr) {
		return taskErr.Kind
	}
	return ""
}

// Outputs is what a task returned.
type Outputs struct {
	node *flowstatev1.Node_Outputs
}

// Node is the raw outputs, for assertions this type does not wrap.
func (o Outputs) Node() *flowstatev1.Node_Outputs { return o.node }

// Names lists the returned output names, sorted.
func (o Outputs) Names() []string {
	return slices.Sorted(maps.Keys(o.node.GetNamedValues()))
}

// Lookup returns one output as a plain Go value — strings, bools, float64 for
// every number, []any, and map[string]any — and whether the task returned it.
func (o Outputs) Lookup(name string) (any, bool) {
	value, ok := o.node.GetNamedValues()[name]
	if !ok {
		return nil, false
	}
	literal := value.GetLiteral()
	if literal == nil {
		return nil, true
	}
	ref, err := cel.ValueToRefValue(flowstatev1.TypeAdapter, literal)
	if err != nil {
		return nil, true
	}
	if native, err := ref.ConvertToNative(reflect.TypeFor[*structpb.Value]()); err == nil {
		if v, ok := native.(*structpb.Value); ok {
			return v.AsInterface(), true
		}
	}
	return ref.Value(), true
}

// Get is [Outputs.Lookup] for an output the test requires.
func (o Outputs) Get(t testing.TB, name string) any {
	t.Helper()
	v, ok := o.Lookup(name)
	if !ok {
		t.Fatalf("plugintest: the task returned no output %q; it returned %v", name, o.Names())
	}
	return v
}

// String is [Outputs.Get] for a string output.
func (o Outputs) String(t testing.TB, name string) string {
	t.Helper()
	s, ok := o.Get(t, name).(string)
	if !ok {
		t.Fatalf("plugintest: output %q is %T, not a string", name, o.Get(t, name))
	}
	return s
}

// taskRuntime is the host-side runtime a worker installs before a step runs:
// the secret store and an allow-all policy over the listed references.
func taskRuntime(t testing.TB, refs map[string]string, subject, namespace string) flowstatev1.TaskRuntime {
	t.Helper()

	byScheme := map[string]staticProvider{}
	for ref, value := range refs {
		parsed, err := secrets.ParseRef(ref)
		if err != nil {
			t.Fatalf("plugintest: WithSecrets: %v", err)
		}
		p, ok := byScheme[parsed.GetScheme()]
		if !ok {
			p = staticProvider{scheme: parsed.GetScheme(), values: map[string]string{}}
			byScheme[parsed.GetScheme()] = p
		}
		p.values[parsed.GetName()] = value
	}

	providers := make([]secrets.Provider, 0, len(byScheme))
	for _, scheme := range slices.Sorted(maps.Keys(byScheme)) {
		providers = append(providers, byScheme[scheme])
	}
	store, err := secrets.NewStore(providers...)
	if err != nil {
		t.Fatalf("plugintest: secret store: %v", err)
	}
	policy, err := (auth.SecretAccessPolicy{Allow: []string{"true"}}).Compile()
	if err != nil {
		t.Fatalf("plugintest: secret policy: %v", err)
	}

	return flowstatev1.TaskRuntime{
		Store:  store,
		Policy: policy,
		Identity: auth.WorkloadIdentity{
			Subject: subject, Issuer: issuer, Namespace: namespace,
		},
		Step: auth.StepRef{Workflow: "plugintest", Run: "plugintest", Step: "step"},
	}
}

// staticProvider resolves a fixed set of names under one scheme.
type staticProvider struct {
	scheme string
	values map[string]string
}

func (p staticProvider) Scheme() string { return p.scheme }

func (p staticProvider) Resolve(_ context.Context, req secrets.Request) (secrets.Secret, error) {
	value, ok := p.values[req.Ref.GetName()]
	if !ok {
		return secrets.Secret{}, secrets.ErrNotFound
	}
	return secrets.NewSecret(req.Ref, value), nil
}

// logWriter sends the host's and the plugin's logs to the test, and drops a
// write that arrives after the test finished: a plugin's stderr pump can
// outlive the test that launched it by a moment.
type logWriter struct{ t testing.TB }

func (w logWriter) Write(p []byte) (int, error) {
	defer func() { _ = recover() }()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}
