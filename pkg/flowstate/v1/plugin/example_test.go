package plugin

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// examplePackage is the worked example plugin, built and run for real by the
// tests below.
const examplePackage = "github.com/picatz/flowstate/pkg/flowstate/v1/plugin/examples/flowstate-plugin-example"

// buildExample compiles the example plugin once per test run and returns a
// directory holding it, ready to be a search path entry.
//
// The fake plugins elsewhere in this package are this binary wearing different
// names, which is fast and hermetic but proves nothing about the SDK: they
// implement the protocol by hand. This one is a real, separately compiled plugin
// written the way an author would write it, so what it proves is that the SDK
// and the host actually agree — including the part hardest to get right, the
// plugin's own message descriptors travelling across and being reconstructed
// into something the engine can validate against.
var buildExample = sync.OnceValues(func() (string, error) {
	dir, err := os.MkdirTemp("", "fsex")
	if err != nil {
		return "", err
	}

	output := filepath.Join(dir, BinaryPrefix+"example")

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// Built from this package's own directory, which is inside the module, so
	// the module context is whatever the test is running under.
	cmd := exec.CommandContext(ctx, "go", "build", "-o", output, examplePackage)
	cmd.Dir = packageDir()

	if out, err := cmd.CombinedOutput(); err != nil {
		os.RemoveAll(dir)
		return "", errors.New("building the example plugin: " + err.Error() + ": " + string(out))
	}

	return dir, nil
})

// packageDir returns this package's directory, for running the compiler
// somewhere inside the module.
func packageDir() string {
	if wd, err := os.Getwd(); err == nil {
		return wd
	}
	return "."
}

// exampleHost builds the example plugin and opens a host over it.
func exampleHost(t *testing.T, extraEnv ...string) *Host {
	t.Helper()

	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so the example plugin cannot be built")
	}

	dir, err := buildExample()
	if err != nil {
		t.Fatalf("%v", err)
	}

	cfg := testConfig(t, dir)
	cfg.Env = extraEnv

	return openHost(t, cfg)
}

// TestExamplePluginAdvertisesBothCapabilities checks the multi-capability case
// the example exists to demonstrate: one process, one handshake, both a secrets
// backend and a task.
func TestExamplePluginAdvertisesBothCapabilities(t *testing.T) {
	t.Parallel()

	host := exampleHost(t)

	p, ok := host.Lookup("example")
	if !ok {
		t.Fatal("the example plugin was not launched")
	}

	if !p.HasCapability(flowstatev1.Capability_CAPABILITY_SECRETS) {
		t.Error("the example does not advertise secret resolution")
	}
	if !p.HasCapability(flowstatev1.Capability_CAPABILITY_TASKS) {
		t.Error("the example does not advertise tasks")
	}

	if schemes := p.Schemes(); len(schemes) != 1 || schemes[0] != "example" {
		t.Errorf("schemes = %v, want [example]", schemes)
	}
	if tasks := p.Tasks(); len(tasks) != 1 || tasks[0].GetName() != "example_greet" {
		t.Errorf("tasks = %v, want [example_greet]", taskNames(p.Tasks()))
	}
	if version := p.Manifest().GetVersion(); version == "" {
		t.Error("the example reports no version")
	}
}

// TestExamplePluginTaskDescriptorsAreReconstructed is the one that matters most
// for tooling: the example's input and output messages are defined in its own
// schema, which this test binary does not import, so the descriptors the engine
// ends up with can only have come across the wire and been rebuilt.
func TestExamplePluginTaskDescriptorsAreReconstructed(t *testing.T) {
	t.Parallel()

	host := exampleHost(t)

	defs := host.TaskDefs()
	if len(defs) != 1 {
		t.Fatalf("host provides %d tasks, want 1", len(defs))
	}

	def := defs[0]

	if def.Inputs == nil {
		t.Fatal("the task has no input descriptor, so nothing could validate a workflow using it")
	}
	if got := string(def.Inputs.FullName()); got != "example.v1.GreetInputs" {
		t.Errorf("input message = %q, want %q", got, "example.v1.GreetInputs")
	}

	// The field names are what an editor completes and what the engine checks a
	// workflow's inputs against.
	for _, field := range []string{"name", "greeting"} {
		if def.Inputs.Fields().ByName(protoreflect.Name(field)) == nil {
			t.Errorf("input descriptor has no field %q", field)
		}
	}

	if def.Outputs == nil {
		t.Fatal("the task has no output descriptor")
	}
	if got := string(def.Outputs.FullName()); got != "example.v1.GreetOutputs" {
		t.Errorf("output message = %q, want %q", got, "example.v1.GreetOutputs")
	}
	for _, field := range []string{"message", "length"} {
		if def.Outputs.Fields().ByName(protoreflect.Name(field)) == nil {
			t.Errorf("output descriptor has no field %q", field)
		}
	}

	if def.Summary == "" {
		t.Error("the task has no summary, which is what `flow tasks` shows")
	}
}

// TestExamplePluginTaskRuns runs the example's task through the engine's own
// registry, which is the whole path a workflow step would take.
func TestExamplePluginTaskRuns(t *testing.T) {
	t.Parallel()

	host := exampleHost(t)

	registry := flowstatev1.NewRegistry()
	if err := host.Register(registry, nil); err != nil {
		t.Fatalf("Register: %v", err)
	}

	def, ok := registry.Lookup("example_greet")
	if !ok {
		t.Fatal("the example's task is not in the registry")
	}

	tests := []struct {
		name    string
		inputs  map[string]*flowstatev1.Value
		want    string
		wantErr bool
	}{
		{
			name:   "with a default greeting",
			inputs: map[string]*flowstatev1.Value{"name": flowstatev1.NewLiteral("world")},
			want:   "Hello, world!",
		},
		{
			name: "with a greeting of its own",
			inputs: map[string]*flowstatev1.Value{
				"name":     flowstatev1.NewLiteral("Kent"),
				"greeting": flowstatev1.NewLiteral("Good morning"),
			},
			want: "Good morning, Kent!",
		},
		{
			name:    "with a required input missing",
			inputs:  map[string]*flowstatev1.Value{},
			wantErr: true,
		},
		{
			name:    "with an input of the wrong type",
			inputs:  map[string]*flowstatev1.Value{"name": flowstatev1.NewLiteral(42)},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outputs, err := def.Fn(t.Context(), test.inputs, nil)

			if test.wantErr {
				if err == nil {
					t.Fatal("the task succeeded, want a failure")
				}
				// A bad input is permanent: retrying re-sends the same inputs.
				var taskErr *flowstatev1.TaskError
				if errors.As(err, &taskErr) && taskErr.Retryable() {
					t.Errorf("a bad input was classified %s, which is retryable", taskErr.Kind)
				}
				return
			}

			if err != nil {
				t.Fatalf("executing: %v", err)
			}

			got := outputs.GetNamedValues()
			if message := got["message"].GetLiteral().GetStringValue(); message != test.want {
				t.Errorf("message = %q, want %q", message, test.want)
			}
			if length := got["length"].GetLiteral().GetInt64Value(); length != int64(len(test.want)) {
				t.Errorf("length = %d, want %d", length, len(test.want))
			}
		})
	}
}

// TestExamplePluginResolvesSecrets checks the example's secrets backend,
// including the namespace scoping that a real backend has to get right.
func TestExamplePluginResolvesSecrets(t *testing.T) {
	t.Parallel()

	host := exampleHost(t,
		"EXAMPLE_SECRET_API_KEY=shared-value",
		"EXAMPLE_SECRET_TEAM_A_API_KEY=team-a-value",
		"EXAMPLE_SECRET_TEAM_B_API_KEY=team-b-value",
	)

	providers := host.SecretProviders()
	if len(providers) != 1 {
		t.Fatalf("host provides %d secret providers, want 1", len(providers))
	}

	store, err := secrets.NewStore(providers...)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	tests := []struct {
		name      string
		namespace string
		secret    string
		want      string
		wantErr   error
	}{
		{name: "in no namespace", secret: "api-key", want: "shared-value"},
		{name: "in one tenant", namespace: "team-a", secret: "api-key", want: "team-a-value"},
		{name: "in another tenant", namespace: "team-b", secret: "api-key", want: "team-b-value"},
		{name: "a secret that is not set", secret: "nope", wantErr: secrets.ErrNotFound},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver, err := store.For(secrets.Namespace(test.namespace))
			if err != nil {
				t.Fatalf("For: %v", err)
			}

			secret, err := resolver.Resolve(t.Context(), secrets.NewRef("example", test.secret))

			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("error = %v, want one wrapping %v", err, test.wantErr)
				}
				if secrets.Retryable(err) {
					t.Errorf("a missing secret was classified retryable")
				}
				return
			}

			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if !secret.EqualString(test.want) {
				t.Errorf("the resolved secret is not the expected one (length %d, want %d)",
					secret.Len(), len(test.want))
			}
			// One tenant's value must never be another's.
			for _, other := range []string{"team-a-value", "team-b-value", "shared-value"} {
				if other != test.want && secret.EqualString(other) {
					t.Errorf("resolving in namespace %q returned another tenant's secret", test.namespace)
				}
			}
		})
	}
}

// TestExamplePluginHealth checks that a plugin reporting its backend unreachable
// is reported rather than restarted, on the real SDK path.
func TestExamplePluginHealth(t *testing.T) {
	t.Parallel()

	t.Run("serving", func(t *testing.T) {
		t.Parallel()

		host := exampleHost(t)
		health := host.CheckHealth(t.Context())

		if got := health["example"].Status; got != HealthServing {
			t.Errorf("health = %v, want serving", got)
		}
	})

	t.Run("not serving", func(t *testing.T) {
		t.Parallel()

		host := exampleHost(t, "EXAMPLE_UNHEALTHY=1")

		p, _ := host.Lookup("example")
		pid := p.PID()

		health := p.CheckHealth(t.Context())
		if health.Status != HealthNotServing {
			t.Fatalf("health = %v, want not serving", health.Status)
		}
		if !strings.Contains(health.Message, "EXAMPLE_UNHEALTHY") {
			t.Errorf("health message = %q, want the plugin's explanation", health.Message)
		}

		// It came up and it is answering, so there is nothing to restart.
		if p.State() != StateReady {
			t.Errorf("state = %v, want ready", p.State())
		}
		if p.PID() != pid {
			t.Error("the plugin was restarted for reporting its backend unreachable")
		}
	})
}

// TestExamplePluginRunDirectlyExplainsItself checks what a curious human gets.
//
// Without the magic cookie this is a binary that prints a handshake line and then
// speaks Protobuf into a terminal. With it, it says what it is and stops — which
// is the entire reason the cookie exists.
func TestExamplePluginRunDirectlyExplainsItself(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("the Go toolchain is not available, so the example plugin cannot be built")
	}

	dir, err := buildExample()
	if err != nil {
		t.Fatalf("%v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, filepath.Join(dir, BinaryPrefix+"example"))
	cmd.Env = []string{} // No cookie: this is someone running it from a shell.

	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("running the plugin directly gave %v, want a non-zero exit", err)
	}
	if code := exitErr.ExitCode(); code != 2 {
		t.Errorf("exit code = %d, want 2", code)
	}

	// Nothing on stdout: that channel is the handshake's, and no handshake
	// happened. Anything printed there would be the confusing output this is
	// meant to avoid.
	if stdout.Len() != 0 {
		t.Errorf("it wrote %q to stdout, want nothing", stdout.String())
	}

	explanation := stderr.String()
	for _, want := range []string{
		"is a Flowstate plugin, not a command",
		"flowstate-plugin-example",
		"plugin search path",
	} {
		if !strings.Contains(explanation, want) {
			t.Errorf("the explanation does not mention %q:\n%s", want, explanation)
		}
	}
}

// TestExamplePluginDiesWithItsHost checks the direction of the lifecycle that
// nothing on the host side can enforce: a plugin whose host vanished without
// cleaning up must not keep running.
//
// This is the leaked-orphan case. The host holds the write end of a pipe the
// plugin inherited, so the operating system closes it whatever happens to the
// host, and the plugin sees EOF.
func TestExamplePluginDiesWithItsHost(t *testing.T) {
	t.Parallel()

	host := exampleHost(t)

	p, ok := host.Lookup("example")
	if !ok {
		t.Fatal("the example plugin was not launched")
	}
	pid := p.PID()

	// Close the pipe and nothing else: no signal, no kill. This stands in for a
	// host process that died, since the kernel does exactly this on exit.
	p.mu.RLock()
	pipe := p.inst.hostPipe
	p.mu.RUnlock()

	if err := pipe.Close(); err != nil {
		t.Fatalf("closing the host pipe: %v", err)
	}

	if !waitForProcessGone(t, pid, 15*time.Second) {
		t.Errorf("process %d outlived its host", pid)
	}
}
