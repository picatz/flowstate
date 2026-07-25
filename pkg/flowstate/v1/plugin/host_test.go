package plugin

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

// TestCloseLeavesNoProcess is the non-negotiable one: a host that has been
// closed has no plugin processes left.
//
// Everything else in this package is about correctness; this is about not
// leaving a credential-holding process running on someone's worker after the
// thing that owned it is gone.
func TestCloseLeavesNoProcess(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "ok", "two"))

	host, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	if err := host.Open(t.Context()); err != nil {
		t.Fatalf("Open: %v", err)
	}

	pids := make(map[string]int)
	for _, p := range host.Plugins() {
		pid := p.PID()
		if pid == 0 {
			t.Fatalf("plugin %q has no process", p.Name())
		}
		if !processAlive(pid) {
			t.Fatalf("plugin %q process %d is not running", p.Name(), pid)
		}
		pids[p.Name()] = pid
	}

	if len(pids) != 2 {
		t.Fatalf("launched %d plugins, want 2", len(pids))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := host.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for name, pid := range pids {
		if !waitForProcessGone(t, pid, 5*time.Second) {
			t.Errorf("plugin %q process %d is still running after Close", name, pid)
		}
	}
}

// TestCloseDuringCallLeavesNoProcess closes the host while a call is in flight,
// which is the shutdown that is easiest to get wrong: the process is busy, it is
// holding a request open, and something has to decide that the request loses.
func TestCloseDuringCallLeavesNoProcess(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "slow"))
	cfg.CallTimeout = 30 * time.Second // Long enough that the close is what ends it.

	host, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	if err := host.Open(t.Context()); err != nil {
		t.Fatalf("Open: %v", err)
	}

	p, ok := host.Lookup("slow")
	if !ok {
		t.Fatal("plugin was not launched")
	}
	pid := p.PID()

	defs := host.TaskDefs()
	if len(defs) != 1 {
		t.Fatalf("host provides %d tasks, want 1", len(defs))
	}

	done := make(chan error, 1)
	go func() {
		_, err := defs[0].Fn(context.Background(), nil, nil)
		done <- err
	}()

	// Let the call reach the plugin before pulling the process out from under
	// it, so that this tests a call in flight rather than one not yet sent.
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := host.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Error("the in-flight call succeeded, want a failure once its plugin was stopped")
		}
	case <-time.After(10 * time.Second):
		t.Error("the in-flight call never returned after its plugin was stopped")
	}

	if !waitForProcessGone(t, pid, 5*time.Second) {
		t.Errorf("process %d is still running after Close", pid)
	}
}

// TestCallCancellationDoesNotKillPlugin checks the other half of that
// distinction: cancelling one call must end that call and nothing else.
//
// The two are easy to conflate, and conflating them is expensive in both
// directions — a cancelled step that kills a plugin takes every other step's
// plugin with it, and a shutdown that only cancels calls leaves the process
// running.
func TestCallCancellationDoesNotKillPlugin(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "slow"))
	cfg.CallTimeout = 30 * time.Second

	host := openHost(t, cfg)

	p, ok := host.Lookup("slow")
	if !ok {
		t.Fatal("plugin was not launched")
	}
	pid := p.PID()

	callCtx, cancelCall := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := host.TaskDefs()[0].Fn(callCtx, nil, nil)
		done <- err
	}()

	time.Sleep(200 * time.Millisecond)
	cancelCall()

	select {
	case err := <-done:
		if err == nil {
			t.Error("the cancelled call succeeded, want a failure")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the cancelled call never returned")
	}

	// The plugin is still there, and still usable by everything else.
	if !processAlive(pid) {
		t.Fatalf("cancelling one call killed the plugin process %d", pid)
	}
	if health := p.CheckHealth(t.Context()); health.Status != HealthServing {
		t.Errorf("health after a cancelled call = %v (%v), want serving", health.Status, health.Err)
	}
}

// TestDuplicateSchemeRefused checks that two plugins claiming one secret scheme
// is a refusal rather than a race between them.
func TestDuplicateSchemeRefused(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "dup-one", "dup-two"))

	host, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer host.Close(context.Background())

	err = host.Open(t.Context())
	if !errors.Is(err, ErrDuplicateScheme) {
		t.Fatalf("Open error = %v, want one wrapping %v", err, ErrDuplicateScheme)
	}

	// Both plugins are named, since an operator has to know which two to look
	// at, and neither is left running.
	message := err.Error()
	for _, name := range []string{"dup-one", "dup-two", "shared"} {
		if !strings.Contains(message, name) {
			t.Errorf("error = %q, want it to name %q", message, name)
		}
	}

	if got := len(host.Plugins()); got != 0 {
		t.Errorf("host holds %d plugins after refusing a conflict, want 0", got)
	}
}

// TestSchemeNotPermitted checks that a deployment listing its permitted schemes
// gets those and nothing a binary dropped into the search path adds.
func TestSchemeNotPermitted(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "not-permitted"))
	cfg.PermittedSchemes = []string{"allowed"}

	host, err := NewHost(cfg)
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer host.Close(context.Background())

	err = host.Open(t.Context())
	if !errors.Is(err, ErrSchemeNotPermitted) {
		t.Fatalf("Open error = %v, want one wrapping %v", err, ErrSchemeNotPermitted)
	}
	if !strings.Contains(err.Error(), "forbidden") {
		t.Errorf("error = %q, want it to name the refused scheme", err.Error())
	}
}

// TestUnknownCapabilityIsIgnoredNotRefused checks the schema's additive rule: a
// plugin that also serves something newer keeps working against this host.
func TestUnknownCapabilityIsIgnoredNotRefused(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "future-cap")))

	p, ok := host.Lookup("future-cap")
	if !ok {
		t.Fatal("a plugin advertising an unknown capability was refused, want it accepted")
	}

	if schemes := p.Schemes(); len(schemes) != 1 || schemes[0] != "future" {
		t.Errorf("schemes = %v, want [future]", schemes)
	}

	// Nothing is dispatched for the capability the host does not know, which is
	// what makes ignoring it still fail closed.
	if p.HasCapability(flowstatev1.Capability(9999)) {
		// The manifest does list it; what matters is that it produced no
		// adapters. Tasks are the only other adapter, and it advertised none.
		if len(host.TaskDefs()) != 0 {
			t.Errorf("host built %d task adapters for a plugin that advertises no tasks", len(host.TaskDefs()))
		}
	}
}

// TestSecretProviderResolves checks the secrets adapter end to end, including
// that the tenant boundary crosses the process boundary intact.
func TestSecretProviderResolves(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "ok")))

	providers := host.SecretProviders()
	if len(providers) != 1 {
		t.Fatalf("host provides %d secret providers, want 1", len(providers))
	}

	provider := providers[0]
	if provider.Scheme() != "ok" {
		t.Fatalf("scheme = %q, want %q", provider.Scheme(), "ok")
	}

	// The registry the engine actually uses must accept it, since a provider the
	// store cannot register is a provider nothing can reach.
	registry := secrets.NewRegistry()
	if err := registry.Register(provider); err != nil {
		t.Fatalf("registering the plugin provider: %v", err)
	}

	store, err := secrets.NewStoreFromRegistry(registry)
	if err != nil {
		t.Fatalf("NewStoreFromRegistry: %v", err)
	}

	tests := []struct {
		name      string
		namespace string
		secret    string
		want      string
		wantErr   error
	}{
		{
			name:   "resolves a secret",
			secret: "api-key",
			want:   "value-for-api-key",
		},
		{
			// The plugin builds the namespace into its answer, so this proves it
			// arrived rather than being dropped on the way.
			name:      "carries the namespace",
			namespace: "team-a",
			secret:    "api-key",
			want:      "value-for-api-key-in-team-a",
		},
		{
			name:      "the same reference differs by namespace",
			namespace: "team-b",
			secret:    "api-key",
			want:      "value-for-api-key-in-team-b",
		},
		{
			name:    "a missing secret is permanent",
			secret:  "missing",
			wantErr: secrets.ErrNotFound,
		},
		{
			name:    "a refused read is permanent",
			secret:  "refused",
			wantErr: secrets.ErrPermission,
		},
		{
			name:    "an unreachable backend is transient",
			secret:  "down",
			wantErr: secrets.ErrUnavailable,
		},
		{
			name:    "an empty value is refused",
			secret:  "empty",
			wantErr: secrets.ErrEmpty,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver, err := store.For(secrets.Namespace(test.namespace))
			if err != nil {
				t.Fatalf("For(%q): %v", test.namespace, err)
			}

			secret, err := resolver.Resolve(t.Context(), secrets.NewRef("ok", test.secret))

			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("Resolve error = %v, want one wrapping %v", err, test.wantErr)
				}
				// Classification is what the retry decision reads.
				if got, want := secrets.Retryable(err), errors.Is(test.wantErr, secrets.ErrUnavailable); got != want {
					t.Errorf("Retryable = %v, want %v", got, want)
				}
				return
			}

			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if !secret.EqualString(test.want) {
				t.Errorf("resolved a secret of length %d, want the one of length %d", secret.Len(), len(test.want))
			}
		})
	}
}

// TestSecretIdentityCrossesBoundary checks that a plugin is told who the
// workload acts as, and only when that identity belongs to the namespace the
// resolution is happening in.
func TestSecretIdentityCrossesBoundary(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "ok")))
	provider := host.SecretProviders()[0]

	tests := []struct {
		name      string
		namespace string
		identity  *flowstatev1.WorkloadIdentity
		want      string
	}{
		{
			name: "no identity",
			want: "value-for-k",
		},
		{
			name:      "identity in the same namespace",
			namespace: "team-a",
			identity:  &flowstatev1.WorkloadIdentity{Subject: "ci", Namespace: "team-a"},
			want:      "value-for-k-in-team-a-as-ci",
		},
		{
			name:      "identity with no namespace of its own",
			namespace: "team-a",
			identity:  &flowstatev1.WorkloadIdentity{Subject: "ci"},
			want:      "value-for-k-in-team-a-as-ci",
		},
		{
			// An identity claiming another tenant is dropped rather than
			// forwarded: sending a plugin one tenant's identity alongside
			// another's namespace invites it to authorize against the wrong one.
			name:      "identity from another namespace is not forwarded",
			namespace: "team-a",
			identity:  &flowstatev1.WorkloadIdentity{Subject: "ci", Namespace: "team-b"},
			want:      "value-for-k-in-team-a",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			if test.identity != nil {
				ctx = NewContextWithIdentity(ctx, test.identity)
			}

			secret, err := provider.Resolve(ctx, secrets.Request{
				Namespace: test.namespace,
				Ref:       secrets.NewRef("ok", "k"),
			})
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if !secret.EqualString(test.want) {
				t.Errorf("the plugin did not receive what was expected; got a value of length %d, want %d",
					secret.Len(), len(test.want))
			}
		})
	}
}

// TestSecretValueDoesNotEscape checks the containment rule for the one value
// that must not travel: a secret crossing this boundary must not turn up in a
// log line or an error.
//
// It checks the shapes rather than only the value, per the lesson in the secrets
// package: a redacting method protects a value printed directly and does nothing
// for one sitting in another struct.
func TestSecretValueDoesNotEscape(t *testing.T) {
	t.Parallel()

	var logs strings.Builder
	cfg := testConfig(t, pluginDir(t, "ok"))
	cfg.Logger = newCapturingLogger(t, &logs)

	host := openHost(t, cfg)
	provider := host.SecretProviders()[0]

	secret, err := provider.Resolve(t.Context(), secrets.Request{
		Ref: secrets.NewRef("ok", "api-key"),
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	const plaintext = "value-for-api-key"
	if !secret.EqualString(plaintext) {
		t.Fatalf("the fake plugin did not return the expected value")
	}

	holder := struct {
		Secret secrets.Secret
		hidden secrets.Secret
	}{Secret: secret, hidden: secret}

	rendered := []string{
		fmtSprint("%v", secret), fmtSprint("%s", secret), fmtSprint("%q", secret), fmtSprint("%#v", secret),
		fmtSprint("%v", holder), fmtSprint("%+v", holder), fmtSprint("%#v", holder),
		fmtSprint("%v", []any{secret}), fmtSprint("%v", map[string]any{"s": secret}),
	}

	for _, text := range rendered {
		if strings.Contains(text, plaintext) {
			t.Errorf("a formatted secret exposed its value: %s", text)
		}
	}

	// And a failure path: the error naming a missing secret must not carry
	// anything the plugin returned.
	if _, err := provider.Resolve(t.Context(), secrets.Request{Ref: secrets.NewRef("ok", "missing")}); err != nil {
		if strings.Contains(err.Error(), plaintext) {
			t.Errorf("an error carried a secret value: %v", err)
		}
	}

	// Nothing the host logged while resolving mentions the value either.
	if strings.Contains(logs.String(), plaintext) {
		t.Errorf("the host logged a secret value")
	}
}

// TestTaskDefExecutes checks the task adapter, including that a plugin task
// registers in the engine's own registry and runs from it.
func TestTaskDefExecutes(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "ok")))

	defs := host.TaskDefs()
	if len(defs) != 1 {
		t.Fatalf("host provides %d tasks, want 1", len(defs))
	}

	def := defs[0]
	if def.Name != "ok_task" {
		t.Errorf("task name = %q, want %q", def.Name, "ok_task")
	}

	// A plugin task carries descriptors like a built-in one, which is what lets
	// validation and tooling read it the same way.
	if def.Inputs == nil || def.Inputs.FullName() != "flowstate.v1.Task.Echo.Inputs" {
		t.Errorf("input descriptor = %v, want flowstate.v1.Task.Echo.Inputs", def.Inputs)
	}
	if def.Outputs == nil {
		t.Error("task has no output descriptor")
	}

	registry := flowstatev1.NewRegistry()
	if err := registry.Register(def); err != nil {
		t.Fatalf("registering the plugin task: %v", err)
	}

	registered, ok := registry.Lookup("ok_task")
	if !ok {
		t.Fatal("the registered plugin task cannot be looked up")
	}

	ctx := NewContextWithIdentity(t.Context(), &flowstatev1.WorkloadIdentity{
		Subject:   "ci",
		Namespace: "team-a",
	})

	outputs, err := registered.Fn(ctx, map[string]*flowstatev1.Value{
		"message": flowstatev1.NewLiteral("hello"),
	}, nil)
	if err != nil {
		t.Fatalf("executing the plugin task: %v", err)
	}

	got := outputs.GetNamedValues()
	if result := got["result"].GetLiteral().GetStringValue(); result != "hello" {
		t.Errorf("result = %q, want %q", result, "hello")
	}
	if ns := got["namespace"].GetLiteral().GetStringValue(); ns != "team-a" {
		t.Errorf("the plugin received namespace %q, want %q", ns, "team-a")
	}
	if subject := got["subject"].GetLiteral().GetStringValue(); subject != "ci" {
		t.Errorf("the plugin received subject %q, want %q", subject, "ci")
	}
	// The manifest did not declare needs_scope, so no scope should travel.
	if got["has_scope"].GetLiteral().GetBoolValue() {
		t.Error("a scope was sent to a task that did not ask for one")
	}
}

// TestTaskErrorClassification checks that a plugin's own verdict on retrying is
// what the engine acts on, since only the plugin knows whether its backend's
// failure was transient.
func TestTaskErrorClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		mode          string
		wantRetryable bool
	}{
		{name: "plugin says the failure is transient", mode: "retryable", wantRetryable: true},
		{name: "plugin says the failure is permanent", mode: "permanent", wantRetryable: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			host := openHost(t, testConfig(t, pluginDir(t, test.mode)))

			_, err := host.TaskDefs()[0].Fn(t.Context(), nil, nil)
			if err == nil {
				t.Fatal("the task succeeded, want a failure")
			}

			var taskErr *flowstatev1.TaskError
			if !errors.As(err, &taskErr) {
				t.Fatalf("error = %v, want a *flowstatev1.TaskError so the engine can classify it", err)
			}

			if got := taskErr.Retryable(); got != test.wantRetryable {
				t.Errorf("Retryable = %v (kind %s), want %v", got, taskErr.Kind, test.wantRetryable)
			}
		})
	}
}

// TestOversizedResponseIsRefused checks that a plugin cannot make the host
// allocate without limit, which is the one thing a plugin fully controls the
// size of.
func TestOversizedResponseIsRefused(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "huge"))
	cfg.MaxResponseBytes = 8 << 10 // Smaller than the 256 KiB the fake returns.

	host := openHost(t, cfg)

	t.Run("task", func(t *testing.T) {
		_, err := host.TaskDefs()[0].Fn(t.Context(), nil, nil)
		if err == nil {
			t.Fatal("an oversized response was accepted")
		}
		if !strings.Contains(err.Error(), "message is larger than configured max") &&
			!strings.Contains(err.Error(), "resource_exhausted") {
			t.Errorf("error = %v, want one about the response being too large", err)
		}
	})

	t.Run("secret", func(t *testing.T) {
		_, err := host.SecretProviders()[0].Resolve(t.Context(), secrets.Request{
			Ref: secrets.NewRef("huge", "big"),
		})
		if err == nil {
			t.Fatal("an oversized response was accepted")
		}
		if !errors.Is(err, secrets.ErrTooLarge) {
			t.Errorf("error = %v, want one wrapping %v", err, secrets.ErrTooLarge)
		}
	})
}

// TestHealthNotServingDoesNotRestart checks the distinction the proto's comments
// call for: a plugin saying its backend is unreachable is working, and
// restarting it would replace a process that is answering correctly with an
// identical one that will answer the same way.
func TestHealthNotServingDoesNotRestart(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "sick"))
	cfg.DisableHealthChecks = false
	cfg.HealthInterval = 50 * time.Millisecond

	host := openHost(t, cfg)

	p, ok := host.Lookup("sick")
	if !ok {
		t.Fatal("plugin was not launched")
	}
	pid := p.PID()

	if !waitFor(t, 5*time.Second, func() bool { return p.Health().Status == HealthNotServing }) {
		t.Fatalf("health = %v, want not serving", p.Health().Status)
	}

	if message := p.Health().Message; !strings.Contains(message, "unreachable") {
		t.Errorf("health message = %q, want the plugin's explanation", message)
	}

	// Several more polls go by, and the plugin is left alone throughout.
	time.Sleep(300 * time.Millisecond)

	if p.PID() != pid {
		t.Errorf("plugin was restarted (pid %d became %d) for reporting its backend unreachable", pid, p.PID())
	}
	if got := p.Restarts(); got != 0 {
		t.Errorf("restarts = %d, want 0", got)
	}
	if state := p.State(); state != StateReady {
		t.Errorf("state = %v, want ready", state)
	}
}

// TestCrashLoopStopsBeingRelaunched checks that a plugin failing every launch is
// given up on and reported, rather than being relaunched forever.
func TestCrashLoopStopsBeingRelaunched(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "short-lived"))
	cfg.MaxRestarts = 2
	cfg.RestartBackoff = 10 * time.Millisecond
	cfg.MaxRestartBackoff = 20 * time.Millisecond

	host := openHost(t, cfg)

	p, ok := host.Lookup("short-lived")
	if !ok {
		t.Fatal("plugin was not launched")
	}

	if !waitFor(t, 15*time.Second, func() bool { return p.State() == StateFailed }) {
		t.Fatalf("state = %v after waiting, want failed", p.State())
	}

	if got := p.Restarts(); got > cfg.MaxRestarts+1 {
		t.Errorf("relaunched %d times, want no more than %d", got, cfg.MaxRestarts+1)
	}

	if err := p.LastError(); err == nil {
		t.Error("a plugin that was given up on reports no reason")
	}

	// A call now fails fast with something retryable, so a step's own policy
	// decides rather than the call blocking on a plugin that is never coming
	// back.
	_, err := host.TaskDefs()[0].Fn(t.Context(), nil, nil)
	if err == nil {
		t.Fatal("a call to a failed plugin succeeded")
	}
	if !strings.Contains(err.Error(), "unavailable") {
		t.Errorf("error = %v, want one reporting the plugin unavailable", err)
	}

	// Nothing is left running.
	if pid := p.PID(); pid != 0 && processAlive(pid) {
		t.Errorf("process %d is still running after the plugin was given up on", pid)
	}
}

// TestRestartRecovers checks the other side of that: a plugin that exits once is
// relaunched and keeps working.
func TestRestartRecovers(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, pluginDir(t, "short-lived"))
	cfg.MaxRestarts = 20
	cfg.RestartBackoff = 10 * time.Millisecond
	cfg.MaxRestartBackoff = 20 * time.Millisecond

	host := openHost(t, cfg)

	p, ok := host.Lookup("short-lived")
	if !ok {
		t.Fatal("plugin was not launched")
	}
	first := p.PID()

	// It dies every 150ms, so a relaunch with a different pid proves the
	// supervisor brought it back rather than leaving it dead.
	if !waitFor(t, 15*time.Second, func() bool {
		return p.Restarts() > 0 && p.State() == StateReady && p.PID() != first && p.PID() != 0
	}) {
		t.Fatalf("plugin was not relaunched: state=%v restarts=%d pid=%d (was %d): %v",
			p.State(), p.Restarts(), p.PID(), first, p.LastError())
	}

	if !waitForProcessGone(t, first, 5*time.Second) {
		t.Errorf("the original process %d was left running after a restart", first)
	}
}

// TestHostRegistersIntoEngineRegistries checks the wiring a worker actually
// writes: plugin tasks and providers going into the registries the rest of the
// engine reads.
func TestHostRegistersIntoEngineRegistries(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "ok", "two")))

	tasks := flowstatev1.NewRegistry()
	providers := secrets.NewRegistry()

	if err := host.Register(tasks, providers); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if got := tasks.Names(); len(got) != 2 {
		t.Errorf("registered tasks = %v, want two", got)
	}
	if got := providers.Schemes(); len(got) != 2 {
		t.Errorf("registered schemes = %v, want two", got)
	}

	// A scheme a non-plugin provider already claims is a conflict the host
	// cannot see on its own, and the registry reports it.
	conflicting := secrets.NewRegistry()
	if err := conflicting.Register(fixedProvider{scheme: "ok"}); err != nil {
		t.Fatalf("registering the built-in provider: %v", err)
	}
	if err := host.Register(nil, conflicting); err == nil {
		t.Error("registering a plugin over an existing provider's scheme succeeded, want a refusal")
	}
}

// fixedProvider is a stand-in for a provider that is not a plugin.
type fixedProvider struct{ scheme string }

func (p fixedProvider) Scheme() string { return p.scheme }

func (p fixedProvider) Resolve(context.Context, secrets.Request) (secrets.Secret, error) {
	return secrets.Secret{}, secrets.ErrNotFound
}
