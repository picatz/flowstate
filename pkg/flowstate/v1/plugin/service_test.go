package plugin

import (
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	pluginv1 "github.com/picatz/flowstate/pkg/flowstate/plugin/v1"
	pluginv1connect "github.com/picatz/flowstate/pkg/flowstate/plugin/v1/pluginv1connect"
	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestServiceIsTheContract checks that a plugin is usable through the generated
// service, with no Go interface in between.
//
// That is the extension point: an in-process implementation and a plugin-backed
// one are the same shape, so nothing consuming a service can tell which it has.
func TestServiceIsTheContract(t *testing.T) {
	t.Parallel()

	host := openHost(t, testConfig(t, pluginDir(t, "ok")))

	p, ok := host.Lookup("ok")
	if !ok {
		t.Fatal("plugin was not launched")
	}

	t.Run("secrets", func(t *testing.T) {
		service, err := p.SecretService()
		if err != nil {
			t.Fatalf("SecretService: %v", err)
		}

		// Used as a handler, which is the substitution the whole design rests
		// on: whatever dispatches to an in-process implementation can dispatch
		// to this without knowing the difference.
		var handler pluginv1connect.SecretServiceHandler = service

		resp, err := handler.Resolve(t.Context(), connect.NewRequest(&pluginv1.ResolveRequest{
			Ref:       &flowstatev1.SecretRef{Scheme: "ok", Name: "api-key"},
			Namespace: "team-a",
		}))
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}

		if got := string(resp.Msg.GetValue()); got != "value-for-api-key-in-team-a" {
			t.Errorf("resolved %q, want the namespaced value", got)
		}
	})

	t.Run("tasks", func(t *testing.T) {
		service, err := p.TaskService()
		if err != nil {
			t.Fatalf("TaskService: %v", err)
		}

		var handler pluginv1connect.TaskServiceHandler = service

		resp, err := handler.Execute(t.Context(), connect.NewRequest(&pluginv1.ExecuteRequest{
			Task: &flowstatev1.Task{
				Name:   "ok_task",
				Inputs: map[string]*flowstatev1.Value{"message": flowstatev1.NewLiteral("hi")},
			},
		}))
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}

		if got := resp.Msg.GetOutputs().GetNamedValues()["result"].GetLiteral().GetStringValue(); got != "hi" {
			t.Errorf("result = %q, want %q", got, "hi")
		}
	})

	t.Run("looked up by scheme", func(t *testing.T) {
		service, owner, ok := host.SecretServiceForScheme("ok")
		if !ok {
			t.Fatal("no service for a scheme the plugin claims")
		}
		if owner.Name() != "ok" {
			t.Errorf("owner = %q, want %q", owner.Name(), "ok")
		}
		if service == nil {
			t.Error("no service returned")
		}

		if _, _, ok := host.SecretServiceForScheme("nobody-claims-this"); ok {
			t.Error("a scheme no plugin claims resolved to a service")
		}
	})
}

// TestServiceRefusesUnadvertisedCapability checks that a plugin is not asked for
// something it never offered.
func TestServiceRefusesUnadvertisedCapability(t *testing.T) {
	t.Parallel()

	// This fake advertises secrets only.
	host := openHost(t, testConfig(t, pluginDir(t, "future-cap")))

	p, ok := host.Lookup("future-cap")
	if !ok {
		t.Fatal("plugin was not launched")
	}

	if _, err := p.SecretService(); err != nil {
		t.Errorf("SecretService on a secrets plugin: %v", err)
	}

	_, err := p.TaskService()
	if !errors.Is(err, ErrCapability) {
		t.Fatalf("TaskService error = %v, want one wrapping %v", err, ErrCapability)
	}
	if !strings.Contains(err.Error(), "CAPABILITY_TASKS") {
		t.Errorf("error = %q, want it to name the capability", err.Error())
	}
}

// TestServiceSurvivesARestart checks that a service holds onto the plugin rather
// than to one of its processes.
//
// A caller keeps a service for the life of the worker, and a plugin may be
// relaunched several times underneath it. A service bound to one process would
// keep dialing a socket that is gone.
func TestServiceSurvivesARestart(t *testing.T) {
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

	// Taken once, before any restart, and used after several.
	service, err := p.TaskService()
	if err != nil {
		t.Fatalf("TaskService: %v", err)
	}

	first := p.PID()
	if !waitFor(t, 15*time.Second, func() bool {
		return p.Restarts() > 0 && p.State() == StateReady && p.PID() != first && p.PID() != 0
	}) {
		t.Fatalf("plugin was not relaunched: state=%v restarts=%d: %v", p.State(), p.Restarts(), p.LastError())
	}

	// The plugin dies a second after each launch, so a call may land during a
	// restart. What
	// matters is that the same service eventually reaches the new process rather
	// than being permanently attached to the old one.
	var lastErr error
	reached := waitFor(t, 15*time.Second, func() bool {
		_, err := service.Execute(t.Context(), connect.NewRequest(&pluginv1.ExecuteRequest{
			Task: &flowstatev1.Task{Name: "short_lived_task"},
		}))
		lastErr = err
		return err == nil
	})

	if !reached {
		t.Errorf("a service taken before a restart never reached the relaunched plugin: %v", lastErr)
	}
}
