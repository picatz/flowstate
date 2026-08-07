package embed

import (
	"strings"
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// newProbeWorker returns a [worker.Worker] that never dials Temporal — a
// lazy client defers connecting until a real call needs one, which
// [RunDurable]'s registration path never does. Enough to prove what got
// registered without standing up a Temporal dev server.
func newProbeWorker(t *testing.T) worker.Worker {
	t.Helper()

	c, err := client.NewLazyClient(client.Options{HostPort: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("client.NewLazyClient: %v", err)
	}
	t.Cleanup(c.Close)

	return worker.New(c, "flowstate-embed-durable-test-queue", worker.Options{})
}

// TestRunDurable_OverriddenBuiltinWithoutInstallIsRefused is the P1-2 fix
// from PR #232's review: a Tasks set overriding a *built-in* task's name
// (here, "log") must be refused by RunDurable when it was never Installed —
// checking that the name "log" exists in [v1.DefaultRegistry] is not enough,
// because the built-in already satisfies that check with nobody having
// installed anything. Without the fix, a durable worker registered this way
// would execute the *built-in* log for every run, while RunLocal (which
// always reads a Tasks set directly) executes the program's own — the two
// drivers silently disagreeing about what one step does.
func TestRunDurable_OverriddenBuiltinWithoutInstallIsRefused(t *testing.T) {
	tasks := NewTasks()
	if err := tasks.Register(Task{Name: "log", Fn: echoTask("log")}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	// Deliberately never installed.

	w := newProbeWorker(t)
	err := RunDurable(w, tasks)
	if err == nil {
		t.Fatal("RunDurable: expected a refusal for an overridden built-in task that was never installed")
	}
	if !strings.Contains(err.Error(), "log") {
		t.Errorf("RunDurable: error does not name the task at fault: %v", err)
	}
	if !strings.Contains(err.Error(), "Install") {
		t.Errorf("RunDurable: error does not name the remedy (Tasks.Install): %v", err)
	}
}

// TestRunDurable_InstalledOverrideProceeds is the positive half of the same
// fix: once the overriding Tasks set is actually Installed — so
// [v1.DefaultRegistry]'s "log" entry really is this set's own — RunDurable
// proceeds.
func TestRunDurable_InstalledOverrideProceeds(t *testing.T) {
	tasks := NewTasks()
	if err := tasks.Register(Task{Name: "durable_installed_override_log", Fn: echoTask("durable_installed_override_log")}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	uninstall, err := tasks.Install()
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	defer uninstall()

	w := newProbeWorker(t)
	if err := RunDurable(w, tasks); err != nil {
		t.Fatalf("RunDurable: unexpected error with the task installed: %v", err)
	}
}

// TestRunDurable_NoTasksProceeds pins that RunDurable with a nil Tasks set —
// no custom tasks at all — never runs the ownership check and always
// proceeds straight to registering the interpreter.
func TestRunDurable_NoTasksProceeds(t *testing.T) {
	w := newProbeWorker(t)
	if err := RunDurable(w, nil); err != nil {
		t.Fatalf("RunDurable: unexpected error with no Tasks set: %v", err)
	}
}
