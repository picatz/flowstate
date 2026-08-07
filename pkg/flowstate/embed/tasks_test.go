package embed

import (
	"context"
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// echoTask returns its "in" input unchanged as "out", which is enough to
// prove a task actually ran without needing a descriptor.
func echoTask(name string) v1.TaskFunc {
	return func(_ context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
		return &v1.Node_Outputs{NamedValues: map[string]*v1.Value{
			"out": inputs["in"],
		}}, nil
	}
}

func echoWorkflowSource(taskName string) []byte {
	return []byte(`
edition: v2026.2
name: echo-test
steps:
  - id: step1
    ` + taskName + `:
      in: hello
`)
}

// TestTasksInstall_ValidationAndExecutionCoherence is the issue #230 risk-2
// test: a properly Installed Tasks set must never disagree between
// validation ([flowfile.Validate]) and execution ([RunLocal]) — and an
// uninstalled set's real, honestly-pinned divergence (validation refuses,
// direct execution still runs it) must be exactly what [Tasks]' doc says it
// is, not something worse.
func TestTasksInstall_ValidationAndExecutionCoherence(t *testing.T) {
	const taskName = "coherence_echo_task"

	t.Run("installed: validation sees it and execution runs it, in agreement", func(t *testing.T) {
		tasks := NewTasks()
		if err := tasks.Register(Task{Name: taskName, Fn: echoTask(taskName)}); err != nil {
			t.Fatalf("Register: %v", err)
		}

		uninstall, err := tasks.Install()
		if err != nil {
			t.Fatalf("Install: %v", err)
		}
		defer uninstall()

		workflow, err := flowfile.Unmarshal(echoWorkflowSource(taskName))
		if err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}

		diags := flowfile.Validate(workflow)
		if len(diags) != 0 {
			t.Fatalf("Validate: expected no diagnostics for an installed task, got: %v", diags)
		}

		outputs, err := RunLocal(context.Background(), workflow, RunOptions{Tasks: tasks})
		if err != nil {
			t.Fatalf("RunLocal: unexpected error: %v", err)
		}
		got := outputs.GetStepValues()["step1"].GetNamedValues()["out"].GetLiteral().GetStringValue()
		if got != "hello" {
			t.Errorf("RunLocal: step1 output = %q, want %q", got, "hello")
		}
	})

	t.Run("uninstalled: validation refuses, direct execution still runs it", func(t *testing.T) {
		tasks := NewTasks()
		if err := tasks.Register(Task{Name: taskName, Fn: echoTask(taskName)}); err != nil {
			t.Fatalf("Register: %v", err)
		}
		// Deliberately never installed.

		workflow, err := flowfile.Unmarshal(echoWorkflowSource(taskName))
		if err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}

		diags := flowfile.Validate(workflow)
		if len(diags) == 0 {
			t.Fatal("Validate: expected an unknown-task diagnostic for an uninstalled task")
		}
		var found bool
		for _, d := range diags {
			if strings.Contains(d.Message, "unknown task") {
				found = true
			}
		}
		if !found {
			t.Errorf(`Validate: expected an "unknown task" diagnostic, got: %v`, diags)
		}

		// The true, honestly-pinned divergence: RunLocal reads opts.Tasks
		// directly, never [v1.DefaultRegistry], so it runs the task fine
		// even though validation just refused this same workflow. See
		// [RunLocal]'s doc.
		outputs, err := RunLocal(context.Background(), workflow, RunOptions{Tasks: tasks})
		if err != nil {
			t.Fatalf("RunLocal: expected the uninstalled task to still execute directly, got error: %v", err)
		}
		got := outputs.GetStepValues()["step1"].GetNamedValues()["out"].GetLiteral().GetStringValue()
		if got != "hello" {
			t.Errorf("RunLocal: step1 output = %q, want %q", got, "hello")
		}
	})

	t.Run("uninstall restores validation-invisibility", func(t *testing.T) {
		tasks := NewTasks()
		if err := tasks.Register(Task{Name: taskName, Fn: echoTask(taskName)}); err != nil {
			t.Fatalf("Register: %v", err)
		}

		uninstall, err := tasks.Install()
		if err != nil {
			t.Fatalf("Install: %v", err)
		}

		workflow, err := flowfile.Unmarshal(echoWorkflowSource(taskName))
		if err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		if diags := flowfile.Validate(workflow); len(diags) != 0 {
			t.Fatalf("Validate: expected no diagnostics while installed, got: %v", diags)
		}

		uninstall()

		if diags := flowfile.Validate(workflow); len(diags) == 0 {
			t.Fatal("Validate: expected an unknown-task diagnostic after uninstall")
		}
	})
}

// TestTasksInstall_ConflictingNameIsRefusedNotCorrupted is the P2 fix from
// PR #232's review, reproducing the exact interleaving the review named:
//
//	A installs "x" -> B installs "x" (saving A's def as what B will restore)
//	-> A uninstalls (removes "x" entirely, unaware B took it over)
//	-> B uninstalls (restores A's def)
//	=> "x" is left permanently registered, owned by neither A nor B.
//
// With ownership tracking, B's Install never succeeds while A still owns
// "x" — it is refused outright — so there is nothing for either uninstall to
// disagree about, and the registry ends back in its original state (no "x"
// registered at all) once A uninstalls.
func TestTasksInstall_ConflictingNameIsRefusedNotCorrupted(t *testing.T) {
	const name = "conflicting_task_name"

	if _, ok := v1.LookupTask(name); ok {
		t.Fatalf("test setup: %q is already registered somehow", name)
	}

	a := NewTasks()
	if err := a.Register(Task{Name: name, Fn: echoTask(name)}); err != nil {
		t.Fatalf("a.Register: %v", err)
	}
	b := NewTasks()
	if err := b.Register(Task{Name: name, Fn: echoTask(name)}); err != nil {
		t.Fatalf("b.Register: %v", err)
	}

	// A installs "x" successfully.
	uninstallA, err := a.Install()
	if err != nil {
		t.Fatalf("a.Install: unexpected error: %v", err)
	}

	// B's attempt to install the same name, while A still owns it, must be
	// refused outright — not layered silently on top.
	uninstallB, err := b.Install()
	if err == nil {
		t.Fatal("b.Install: expected a conflict error while a still owns the name")
	}
	if uninstallB != nil {
		t.Error("b.Install: expected a nil uninstall alongside the conflict error")
	}
	if !strings.Contains(err.Error(), name) {
		t.Errorf("b.Install: error does not name the conflicting task: %v", err)
	}

	// A's def must still be the one registered — B's refused Install
	// registered nothing.
	if _, ok := v1.LookupTask(name); !ok {
		t.Fatal("expected a's task to still be registered")
	}

	// A uninstalls. Because B's Install never succeeded, this is the only
	// live claim on "x", and removing it must return the registry to
	// exactly its original state.
	uninstallA()

	if _, ok := v1.LookupTask(name); ok {
		t.Fatal("expected the task to be fully unregistered after a's uninstall — " +
			"the original state, with nothing left behind by either a or b")
	}

	// Now that A has released it, B may install it cleanly.
	uninstallB2, err := b.Install()
	if err != nil {
		t.Fatalf("b.Install after a released the name: unexpected error: %v", err)
	}
	if _, ok := v1.LookupTask(name); !ok {
		t.Fatal("expected b's task to be registered")
	}
	uninstallB2()

	if _, ok := v1.LookupTask(name); ok {
		t.Fatal("expected the task to be fully unregistered after b's uninstall too")
	}
}

// TestTasksInstall_DoubleInstallSameSetIsRefused pins the same refusal for
// the same Tasks set installed twice without an uninstall in between — a
// second, unrelated Install call racing the first is exactly the collision
// [TestTasksInstall_ConflictingNameIsRefusedNotCorrupted] covers, just with
// the same set on both sides.
func TestTasksInstall_DoubleInstallSameSetIsRefused(t *testing.T) {
	const name = "double_install_task_name"

	tasks := NewTasks()
	if err := tasks.Register(Task{Name: name, Fn: echoTask(name)}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	uninstall1, err := tasks.Install()
	if err != nil {
		t.Fatalf("first Install: unexpected error: %v", err)
	}
	defer func() {
		if _, ok := v1.LookupTask(name); ok {
			uninstall1()
		}
	}()

	_, err = tasks.Install()
	if err == nil {
		t.Fatal("second Install: expected an error while the first is still live")
	}

	uninstall1()
	if _, ok := v1.LookupTask(name); ok {
		t.Fatal("expected the task to be unregistered after uninstall")
	}
}

func TestTasksRegister_RejectsInvalidTasks(t *testing.T) {
	tasks := NewTasks()

	if err := tasks.Register(Task{Fn: echoTask("x")}); err == nil {
		t.Error("Register: expected an error for a task with no name")
	}
	if err := tasks.Register(Task{Name: "x"}); err == nil {
		t.Error("Register: expected an error for a task with no function")
	}
	// "if" is a reserved step key ([v1.ReservedStepKeys]); registering a
	// task under it would make `if:` ambiguous between the step property
	// and a task invocation.
	if err := tasks.Register(Task{Name: "if", Fn: echoTask("if")}); err == nil {
		t.Error("Register: expected an error for a task named after a reserved step key")
	}
}

func TestTasksInstall_RestoresAPreexistingTask(t *testing.T) {
	// Installing a Tasks set that reuses a name already registered in
	// [v1.DefaultRegistry] (a built-in task's name) must restore the
	// original definition on uninstall, not remove it.
	tasks := NewTasks()
	if err := tasks.Register(Task{Name: "log", Fn: echoTask("log")}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	before, ok := v1.LookupTask("log")
	if !ok {
		t.Fatal("expected the built-in log task to be registered")
	}

	uninstall, err := tasks.Install()
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	overridden, ok := v1.LookupTask("log")
	if !ok || overridden.Summary != "" {
		t.Fatalf("expected the log task to be overridden while installed, got %+v", overridden)
	}
	uninstall()

	after, ok := v1.LookupTask("log")
	if !ok {
		t.Fatal("expected the log task to still be registered after uninstall")
	}
	if after.Summary != before.Summary {
		t.Errorf("Unregister/restore did not put back the original log task: got summary %q, want %q",
			after.Summary, before.Summary)
	}
}
