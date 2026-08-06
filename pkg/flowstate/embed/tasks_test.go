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

		uninstall := tasks.Install()
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

		uninstall := tasks.Install()

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

	uninstall := tasks.Install()
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
