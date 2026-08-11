package embed

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestRunLocal_ConcurrentDifferentTasksDoNotInterfere is issue #195's lesson,
// applied to this package: two goroutines each running their own workflow
// against their own [Tasks] set, at the same time, must never see the
// other's task — the per-run registry [RunLocal] builds is what makes
// isolation a structural property instead of a timing-dependent one (see
// [RunLocal]'s doc). Run with -race, this fails immediately if RunLocal ever
// starts reading or mutating shared, mutable state per call.
func TestRunLocal_ConcurrentDifferentTasksDoNotInterfere(t *testing.T) {
	const goroutines = 16
	const itersPerGoroutine = 20

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()

			taskName := fmt.Sprintf("concurrent_task_%d", g)
			multiplier := int64(g + 1)

			tasks := NewTasks()
			err := tasks.Register(Task{
				Name: taskName,
				Fn: func(_ context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
					n := inputs["n"].GetLiteral().GetInt64Value()
					return &v1.Node_Outputs{NamedValues: v1.NewNamedValues(map[string]any{
						"result": n * multiplier,
					})}, nil
				},
			})
			if err != nil {
				errs <- fmt.Errorf("goroutine %d: Register: %w", g, err)
				return
			}

			workflow, diags, err := Compile([]byte(fmt.Sprintf(`
edition: v2026.3
name: concurrent-%d
steps:
  - id: step1
    %s:
      n: 10
`, g, taskName)))
			if err != nil {
				errs <- fmt.Errorf("goroutine %d: Compile: %w (diags: %v)", g, err, diags)
				return
			}

			for i := 0; i < itersPerGoroutine; i++ {
				outputs, runErr := RunLocal(context.Background(), workflow, RunOptions{Tasks: tasks})
				if runErr != nil {
					errs <- fmt.Errorf("goroutine %d iter %d: RunLocal: %w", g, i, runErr)
					return
				}
				got := outputs.GetStepValues()["step1"].GetNamedValues()["result"].GetLiteral().GetInt64Value()
				want := 10 * multiplier
				if got != want {
					errs <- fmt.Errorf("goroutine %d iter %d: result = %d, want %d — saw another goroutine's task", g, i, got, want)
					return
				}
			}
		}(g)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestRunLocal_ConcurrentInstallAndRunDoNotInterfere runs [Tasks.Install] and
// [RunLocal] from several goroutines at once against distinct Tasks sets,
// exercising both halves of the dual-registration pattern under
// concurrency: the global installs, serialized by [v1.LockDefaultRegistry],
// and the per-run registries RunLocal builds independently of them.
func TestRunLocal_ConcurrentInstallAndRunDoNotInterfere(t *testing.T) {
	const goroutines = 8

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()

			taskName := fmt.Sprintf("install_race_task_%d", g)
			tasks := NewTasks()
			if err := tasks.Register(Task{Name: taskName, Fn: doubleTaskFn}); err != nil {
				errs <- fmt.Errorf("goroutine %d: Register: %w", g, err)
				return
			}

			uninstall, err := tasks.Install()
			if err != nil {
				errs <- fmt.Errorf("goroutine %d: Install: %w", g, err)
				return
			}
			defer uninstall()

			workflow, diags, err := Compile([]byte(fmt.Sprintf(`
edition: v2026.3
name: install-race-%d
steps:
  - id: step1
    %s:
      n: 5
`, g, taskName)))
			if err != nil {
				errs <- fmt.Errorf("goroutine %d: Compile: %w (diags: %v)", g, err, diags)
				return
			}

			outputs, runErr := RunLocal(context.Background(), workflow, RunOptions{Tasks: tasks})
			if runErr != nil {
				errs <- fmt.Errorf("goroutine %d: RunLocal: %w", g, runErr)
				return
			}
			got := outputs.GetStepValues()["step1"].GetNamedValues()["result"].GetLiteral().GetInt64Value()
			if got != 10 {
				errs <- fmt.Errorf("goroutine %d: result = %d, want 10", g, got)
			}
		}(g)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestTasksInstall_ConcurrentSameNameOnlyOneWins races many goroutines each
// trying to Install a distinct [Tasks] set that all name the *same* task, so
// the ownership check in [Tasks.Install] itself — not just the sequential
// trace in TestTasksInstall_ConflictingNameIsRefusedNotCorrupted — is what
// is under test here, with -race able to catch any unsynchronized access to
// [installOwners] the sequential test cannot exercise.
//
// Exactly one goroutine must succeed; every other must be refused; and once
// the winner uninstalls, the name must be fully unregistered again — the
// registry ends in its original state no matter how the attempts interleave.
func TestTasksInstall_ConcurrentSameNameOnlyOneWins(t *testing.T) {
	const goroutines = 16
	const name = "concurrent_conflict_task_name"

	if _, ok := v1.LookupTask(name); ok {
		t.Fatalf("test setup: %q is already registered somehow", name)
	}

	var wg sync.WaitGroup
	var wins atomic.Int32
	uninstalls := make(chan func(), goroutines)
	errs := make(chan error, goroutines)

	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()

			tasks := NewTasks()
			if err := tasks.Register(Task{Name: name, Fn: echoTask(name)}); err != nil {
				errs <- fmt.Errorf("goroutine %d: Register: %w", g, err)
				return
			}

			<-start
			uninstall, err := tasks.Install()
			if err == nil {
				wins.Add(1)
				uninstalls <- uninstall
				return
			}
			if uninstall != nil {
				errs <- fmt.Errorf("goroutine %d: got a non-nil uninstall alongside an error", g)
			}
		}(g)
	}
	close(start)
	wg.Wait()
	close(uninstalls)
	close(errs)

	for err := range errs {
		t.Error(err)
	}
	if wins.Load() != 1 {
		t.Fatalf("expected exactly one goroutine to win Install, got %d", wins.Load())
	}

	for uninstall := range uninstalls {
		uninstall()
	}

	if _, ok := v1.LookupTask(name); ok {
		t.Fatal("expected the task to be fully unregistered after the winner uninstalled")
	}
}
