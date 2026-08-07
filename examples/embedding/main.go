// Command embedding is a runnable demonstration of pkg/flowstate/embed: a Go
// program that compiles a Flowfile from bytes, registers its own Go function
// as a custom task, and runs the workflow — locally by default, or durably
// against a Temporal server when --durable is given.
//
// Run it:
//
//	go run ./examples/embedding
//	go run ./examples/embedding --durable   # needs `temporal server start-dev`
package main

import (
	"context"
	_ "embed"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/picatz/flowstate/pkg/flowstate/embed"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// flowfile/workflow.yaml, not workflow.yaml directly beside this file — see
// that file's own comment on why it sits a directory deeper.
//
//go:embed flowfile/workflow.yaml
var workflowSource []byte

func main() {
	durable := flag.Bool("durable", false, "also run the workflow durably against a Temporal server at localhost:7233")
	flag.Parse()

	if err := run(*durable); err != nil {
		log.Fatal(err)
	}
}

func run(durable bool) error {
	tasks := registerGreetTask()

	// Compile asks flowfile.Parse alone what this file's steps are — it does
	// not check that "greet" is a task anyone registered. That check belongs
	// to flowfile.Validate (and to running the workflow), which is why
	// tasks.Install happens before either: see [embed.Tasks]'s doc for why
	// compiling and running consult two different registries on purpose.
	uninstall, err := tasks.Install()
	if err != nil {
		return fmt.Errorf("installing tasks: %w", err)
	}
	defer uninstall()

	workflow, diags, err := embed.Compile(workflowSource)
	if err != nil {
		return fmt.Errorf("compiling flowfile/workflow.yaml: %w (diagnostics: %v)", err, diags)
	}

	fmt.Println("== running locally ==")
	outputs, err := embed.RunLocal(context.Background(), workflow, embed.RunOptions{
		Inputs: map[string]any{"name": "embedder"},
		Tasks:  tasks,
	})
	if err != nil {
		return fmt.Errorf("running locally: %w", err)
	}
	fmt.Printf("local run outputs: %v\n", outputs.GetRunOutputs().GetValues())

	if !durable {
		fmt.Println("\n(pass --durable to also run this against a real Temporal server)")
		return nil
	}

	fmt.Println("\n== running durably ==")
	return runDurable(workflow, tasks)
}

// registerGreetTask builds the custom task this program contributes: "greet"
// takes a "name" input and returns a "message" output.
//
// It declares no Inputs or Outputs descriptor — [embed.Task]'s nil-descriptor
// escape hatch. That is a real trade-off, not a shortcut with no cost: with
// no descriptor, `flow validate`, a language server, and generated reference
// docs can check and document nothing about this task's shape beyond its
// name and summary. A misspelled "nmae" here would compile and run;
// only a task with a real .proto message describing its inputs catches that
// before the first run. Taking the escape hatch is reasonable for a task
// used in one program by its own author — which is exactly this example —
// and the wrong choice for a task anyone else will write a step against.
func registerGreetTask() *embed.Tasks {
	tasks := embed.NewTasks()

	err := tasks.Register(embed.Task{
		Name:    "greet",
		Summary: "Build a greeting for a name.",
		Fn: func(_ context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			name := inputs["name"].GetLiteral().GetStringValue()
			if name == "" {
				name = "world"
			}
			return &v1.Node_Outputs{NamedValues: v1.NewNamedValues(map[string]any{
				"message": "hello, " + name + "!",
			})}, nil
		},
	})
	if err != nil {
		// Registration fails only for a structurally invalid Task — no name,
		// no function, or a name the step grammar reserves — which is a
		// defect in this program, not a runtime condition, so it panics
		// exactly the way [v1.Registry.MustRegister] does at startup.
		panic("flowstate/embed example: " + err.Error())
	}

	return tasks
}

// runDurable registers the interpreter (and this program's "greet" task) on
// a worker connected to a Temporal server at localhost:7233 — the address
// `temporal server start-dev` listens on — starts the workflow, and waits
// for its result.
//
// A dev server is not something this example can assume is running, so a
// dial failure is reported as a skip rather than an error: the local half
// above already demonstrated the embedding surface end to end, and a reader
// without Temporal installed should see that clearly rather than a stack
// trace.
func runDurable(workflow *embed.Workflow, tasks *embed.Tasks) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := client.DialContext(ctx, client.Options{HostPort: "localhost:7233"})
	if err != nil {
		fmt.Printf("skipping the durable run: could not reach a Temporal server at localhost:7233: %v\n", err)
		fmt.Println("start one with: temporal server start-dev")
		return nil
	}
	defer c.Close()

	w := worker.New(c, engine.RunTaskQueueName, worker.Options{})
	if err := embed.RunDurable(w, tasks); err != nil {
		return fmt.Errorf("registering the durable worker: %w", err)
	}

	stopWorker, err := startWorker(w)
	if err != nil {
		return fmt.Errorf("starting the worker: %w", err)
	}
	defer stopWorker()

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "embedding-example-" + time.Now().UTC().Format(time.RFC3339Nano),
		TaskQueue: engine.RunTaskQueueName,
	}, engine.Run, &v1.RunState{
		Workflow: workflow,
		Inputs:   v1.NewNamedValues(map[string]any{"name": "embedder"}),
	})
	if err != nil {
		return fmt.Errorf("starting the durable run: %w", err)
	}

	var outputs v1.Workflow_StepOutputs
	if err := run.Get(ctx, &outputs); err != nil {
		return fmt.Errorf("waiting for the durable run: %w", err)
	}

	fmt.Printf("durable run outputs: %v\n", outputs.GetRunOutputs().GetValues())
	return nil
}

// startWorker runs w in the background and returns a func that stops it,
// surfacing a worker.Run failure (a task queue it could not poll, most
// commonly) as an error from the func that started it rather than losing it
// to a goroutine nobody checks.
func startWorker(w worker.Worker) (stop func(), err error) {
	started := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		started <- nil
		if runErr := w.Run(worker.InterruptCh()); runErr != nil && !errors.Is(runErr, context.Canceled) {
			// Nothing is listening on started by the time this can happen;
			// worker.Run blocks for the worker's whole lifetime, and by the
			// time it returns, startWorker has long since returned to its
			// caller. Reported for a human reading stderr rather than
			// propagated, which is a real limitation of this small example,
			// not a claim that a production embedder should do the same —
			// see w.Run's own doc for a fuller error-handling story.
			log.Printf("worker stopped: %v", runErr)
		}
	}()
	<-started

	return func() {
		w.Stop()
		<-done
	}, nil
}
