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

		// The worker above shares this client, which is the one topology
		// Temporal's eager workflow start pays off in: the first workflow task
		// comes back on this response for that worker to execute, instead of
		// going through matching for it to poll for. An embedder without a
		// co-located worker gets an ordinary dispatch and nothing else changes.
		//
		// Sound here because this program registers one unversioned worker.
		// Eager start does not respect worker versioning, so an embedder
		// running versioned workers should leave it off — the same reason
		// `server.WithEagerWorkflowStart` is an option `flow server dev` opts
		// into rather than a default.
		EnableEagerStart: true,
	}, engine.Run, &v1.RunState{
		Workflow: workflow,
		Inputs:   v1.NewNamedValues(map[string]any{"name": "embedder"}),

		// The name run-lifecycle metrics may export. Set here because this
		// program *is* the deployment — the specification came from a file it
		// chose, not from a request — which is the same test the server applies
		// before it fills this field in. Left empty, the run still counts; it
		// just carries no `flowstate.workflow.name`.
		MetricWorkflowName: workflow.GetName(),
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

// startWorker starts w and returns a func that stops it.
//
// [worker.Worker.Start] rather than Run in a goroutine, and that difference is
// load-bearing rather than stylistic. Start is what registers this worker with
// its client's eager dispatcher, and it does so before returning
// (`internal/internal_worker.go`, where Start registers the workflow worker,
// and Run is exactly Start plus a block on an interrupt channel) — so by the
// time this returns, the start above is guaranteed to find a worker to hand
// the first workflow task to.
//
// The shape this replaced ran Run in a goroutine and signalled readiness from
// inside it *before* the call, so the ExecuteWorkflow could reach the SDK
// first and the eager request would be dropped with nothing said. A race
// whose only symptom is an optimization quietly not happening is one to
// remove from an example rather than to explain in it.
//
// What Start does not do is report a failure that arrives *after* start-up,
// which Run would have returned; a program that runs for longer than this one
// should watch for that rather than copy this.
func startWorker(w worker.Worker) (stop func(), err error) {
	if err := w.Start(); err != nil {
		return nil, err
	}

	return w.Stop, nil
}
