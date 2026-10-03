package embed_test

import (
	"context"
	"fmt"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/embed"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Example_debug holds an embedded run at a conditional breakpoint, reads the
// held scope with CEL, steps, and lets it finish — all in-process, through the
// same session the CLI, the editor adapter and the MCP tools drive. A custom
// task reports its own progress through [v1.NoteTask], which lands in the
// session's observations.
func Example_debug() {
	tasks := embed.NewTasks()
	_ = tasks.Register(embed.Task{
		Name: "charge",
		Fn: func(ctx context.Context, inputs map[string]*v1.Value, _ *v1.Scope) (*v1.Node_Outputs, error) {
			amount := inputs["amount"].GetLiteral().GetInt64Value()
			v1.NoteTask(ctx, fmt.Sprintf("authorizing %d", amount))

			return &v1.Node_Outputs{NamedValues: v1.NewNamedValues(map[string]any{"charged": amount})}, nil
		},
	})
	uninstall, _ := tasks.Install()
	defer uninstall()

	workflow, _, err := embed.Compile([]byte(`
edition: v2026.4
name: billing
steps:
  - id: orders
    for_each:
      items: ${[120, 900, 40]}
      as: amount
      steps:
        - id: charge
          charge:
            amount: ${amount}
  - id: done
    log:
      message: billed
`))
	if err != nil {
		fmt.Println("compile:", err)
		return
	}

	ctx := context.Background()
	debugging, err := embed.Debug(ctx, workflow, embed.DebugOptions{
		RunOptions:  embed.RunOptions{Tasks: tasks},
		Continue:    true,
		Breakpoints: []*v1.DebugBreakpoint{{Step: "orders/charge", Condition: "amount > 500"}},
	})
	if err != nil {
		fmt.Println("debug:", err)
		return
	}

	held, _ := debugging.WaitSnapshot(ctx, 0)
	for held.GetState() != v1.DebugRunState_DEBUG_RUN_STATE_HELD {
		held, _ = debugging.WaitSnapshot(ctx, held.GetRevision())
	}
	fmt.Println("held at", held.GetOccurrence().GetAddress(), "for", strings.ToLower(held.GetReason().String()[len("DEBUG_STOP_REASON_"):]))

	answer, _ := debugging.Inspect(ctx, &v1.DebugInspectRequest{Revision: held.GetRevision(), Expression: "amount * 2"})
	fmt.Println("amount * 2 =", answer.GetValue().GetRendered(), answer.GetValue().GetType())

	driver := debugging.Driver()
	next, _ := driver.Do(ctx, "next")
	fmt.Println("next stops at", next.Snapshot.GetOccurrence().GetAddress())

	var notes []string
	for _, observation := range next.Snapshot.GetObservations() {
		if observation.GetKind() == v1.DebugObservationKind_DEBUG_OBSERVATION_KIND_TASK {
			notes = append(notes, observation.GetText())
		}
	}
	fmt.Println(strings.Join(notes, "; "))

	_ = debugging.Close()
	if _, err := debugging.Wait(ctx); err != nil {
		fmt.Println("run:", err)
		return
	}
	fmt.Println("finished")

	// Output:
	// held at orders[1]/charge for breakpoint
	// amount * 2 = 1800 int
	// next stops at orders[2]/charge
	// charge: authorizing 120; charge: authorizing 900
	// finished
}
