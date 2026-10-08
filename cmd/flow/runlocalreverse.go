package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// The values of `flow run local --debug --reverse`.
const (
	// reverseSafe steps back through a run whose every task can be run again
	// without anything outside the process noticing.
	reverseSafe = "safe"

	// reverseUnsafe steps back through any run, and so runs its tasks again.
	reverseUnsafe = "unsafe"
)

// replaySafeTasks are the built-in tasks whose second execution leaves nothing
// behind but another line of the run's own account. It is an allowlist because
// the cost of a task missing from it is a refusal with a remedy, and the cost
// of a task wrongly on a denylist is a card charged twice: a plugin, `http` and
// `exec` are not on it, and neither is a task this list has not heard of.
var replaySafeTasks = []string{"log"}

func addReverseFlag(cmd *cobra.Command) {
	cmd.Flags().String("reverse", "", "with --debug at a terminal, make `back` and `reverse-continue` work "+
		"by running the workflow again from its start and replaying your commands up to the earlier stop; "+
		"every task runs again, so the flag is refused for a workflow with a task that may act outside "+
		"this process unless it is given as --reverse=unsafe")
	cmd.Flags().Lookup("reverse").NoOptDefVal = reverseSafe
}

// reverseRequested is the mode `--reverse` asked for, or an error when the
// combination it was asked for cannot hold. Every refusal names the two things
// that disagree. It runs before the terminal is touched.
func reverseRequested(cmd *cobra.Command, workflow *v1.Workflow, debugging bool, signals []string) (string, error) {
	mode, _ := cmd.Flags().GetString("reverse")
	switch mode {
	case "":
		return "", nil
	case reverseSafe, reverseUnsafe:
	default:
		return "", fmt.Errorf("--reverse takes %q (the default) or %q, got %q", reverseSafe, reverseUnsafe, mode)
	}
	if !debugging {
		return "", errors.New("--reverse steps back through a debugged run; add --debug, or drop --reverse")
	}
	if len(signals) > 0 {
		// A signal is delivered once; the replay would find it already spent.
		return "", errors.New("--reverse runs the workflow again from its start, and --signal delivers each " +
			"signal once; drop one of them")
	}
	if mode == reverseSafe {
		required, err := v1.RequiredTaskNames(workflow)
		if err != nil {
			return "", err
		}
		var unsafe []string
		for _, name := range required {
			if !slices.Contains(replaySafeTasks, name) {
				unsafe = append(unsafe, name)
			}
		}
		if len(unsafe) > 0 {
			return "", fmt.Errorf("--reverse runs every task again when it steps back, and %s may act outside "+
				"this process; use --reverse=unsafe if running them twice is acceptable, or drop --reverse",
				quotedList(unsafe))
		}
	}

	return mode, nil
}

// localOutcomes holds what a pass of the run produced, by the report that stands
// for its verdict, until the pass that is shown is asked for.
type localOutcomes struct {
	m sync.Map // *v1.TestReport -> localOutcome
}

type localOutcome struct {
	outputs *v1.Workflow_StepOutputs
	err     error
}

// gatedWriter writes only while allowed says the pass it belongs to is shown.
type gatedWriter struct {
	w       io.Writer
	allowed func() bool
}

func (g gatedWriter) Write(p []byte) (int, error) {
	if !g.allowed() {
		return len(p), nil
	}

	return g.w.Write(p)
}

// runReversibly runs the workflow under a [reversibleFront] and returns what the
// pass that is shown produced.
func runReversibly(
	ctx context.Context,
	front *reversibleFront,
	workflow *v1.Workflow,
	inputs map[string]*v1.Value,
	narrate io.Writer,
	theme ui.Theme,
) (*v1.Workflow_StepOutputs, error) {
	var (
		outcomes localOutcomes
		begun    = time.Now()
	)
	front.Program = workflow
	front.Failure = func(report *v1.TestReport) error {
		return reportError(report)
	}
	front.Execute = func(passCtx context.Context, debugger v1.Debugger) flowtest.RunResult {
		// The instant `run.started_at` reports in every pass, so a stop reached
		// again shows what it showed the first time.
		passCtx = v1.NewContextWithRunStart(passCtx, begun)
		passCtx = v1.NewContextWithDebugger(passCtx, debugger)
		if observer, ok := debugger.(v1.RunObserver); ok {
			passCtx = v1.NewContextWithRunObserver(passCtx, observer)
		}
		// Only the pass the person is looking at speaks: a replay running the
		// `log:` steps again would say each of them twice.
		passCtx = v1.ContextWithLogger(passCtx, slog.New(telemetryLogHandler(newRunLogHandler(
			gatedWriter{w: narrate, allowed: func() bool { return front.Speaks(debugger) }}, theme))))

		// The context the caller built carries the task runtime and the logger
		// the debugger-free path uses; this pass's own overrides sit on top.
		outputs, err := v1.RunWithInputs(passCtx, workflow, inputs)

		passed := err == nil
		report := &v1.TestReport{Cases: []*v1.TestCase{{Name: workflow.GetName(), Passed: passed}}}
		if err != nil {
			report.Cases[0].Error = err.Error()
		}
		// A pass a rewind cancelled ends in its cancellation, and nothing will
		// ask for its outcome.
		if ctx.Err() == nil && passCtx.Err() == nil {
			outcomes.m.Store(report, localOutcome{outputs: outputs, err: err})
		}

		return flowtest.RunResult{Report: report}
	}

	result, err := front.run(ctx)
	if err != nil {
		return nil, err
	}
	if outcome, ok := outcomes.m.LoadAndDelete(result.Report); ok {
		return outcome.(localOutcome).outputs, outcome.(localOutcome).err
	}
	// The run was ended by the person before it could say how: the report is the
	// verdict, and carries the reason.
	return nil, reportError(result.Report)
}

// reportError is the failure a pass's report carries.
func reportError(report *v1.TestReport) error {
	if cases := report.GetCases(); len(cases) > 0 && cases[0].GetError() != "" {
		return errors.New(cases[0].GetError())
	}

	return errors.New("the run did not finish")
}
