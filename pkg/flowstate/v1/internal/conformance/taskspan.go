package conformance

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Shared cases for #523's gap 3: the `flowstate.*` spans a run opens, run
// against both execution drivers — `flowstatev1_test.TestRunWorkflowTaskSpans`
// locally and `engine.TestRunWorkflowTaskSpans` durably.
//
// # What was wrong, and what "agree" has to mean here
//
// The task span lived in `engine`, so it existed for durable execution and did
// not exist at all for local execution: an author rehearsing with `flow run
// local` and an OTLP endpoint configured got an empty trace for a workflow that
// produces one span per task in production. Local runs exist to tell an author
// what production will do, and this told them nothing.
//
// So the claim below is deliberately about the *tree* and not about one span. A
// driver that opened a span with the right name in the wrong place, or opened
// one per step where the other opens one per attempt, would satisfy any
// count-shaped assertion and still hand an author a different picture of the
// same workflow.
//
// The durable driver has spans this one cannot: Temporal's own workflow and
// activity spans, when its tracing interceptor is installed. Those are *extra*,
// and the assertion is written to permit them — everything outside the
// `flowstate.` namespace is ignored entirely, and a driver-only attribute key is
// permitted as long as it is one of the shared vocabulary's.
//
// # The asymmetry this case used to record, and what closed it
//
// Two of the five attributes were one-sided, and the comment that stood here
// described why rather than fixing it. [v1.SpanAttributeStepID] was written by
// the local driver for every step and by the durable driver only for the two
// activity entry points that already received a step id — `TaskAuthorized` and
// `TaskInScopeAuthorized`, the arms `executor.dispatch` selects for a task
// needing authority. `Task` and `TaskInScope` had no parameter to carry one.
// [v1.SpanAttributeAttempt] was one-sided the other way: durable only, from
// `activity.GetInfo`.
//
// Neither was a difference of meaning, and both are now closed, by different
// moves that are worth keeping apart.
//
// The step id was plumbing, and the plumbing was avoided because giving a
// registered activity another parameter looked like a versioned change. It is
// not one, as long as the parameter is appended and the activity keeps its
// name: replay compares the activity *type name*, never the inputs, and a
// payload list shorter than the parameter list decodes the remainder to zero
// values. `Task` and `TaskInScope` therefore take a trailing `stepID` under
// their existing names, exactly as #756 gave them a trailing `continueOnError`,
// and the replay corpus — every entry of which predates both — still replays.
// engine/versioning.go carries the rule and the reasoning.
//
// The attempt was a judgement, and the judgement was reversed. It was withheld
// from the local driver on the grounds that the substrate owns the number and
// an in-process counter is not the same fact. But a local run is the process:
// there is no crash for its counter to misreport across, so the number is a
// true answer to what the key asks. Withholding it made the rehearsal quieter
// than the thing it rehearses. [v1.StartTaskSpan]'s doc has the full argument
// and the one thing the old reasoning was right about.
//
// So the assertion below requires both, on every task-attempt span, from both
// drivers — which is the only form in which either fix stays fixed. An
// assertion that merely permits an attribute cannot tell a driver that stopped
// writing it from one that never did.

// TaskSpanSecret is the value this case hides in a task input, distinctive
// enough that a substring search cannot match it by accident.
//
// An input, specifically. The rule the span code is built on is not "no
// secrets" but "no *values*" — a span leaves the process for a collector that
// is not tenant-scoped, so an input is as unwelcome there as a credential is,
// and an input is the thing an author can most easily make carry one.
const TaskSpanSecret = "s3cr3t-input-value-that-must-never-be-exported"

// TaskSpanWorkflow returns the workflow both drivers run for the span
// comparison.
//
// Three task executions from two steps, which is the point of the loop: the
// body runs once per item under either driver, so a driver that opened its span
// per *step* rather than per execution would produce two spans where the other
// produced three. It also puts a non-task node in the run — the `for_each`
// itself — which opens no `flowstate.*` span on either driver, and the shape
// below says so by not listing one. That stayed true when #523's gap 4 landed:
// a span per non-task step was decided *against* (see [v1.StartRunSpan]'s doc
// for the cardinality argument), and the run-level span is asserted by
// [AssertRunIsOneTree] rather than folded in here.
func TaskSpanWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    "task-spans",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id: "announce",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name: "log",
					Inputs: map[string]*v1.Value{
						// The value that must not reach a collector, carried
						// the way an author would carry one by accident.
						"message": v1.NewLiteral("starting with " + TaskSpanSecret),
					},
				}},
			},
			{
				Id: "each",
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items: v1.NewLiteralList("one", "two"),
					Body: []*v1.Node{{
						Id: "note",
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "log",
							Inputs: map[string]*v1.Value{"message": v1.NewExpr(`"item " + item`)},
						}},
					}},
				}},
			},
		},
	}
}

// TaskSpanExpectedOutputs is what a [TaskSpanWorkflow] run produces.
//
// A log step records no values, and a loop records one entry per iteration
// holding its body's, which is why the loop's own outputs are two empty
// mappings rather than nothing — the shape log.go's own loop case already pins.
func TaskSpanExpectedOutputs() *v1.Workflow_StepOutputs {
	return &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"announce": {},
		"each": {NamedValues: map[string]*v1.Value{
			"results": v1.NewLiteralList(
				map[string]any{"note": map[string]any{}},
				map[string]any{"note": map[string]any{}},
			),
		}},
	}}
}

// taskSpanPrefix is what a task span's name starts with, and the filter every
// reduction below applies.
//
// Narrower than `flowstate.` on purpose, since #547: the local driver opens a
// `flowstate.run/<workflow>` span covering the whole run and the durable driver
// does not, because Temporal's interceptor already opens one at that seam (see
// [v1.StartRunSpan]). Reducing to *task* spans keeps this expectation the claim
// it was written to be — the tree of task executions, identical under either
// driver — and leaves the run root to [AssertRunIsOneTree], which takes the
// root's name as a parameter because the two drivers legitimately name it
// differently.
const taskSpanPrefix = "flowstate.task/"

// TaskSpanNode is one task span reduced to what both drivers must say the same
// way: its name, nearest task-span parent, step id, and attempt number. Parent
// is empty where there is none, so a Temporal activity span sitting between
// two first-party spans changes nothing.
type TaskSpanNode struct {
	Name    string
	Parent  string
	StepID  string
	Attempt int64
}

// ExpectedTaskSpans is the tree a [TaskSpanWorkflow] run opens, under either
// driver.
//
// Written out rather than derived from the workflow, because a derivation that
// walked the same nodes the executor walks would agree with a driver that
// walked them wrongly.
func ExpectedTaskSpans() []TaskSpanNode {
	return []TaskSpanNode{
		{Name: v1.TaskSpanName("log"), StepID: "announce", Attempt: 1},
		{Name: v1.TaskSpanName("log"), StepID: "note", Attempt: 1},
		{Name: v1.TaskSpanName("log"), StepID: "note", Attempt: 1},
	}
}

// TaskSpanRetryTaskName is the fixture task used to distinguish a real attempt
// number from a constant positive value.
const TaskSpanRetryTaskName = "test.task_span_retry"

// TaskSpanRetryStepID is the step whose two attempts both drivers trace.
const TaskSpanRetryStepID = "retrying"

// TaskSpanRetryTaskDef fails once and then succeeds. The counter is supplied by
// the driver test so the fixture has no process-global state of its own.
func TaskSpanRetryTaskDef(attempts *atomic.Int32) v1.TaskDef {
	return v1.TaskDef{
		Name: TaskSpanRetryTaskName,
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			if attempts.Add(1) == 1 {
				return nil, v1.NewTaskError(TaskSpanRetryTaskName, v1.ErrorKindUpstream,
					errors.New("fixture fails on its first attempt with "+TaskSpanSecret))
			}

			return &v1.Node_Outputs{}, nil
		},
	}
}

// TaskSpanRetryWorkflow makes the retry fast while still using each driver's
// actual retry mechanism. A pair of spans numbered 1 and 2 proves the local
// loop and Temporal activity info are the sources, rather than a hard-coded
// positive value that the ordinary success case could not distinguish.
func TaskSpanRetryWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    "task-span-retry",
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{{
			Id:   TaskSpanRetryStepID,
			Kind: &v1.Node_Task{Task: &v1.Task{Name: TaskSpanRetryTaskName}},
			Policy: &v1.StepPolicy{Retry: &v1.RetryPolicy{
				MaxAttempts:        2,
				InitialInterval:    durationpb.New(time.Millisecond),
				BackoffCoefficient: 1,
				MaxInterval:        durationpb.New(time.Millisecond),
			}},
		}},
	}
}

// AssertRetryingTaskSpans requires one span per attempt with the durable fact
// each driver owns: the local retry-loop counter or Temporal's activity info.
func AssertRetryingTaskSpans(tb testing.TB, recorder *tracetest.SpanRecorder, outputs *v1.Workflow_StepOutputs, err error) {
	tb.Helper()

	if err != nil {
		tb.Fatalf("the retrying run failed: %v", err)
	}
	if want := (&v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{TaskSpanRetryStepID: {}}}); !proto.Equal(want, outputs) {
		tb.Fatalf("the retrying run produced %v, want %v", outputs, want)
	}

	got := recordedTaskSpans(recorder)
	want := []TaskSpanNode{
		{Name: v1.TaskSpanName(TaskSpanRetryTaskName), StepID: TaskSpanRetryStepID, Attempt: 1},
		{Name: v1.TaskSpanName(TaskSpanRetryTaskName), StepID: TaskSpanRetryStepID, Attempt: 2},
	}
	sortTaskSpans(got)
	sortTaskSpans(want)
	if !sameTaskSpans(got, want) {
		tb.Fatalf("retrying task spans are %v, want %v; every span recorded: %v", got, want, spanNames(recorder))
	}

	assertTaskSpanAttributes(tb, recorder)
}

// AssertTaskSpans is the shared assertion both drivers make.
func AssertTaskSpans(tb testing.TB, recorder *tracetest.SpanRecorder, outputs *v1.Workflow_StepOutputs, err error) {
	tb.Helper()

	if err != nil {
		tb.Fatalf("the run failed: %v", err)
	}
	if want := TaskSpanExpectedOutputs(); !proto.Equal(want, outputs) {
		tb.Fatalf("the run produced %v, want %v", outputs, want)
	}

	got := recordedTaskSpans(recorder)
	want := ExpectedTaskSpans()
	sortTaskSpans(got)
	sortTaskSpans(want)

	if len(got) != len(want) {
		tb.Fatalf("the run opened %d flowstate spans (%v), want %d (%v) — every span recorded: %v",
			len(got), got, len(want), want, spanNames(recorder))
	}
	if !sameTaskSpans(got, want) {
		tb.Fatalf("flowstate span tree is %v, want %v", got, want)
	}

	assertTaskSpanAttributes(tb, recorder)

	// And the containment, in the direction that can fail: rendered through the
	// %v family over the whole batch, each span, and a struct holding one —
	// which is the shape CLAUDE.md names, because `fmt` reaching a value through
	// an unexported field prints the fields instead of calling any accessor.
	for _, rendered := range RenderedSpans(recorder) {
		if strings.Contains(rendered, TaskSpanSecret) {
			tb.Fatalf("an input value reached a span, which is exported to a collector")
		}
	}
}

// assertTaskSpanAttributes holds every `flowstate.*` span to the shared
// vocabulary.
//
// The subset check is the half that catches a driver inventing a spelling: a
// key neither driver's constant names is a parallel scheme starting, and it
// fails here on the first run rather than in whatever dashboard notices two
// half-populated attributes a year later.
func assertTaskSpanAttributes(tb testing.TB, recorder *tracetest.SpanRecorder) {
	tb.Helper()

	allowed := map[string]struct{}{
		v1.SpanAttributeTaskName:       {},
		v1.SpanAttributeStepID:         {},
		v1.SpanAttributeAttempt:        {},
		v1.SpanAttributeSecretRefs:     {},
		v1.SpanAttributeSecretRefCount: {},
	}

	for _, stub := range tracetest.SpanStubsFromReadOnlySpans(recorder.Ended()) {
		if !strings.HasPrefix(stub.Name, taskSpanPrefix) {
			continue
		}

		if stub.SpanKind != trace.SpanKindInternal {
			tb.Fatalf("%s is a %s span, want an internal one — a task execution is work this process did, not a call it made",
				stub.Name, stub.SpanKind)
		}

		named := false
		for _, attr := range stub.Attributes {
			if _, ok := allowed[string(attr.Key)]; !ok {
				tb.Fatalf("%s carries %q, which is not in the vocabulary both drivers share; add it to pkg/flowstate/v1/taskspan.go so the other driver spells it the same way",
					stub.Name, attr.Key)
			}
			if string(attr.Key) == v1.SpanAttributeTaskName {
				named = true
				if want := strings.TrimPrefix(stub.Name, "flowstate.task/"); attr.Value.AsString() != want {
					tb.Fatalf("%s names task %q in its attributes", stub.Name, attr.Value.AsString())
				}
			}
		}
		if !named {
			tb.Fatalf("%s carries no %s, so nothing but the span's own name says what ran",
				stub.Name, v1.SpanAttributeTaskName)
		}
	}
}

// recordedTaskSpans reduces what was recorded to the `flowstate.*` tree.
//
// Parentage is resolved through spans that are not ours: a durable run under
// Temporal's tracing interceptor has an activity span between the workflow and
// the task span, and a local run does not. Naming the nearest `flowstate.*`
// ancestor is the comparison that survives that, and it is also the one that
// would catch the real regression — a task span parented off the wrong step.
func recordedTaskSpans(recorder *tracetest.SpanRecorder) []TaskSpanNode {
	stubs := tracetest.SpanStubsFromReadOnlySpans(recorder.Ended())

	byID := make(map[trace.SpanID]tracetest.SpanStub, len(stubs))
	for _, stub := range stubs {
		byID[stub.SpanContext.SpanID()] = stub
	}

	var nodes []TaskSpanNode
	for _, stub := range stubs {
		if !strings.HasPrefix(stub.Name, taskSpanPrefix) {
			continue
		}

		node := TaskSpanNode{Name: stub.Name}
		for _, attr := range stub.Attributes {
			switch string(attr.Key) {
			case v1.SpanAttributeStepID:
				node.StepID = attr.Value.AsString()
			case v1.SpanAttributeAttempt:
				node.Attempt = attr.Value.AsInt64()
			}
		}
		for parent := stub.Parent; parent.IsValid(); {
			above, ok := byID[parent.SpanID()]
			if !ok {
				break
			}
			if strings.HasPrefix(above.Name, taskSpanPrefix) {
				node.Parent = above.Name

				break
			}
			parent = above.Parent
		}

		nodes = append(nodes, node)
	}

	return nodes
}

// sortTaskSpans orders a tree for comparison, since spans end in whatever order
// the work finished and a driver is not obliged to run a loop's iterations the
// way the other one does.
func sortTaskSpans(nodes []TaskSpanNode) {
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Name != nodes[j].Name {
			return nodes[i].Name < nodes[j].Name
		}
		if nodes[i].StepID != nodes[j].StepID {
			return nodes[i].StepID < nodes[j].StepID
		}
		if nodes[i].Attempt != nodes[j].Attempt {
			return nodes[i].Attempt < nodes[j].Attempt
		}

		return nodes[i].Parent < nodes[j].Parent
	})
}

func sameTaskSpans(got, want []TaskSpanNode) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}

	return true
}

// String renders one node for a failure message that reads like a tree.
func (n TaskSpanNode) String() string {
	detail := fmt.Sprintf("%s step=%s attempt=%d", n.Name, n.StepID, n.Attempt)
	if n.Parent == "" {
		return detail
	}

	return fmt.Sprintf("%s under %s", detail, n.Parent)
}
