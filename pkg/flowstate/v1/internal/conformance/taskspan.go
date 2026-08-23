package conformance

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

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
// activity spans, when its tracing interceptor is installed, and
// [v1.SpanAttributeAttempt], which the substrate owns. Those are *extra*, and
// the assertion is written to permit them — everything outside the `flowstate.`
// namespace is ignored entirely, and a driver-only attribute key is permitted
// as long as it is one of the shared vocabulary's.
//
// # The one asymmetry this case records rather than asserts away
//
// [v1.SpanAttributeStepID] is written by the local driver for every task step,
// and by the durable driver only for the two activity entry points that receive
// a step id — `TaskAuthorized` and `TaskInScopeAuthorized`, the arms
// `executor.dispatch` selects for a task needing authority. The other two
// activities (`Task`, `TaskInScope`) have no parameter to carry it, and giving
// them one changes a registered activity's signature, which is a versioned
// change rather than a tracing one. It is a plumbing gap in the durable
// driver, not a difference of meaning, and it is left as a named follow-up on
// #523 rather than silently mirrored by making the local driver omit an id it
// knows. Absence beats fabrication; so does not throwing away a fact to make a
// test symmetrical.

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
// below says so by not listing one. Run-level and step-level spans are #523's
// gap 4 and stay out of this slice; when they land, this expectation is where
// the two drivers have to start agreeing about them.
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

// TaskSpanNode is one `flowstate.*` span reduced to what both drivers must say
// the same way: its name, and the name of the nearest `flowstate.*` span above
// it — empty where there is none, so a Temporal activity span sitting in
// between changes nothing.
type TaskSpanNode struct {
	Name   string
	Parent string
}

// ExpectedTaskSpans is the tree a [TaskSpanWorkflow] run opens, under either
// driver.
//
// Written out rather than derived from the workflow, because a derivation that
// walked the same nodes the executor walks would agree with a driver that
// walked them wrongly.
func ExpectedTaskSpans() []TaskSpanNode {
	return []TaskSpanNode{
		{Name: v1.TaskSpanName("log")},
		{Name: v1.TaskSpanName("log")},
		{Name: v1.TaskSpanName("log")},
	}
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
	for i := range want {
		if got[i] != want[i] {
			tb.Fatalf("flowstate span tree is %v, want %v", got, want)
		}
	}

	assertTaskSpanAttributes(tb, recorder)

	// And the containment, in the direction that can fail: rendered through the
	// %v family over the whole batch, each span, and a struct holding one —
	// which is the shape CLAUDE.md names, because `fmt` reaching a value through
	// an unexported field prints the fields instead of calling any accessor.
	for _, rendered := range renderedSpans(recorder) {
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
		if !strings.HasPrefix(stub.Name, "flowstate.") {
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
		if !strings.HasPrefix(stub.Name, "flowstate.") {
			continue
		}

		node := TaskSpanNode{Name: stub.Name}
		for parent := stub.Parent; parent.IsValid(); {
			above, ok := byID[parent.SpanID()]
			if !ok {
				break
			}
			if strings.HasPrefix(above.Name, "flowstate.") {
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

		return nodes[i].Parent < nodes[j].Parent
	})
}

// String renders one node for a failure message that reads like a tree.
func (n TaskSpanNode) String() string {
	if n.Parent == "" {
		return n.Name
	}

	return fmt.Sprintf("%s under %s", n.Name, n.Parent)
}
