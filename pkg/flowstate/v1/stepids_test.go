package flowstatev1

import (
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// logStep returns a `log` task, or a `for_each` holding body when one is given.
func logStep(id string, body ...*Node) *Node {
	node := &Node{Id: id, Kind: &Node_Task{Task: &Task{Name: "log"}}}
	if len(body) > 0 {
		node.Kind = &Node_ForEach{ForEach: &ForEach{Body: body}}
	}
	return node
}

// TestStepIDIssues holds both directions of the scope rules: what is refused and
// what must keep running, since a rule that also refused a legal file would be
// worse than the hole it closed.
func TestStepIDIssues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		steps []*Node
		want  string // fragment of the first issue; empty means none
	}{
		{name: "plain ids", steps: []*Node{logStep("a"), logStep("_b"), logStep("C1")}},
		{name: "a root's name with more after it", steps: []*Node{logStep("vars_"), logStep("steps2")}},
		{
			name:  "sibling loop bodies may each reuse an id, since body outputs do not escape",
			steps: []*Node{logStep("one", logStep("page")), logStep("two", logStep("page"))},
		},
		{
			name:  "an id a finished loop body used is free again afterwards",
			steps: []*Node{logStep("one", logStep("page")), logStep("page")},
		},
		{name: "a root", steps: []*Node{logStep("trigger")}, want: `id "trigger" is the root`},
		{name: "a duplicate", steps: []*Node{logStep("a"), logStep("a")}, want: `duplicate id "a"`},
		{name: "a digit first", steps: []*Node{logStep("1a")}, want: "not a valid identifier"},
		{name: "a leading dash", steps: []*Node{logStep("-x")}, want: "not a valid identifier"},
		{name: "an interior dash", steps: []*Node{logStep("a-b")}, want: "not a valid identifier"},
		{name: "a lexer word", steps: []*Node{logStep("null")}, want: "punctuation in CEL"},
		{name: "no id", steps: []*Node{logStep("")}, want: "step has no id"},
		{
			name:  "a loop body step reusing a still-visible earlier id",
			steps: []*Node{logStep("a"), logStep("each", logStep("a"))},
			want:  `duplicate id "a"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			issues := StepIDIssues(&Workflow{Name: "w", Steps: tt.steps})
			if tt.want == "" {
				if len(issues) != 0 {
					t.Fatalf("a legal workflow was refused: %v", issues)
				}
				return
			}
			if len(issues) == 0 {
				t.Fatalf("a workflow breaking a scope rule was accepted; want %q", tt.want)
			}
			if !strings.Contains(issues[0].Message, tt.want) {
				t.Errorf("first issue = %q, want it to contain %q", issues[0].Message, tt.want)
			}
		})
	}
}

// TestStepIDIssuesBoundsAnAdversarialNest proves the walk spends heap rather than
// Go stack on nesting chosen by whoever built the specification, and stops at its
// node budget rather than at the end of the input.
func TestStepIDIssuesBoundsAnAdversarialNest(t *testing.T) {
	t.Parallel()

	// Deep enough to trouble a recursive walk, and all legal: ids are distinct and
	// each step is nested inside the one before.
	const depth = 20_000
	node := logStep("n0")
	for i := 1; i < depth; i++ {
		node = logStep("n"+strconv.Itoa(i), node)
	}
	if issues := StepIDIssues(&Workflow{Name: "w", Steps: []*Node{node}}); len(issues) != 0 {
		t.Fatalf("a deep but legal nest was refused: %v", issues[0])
	}

	wide := make([]*Node, 0, maxStepIDWalkNodes+1)
	for i := range maxStepIDWalkNodes + 1 {
		wide = append(wide, logStep("w"+strconv.Itoa(i)))
	}
	issues := StepIDIssues(&Workflow{Name: "w", Steps: wide})
	if len(issues) == 0 || !strings.Contains(issues[len(issues)-1].Message, "nothing further was checked") {
		t.Fatalf("a workflow past the node budget was walked to the end: %d issues", len(issues))
	}
}

// TestStepIDIssuesNeverQueuesPastItsBudget proves the budget bounds what is
// queued, not only what is visited: a group thirty times the budget must not be
// copied onto the walk's stack before the budget sees one entry of it. The
// allocation is the evidence, since the issues returned are the same either way.
func TestStepIDIssuesNeverQueuesPastItsBudget(t *testing.T) {
	// Not parallel: it reads the process-wide allocation counter.
	shared := logStep("")
	steps := make([]*Node, 30*maxStepIDWalkNodes)
	for i := range steps {
		steps[i] = shared
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	issues := StepIDIssues(&Workflow{Name: "w", Steps: steps})
	runtime.ReadMemStats(&after)

	// Queuing every entry costs over a GiB here; the budget's worth is a
	// fraction of it.
	if spent := after.TotalAlloc - before.TotalAlloc; spent > 512<<20 {
		t.Fatalf("walking a group past the budget allocated %d MiB; it queued entries it would never visit", spent>>20)
	}

	var budget int
	for _, issue := range issues {
		if strings.Contains(issue.Message, "nothing further was checked") {
			budget++
		}
	}
	if budget != 1 {
		t.Fatalf("got %d budget refusals, want 1", budget)
	}
}
