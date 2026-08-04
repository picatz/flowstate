package tests_test

import (
	"strings"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/tests"
)

// httpTask returns a minimal http task whose only input is url, matching what
// PointAtStandIn looks at.
func httpTask(url string) *v1.Node_Task {
	return &v1.Node_Task{
		Task: &v1.Task{
			Name:   "http",
			Inputs: map[string]*v1.Value{"url": v1.NewLiteral(url)},
		},
	}
}

// httpTaskExpr is [httpTask] for a url given as a CEL expression rather than a
// literal — the shape a for_each body or a step var writes.
func httpTaskExpr(expr string) *v1.Node_Task {
	return &v1.Node_Task{
		Task: &v1.Task{
			Name:   "http",
			Inputs: map[string]*v1.Value{"url": v1.NewExpr(expr)},
		},
	}
}

// TestPointAtStandInForEachOverEndpoints is the case #183 names directly: a
// for_each body whose url varies per item, written the natural way —
// `${service.url}` reading the loop's own iterator — rather than unrolled into
// literal parallel branches the way ops-healthcheck was forced to for #175.
//
// Before the fix this reported every iteration's step as unpointable, because
// PointAtStandIn only ever rewrote a literal `url:`. After it, the walk traces
// `service.url` back to the for_each's own `items:` — itself written as plain
// literals — and rewrites every entry's url in place, which is what makes every
// iteration's request land on the stand-in rather than just the first.
func TestPointAtStandInForEachOverEndpoints(t *testing.T) {
	t.Parallel()

	items := v1.NewLiteralList(
		map[string]any{"name": "checkout", "url": "https://checkout.example/health"},
		map[string]any{"name": "billing", "url": "https://billing.example/health"},
	)

	nodes := []*v1.Node{
		{
			Id: "check-each-service",
			Kind: &v1.Node_ForEach{
				ForEach: &v1.ForEach{
					Items:    items,
					Iterator: "service",
					Body: []*v1.Node{
						{Id: "check", Kind: httpTaskExpr("service.url")},
					},
				},
			},
		},
	}

	unpointable := tests.PointAtStandIn(nodes, "http://127.0.0.1:9"+"999")
	if len(unpointable) != 0 {
		t.Fatalf("for_each over endpoints should now be pointable, got unpointable: %v", unpointable)
	}

	// The rewrite has to have actually happened, not merely been reported as
	// possible: every entry's url now carries the stand-in's host, and the path
	// each example wrote survives — the same "only the scheme and host move"
	// rule a literal url gets.
	list := items.GetLiteral().GetListValue().GetValues()
	if len(list) != 2 {
		t.Fatalf("items literal should still have 2 entries, got %d", len(list))
	}
	for _, entry := range list {
		var got string
		for _, kv := range entry.GetMapValue().GetEntries() {
			if kv.GetKey().GetStringValue() == "url" {
				got = kv.GetValue().GetStringValue()
			}
		}
		if !strings.HasPrefix(got, "http://127.0.0.1:9999/") {
			t.Errorf("item url was not rewritten onto the stand-in: %q", got)
		}
		if !strings.HasSuffix(got, "/health") {
			t.Errorf("item url lost its path in the rewrite: %q", got)
		}
	}
}

// TestPointAtStandInForEachDefaultIterator checks the unnamed spelling — a
// for_each with no `as:` binds the current item under "item" (ForEach.iterator's
// own default) — traces the same way as a named iterator.
func TestPointAtStandInForEachDefaultIterator(t *testing.T) {
	t.Parallel()

	items := v1.NewLiteralList(map[string]any{"url": "https://svc.example/ping"})

	nodes := []*v1.Node{
		{
			Id: "ping-each",
			Kind: &v1.Node_ForEach{
				ForEach: &v1.ForEach{
					Items: items,
					// Iterator left empty on purpose: this is the "as: unwritten" case.
					Body: []*v1.Node{
						{Id: "ping", Kind: httpTaskExpr("item.url")},
					},
				},
			},
		},
	}

	if got := tests.PointAtStandIn(nodes, "http://127.0.0.1:9999"); len(got) != 0 {
		t.Fatalf("default iterator's item.url should be traceable, got unpointable: %v", got)
	}
}

// TestPointAtStandInStepVar checks the other traceable shape: a step's own var,
// read bare per the schema's own rule (Node.vars's doc comment — "read *bare*
// inside it: `${modified}`", never `${vars.modified}`).
func TestPointAtStandInStepVar(t *testing.T) {
	t.Parallel()

	nodes := []*v1.Node{
		{
			Id:   "fetch",
			Vars: map[string]*v1.Value{"target": v1.NewLiteral("https://svc.example/ping")},
			Kind: httpTaskExpr("target"),
		},
	}

	if got := tests.PointAtStandIn(nodes, "http://127.0.0.1:9999"); len(got) != 0 {
		t.Fatalf("a step's own literal var should be traceable, got unpointable: %v", got)
	}

	if v := nodes[0].GetVars()["target"].GetLiteral().GetStringValue(); v != "http://127.0.0.1:9999/ping" {
		t.Errorf("step var was not rewritten onto the stand-in: %q", v)
	}
}

// TestPointAtStandInRefusesUntraceableForEachItems is the honest-refusal half:
// items computed at run time have no literal for this walk to rewrite, so the
// example is still refused — but now the reason names the expression and why,
// rather than only the step id.
func TestPointAtStandInRefusesUntraceableForEachItems(t *testing.T) {
	t.Parallel()

	nodes := []*v1.Node{
		{Id: "discover", Kind: httpTask("https://directory.example/services")},
		{
			Id: "check-each",
			Kind: &v1.Node_ForEach{
				ForEach: &v1.ForEach{
					// Computed from an earlier step's output, not written as data — the
					// shape this walk can never see into no matter how it is taught,
					// because the list does not exist until the run does.
					Items:    v1.NewExpr("steps.discover.json.services"),
					Iterator: "service",
					Body: []*v1.Node{
						{Id: "check", Kind: httpTaskExpr("service.url")},
					},
				},
			},
		},
	}

	got := tests.PointAtStandIn(nodes, "http://127.0.0.1:9999")
	if len(got) != 1 {
		t.Fatalf("expected exactly the computed for_each's step reported, got: %v", got)
	}

	msg := got[0]
	if !strings.HasPrefix(msg, "check") {
		t.Errorf("diagnosis does not name the step: %q", msg)
	}
	if !strings.Contains(msg, "service.url") {
		t.Errorf("diagnosis does not name the expression it could not trace: %q", msg)
	}
	if !strings.Contains(msg, "items") {
		t.Errorf("diagnosis does not say what it traced the expression back to: %q", msg)
	}
}

// TestPointAtStandInRefusesUnrelatedExpression checks the general case: an
// expression this walk was never taught any tracing for at all — an earlier
// step's output referenced directly rather than through a loop or a var — is
// refused with the expression named, not silently mistaken for pointable.
func TestPointAtStandInRefusesUnrelatedExpression(t *testing.T) {
	t.Parallel()

	nodes := []*v1.Node{
		{Id: "discover", Kind: httpTask("https://directory.example/services")},
		{Id: "check", Kind: httpTaskExpr("steps.discover.json.primary_url")},
	}

	got := tests.PointAtStandIn(nodes, "http://127.0.0.1:9999")
	if len(got) != 1 {
		t.Fatalf("expected exactly the untraceable step reported, got: %v", got)
	}
	if !strings.Contains(got[0], "steps.discover.json.primary_url") {
		t.Errorf("diagnosis does not name the expression: %q", got[0])
	}
}

// TestPointAtStandInStillHandlesLiterals pins the case #183 left alone: a
// literal `url:`, on both a task and its undo, is unaffected by any of this.
func TestPointAtStandInStillHandlesLiterals(t *testing.T) {
	t.Parallel()

	nodes := []*v1.Node{
		{
			Id:   "provision",
			Kind: httpTask("https://svc.example/create"),
			Undo: &v1.Compensation{
				Task: &v1.Task{
					Name:   "http",
					Inputs: map[string]*v1.Value{"url": v1.NewLiteral("https://svc.example/delete")},
				},
			},
		},
	}

	if got := tests.PointAtStandIn(nodes, "http://127.0.0.1:9999"); len(got) != 0 {
		t.Fatalf("literal urls, including an undo's, should still be pointable, got: %v", got)
	}
}
