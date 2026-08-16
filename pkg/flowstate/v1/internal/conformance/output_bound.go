package conformance

import (
	"net/http"
	"strconv"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// taskOutputElementBound is the same 10,000-element ceiling
// [v1.CheckInputConstraints] applies to a caller's own submitted input,
// mirrored here rather than imported: the constant lives unexported in
// flowstatev1, and a public test package pinning the number a second time is
// what proves the two sides that are supposed to agree actually do — a
// change to the real constant that this file's cases stop tripping is a
// signal, not a maintenance chore to silence.
const taskOutputElementBound = 10_000

// TaskOutputElementBoundCases returns the shared cases for the *other* half
// of #204: a task's own result, rather than a caller's submitted input,
// carrying more list elements than a later `if:`/`for_each`/`${...}` can
// evaluate an expression over cheaply. See
// [v1.CheckInputConstraints]/[v1.checkInputListElementBound]'s own doc for
// the caller-input half this does not repeat, and eval.go's
// `Task.EvalInScope` for where this is actually bounded — the one place both
// execution drivers funnel every task's result through, built-in or plugin.
//
// The `http` task is what exercises this without a stub task or a registry
// swap: `/json-array/<n>` (see [NewHTTPServer]) returns a body well under the
// byte cap that still carries an arbitrary number of elements, which is
// exactly the asymmetry #204 found — a response bounded in bytes is not
// bounded in element count.
//
// httpBaseURL should come from [NewHTTPServer].
func TaskOutputElementBoundCases(httpBaseURL string) []Case {
	return []Case{
		{
			// At the bound, not past it: [v1.checkTaskOutputElementBound] (via
			// the shared walker both the input and output paths reuse) refuses
			// only once the running total exceeds the bound, so exactly
			// [taskOutputElementBound] elements is the largest a task's result
			// may legitimately carry.
			Name:     "a task result at the element bound succeeds",
			Workflow: fetchesJSONArray("at-bound", httpBaseURL, taskOutputElementBound),
			ExpectedOutputsPredicate: func(out *v1.Workflow_StepOutputs) bool {
				return jsonArrayLen(out, "fetch") == taskOutputElementBound
			},
		},
		{
			// One element past the bound. The point of this case, per #204's own
			// closing line: assert the bound is *reached* on a list sized past
			// it, not merely that a small list is let through.
			Name:          "a task result past the element bound is refused",
			Workflow:      fetchesJSONArray("past-bound", httpBaseURL, taskOutputElementBound+1),
			ExpectFailure: true,
		},
	}
}

// fetchesJSONArray builds a one-step workflow whose `http` step fetches a
// JSON array of n elements and reports it whole as the step's `items`
// output — the shape that puts a large list in a *task's result* rather than
// in anything the caller submitted, which is the resource this bound covers.
func fetchesJSONArray(name, httpBaseURL string, n int) *v1.Workflow {
	return &v1.Workflow{
		Name:    name,
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id: "fetch",
				Kind: &v1.Node_Task{Task: &v1.Task{
					Name: "http",
					Inputs: map[string]*v1.Value{
						"method":     v1.NewLiteral(http.MethodGet),
						"url":        v1.NewLiteral(httpBaseURL + jsonArrayPath(n)),
						"parse_json": v1.NewLiteral(true),
						"outputs":    v1.NewExpr(`{"items": response.json}`),
					},
				}},
			},
		},
	}
}

// jsonArrayPath formats the path [NewHTTPServer]'s `/json-array/` handler
// reads its element count from.
func jsonArrayPath(n int) string {
	return "/json-array/" + strconv.Itoa(n)
}

// forEachResultsByteBound mirrors [v1.MaxLoopResultsBytes] the same way
// [taskOutputElementBound] mirrors the element ceiling: the real constant is
// exported, and pinning the number a second time here is what proves the sizing
// these cases depend on still holds — a change to the real bound that stops
// these cases straddling it is a signal, not a chore.
const forEachResultsByteBound = v1.MaxLoopResultsBytes

// forEachResultsChunkBytes is the body size each iteration below contributes to
// `results` — 64 KiB, comfortably under the 1 MiB response cap yet large enough
// that a single-digit item count brackets the bound: a handful of iterations
// sit under it, a couple more cross it.
const forEachResultsChunkBytes = 64 << 10

// ForEachResultsBoundCases are the shared cases holding both drivers to #229's
// byte bound for the *`for_each`* construct — the sibling of `loop:` that
// accumulates the identical [v1.LoopResultsField] and, until this, did so with
// no size check on either driver.
//
// Run by both the local driver ([flowstatev1] eval_test.go's
// TestRunWorkflowForEachResultsBound) and the durable driver (engine
// workflow_test.go's identically-named test), which is what makes "a for_each
// whose results cross [v1.MaxLoopResultsBytes] fails, and one just under
// succeeds" something the two cannot disagree about — invariant 3's exact shape,
// the same discipline [LoopCases] holds the `loop:` construct to.
//
// The concurrent case (`max_parallel:`) matters twice over. On the durable
// driver it exercises a genuinely different code path — bounded fan-out whose
// iterations land out of order, checked at the join — while on the local driver
// (which runs every `for_each` sequentially by design) it exercises the ordinary
// sequential path. Both must reach the same verdict: the run fails, because the
// bound is a property of the accumulated `results`, not of how they were
// scheduled.
//
// Each iteration fetches a fixed 64 KiB body and reports it whole under `blob`,
// so `results` grows by ~64 KiB per item with a specification that stays tiny —
// the same tiny-spec/large-state asymmetry #229 is about. httpBaseURL should
// come from [NewHTTPServer].
func ForEachResultsBoundCases(httpBaseURL string) []Case {
	// Comfortably under the bound: these must all pass, which is what asserts the
	// bound is *reached* rather than merely never exceeded — a for_each that
	// accumulates real bytes and is still let through.
	underItems := (forEachResultsByteBound / forEachResultsChunkBytes) - 1
	// Comfortably over the bound, so the crossing iteration is unambiguous even
	// once per-iteration proto overhead is counted.
	overItems := (forEachResultsByteBound / forEachResultsChunkBytes) + 3

	return []Case{
		{
			Name:     "a for_each just under the results byte bound succeeds",
			Workflow: fetchesBytesEach("under-bound", httpBaseURL, underItems, forEachResultsChunkBytes, 0),
			ExpectedOutputsPredicate: func(out *v1.Workflow_StepOutputs) bool {
				return forEachResultsLen(out, "fan") == underItems
			},
		},
		{
			// Sequential on both drivers (no `max_parallel:`): the accumulation
			// crosses the bound part-way through and the run fails there.
			Name:          "a for_each over the results byte bound is refused (sequential)",
			Workflow:      fetchesBytesEach("over-bound-seq", httpBaseURL, overItems, forEachResultsChunkBytes, 0),
			ExpectFailure: true,
		},
		{
			// `max_parallel:` drives the durable driver's concurrent fan-out, whose
			// iterations complete out of order and are weighed at the join; the
			// local driver runs it sequentially. Either way the accumulated
			// `results` cross the bound, so either way the run fails — the outcome
			// the two drivers must agree on regardless of scheduling.
			Name:          "a for_each over the results byte bound is refused (concurrent)",
			Workflow:      fetchesBytesEach("over-bound-conc", httpBaseURL, overItems, forEachResultsChunkBytes, 4),
			ExpectFailure: true,
		},
	}
}

// fetchesBytesEach builds a one-step `for_each` that iterates n items and, for
// each, fetches a body of chunkBytes from the loopback server and reports it
// whole under `blob` — so the step's `results` accumulate ~chunkBytes per item.
// maxParallel over 1 requests bounded concurrency (honoured by the durable
// driver, run sequentially by the local one).
func fetchesBytesEach(name, httpBaseURL string, n, chunkBytes, maxParallel int) *v1.Workflow {
	return &v1.Workflow{
		Name:    name,
		Profile: v1.CurrentProfile,
		Steps: []*v1.Node{
			{
				Id: "fan",
				Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
					Items:       v1.NewExpr(rangeExpr(n)),
					MaxParallel: int32(maxParallel),
					Body: []*v1.Node{
						{
							Id: "chunk",
							Kind: &v1.Node_Task{Task: &v1.Task{
								Name: "http",
								Inputs: map[string]*v1.Value{
									"method":  v1.NewLiteral(http.MethodGet),
									"url":     v1.NewLiteral(httpBaseURL + "/bytes/" + strconv.Itoa(chunkBytes)),
									"outputs": v1.NewExpr(`{"blob": response.body}`),
								},
							}},
						},
					},
				}},
			},
		},
	}
}

// rangeExpr formats a CEL list literal [0, 1, ..., n-1] — the items a
// [fetchesBytesEach] for_each iterates. The values are unused by the body; only
// the count matters, so any n-element list would do.
func rangeExpr(n int) string {
	elems := make([]string, n)
	for i := range elems {
		elems[i] = strconv.Itoa(i)
	}
	return "[" + strings.Join(elems, ", ") + "]"
}

// forEachResultsLen reports how many iterations a [fetchesBytesEach] for_each
// recorded under `results`, or -1 when the step or its `results` list is absent
// — a count rather than a byte assertion, since what a passing case proves is
// that every iteration was accumulated and let through, not the exact size.
func forEachResultsLen(out *v1.Workflow_StepOutputs, stepID string) int {
	step, ok := out.GetStepValues()[stepID]
	if !ok {
		return -1
	}
	results, ok := step.GetNamedValues()["results"]
	if !ok {
		return -1
	}
	list := results.GetLiteral().GetListValue()
	if list == nil {
		return -1
	}
	return len(list.GetValues())
}

// jsonArrayLen reads the length of the `items` list a [fetchesJSONArray] step
// reported, or -1 if the step is absent or `items` is not a list — a
// predicate rather than an exact-value comparison, since asserting equality
// against a 10,000-element literal would only restate the fixture.
func jsonArrayLen(out *v1.Workflow_StepOutputs, stepID string) int {
	step, ok := out.GetStepValues()[stepID]
	if !ok {
		return -1
	}
	items, ok := step.GetNamedValues()["items"]
	if !ok {
		return -1
	}
	list := items.GetLiteral().GetListValue()
	if list == nil {
		return -1
	}
	return len(list.GetValues())
}
