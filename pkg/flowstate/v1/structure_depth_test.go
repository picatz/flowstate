package flowstatev1_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// This file is the regression suite for #334: the compiler's document-nesting
// bound and the schema-side walks' structure-nesting bound used to disagree
// (64 versus 32), and the gap between them was a value the compiler admitted
// that a schema-layer walk could not fully inspect — the same fail-open shape
// #329 found in the secret walk, one layer further down the stack. See
// [v1.MaxStructureDepth]'s doc for the decision and the reproduction that
// established the premise.
//
// Every test here pins one of two things: that [v1.MaxStructureDepth] is
// enforced at exactly the boundary in both directions, or that a walk this
// package runs over a [v1.Value_Structure] fails *closed* — answers
// conservatively rather than silently under-reporting — when it hits that
// bound. The second kind is the real deliverable: an assertion that only
// checks the refusal is satisfied by a walk that gives up early and says
// nothing is wrong.

// wrapStructure nests inner in levels of one-entry structure maps, the same
// shape [TestASecretRefBelowTheDepthBoundStillAnswersTrue] already uses to
// probe [v1.ValueHoldsSecretRef].
func wrapStructure(inner *v1.Value, levels int) *v1.Value {
	for range levels {
		inner = v1.NewStructureMap(map[string]*v1.Value{"k": inner})
	}
	return inner
}

// wrapStructureNamed is [wrapStructure] but keeps a distinct field name at
// every level, so a Continue-As-New style walk that reports a dotted path can
// be asked whether it actually reached the bottom.
func wrapStructureNamed(inner *v1.Value, levels int) *v1.Value {
	for range levels {
		inner = v1.NewStructureMap(map[string]*v1.Value{"level": inner})
	}
	return inner
}

func TestCheckStructureDepthBothDirections(t *testing.T) {
	t.Parallel()

	leaf := v1.NewLiteral("x")

	within := &v1.Workflow{
		Name: "within",
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "http",
				Inputs: map[string]*v1.Value{
					"headers": wrapStructure(leaf, v1.MaxStructureDepth),
				},
			}},
		}},
	}
	require.NoError(t, v1.CheckStructureDepth(within),
		"a structure exactly at the bound must be accepted, not refused early")

	over := &v1.Workflow{
		Name: "over",
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name: "http",
				Inputs: map[string]*v1.Value{
					"headers": wrapStructure(leaf, v1.MaxStructureDepth+1),
				},
			}},
		}},
	}
	err := v1.CheckStructureDepth(over)
	require.Error(t, err, "a structure one level past the bound must be refused")
	require.Contains(t, err.Error(), "32",
		"the refusal must name the bound so an author has a number to act on")
}

// deepForEachChain builds a `for_each` chain nested levels deep — the shape
// whose control-flow depth is chosen entirely by the sender while its byte
// size stays far under [v1.MaxSpecBytes], so it is what an admission bound
// meets after every size precheck has already said yes.
func deepForEachChain(levels int) []*v1.Node {
	var body []*v1.Node
	for range levels {
		body = []*v1.Node{{Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{Body: body}}}}
	}
	return body
}

// stackGrowthDuring runs fn three times, each on a fresh goroutine of its own,
// and reports the largest number of goroutine-stack bytes the process gained
// during any single run.
//
// Both reads happen inside the goroutine, while it is still alive: a stack a
// recursive walk grew is still in use at the second read — the runtime only
// shrinks it at a later GC, and by at most half per cycle — whereas waiting
// for the goroutine to exit would hand its stack back to the allocator and
// erase exactly the growth this exists to see. A dedicated goroutine rather
// than the test's own, so every sample starts from a fresh small stack instead
// of whatever the test harness has already grown.
//
// The maximum of three, and the direction matters more than the count. The two
// ways a sample can lie are not symmetric. StackInuse is process-global, so a
// collection that frees a *previous* sample's dead 16 MiB stack inside this
// sample's window makes the delta read as a net decrease — reported as zero —
// even though this sample's own traversal grew a stack exactly as much as the
// others. Taking the minimum lets one such sample collapse the answer to zero
// and pass a recursive walk: the earlier spelling of this helper did exactly
// that, passing in isolation while failing beside its neighbours. Noise in the
// other direction (another goroutine growing its own stack in the window) can
// only inflate, and would have to invent four megabytes to matter against an
// iterative reading of tens of kilobytes. So the maximum is the reading that
// cannot produce a false pass, and runtime.GC below settles the previous
// sample's dead stack before each measurement rather than letting it land
// inside one.
func stackGrowthDuring(fn func()) uint64 {
	highest := uint64(0)
	for range 3 {
		// Settle the previous sample's dead stack here, where it cannot be
		// mistaken for this sample's own growth.
		runtime.GC()

		done := make(chan uint64, 1)
		go func() {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			fn()
			runtime.ReadMemStats(&after)
			if after.StackInuse <= before.StackInuse {
				done <- 0
				return
			}
			done <- after.StackInuse - before.StackInuse
		}()
		if growth := <-done; growth > highest {
			highest = growth
		}
	}
	return highest
}

// stackGrowthBudget is what the two stack-growth regressions below allow a
// bounded walk to grow the goroutine stack by while traversing a 50,000-level
// chain.
//
// The number discriminates, with headroom on both sides. Measured on this
// tree: a work-stack traversal grows the goroutine stack by 0–32 KiB (the
// goroutine's own locals), while the recursive traversal these tests regress
// against grows it by 16–32 MiB — one Go frame per nesting level, rounded up
// by the runtime's stack doubling, so the exact figure moves with Go version
// and frame layout but only by a constant factor. 4 MiB sits two orders of
// magnitude above the iterative reading and four below where a recursive walk
// could plausibly land even if a future toolchain halved its frame sizes, so
// neither an honest pass nor a real regression can drift across the line.
const stackGrowthBudget = 4 << 20

func TestCheckStructureDepthBoundsDeepControlFlowWithoutRecursiveTraversal(t *testing.T) {
	// Not parallel: stackGrowthDuring reads runtime.MemStats.StackInuse, a
	// process-global counter, and sharing the process with tests concurrently
	// growing their own goroutine stacks would make the delta theirs as much
	// as ours.

	wf := &v1.Workflow{Name: "deep-control-flow", Steps: deepForEachChain(50_000)}

	require.Less(t, proto.Size(wf), v1.MaxSpecBytes,
		"the regression must fit under the byte precheck that runs before structure validation")

	// Acceptance alone cannot see this regression: Go grows a goroutine stack
	// to 32 MiB before giving up, so a recursive walk also returns nil here
	// and the mutation this test exists to catch — putting Go recursion back
	// in [v1.CheckStructureDepth] — passed the earlier spelling that asserted
	// only NoError. Stack growth is the property the test's name claims, so
	// stack growth is what it measures.
	var err error
	growth := stackGrowthDuring(func() { err = v1.CheckStructureDepth(wf) })
	require.NoError(t, err,
		"control-flow depth chosen by a wire caller must still be accepted by the structure check")
	require.Less(t, growth, uint64(stackGrowthBudget),
		"checked control-flow depth must not become Go recursion depth: the walk grew the goroutine "+
			"stack by %d bytes, which only a recursive traversal does at this depth", growth)
}

// TestRequiredTaskNamesBoundsDeepControlFlowWithoutRecursiveTraversal is
// #1284's admission-path half: [v1.CheckStructureDepth] was made iterative so
// checked depth does not become Go recursion depth, and the very next call in
// submission validation — [v1.ResolveTaskCapabilities], through
// [v1.RequiredTaskNames]'s callee walk — handed each workflow to
// [v1.WalkWorkflow] *before* the callee walk's own depth guard had seen that
// workflow's steps. The guard still refused the specification; it just
// refused it after the recursive traversal had already grown the admission
// goroutine's stack by 32 MiB, once per in-flight submission. Both halves are
// asserted: the refusal (fail closed at a depth nothing past the guard can
// vouch for) and the stack staying flat on the way to it.
func TestRequiredTaskNamesBoundsDeepControlFlowWithoutRecursiveTraversal(t *testing.T) {
	// Not parallel, for stackGrowthDuring's reason above.

	wf := &v1.Workflow{Name: "deep-requirements", Steps: deepForEachChain(50_000)}

	require.Less(t, proto.Size(wf), v1.MaxSpecBytes,
		"the regression must fit under the byte precheck that runs before the requirement walk")

	var err error
	growth := stackGrowthDuring(func() { _, err = v1.RequiredTaskNames(wf) })
	require.Error(t, err,
		"a chain nested past what the callee walk is checked to must be refused, not silently "+
			"under-scanned")
	require.ErrorContains(t, err, "steps nest more than",
		"the refusal must be the callee walk's own depth guard, so an author is told the shape of "+
			"the problem")
	require.Less(t, growth, uint64(stackGrowthBudget),
		"the requirement walk must not spend a recursive stack before the guard refuses: it grew "+
			"the goroutine stack by %d bytes", growth)
}

// TestCollectNodeRefsFailsClosedPastTheDepthBound is the CAN-compaction half
// of the sweep. Before this, [v1.CollectValueRefs] silently stopped
// descending at the bound and recorded nothing for what lay beneath — an
// expression referencing a prior step, nested past the bound, would have had
// that step's outputs pruned at the next Continue-As-New, and the run would
// fail the first time it tried to read them.
func TestCollectNodeRefsFailsClosedPastTheDepthBound(t *testing.T) {
	t.Parallel()

	prev := &v1.Workflow_StepOutputs{StepValues: map[string]*v1.Node_Outputs{
		"first":  {},
		"second": {},
	}}

	// Within the bound: an exact answer. Only the step actually referenced is
	// retained, and the other one is not swept in for free.
	within := &v1.Node{
		Id: "a",
		Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
			"headers": wrapStructure(v1.NewExpr("steps.first.value"), v1.MaxStructureDepth-1),
		}}},
	}
	refs := map[string]map[string]struct{}{}
	v1.CollectNodeRefs(within, prev, refs)
	require.Contains(t, refs, "first", "a reference within the walk's reach must be found")
	require.NotContains(t, refs, "second",
		"a walk that cannot tell which step is referenced must not retain everything by default — "+
			"only past its bound may it fall back to that")

	// Past the bound: the walk cannot see the expression at all, so it must
	// retain every step [prev] carries rather than silently keeping neither.
	// Dropping "second" here (which nothing found a reference to, because
	// nothing could look) would be exactly the pruning bug this pins against.
	over := &v1.Node{
		Id: "a",
		Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
			"headers": wrapStructure(v1.NewExpr("steps.first.value"), v1.MaxStructureDepth+8),
		}}},
	}
	refs = map[string]map[string]struct{}{}
	v1.CollectNodeRefs(over, prev, refs)
	require.Contains(t, refs, "first",
		"past the bound every known step must be retained, including the one actually referenced")
	require.Contains(t, refs, "second",
		"past the bound the walk cannot know which step an expression names, so it must keep them all "+
			"rather than pruning one that turns out to be needed")
	_, whole := refs["first"][v1.WholeStep]
	require.True(t, whole, "past the bound a step must be retained whole, not by a field guess")
	_, whole = refs["second"][v1.WholeStep]
	require.True(t, whole, "past the bound a step must be retained whole, not by a field guess")
}

// TestWalkTruncatedFiresAtTheDepthBound pins the shared traversal's own
// signal: a caller that cares about "I stopped looking" versus "there was
// nothing to find" (today, only [v1.CollectNodeRefs]) must be told when
// [v1.Walk.nested] gives up on a structure with more beneath it, rather than
// the walk returning silently as though the value ended there.
func TestWalkTruncatedFiresAtTheDepthBound(t *testing.T) {
	t.Parallel()

	within := wrapStructureNamed(v1.NewLiteral("x"), v1.MaxStructureDepth-1)
	over := wrapStructureNamed(v1.NewLiteral("x"), v1.MaxStructureDepth+8)

	wf := func(headers *v1.Value) *v1.Workflow {
		return &v1.Workflow{
			Name: "t",
			Steps: []*v1.Node{{
				Id: "a",
				Kind: &v1.Node_Task{Task: &v1.Task{Name: "http", Inputs: map[string]*v1.Value{
					"headers": headers,
				}}},
			}},
		}
	}

	var truncated int
	v1.WalkWorkflow(wf(within), v1.Walk{
		Value:     func(v1.ValueSite) {},
		Truncated: func(v1.ValueSite) { truncated++ },
	})
	require.Zero(t, truncated, "a structure within the bound must never report truncation")

	truncated = 0
	var sites []string
	v1.WalkWorkflow(wf(over), v1.Walk{
		Value: func(v1.ValueSite) {},
		Truncated: func(site v1.ValueSite) {
			truncated++
			sites = append(sites, site.Field())
		},
	})
	require.NotZero(t, truncated, "a structure past the bound must report that the walk was cut off")
	require.NotEmpty(t, sites, "the truncation must name the position it happened at")
	require.True(t, strings.HasPrefix(sites[0], "headers"),
		"the truncated site should still be addressable by the caller, got %q", sites[0])
}

// TestNestedSecretStructureCompilesOnlyWithinTheBound is the compile-time
// half, exercised from this package via the same value shape a Flowfile
// compiles a nested reference into: the compiler now refuses a structure
// past [v1.MaxStructureDepth] directly (see
// pkg/flowstate/v1/flowfile/structure_depth_test.go for the Flowfile-level
// reproduction), and this pins the schema-side counterpart so a hand-built
// [v1.Workflow] arriving without a compiler in front of it — the RPC path —
// gets refused by the identical bound rather than a looser or absent one.
func TestNestedSecretStructureCompilesOnlyWithinTheBound(t *testing.T) {
	t.Parallel()

	ref := &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "TOKEN"}}}

	within := &v1.Workflow{
		Name: "within",
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "http",
				Inputs: map[string]*v1.Value{"headers": wrapStructure(ref, v1.MaxStructureDepth)},
			}},
		}},
	}
	require.NoError(t, v1.CheckStructureDepth(within))
	require.Equal(t, []string{"env:TOKEN"},
		v1.SecretRefsIn(within.GetSteps()[0].GetTask()),
		"a reference exactly at the bound must still be named, not just detected")

	over := &v1.Workflow{
		Name: "over",
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "http",
				Inputs: map[string]*v1.Value{"headers": wrapStructure(ref, v1.MaxStructureDepth+1)},
			}},
		}},
	}
	require.Error(t, v1.CheckStructureDepth(over),
		"a hand-built specification past the bound must be refused before it reaches any walk that "+
			"cannot fully inspect it")
}

// callWorkflow builds a workflow with a single step of id, which calls callee
// with no arguments — the shape of a `call:` [v1.WalkWorkflow] deliberately does
// not descend into (see [v1.NodeRecursionEdges]), and the shape
// [v1.CheckStructureDepth] must descend into anyway.
func callWorkflow(id string, callee *v1.Workflow) *v1.Workflow {
	return &v1.Workflow{
		Name: "caller-" + id,
		Steps: []*v1.Node{{
			Id:   id,
			Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}},
		}},
	}
}

// taskWorkflow builds a workflow with a single task step holding headers.
func taskWorkflow(name string, headers *v1.Value) *v1.Workflow {
	return &v1.Workflow{
		Name: name,
		Steps: []*v1.Node{{
			Id: "a",
			Kind: &v1.Node_Task{Task: &v1.Task{
				Name:   "http",
				Inputs: map[string]*v1.Value{"headers": headers},
			}},
		}},
	}
}

// TestCheckStructureDepthReachesAnInlinedCallee is Finding 1's regression: a
// [v1.Value_Structure] over the bound hidden entirely inside a `call:`'s
// inlined callee has to be refused, at any nesting of calls the bound
// permits — not only at the top level and not only one call deep.
//
// [v1.WalkWorkflow] deliberately skips `Workflow.steps[].call.workflow` for
// every other caller sharing that traversal (diagnostics, reference
// collection, the negation-drift lint), which is why this cannot simply lean
// on that walk: it is testing that [v1.CheckStructureDepth] does something
// none of those other callers may do, not that the shared traversal changed.
func TestCheckStructureDepthReachesAnInlinedCallee(t *testing.T) {
	t.Parallel()

	leaf := v1.NewLiteral("x")
	overStructure := wrapStructure(leaf, v1.MaxStructureDepth+1)

	t.Run("one call deep", func(t *testing.T) {
		t.Parallel()

		callee := taskWorkflow("callee", overStructure)
		caller := callWorkflow("call-1", callee)

		require.Error(t, v1.CheckStructureDepth(callee),
			"sanity: the callee alone must be refused too, or this is not testing what it claims")
		err := v1.CheckStructureDepth(caller)
		require.Error(t, err,
			"an over-depth structure hidden only inside a called workflow's own step must be refused")
		require.Contains(t, err.Error(), "32",
			"the refusal must still name the bound even when it was found inside a callee")
	})

	t.Run("nested at every depth the bound permits", func(t *testing.T) {
		t.Parallel()

		// A chain of calls exactly [v1.MaxCallDepth] deep, with the offending
		// structure at the bottom of the chain — the deepest position
		// [v1.CheckCallDepth] still lets execution reach at all. Anything this
		// walk accepts as reachable must actually be inspected.
		wf := taskWorkflow("leaf", overStructure)
		for i := v1.MaxCallDepth; i >= 1; i-- {
			wf = callWorkflow(fmt.Sprintf("call-%d", i), wf)
		}

		err := v1.CheckStructureDepth(wf)
		require.Error(t, err,
			"a structure past the bound must be refused however many calls deep it is hidden, up to "+
				"the depth the engine will actually execute")
	})

	t.Run("a within-bound structure inside a callee is accepted", func(t *testing.T) {
		t.Parallel()

		withinStructure := wrapStructure(leaf, v1.MaxStructureDepth)
		callee := taskWorkflow("callee", withinStructure)
		caller := callWorkflow("call-1", callee)

		require.NoError(t, v1.CheckStructureDepth(caller),
			"a callee whose own structure sits exactly at the bound must not be refused just for "+
				"being reached through a call")
	})
}

// TestCheckStructureDepthCallGraphTerminates pins the traversal's own two
// bounds — call-nesting depth and total steps visited — against the two
// shapes that could otherwise make it run forever or run long: a call that
// (directly or through others) calls itself, and a diamond where several call
// steps share a callee, multiplying the walk's breadth the same way a
// billion-laughs YAML document multiplies alias expansion.
//
// CLAUDE.md: "when a bound exists, assert it was reached as well as not
// exceeded" — a walk that gives up after one node also satisfies
// `err == nil` on a small input, so this asserts termination on inputs
// specifically built to demand many nodes of budget, not merely that small
// ones return quickly.
func TestCheckStructureDepthCallGraphTerminates(t *testing.T) {
	t.Parallel()

	t.Run("a self-referential call terminates rather than recursing forever", func(t *testing.T) {
		t.Parallel()

		// A structure violation sits beside the self-call rather than inside
		// it: the cycle itself carries no structure at all — a pure
		// self-reference is legal right up to [v1.MaxCallDepth], same as at
		// execution — so the assertion below is on termination, not on the
		// cycle somehow being the thing refused. The violation on the sibling
		// step is what proves the walk still finished and kept looking,
		// rather than hanging inside the cycle and never returning at all.
		cyclic := &v1.Workflow{
			Name: "cyclic",
			Steps: []*v1.Node{
				{Id: "self", Kind: &v1.Node_Call{Call: &v1.Call{}}},
				{
					Id: "task",
					Kind: &v1.Node_Task{Task: &v1.Task{
						Name: "http",
						Inputs: map[string]*v1.Value{
							"headers": wrapStructure(v1.NewLiteral("x"), v1.MaxStructureDepth+1),
						},
					}},
				},
			},
		}
		// Point the first step's call back at the caller itself, forging the
		// cycle a hand-built specification can express even though the
		// compiler's own file-reading walk would refuse it.
		cyclic.Steps[0].GetKind().(*v1.Node_Call).Call.Workflow = cyclic

		done := make(chan error, 1)
		go func() { done <- v1.CheckStructureDepth(cyclic) }()

		select {
		case err := <-done:
			require.Error(t, err,
				"the walk must finish past the cycle and still catch the structure violation on the "+
					"step beside it, rather than never returning at all")
			require.Contains(t, err.Error(), "32")
		case <-time.After(10 * time.Second):
			t.Fatal("CheckStructureDepth did not terminate on a cyclic call graph")
		}
	})

	t.Run("a diamond of calls terminates and is still refused", func(t *testing.T) {
		t.Parallel()

		leaf := taskWorkflow("leaf", wrapStructure(v1.NewLiteral("x"), v1.MaxStructureDepth+1))

		// Widened at every level: each of several call steps at one level calls
		// several callees at the next, each of which shares leaf as its own
		// callee — a diamond that multiplies breadth at every level the way a
		// self-referential call multiplies depth.
		const fanOut = 6
		const levels = 4

		wf := leaf
		for level := 0; level < levels; level++ {
			branches := make([]*v1.Node, 0, fanOut)
			for i := range fanOut {
				branches = append(branches, &v1.Node{
					Id:   fmt.Sprintf("l%d-c%d", level, i),
					Kind: &v1.Node_Call{Call: &v1.Call{Workflow: wf}},
				})
			}
			wf = &v1.Workflow{Name: fmt.Sprintf("level-%d", level), Steps: branches}
		}

		done := make(chan error, 1)
		go func() { done <- v1.CheckStructureDepth(wf) }()

		select {
		case err := <-done:
			require.Error(t, err, "a diamond of calls burying an over-depth structure must be refused")
		case <-time.After(10 * time.Second):
			t.Fatal("CheckStructureDepth did not terminate on a diamond-shaped call graph")
		}
	})
}

// TestCheckStructureDepthCallWalkNodeBudgetIsReached pins the traversal's total
// -nodes-visited bound on its own, separate from the depth bound: a call graph
// no single chain of which goes anywhere near [v1.MaxCallDepth], but wide
// enough at every level that the walk following every `call:` would visit far
// more steps than [v1.CheckStructureDepth] can afford, must be refused for
// exactly that — not accepted for lack of a structure violation, and not
// merely rejected via CheckCallDepth for being too deep, since it never is.
//
// This is the "reached, not just not exceeded" half CLAUDE.md's paging
// section asks for: a walk that gave up after one node would also satisfy
// "no structure violation found" on a small input, so this input is built
// wide enough that only a walker that actually tried to visit everything
// would notice it ran out of budget.
func TestCheckStructureDepthCallWalkNodeBudgetIsReached(t *testing.T) {
	t.Parallel()

	// No leaf holds an over-depth structure at all — every leaf value is an
	// ordinary literal — so a refusal here can only come from the walk's own
	// node budget, not from [v1.MaxStructureDepth].
	leaf := taskWorkflow("leaf", v1.NewLiteral("x"))

	// fanOut steps per level, each calling the same shared callee, four
	// levels deep: at fanOut=20 that is up to 20^4 = 160,000 steps the walk
	// would visit just entering the innermost level, well past the 100,000
	// budget, while every chain is only 4 calls deep — nowhere near
	// [v1.MaxCallDepth]'s 8.
	const fanOut = 20
	const levels = 4

	wf := leaf
	for level := range levels {
		branches := make([]*v1.Node, 0, fanOut)
		for i := range fanOut {
			branches = append(branches, &v1.Node{
				Id:   fmt.Sprintf("l%d-c%d", level, i),
				Kind: &v1.Node_Call{Call: &v1.Call{Workflow: wf}},
			})
		}
		wf = &v1.Workflow{Name: fmt.Sprintf("wide-%d", level), Steps: branches}
	}

	done := make(chan error, 1)
	go func() { done <- v1.CheckStructureDepth(wf) }()

	select {
	case err := <-done:
		require.Error(t, err,
			"a call graph too wide for the walk's node budget must be refused rather than silently "+
				"under-inspected")
		require.Contains(t, err.Error(), "100000",
			"the refusal should name the node budget so the shape of the problem (breadth, not depth) "+
				"is legible")
	case <-time.After(10 * time.Second):
		t.Fatal("CheckStructureDepth did not terminate on a call graph wide enough to exceed its node budget")
	}
}
