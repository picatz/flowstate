package flowstatev1

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// A [Value] used to be one of three flat things — an expression, a literal, or a
// reference — and a mapping written in a Flowfile was compiled into one of the
// first two. That is why `bearer:` had to exist before a secret could be sent at
// all: an `Authorization` entry inside `headers:` was part of a single expression
// the *workflow* evaluated, and evaluating a reference is precisely what writes a
// secret into durable history.
//
// [Value_Structure] is the shape that lifts that limitation without moving the
// resolution anywhere it must not go. A structure's entries are Values, so a
// reference nested in one stays a reference through compilation, through the
// specification, across the activity payload, and is resolved only by the worker
// applying the header or encoding the body.
//
// Two rules keep it honest, and both are enforced where the file is compiled:
//
//   - A structure's entries are literals and references. An expression nested in
//     one would be invisible to the walkers that decide which step outputs a
//     resumed run has to carry, and an output they miss is pruned from a run that
//     then fails after Continue-As-New.
//   - Only a task input the task *applies itself*, entry by entry, accepts one.
//     See [TaskDef.NestedSecretInputs].

// MaxStructureDepth bounds how deeply a [Value_Structure] may nest.
//
// Exported, and the only definition of this number in the module, per the
// one-constant rule: every walk below that descends into a structure reads
// this one, `pkg/flowstate/v1/flowfile` reads it to refuse a Flowfile whose
// compiled structure would exceed it before it ever reaches a walk that
// enforces it a second time, and `CheckStructureDepth` reads it to refuse a
// hand-built specification arriving over the RPC path without ever passing
// through the compiler at all. Before this was unified, the compiler's own
// document-nesting bound (`flowfile.maxDepth`, 64) and this bound disagreed —
// two constants for what reads as one concept, and the gap between them was
// exactly where #329's depth-33 fail-open evasion lived: a Flowfile the
// compiler accepted could still compile a structure deeper than the walks
// below were prepared to fully inspect. Document nesting and structure
// nesting share one YAML depth budget during compilation (a `Value_Structure`
// literal is built by descending the same document tree flowfile.maxDepth
// bounds), but 64 is loose enough to admit a structure past 32 while staying
// well inside the document bound — see structure_depth_test.go for the
// reproduction. 32 is the bound that wins: fail closed at the number every
// walk below can actually afford to inspect, everywhere a structure can
// arrive, rather than loosening every walk to match a looser parser limit
// that exists to bound a different resource (total YAML document depth, not
// structure nesting specifically).
//
// The walks below are recursive and a specification does not have to have come
// from a Flowfile — the compiler's own nesting bound says nothing about a message
// submitted over the wire, which is a value an outside party chooses. Depth is the
// resource that walk spends, so depth is what is bounded here; breadth costs one
// pass per entry and is bounded by the size of the specification itself.
const MaxStructureDepth = 32

// maxStructureWalkNodes bounds how many steps [CheckStructureDepth] will visit
// in total while following `call:` into inlined callees.
//
// [WalkWorkflow] deliberately does not follow `Workflow.steps[].call.workflow`
// — see [NodeRecursionEdges] — because most of what runs over one traversal
// (diagnostics, reference collection, the negation-drift lint) must not count a
// library workflow's expressions against its caller. Structure depth is the
// exception: a `Value_Structure` the engine cannot fully inspect is exactly as
// dangerous nested inside an inlined callee as it is at the top level, because
// [Call.Workflow] carries the callee's specification whole — see that field's
// doc — so its steps are steps of *this* submission by the time execution
// reaches them.
//
// So [CheckStructureDepth] runs its own bounded descent into `call.workflow`,
// separate from [WalkWorkflow]'s contract, which stays exactly what every
// other caller of that traversal depends on. Two resources are bounded, for
// the reason CLAUDE.md gives generally: a diamond of calls — several call
// steps whose callees themselves call a callee in common — multiplies breadth
// at every level exactly as a billion-laughs YAML document does, so depth
// alone does not stop it.
//
//   - Call-nesting depth is bounded by [MaxCallDepth], the same number the
//     engine itself refuses a call chain past at execution — reused rather
//     than invented, per the one-constant rule, since a chain nested deeper
//     than that is refused at runtime regardless of what this check finds.
//   - Total steps visited, across every callee entered, is bounded here by
//     this constant, mirroring `flowfile.maxNodes` (100,000): call-nesting
//     depth bounds one chain, and a document can hold many call chains side
//     by side, each nested to the limit, so the walk's total cost is the
//     resource that actually needs its own number.
//
// In production this is a backstop rather than the load-bearing bound: every
// caller of [CheckStructureDepth] runs [CheckSpecSize] first, and a call's
// callee is carried whole in the specification's own bytes, so a diamond wide
// enough to threaten this budget has already been refused for its size before
// this walk ever starts. The budget exists for the caller that does not enjoy
// that ordering — a direct call to [CheckStructureDepth], from a test or from
// code written later — because a bound only reachable through a particular
// caller's ordering is not a bound this function actually has.
const maxStructureWalkNodes = 100_000

// CheckStructureDepth refuses a workflow holding a [Value_Structure] nested
// deeper than [MaxStructureDepth], at any value position reachable from wf —
// including one nested inside a `call:`'s inlined callee, any number of calls
// deep up to [MaxCallDepth].
//
// This is the RPC-path half of the bound. A Flowfile can never compile one
// this deep — see [MaxStructureDepth]'s doc — but a specification submitted
// directly over the RPC boundary arrives without the compiler in front of it,
// so the schema-level walks that decide authority, references and Continue-
// As-New retention are the only defense a hand-built spec meets. Called from
// submission validation, alongside [Validate] and [CheckSpecSize]: those ask
// whether the message is well-formed and whether it fits, and this asks
// whether every walk this package runs over it later can actually see all of
// it. A structure too deep to inspect is refused here rather than silently
// under-inspected later.
//
// [WalkWorkflow] does not descend into a call's callee — every other walk
// that shares it must not — so this function does that part itself, bounded
// by [maxStructureWalkNodes]; see that constant's doc for both bounds and why
// a cyclic or diamond-shaped call graph cannot make the walk run long or loop
// forever.
func CheckStructureDepth(wf *Workflow) error {
	var violation *ValueSite
	var violationChain []string
	nodesLeft := maxStructureWalkNodes
	exhausted := false

	type frame struct {
		workflow  *Workflow
		nodes     []*Node
		callDepth int
		chain     []string
	}
	stack := []frame{{workflow: wf, nodes: wf.GetSteps()}}

	for len(stack) > 0 && violation == nil && !exhausted {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]

		// Workflow-level values belong to every inlined callee too. A shallow
		// copy lets the shared value-position walk inspect them without entering
		// the recursive node traversal this bounded validator must avoid.
		if current.workflow != nil {
			shallow := *current.workflow
			shallow.Steps = nil
			WalkWorkflow(&shallow, Walk{Value: func(site ValueSite) {
				if structureDepth(site.Value, 0) > MaxStructureDepth {
					s := site
					violation = &s
					violationChain = slices.Clone(current.chain)
				}
			}})
			if violation != nil {
				break
			}
		}

		for i := len(current.nodes) - 1; i >= 0; i-- {
			stack = append(stack, frame{
				nodes:     []*Node{current.nodes[i]},
				callDepth: current.callDepth,
				chain:     current.chain,
			})
		}
		for len(stack) > 0 && stack[len(stack)-1].workflow == nil && violation == nil && !exhausted {
			last = len(stack) - 1
			nodeFrame := stack[last]
			stack = stack[:last]
			node := nodeFrame.nodes[0]
			if node == nil {
				continue
			}
			if nodesLeft <= 0 {
				exhausted = true
				break
			}
			nodesLeft--

			walkNodeValues(node, Walk{Value: func(site ValueSite) {
				if violation == nil && structureDepth(site.Value, 0) > MaxStructureDepth {
					s := site
					violation = &s
					violationChain = slices.Clone(nodeFrame.chain)
				}
			}})
			if violation != nil {
				break
			}

			var children [][]*Node
			switch kind := node.GetKind().(type) {
			case *Node_ForEach:
				children = append(children, kind.ForEach.GetBody())
			case *Node_Loop:
				children = append(children, kind.Loop.GetBody())
			case *Node_Parallel:
				for _, branch := range kind.Parallel.GetBranches() {
					children = append(children, branch.GetSteps())
				}
			case *Node_Switch:
				children = append(children, SwitchBodies(kind.Switch)...)
			case *Node_Call:
				callee := kind.Call.GetWorkflow()
				nextDepth := nodeFrame.callDepth + 1
				if callee != nil && CheckCallDepth(nextDepth) == nil {
					chain := append(slices.Clone(nodeFrame.chain), node.GetId())
					stack = append(stack, frame{workflow: callee, nodes: callee.GetSteps(), callDepth: nextDepth, chain: chain})
				}
			}
			for i := len(children) - 1; i >= 0; i-- {
				for j := len(children[i]) - 1; j >= 0; j-- {
					stack = append(stack, frame{nodes: []*Node{children[i][j]}, callDepth: nodeFrame.callDepth, chain: nodeFrame.chain})
				}
			}
		}
	}

	if exhausted && violation == nil {
		return fmt.Errorf(
			"the workflow's call graph holds more than %d steps once every `call:`'s callee is walked, "+
				"which is more than this server can inspect for a nested structure while deciding whether "+
				"a step reads a secret; flatten the call graph or reduce how many workflows it calls",
			maxStructureWalkNodes)
	}
	if violation == nil {
		return nil
	}

	field := violation.Field()
	where := violation.Step
	if where == "" {
		where = "the workflow"
	} else if field != "" {
		where = fmt.Sprintf("step %q", where)
	}

	calledFrom := ""
	if len(violationChain) > 0 {
		calledFrom = fmt.Sprintf(" (reached by calling %s)", callChainText(violationChain))
	}

	if field != "" {
		return fmt.Errorf(
			"%s's %s nests a structure more than %d levels deep%s, which is deeper than this server can "+
				"walk cheaply while deciding whether a step reads a secret; flatten it, or have a step "+
				"read it from a reference instead of submitting it nested this deep",
			where, field, MaxStructureDepth, calledFrom)
	}
	return fmt.Errorf(
		"%s nests a structure more than %d levels deep%s, which is deeper than this server can walk "+
			"cheaply while deciding whether a step reads a secret; flatten it, or have a step read it "+
			"from a reference instead of submitting it nested this deep",
		where, MaxStructureDepth, calledFrom)
}

// callChainText renders the steps a violation was reached through as
// `step "a" > step "b"`, the same arrow [Call]'s doc uses to describe a
// position that stays a path rather than a flattened name.
func callChainText(chain []string) string {
	parts := make([]string, len(chain))
	for i, id := range chain {
		parts[i] = fmt.Sprintf("step %q", id)
	}
	return strings.Join(parts, " > ")
}

// structureDepth measures how many levels of [Value_Structure] nest inside v,
// stopping early once it has already proven the answer is over the bound: the
// caller only needs to know "too deep" versus a precise number, and stopping
// early keeps this cheap against a value built to make it expensive.
func structureDepth(v *Value, depth int) int {
	if depth > MaxStructureDepth {
		return depth
	}

	structure := v.GetStructure()
	if structure == nil {
		return depth
	}

	max := depth
	for _, entry := range StructureValues(structure) {
		if d := structureDepth(entry, depth+1); d > max {
			max = d
			if max > MaxStructureDepth {
				return max
			}
		}
	}
	return max
}

// ValueHoldsSecretRef reports whether v is a secret reference or contains one at
// any depth.
//
// It answers the question "does executing this need the authority to read a
// secret", which is why it looks inside structures where [Value.GetSecretRef]
// cannot: a reference in a header map is as much a secret read as one written as
// the whole value of `bearer:`.
// A value nested past the walk's depth bound answers true: the question is
// whether executing needs the authority, and a structure too deep to inspect
// may need it, so every consumer (the registry's authority gate, the plugin
// input and output refusals, flow test's resolver gate) fails closed rather
// than open at depth.
func ValueHoldsSecretRef(v *Value) bool {
	found := false
	walkSecretRefs(v, 0, func(*SecretRef) bool {
		found = true
		return false
	})
	return found
}

// SecretRefsIn returns every reference a task's inputs name, rendered as
// `scheme:name` and sorted.
//
// References and never values, which is what makes it safe to log or attach to a
// span: a [SecretRef] is a scheme and a name and holds no material by
// construction. Sorted because inputs are a map, and a set of attributes that
// reorders between two runs of one step is a diff nobody can read.
//
// It exists in this package rather than in a caller because there is more than one
// caller and the walk has to agree: the durable driver names them on a span, and
// anything else that wants to say which secrets a step will read gets the same
// answer, including the ones nested inside a structure that a top-level look
// cannot see.
func SecretRefsIn(task *Task) []string {
	var refs []string
	for _, value := range task.GetInputs() {
		walkSecretRefs(value, 0, func(ref *SecretRef) bool {
			if ref == nil {
				// The walk hit its depth bound: something below may be a
				// reference it cannot name. Naming surfaces stay exact and
				// skip it; the authority question is ValueHoldsSecretRef's,
				// which answers conservatively.
				return true
			}
			refs = append(refs, secretRefText(ref))
			return true
		})
	}

	slices.Sort(refs)
	return slices.Compact(refs)
}

// walkSecretRefs visits every reference in v, stopping early when visit says so.
//
// Past MaxStructureDepth the walk cannot see what is below, and it visits nil to
// say so rather than walking on or staying silent: a visitor deciding an
// authority or refusal question must treat "too deep to scan" as "may hold one",
// because the compiler admits deeper nesting than this walk inspects and a
// silent cutoff turned every consumer into a fail-open gate at depth 33
// (#329 review). A visitor that only names references skips the nil.
func walkSecretRefs(v *Value, depth int, visit func(*SecretRef) bool) bool {
	if v == nil {
		return true
	}
	if depth > MaxStructureDepth {
		return visit(nil)
	}

	switch kind := v.GetKind().(type) {
	case *Value_SecretRef:
		return visit(kind.SecretRef)
	case *Value_Structure_:
		for _, entry := range StructureValues(kind.Structure) {
			if !walkSecretRefs(entry, depth+1, visit) {
				return false
			}
		}
	}

	return true
}

// StructureValues returns the values a structure holds, in a fixed order: a list's
// elements as written, and a mapping's entries by sorted key.
//
// Sorted rather than in map order because more than one thing renders a structure
// — a form body, a query string, a diagnostic — and a rendering that reorders
// between two runs of the same specification is a difference nobody made.
func StructureValues(s *Value_Structure) []*Value {
	switch kind := s.GetKind().(type) {
	case *Value_Structure_List_:
		return kind.List.GetValues()
	case *Value_Structure_Map_:
		entries := kind.Map.GetEntries()
		values := make([]*Value, 0, len(entries))
		for _, name := range slices.Sorted(maps.Keys(entries)) {
			values = append(values, entries[name])
		}
		return values
	default:
		return nil
	}
}

// StructureMap returns the entries of a structure written as a mapping, and false
// for one written as a list.
func StructureMap(v *Value) (map[string]*Value, bool) {
	structure := v.GetStructure()
	if structure == nil {
		return nil, false
	}
	kind, ok := structure.GetKind().(*Value_Structure_Map_)
	if !ok {
		return nil, false
	}
	return kind.Map.GetEntries(), true
}

// NewStructureMap builds a mapping-shaped structure value.
func NewStructureMap(entries map[string]*Value) *Value {
	return &Value{Kind: &Value_Structure_{Structure: &Value_Structure{
		Kind: &Value_Structure_Map_{Map: &Value_Structure_Map{Entries: entries}},
	}}}
}

// NewStructureList builds a list-shaped structure value.
func NewStructureList(values ...*Value) *Value {
	return &Value{Kind: &Value_Structure_{Structure: &Value_Structure{
		Kind: &Value_Structure_List_{List: &Value_Structure_List{Values: values}},
	}}}
}
