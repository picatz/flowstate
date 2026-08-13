package flowstatev1

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The completeness guard for the one traversal (#508).
//
// The defect class this closes is a walk that keeps its own list of places to look,
// written on the day its question was asked, and then silently skipping a branch
// the schema grew afterwards. Moving every walk onto one traversal removes four of
// the five lists; this file removes the fifth, by checking the remaining one
// against the schema rather than against another list somebody wrote.
//
// Two properties, and they are different. The first two tests say the traversal
// *knows about* every position and every recursion edge in the schema. The third
// says it actually *emits* what it claims to know: a table naming a slot the walk
// never delivers would satisfy the first test and help nobody.
//
// Together they mean a `Value` field added to the schema, or a new place the schema
// nests steps, fails a named test rather than being quietly skipped by whichever
// walks nobody thought to update. See walk.go's "What the guarantee actually is"
// for why this is a test rather than a compile error.

// valuePositionsInSchema returns every field of type [Value] reachable from
// [Workflow] by following message-typed fields, spelled as a path with `{}` for a
// map's values and `[]` for a repeated field.
//
// Recursion is cut when a message is re-entered, which is what keeps the answer a
// set of *positions* rather than an infinite set of paths: `Node.condition` is one
// position whether the step is at the top level or inside three loops. Which of
// those nestings the traversal descends is [TestEveryNodeRecursionEdgeIsDeclared]'s
// question, and the walker guards in walkers_guard_test.go check the descent
// itself.
//
// A [Value]'s own interior — a structure literal's entries, a list's elements — is
// not a document position and is where the walk stops, so the search never
// descends into one.
func valuePositionsInSchema(t *testing.T) []string {
	t.Helper()

	var found []string
	visiting := map[protoreflect.FullName]bool{}

	var walk func(md protoreflect.MessageDescriptor, path string)
	walk = func(md protoreflect.MessageDescriptor, path string) {
		if visiting[md.FullName()] {
			return
		}
		visiting[md.FullName()] = true
		defer delete(visiting, md.FullName())

		fields := md.Fields()
		for i := range fields.Len() {
			field := fields.Get(i)
			target, at, ok := messageField(field, path)
			if !ok {
				continue
			}
			if target.FullName() == valueMessageName {
				found = append(found, at)

				continue
			}
			walk(target, at)
		}
	}

	walk((&Workflow{}).ProtoReflect().Descriptor(), "Workflow")
	slices.Sort(found)

	return found
}

// nodeRecursionEdgesInSchema returns every field reachable from [Workflow] that
// holds further steps: a `repeated Node`, or the [Workflow] a `call:` embeds.
func nodeRecursionEdgesInSchema(t *testing.T) []string {
	t.Helper()

	var found []string
	visiting := map[protoreflect.FullName]bool{}

	var walk func(md protoreflect.MessageDescriptor, path string)
	walk = func(md protoreflect.MessageDescriptor, path string) {
		if visiting[md.FullName()] {
			return
		}
		visiting[md.FullName()] = true
		defer delete(visiting, md.FullName())

		fields := md.Fields()
		for i := range fields.Len() {
			field := fields.Get(i)
			target, at, ok := messageField(field, path)
			if !ok {
				continue
			}
			switch target.FullName() {
			case nodeMessageName:
				// A place steps nest, and also the message the next places nest
				// inside, so it is recorded and then descended.
				found = append(found, at)
				walk(target, at)
			case workflowMessageName:
				// A callee. Recorded, and never descended: it is the same shape
				// again, and the edges inside it are the ones already found.
				found = append(found, at)
			case valueMessageName:
				// A value's interior holds no steps.
			default:
				walk(target, at)
			}
		}
	}

	walk((&Workflow{}).ProtoReflect().Descriptor(), "Workflow")
	slices.Sort(found)

	return found
}

const (
	valueMessageName    protoreflect.FullName = "flowstate.v1.Value"
	nodeMessageName     protoreflect.FullName = "flowstate.v1.Node"
	workflowMessageName protoreflect.FullName = "flowstate.v1.Workflow"
)

// messageField reports the message a field holds and the path that names it, and
// false for a field that holds no message at all.
func messageField(field protoreflect.FieldDescriptor, path string) (protoreflect.MessageDescriptor, string, bool) {
	at := path + "." + string(field.Name())

	if field.IsMap() {
		value := field.MapValue()
		if value.Kind() != protoreflect.MessageKind {
			return nil, "", false
		}

		return value.Message(), at + "{}", true
	}

	if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
		return nil, "", false
	}

	if field.IsList() {
		at += "[]"
	}

	return field.Message(), at, true
}

// TestEveryValuePositionInTheSchemaIsWalked is the guard the whole refactor is for.
//
// Adding a `Value` field anywhere the schema reaches from [Workflow] fails here,
// naming the path that was added, until it has a [ValueSlot] — after which every
// walk sharing the traversal receives it, and the ones that do not want it say so
// in a named case with a reason rather than by silently not having a line.
func TestEveryValuePositionInTheSchemaIsWalked(t *testing.T) {
	declared := map[string]ValueSlot{}
	for slot, path := range ValueSlotSchemaPath() {
		previous, duplicate := declared[path]
		require.Falsef(t, duplicate,
			"slots %d and %d both claim schema position %q; one position is one slot", previous, slot, path)
		declared[path] = slot
	}

	for _, path := range valuePositionsInSchema(t) {
		require.Containsf(t, declared, path,
			"the schema holds a Value at %q and walk.go names no slot for it, so every walk over a "+
				"workflow document is now blind to it. Add a ValueSlot, emit it from WalkWorkflow or "+
				"WalkNode, and check each caller's switch to decide whether the default arm is right "+
				"for it", path)
		delete(declared, path)
	}

	require.Emptyf(t, declared,
		"walk.go names slots for schema positions that no longer exist: %v", declared)
}

// TestEveryNodeRecursionEdgeIsDeclared is the same guard for descent rather than
// for positions.
//
// A new place the schema nests steps is a new place every walk can be blind to, and
// the traversal either follows it or records why not. [NodeRecursionEdges] is that
// record; this checks it against the schema.
func TestEveryNodeRecursionEdgeIsDeclared(t *testing.T) {
	declared := NodeRecursionEdges()

	for _, edge := range nodeRecursionEdgesInSchema(t) {
		require.Containsf(t, declared, edge,
			"the schema nests steps at %q and NodeRecursionEdges says nothing about it. Either "+
				"WalkNode descends it — in which case add it as true — or it must carry a written "+
				"reason for being left alone, the way a `call:`'s callee does", edge)
		delete(declared, edge)
	}

	require.Emptyf(t, declared,
		"NodeRecursionEdges declares edges the schema no longer has: %v", declared)
}

// TestWalkEmitsEverySlotItDeclares closes the gap the two tests above leave: a table
// is only a claim, and a slot named there but never delivered would satisfy them
// both while helping nobody.
//
// The document below writes something into every position at once, which is why it
// is a shape no author would produce. That is deliberate: a per-feature fixture
// would test the features, and what is under test here is the enumeration.
func TestWalkEmitsEverySlotItDeclares(t *testing.T) {
	wf := workflowUsingEveryValuePosition()

	seen := map[ValueSlot]int{}
	WalkWorkflow(wf, Walk{Value: func(site ValueSite) {
		require.NotZerof(t, site.Slot, "a position was emitted with no slot, at field %q", site.Field())
		require.NotNil(t, site.Value, "a position was emitted with no value, at field %q", site.Field())
		seen[site.Slot]++
	}})

	for slot, path := range ValueSlotSchemaPath() {
		require.Containsf(t, seen, slot,
			"walk.go declares a slot for schema position %q and the traversal never emits it, so "+
				"every caller's switch has a dead arm and the position is unwalked in fact", path)
	}
}

// workflowUsingEveryValuePosition builds a document that writes a value into every
// position [ValueSlotSchemaPath] names.
func workflowUsingEveryValuePosition() *Workflow {
	return &Workflow{
		Name: "every-position",
		DeclaredInputs: []*InputDeclaration{{
			Name:    "who",
			Default: NewLiteral("nobody"),
			Example: NewLiteral("somebody"),
		}},
		Vars: map[string]*Value{"greeting": NewExpr("'hello'")},
		Steps: []*Node{
			{
				Id:        "task",
				Condition: NewExpr("true"),
				Vars:      map[string]*Value{"local": NewExpr("1")},
				Kind:      &Node_Task{Task: &Task{Name: "log", Inputs: map[string]*Value{"message": NewExpr("'hi'")}}},
				Undo: &Compensation{
					Task: &Task{Name: "log", Inputs: map[string]*Value{"message": NewExpr("'sorry'")}},
				},
			},
			{
				Id: "each",
				Kind: &Node_ForEach{ForEach: &ForEach{
					Items: NewLiteralList("x"),
					Body:  []*Node{{Id: "inner", Kind: &Node_Value{Value: NewExpr("1")}}},
				}},
			},
			{
				Id: "loop",
				Kind: &Node_Loop{Loop: &Loop{
					Until:         NewExpr("true"),
					Initial:       NewExpr("0"),
					Update:        NewExpr("1"),
					MaxIterations: 2,
				}},
			},
			{
				Id: "parallel",
				Kind: &Node_Parallel{Parallel: &Parallel{
					Branches: []*Parallel_Branch{{Steps: []*Node{{Id: "branch", Kind: &Node_Value{Value: NewExpr("2")}}}}},
				}},
			},
			{
				Id: "switch",
				Kind: &Node_Switch{Switch: &Switch{
					Value:   NewExpr("'x'"),
					Cases:   []*Switch_Case{{Values: []*Value{NewLiteral("x")}}},
					Default: &Switch_Default{Steps: []*Node{{Id: "fallback", Kind: &Node_Value{Value: NewExpr("3")}}}},
				}},
			},
			// A wait's kind is a oneof, so the three spellings need three steps.
			{
				Id: "wait_signal",
				Kind: &Node_Wait{Wait: &Wait{
					Kind: &Wait_Signal{Signal: &Signal{
						Name:    "approval",
						Prompt:  NewExpr("'approve?'"),
						Outputs: map[string]*Value{"decision": NewExpr("'yes'")},
					}},
					TimeoutExpr: NewExpr("'1m'"),
				}},
			},
			{
				Id:   "wait_until",
				Kind: &Node_Wait{Wait: &Wait{Kind: &Wait_Until{Until: NewExpr("true")}}},
			},
			{
				Id:   "wait_sleep",
				Kind: &Node_Wait{Wait: &Wait{Kind: &Wait_DurationExpr{DurationExpr: NewExpr("'1s'")}}},
			},
			{
				Id: "call",
				Kind: &Node_Call{Call: &Call{
					Workflow:  &Workflow{Name: "callee", Steps: []*Node{{Id: "callee_step", Kind: &Node_Value{Value: NewExpr("4")}}}},
					Arguments: map[string]*Value{"who": NewExpr("'you'")},
				}},
			},
		},
		DeclaredOutputs: []*OutputDeclaration{{Name: "answer", Value: NewExpr("42")}},
		Signals: map[string]*SignalPolicy{
			"approval": {Allow: []*SignalPolicyRule{{SubjectFrom: NewExpr("'a#b'")}}},
		},
		Triggers: &Triggers{
			Webhooks: []*WebhookTrigger{{
				Name:           "payments",
				IdempotencyKey: NewExpr("event.id"),
				Arguments:      map[string]*Value{"order": NewExpr("event.body.order")},
				Verify:         map[string]*Value{"stripe": NewLiteral("secret")},
			}},
		},
	}
}

// TestWalkDoesNotDescendIntoACallee pins the one recursion edge the traversal
// declines, so the reason recorded in [NodeRecursionEdges] is enforced rather than
// merely written down. A callee's steps run in an isolated scope; visiting them
// here would resolve another file's references against this file's steps and report
// its diagnostics on this file's lines.
func TestWalkDoesNotDescendIntoACallee(t *testing.T) {
	var visited []string
	WalkWorkflow(workflowUsingEveryValuePosition(), Walk{
		Node: func(node *Node) { visited = append(visited, node.GetId()) },
	})

	require.Contains(t, visited, "call", "the calling step itself is a step of this document")
	require.NotContains(t, visited, "callee_step",
		"the traversal descended into a callee, which resolves another workflow's expressions "+
			"against this one's steps; see NodeRecursionEdges")
}

// TestEverySlotHasAFieldName is the third table-versus-reality check, over the
// labels rather than over the positions.
//
// [ValueSite.Field] is a switch, so a slot added without an arm falls to the
// default and every diagnostic reported at that position names no key at all —
// which reads to an author as a diagnostic about the whole step rather than about
// what they wrote.
func TestEverySlotHasAFieldName(t *testing.T) {
	for slot, path := range ValueSlotSchemaPath() {
		site := ValueSite{Slot: slot, Name: "a_name", Owner: "an_owner"}
		require.NotEmptyf(t, site.Field(),
			"the slot for %q has no arm in ValueSite.Field, so a diagnostic reported at that "+
				"position would name no key and read as one about the whole step", path)
	}
}
