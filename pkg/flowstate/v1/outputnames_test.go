package flowstatev1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
)

// nodeKindBuilders holds one minimal, valid *Node per arm of the [Node] `kind`
// oneof, keyed by the oneof field's schema name.
//
// A hand-kept map, deliberately: [OutputNames] needs a real instance of each
// kind to answer about, and there is no way to derive "a node that is a loop"
// from a descriptor alone. What is derived is the *set of kinds* below, from
// the oneof itself — so the day a ninth kind lands on [Node], this map no
// longer has an entry for it, TestEveryNodeKindIsCovered's set comparison
// fails by name, and whoever added the kind is told exactly which map is
// missing an entry rather than discovering it from a silent gap in an editor's
// completion menu months later.
func nodeKindBuilders() map[string]*Node {
	return map[string]*Node{
		"task": {Kind: &Node_Task{Task: &Task{
			Name:   "log",
			Inputs: map[string]*Value{"message": NewLiteral("hi")},
		}}},
		"for_each": {Kind: &Node_ForEach{ForEach: &ForEach{
			Items: NewLiteralList("a", "b"),
		}}},
		"parallel": {Kind: &Node_Parallel{Parallel: &Parallel{}}},
		"wait": {Kind: &Node_Wait{Wait: &Wait{
			Kind: &Wait_Duration{Duration: durationpb.New(0)},
		}}},
		"call": {Kind: &Node_Call{Call: &Call{
			Workflow: &Workflow{Name: "callee"},
		}}},
		"loop": {Kind: &Node_Loop{Loop: &Loop{
			Until: NewLiteral(true),
		}}},
		"value": {Kind: &Node_Value{Value: NewLiteral(1)}},
		"switch": {Kind: &Node_Switch{Switch: &Switch{
			Value: NewLiteral("x"),
		}}},
	}
}

// nodeKindOneofNames reads the [Node] `kind` oneof's arms straight from the
// generated descriptor, the same way [NodeContainerKinds] does — so this test
// cannot go stale by someone forgetting to update a hand-written list when the
// schema grows an arm.
func nodeKindOneofNames(t *testing.T) []string {
	t.Helper()
	nodeDesc := (&Node{}).ProtoReflect().Descriptor()
	kind := nodeDesc.Oneofs().ByName("kind")
	require.NotNil(t, kind, "the Node message no longer has a `kind` oneof")

	names := make([]string, 0, kind.Fields().Len())
	for i := range kind.Fields().Len() {
		names = append(names, string(kind.Fields().Get(i).Name()))
	}
	return names
}

// TestEveryNodeKindIsCovered is the point of this file: every arm the schema
// currently defines for Node.kind has a builder above, and [OutputNames]
// answers something — never silence — for every one of them.
//
// This is what a hand-written switch over eight kinds cannot promise on its
// own: today the count happens to be eight in both the schema and every call
// site, but nothing before this test made a ninth arm's absence from
// [OutputNames] (or from the language server, or from switchDomain) loud
// rather than a name nobody thought to ask about.
func TestEveryNodeKindIsCovered(t *testing.T) {
	t.Parallel()

	schemaKinds := nodeKindOneofNames(t)
	builders := nodeKindBuilders()

	builtKinds := make([]string, 0, len(builders))
	for name := range builders {
		builtKinds = append(builtKinds, name)
	}

	assert.ElementsMatch(t, schemaKinds, builtKinds,
		"the Node.kind oneof and this test's builder map have drifted apart; "+
			"a kind present in one and not the other means OutputNames (and its "+
			"callers) were never asked about it")

	for _, kindName := range schemaKinds {
		node, ok := builders[kindName]
		require.True(t, ok, "no builder for kind %q", kindName)

		t.Run(kindName, func(t *testing.T) {
			t.Parallel()

			names, ok := OutputNames(node, nil)
			if kindName == "parallel" {
				assert.False(t, ok, "a parallel block exposes nothing under its own step id")
				assert.Empty(t, names)
				return
			}

			assert.True(t, ok, "kind %q must report ok=true: it exposes something, even if not statically nameable", kindName)
			require.NotEmpty(t, names, "kind %q reported ok=true but returned no entries at all", kindName)
			for _, n := range names {
				assert.NotEmpty(t, n.Description, "every NamedOutput needs a description, named or not")
			}
		})
	}
}

func TestOutputNamesForATimerWaitIsTimedOutAlone(t *testing.T) {
	t.Parallel()

	node := &Node{Kind: &Node_Wait{Wait: &Wait{Kind: &Wait_DurationExpr{DurationExpr: NewExpr("duration('5s')")}}}}
	names, ok := OutputNames(node, nil)
	require.True(t, ok)
	require.Len(t, names, 1)
	assert.Equal(t, TimedOutOutput, names[0].Name)
}

func TestOutputNamesForAnUnshapedSignalWaitIsThreeNames(t *testing.T) {
	t.Parallel()

	node := &Node{Kind: &Node_Wait{Wait: &Wait{Kind: &Wait_Signal{Signal: &Signal{Name: "approval"}}}}}
	names, ok := OutputNames(node, nil)
	require.True(t, ok)

	var got []string
	for _, n := range names {
		got = append(got, n.Name)
		assert.Nil(t, n.Source, "an unshaped wait's names are the engine's own record, not a written expression")
	}
	assert.ElementsMatch(t, []string{TimedOutOutput, PayloadOutput, SenderOutput}, got)
}

func TestOutputNamesForAShapedSignalWaitReplacesTheDefaultNames(t *testing.T) {
	t.Parallel()

	approvedExpr := NewExpr("has(payload.approved) && payload.approved")
	node := &Node{Kind: &Node_Wait{Wait: &Wait{Kind: &Wait_Signal{Signal: &Signal{
		Name: "approval",
		Outputs: map[string]*Value{
			"approved": approvedExpr,
		},
	}}}}}

	names, ok := OutputNames(node, nil)
	require.True(t, ok)
	require.Len(t, names, 1, "shaping replaces the wait's outputs; it does not add to them")
	assert.Equal(t, "approved", names[0].Name)
	assert.Same(t, approvedExpr, names[0].Source, "a shaped wait output's Source is the written expression, for domain inference")

	for _, reserved := range []string{TimedOutOutput, PayloadOutput, SenderOutput} {
		for _, n := range names {
			assert.NotEqual(t, reserved, n.Name, "shaping dropped %q; it must not still be offered", reserved)
		}
	}
}

func TestOutputNamesForAValueStepCarriesItsExpressionAsSource(t *testing.T) {
	t.Parallel()

	expr := NewExpr("1 + 1")
	node := &Node{Kind: &Node_Value{Value: expr}}
	names, ok := OutputNames(node, nil)
	require.True(t, ok)
	require.Len(t, names, 1)
	assert.Equal(t, ValueOutput, names[0].Name)
	assert.Same(t, expr, names[0].Source)
}

func TestOutputNamesForALoopWithoutStateIsResultsAlone(t *testing.T) {
	t.Parallel()

	node := &Node{Kind: &Node_Loop{Loop: &Loop{Until: NewLiteral(true)}}}
	names, ok := OutputNames(node, nil)
	require.True(t, ok)
	require.Len(t, names, 1)
	assert.Equal(t, LoopResultsField, names[0].Name)
}

func TestOutputNamesForALoopWithStateAddsState(t *testing.T) {
	t.Parallel()

	node := &Node{Kind: &Node_Loop{Loop: &Loop{
		Until: NewLiteral(true),
		State: "cursor",
	}}}
	names, ok := OutputNames(node, nil)
	require.True(t, ok)

	var got []string
	for _, n := range names {
		got = append(got, n.Name)
	}
	assert.ElementsMatch(t, []string{LoopResultsField, LoopStateField}, got)
}

func TestOutputNamesForAForEachIsResultsAlone(t *testing.T) {
	t.Parallel()

	node := &Node{Kind: &Node_ForEach{ForEach: &ForEach{Items: NewLiteralList()}}}
	names, ok := OutputNames(node, nil)
	require.True(t, ok)
	require.Len(t, names, 1)
	assert.Equal(t, LoopResultsField, names[0].Name)
}

func TestOutputNamesForAParallelIsExplicitlyNothing(t *testing.T) {
	t.Parallel()

	node := &Node{Kind: &Node_Parallel{Parallel: &Parallel{}}}
	names, ok := OutputNames(node, nil)
	assert.False(t, ok, "a parallel block's branch outputs merge into the enclosing scope; nothing is exposed under its own id")
	assert.Nil(t, names)
}

func TestOutputNamesForACallListsTheCalleesDeclaredOutputs(t *testing.T) {
	t.Parallel()

	node := &Node{Kind: &Node_Call{Call: &Call{Workflow: &Workflow{
		Name: "callee",
		DeclaredOutputs: []*OutputDeclaration{
			{Name: "digest", Value: NewLiteral("sha256:abc")},
		},
	}}}}
	names, ok := OutputNames(node, nil)
	require.True(t, ok)
	require.Len(t, names, 1)
	assert.Equal(t, "digest", names[0].Name)
}

func TestOutputNamesForACallWithNoDeclaredOutputsSaysSo(t *testing.T) {
	t.Parallel()

	node := &Node{Kind: &Node_Call{Call: &Call{Workflow: &Workflow{Name: "callee"}}}}
	names, ok := OutputNames(node, nil)
	require.True(t, ok)
	require.Len(t, names, 1)
	assert.Empty(t, names[0].Name)
	assert.NotEmpty(t, names[0].Description)
}

func TestOutputNamesForATaskUsesDeclaredOutputsFromTheRegistry(t *testing.T) {
	t.Parallel()

	node := &Node{Kind: &Node_Task{Task: &Task{
		Name: "http",
		Inputs: map[string]*Value{
			"method": NewLiteral("GET"),
			"url":    NewLiteral("https://example.invalid"),
		},
	}}}
	names, ok := OutputNames(node, DefaultRegistry())
	require.True(t, ok)
	require.NotEmpty(t, names, "the http task declares outputs")

	var got []string
	for _, n := range names {
		got = append(got, n.Name)
	}
	assert.Contains(t, got, "status_code")
	assert.Contains(t, got, "body")
}

func TestOutputNamesForAShapedTaskWithLiteralShapingListsTheShapedNames(t *testing.T) {
	t.Parallel()

	node := &Node{Kind: &Node_Task{Task: &Task{
		Name: "http",
		Inputs: map[string]*Value{
			"method": NewLiteral("GET"),
			"url":    NewLiteral("https://example.invalid"),
			ShapingInput: NewExpr(
				`{"records": response.json.records, "next_cursor": response.json.next_cursor}`),
		},
	}}}
	names, ok := OutputNames(node, DefaultRegistry())
	require.True(t, ok)

	var got []string
	for _, n := range names {
		got = append(got, n.Name)
	}
	assert.ElementsMatch(t, []string{"records", "next_cursor"}, got)
	for _, declared := range []string{"status_code", "headers", "body"} {
		assert.NotContains(t, got, declared, "shaping replaces the declared outputs; they must not still be offered")
	}
}

func TestOutputNamesForAnUnregisteredTaskSaysSoRatherThanGuessing(t *testing.T) {
	t.Parallel()

	node := &Node{Kind: &Node_Task{Task: &Task{Name: "nope.nonexistent"}}}
	names, ok := OutputNames(node, NewRegistry())
	require.True(t, ok)
	require.Len(t, names, 1)
	assert.Empty(t, names[0].Name)
	assert.Contains(t, names[0].Description, "not registered")
}

func TestOutputNamesForASwitchIsValueAndCase(t *testing.T) {
	t.Parallel()

	node := &Node{Kind: &Node_Switch{Switch: &Switch{Value: NewLiteral("x")}}}
	names, ok := OutputNames(node, nil)
	require.True(t, ok)

	var got []string
	for _, n := range names {
		got = append(got, n.Name)
	}
	assert.ElementsMatch(t, []string{SwitchValueOutput, SwitchCaseOutput}, got)
}
