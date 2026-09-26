package mcp

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestDebugSchemaAdvertisesTheSharedBacktraceCommand(t *testing.T) {
	t.Parallel()

	properties := debugInputSchema()["properties"].(map[string]any)
	commands := properties["commands"].(map[string]any)
	description := commands["description"].(string)
	assert.Contains(t, description, "`backtrace`",
		"the MCP front accepts the shared command but does not advertise it")
}

// TestTheSchemaSaysWhatTheServerWillRefuse pins the required derivation review
// asked for: a client validating {} against the advertised schema must be
// refused client-side where the tool boundary would refuse it, from the same
// protovalidate rules the server enforces.
func TestTheSchemaSaysWhatTheServerWillRefuse(t *testing.T) {
	t.Parallel()

	schema := SchemaForMessage((&v1.ValidateRequest{}).ProtoReflect().Descriptor())
	require.Contains(t, schema, "required",
		"ValidateRequest requires files (min_items 1) and the schema says nothing is required")
	assert.Contains(t, schema["required"], "files")

	run := SchemaForMessage((&v1.RunRequest{}).ProtoReflect().Descriptor())
	require.Contains(t, run, "required")
	assert.Contains(t, run["required"], "workflow")

	get := SchemaForMessage((&v1.GetRequest{}).ProtoReflect().Descriptor())
	require.Contains(t, get, "required")
	assert.Contains(t, get["required"], "workflowId",
		"required names must be the protojson spelling, since that is what the arguments arrive in")

	// The negative direction, so this is not simply answering everything.
	list := SchemaForMessage((&v1.ListRequest{}).ProtoReflect().Descriptor())
	assert.NotContains(t, list, "required",
		"ListRequest requires nothing and the schema claims otherwise, so a bare listing "+
			"would be refused client-side")
}

func TestTheSchemaDescribesUnpopulatedProtoJSONMessages(t *testing.T) {
	t.Parallel()

	get := SchemaForMessage((&v1.GetResponse{}).ProtoReflect().Descriptor())
	properties := get["properties"].(map[string]any)
	runOutputs := properties["runOutputs"].(map[string]any)
	assert.Equal(t, []any{"object", "null"}, runOutputs["type"],
		"an absent response message is emitted as null but the schema rejects it")

	run := SchemaForMessage((&v1.RunRequest{}).ProtoReflect().Descriptor())
	properties = run["properties"].(map[string]any)
	workflow := properties["workflow"].(map[string]any)
	assert.Equal(t, "object", workflow["type"],
		"a required request message became nullable even though the server rejects it when unset")
}

// TestAnAcyclicDescriptorCannotExplodeTheProjection is the regression test for
// what [maxSchemaNodes] was added for.
//
// The projection cut cycles and nothing else, and a cycle is not the only way a
// type graph revisits a message. Twelve messages, each holding four fields of
// the next one's type, is acyclic — every path through it strictly descends, so
// `visiting` never fires once — and it reaches the twelfth message along 4^12
// distinct paths, projecting a fresh subtree at each. Before the bound, the
// unbounded projection of the depth-10 version of this took 1.15s from a
// 982-byte descriptor, having quadrupled at every level below it
// ([maxSchemaNodes] carries the measurements); depth 12 is that times sixteen,
// in objects held live rather than in work done and thrown away.
//
// The assertion is a size, not a duration: a timing test is a flake on a busy
// machine, and the bound is on nodes because nodes are what the attacker
// multiplies.
func TestAnAcyclicDescriptorCannotExplodeTheProjection(t *testing.T) {
	t.Parallel()

	files, err := protodesc.NewFiles(dagDescriptorSet(12, 4))
	require.NoError(t, err)

	fd, err := files.FindFileByPath("fuzzschema/v1/dag.proto")
	require.NoError(t, err)

	schema := SchemaForMessage(fd.Messages().Get(0))
	assert.LessOrEqual(t, countSchemaNodes(schema), maxSchemaNodes,
		"an acyclic descriptor under 1.2 KiB projected past the node bound")

	// The other half of the bound's contract, and the half that would break a
	// client rather than a server. This projection is truncated — the budget is
	// gone long before the root's last field — so the root must not also be
	// claiming that the properties it managed to describe are all the message
	// has. `additionalProperties: false` over a partial properties map refuses
	// arguments the server would accept, which is a truncation inventing a
	// refusal rather than losing precision.
	assert.NotContains(t, schema, "additionalProperties",
		"a truncated schema still says additionalProperties: false, so a client "+
			"validating against it refuses arguments the tool boundary accepts")

	// And the truncation is real rather than assumed by the test above: an
	// untruncated projection of this descriptor would carry the key.
	assert.Contains(t, SchemaForMessage((&v1.GetRequest{}).ProtoReflect().Descriptor()),
		"additionalProperties",
		"a schema that fits in the budget must still refuse unknown arguments")
}

// TestTheAdvertisedSchemasStayWellUnderTheNodeBound points the bound's failure
// in the direction it has to fail in.
//
// Exhausting the budget truncates: fields past the exhaustion point are left
// out and the schema stops saying `additionalProperties: false`, which is right
// for a hostile descriptor and wrong for a real one — it would advertise a tool
// whose arguments or result are only partly described. So the thing to catch is
// a *real* request or response message growing toward the bound, and a quarter
// of it is the line: far enough above today's largest to not be noise, close
// enough that nothing reaches truncation without failing here first.
func TestTheAdvertisedSchemasStayWellUnderTheNodeBound(t *testing.T) {
	t.Parallel()

	for _, method := range WorkflowServiceMethods() {
		for _, schema := range []struct {
			direction  string
			descriptor protoreflect.MessageDescriptor
		}{
			{direction: "input", descriptor: method.Input},
			{direction: "output", descriptor: method.Output},
		} {
			nodes := countSchemaNodes(SchemaForMessage(schema.descriptor))
			assert.Less(t, nodes, maxSchemaNodes/4,
				"%s advertises %d %s schema nodes, over a quarter of the %d bound: raise the bound "+
					"deliberately rather than letting a real tool schema be truncated",
				method.Name, nodes, schema.direction, maxSchemaNodes)
		}
	}
}

// TestAFieldsSchemaCommentReachesTheToolSchema pins that a request field's
// description is the schema's own prose, derived rather than written here, and
// that how much of it travels depends on depth: a model fills in the request's
// own fields, so those carry their comment; one level down carries a first
// sentence; below that the shape alone.
func TestAFieldsSchemaCommentReachesTheToolSchema(t *testing.T) {
	t.Parallel()

	properties := func(schema map[string]any) map[string]any {
		t.Helper()
		out, ok := schema["properties"].(map[string]any)
		require.True(t, ok, "schema has no properties: %v", schema)
		return out
	}
	field := func(schema map[string]any, name string) map[string]any {
		t.Helper()
		out, ok := properties(schema)[name].(map[string]any)
		require.True(t, ok, "schema has no %q property", name)
		return out
	}

	signal := SchemaForMessage((&v1.SignalRequest{}).ProtoReflect().Descriptor())
	assert.Contains(t, field(signal, "payload")["description"], "${steps.<id>.payload.<key>}",
		"SignalRequest.payload's comment does not reach flowstate_signal's schema")

	list := SchemaForMessage((&v1.ListRequest{}).ProtoReflect().Descriptor())
	assert.Contains(t, field(list, "pageSize")["description"], "50",
		"ListRequest.page_size's default does not reach flowstate_list's schema")

	run := SchemaForMessage((&v1.RunRequest{}).ProtoReflect().Descriptor())
	workflow := field(run, "workflow")
	assert.Contains(t, workflow["description"], "Compile",
		"RunRequest.workflow's comment does not reach flowstate_run's schema")

	// One level down: a first sentence, and only that.
	name, ok := field(workflow, "name")["description"].(string)
	require.True(t, ok, "Workflow.name carries no description one level down")
	assert.NotContains(t, name, "\n\n", "a nested field carries more than its first sentence")

	// Two levels down: nothing, however well documented the field is.
	steps := field(workflow, "steps")
	items, ok := steps["items"].(map[string]any)
	require.True(t, ok, "Workflow.steps has no item schema")
	assert.NotContains(t, field(items, "id"), "description",
		"a field two messages down carries prose, which the depth bound exists to prevent")
}

// TestADurationFieldSaysHowADurationIsSpelled pins the one well-known type
// whose protojson spelling a model gets wrong: `1h` is refused by the
// decoder, `3600s` is accepted.
func TestADurationFieldSaysHowADurationIsSpelled(t *testing.T) {
	t.Parallel()

	schema := SchemaForMessage((&v1.ScheduleTrigger{}).ProtoReflect().Descriptor())
	every, ok := schema["properties"].(map[string]any)["every"].(map[string]any)
	require.True(t, ok, "ScheduleTrigger has no `every` property")

	pattern, ok := every["pattern"].(string)
	require.True(t, ok, "a Duration field advertises no pattern")
	re := regexp.MustCompile(pattern)
	for _, accepted := range []string{"3600s", "1.5s", "-2s", "0.000000001s"} {
		assert.True(t, re.MatchString(accepted), "the pattern refuses %q, which protojson accepts", accepted)
	}
	for _, refused := range []string{"1h", "30m", "3600", "1.0000000001s"} {
		assert.False(t, re.MatchString(refused), "the pattern accepts %q, which protojson refuses", refused)
	}

	description, _ := every["description"].(string)
	assert.Contains(t, description, "Every fires on a fixed interval",
		"the field's own comment is missing")
	assert.Contains(t, description, "3600s", "the spelling hint is missing")
}
