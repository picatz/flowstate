package mcp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protodesc"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

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
// whose arguments are only partly described. So the thing to catch is a *real*
// request message growing toward the bound, and a quarter of it is the line:
// far enough above today's largest (SignalWithStart, 5,068 nodes) to not be
// noise, close enough that nothing reaches truncation without failing here
// first.
func TestTheAdvertisedSchemasStayWellUnderTheNodeBound(t *testing.T) {
	t.Parallel()

	for _, method := range WorkflowServiceMethods() {
		nodes := countSchemaNodes(SchemaForMessage(method.Input))
		assert.Less(t, nodes, maxSchemaNodes/4,
			"%s advertises %d schema nodes, over a quarter of the %d bound: raise the bound "+
				"deliberately rather than letting a real tool schema be truncated",
			method.Name, nodes, maxSchemaNodes)
	}
}
