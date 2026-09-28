package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestMaxRunInputsIsTheSchemasBound: the count a JSON object of arguments is
// refused past is the schema's own, read from both places it is stated, so the
// two cannot drift apart and leave a surface refusing what a run takes.
func TestMaxRunInputsIsTheSchemasBound(t *testing.T) {
	t.Parallel()

	rules := func(message protoreflect.MessageDescriptor, name protoreflect.Name) *validate.FieldRules {
		t.Helper()
		field := message.Fields().ByName(name)
		require.NotNil(t, field, "%s.%s is gone", message.FullName(), name)
		rules, _ := proto.GetExtension(field.Options(), validate.E_Field).(*validate.FieldRules)
		require.NotNil(t, rules, "%s.%s carries no validation rules", message.FullName(), name)

		return rules
	}

	declared := rules((&v1.Workflow{}).ProtoReflect().Descriptor(), "declared_inputs")
	assert.Equal(t, uint64(maxRunInputs), declared.GetRepeated().GetMaxItems(), "Workflow.declared_inputs")
	started := rules((&v1.RunRequest{}).ProtoReflect().Descriptor(), "inputs")
	assert.Equal(t, uint64(maxRunInputs), started.GetMap().GetMaxPairs(), "RunRequest.inputs")
}

// TestAnObjectNamingMoreInputsThanARunTakesIsRefusedUnread: a JSON object of
// arguments naming more inputs than any run takes is refused by its count,
// before it is re-encoded and decoded, on every surface that binds one. At the
// bound it is read, and refused only for what it names.
func TestAnObjectNamingMoreInputsThanARunTakesIsRefusedUnread(t *testing.T) {
	t.Parallel()

	workflow := &v1.Workflow{Name: "one"}
	names := func(n int) map[string]json.RawMessage {
		submitted := make(map[string]json.RawMessage, n)
		for i := range n {
			submitted[fmt.Sprintf("input_%03d", i)] = json.RawMessage(`"x"`)
		}

		return submitted
	}

	_, err := jsonRunInputs(workflow, names(maxRunInputs+1), "the launch configuration's inputs", "where they go")
	require.Error(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("names %d inputs, and a run takes at most %d", maxRunInputs+1, maxRunInputs))
	assert.Contains(t, err.Error(), "where they go", "the refusal dropped the surface's advice")

	_, err = jsonRunInputs(workflow, names(maxRunInputs), "the launch configuration's inputs", "where they go")
	require.Error(t, err, "a workflow declaring no inputs bound an argument")
	assert.NotContains(t, err.Error(), "a run takes at most", "an object at the bound was refused by its count")
}
