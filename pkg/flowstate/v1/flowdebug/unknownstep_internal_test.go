package flowdebug

import (
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestMaxStepIDLengthMatchesTheSchema keeps the suggestion bound tied to what a
// step may actually be called. [maxStepSuggestionInput] is the schema's own
// `max_len` plus [nearest.MaxDistance], and the point of deriving it is that a
// schema which widens the id widens the bound with it — which only holds while
// [maxStepIDLength] is the schema's number. Read off the descriptor rather than
// restated here, so a change to `Node.id` cannot leave the prompt silently
// skipping a suggestion `flow debug replay` still offers.
func TestMaxStepIDLengthMatchesTheSchema(t *testing.T) {
	t.Parallel()

	field := (&v1.Node{}).ProtoReflect().Descriptor().Fields().ByName("id")
	require.NotNil(t, field, "Node has no id field")

	rules, ok := proto.GetExtension(field.Options(), validate.E_Field).(*validate.FieldRules)
	require.True(t, ok, "Node.id carries no protovalidate field constraints")
	require.NotNil(t, rules.GetString_(), "Node.id declares no string constraints")
	require.NotNil(t, rules.GetString_().MaxLen, "Node.id declares no max_len")

	assert.Equal(t, int(rules.GetString_().GetMaxLen()), maxStepIDLength,
		"the did-you-mean bound is derived from Node.id's max_len and has drifted from it")
}
