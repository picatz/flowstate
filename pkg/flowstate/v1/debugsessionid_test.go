package flowstatev1_test

import (
	"strings"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestValidDebugSessionIDIsTheSchemasGrammar pins the hand-written edge check to
// the grammar and bound the schema gives a session id, so the two cannot drift:
// the check guards the one door the schema does not reach, a raw Signal (#2648).
func TestValidDebugSessionIDIsTheSchemasGrammar(t *testing.T) {
	t.Parallel()

	field := (&v1.DebugAttachRequest{}).ProtoReflect().Descriptor().Fields().ByName("session_id")
	require.NotNil(t, field)
	rules, _ := proto.GetExtension(field.Options(), validate.E_Field).(*validate.FieldRules)
	require.NotNil(t, rules.GetString())
	require.Equal(t, "^([A-Za-z0-9._:-]+)?$", rules.GetString().GetPattern(), "the edge check's grammar is this one, minus the empty id")
	require.EqualValues(t, v1.MaxDebugEdgeSessionIDBytes, rules.GetString().GetMaxLen())

	for _, ok := range []string{"s", "3f2b9c1e-8a64-4d2e-9b7a-0c5d1e2f3a4b", "a.b_c:d-e", strings.Repeat("a", v1.MaxDebugEdgeSessionIDBytes)} {
		require.True(t, v1.ValidDebugSessionID(ok), "%q is a session id", ok)
	}
	for _, bad := range []string{
		"", "has space", "x held by y", "tab\there", "new\nline", "semi;colon", "slash/id", "unié",
		strings.Repeat("a", v1.MaxDebugEdgeSessionIDBytes+1),
	} {
		require.False(t, v1.ValidDebugSessionID(bad), "%q is not a session id", bad)
	}
}
