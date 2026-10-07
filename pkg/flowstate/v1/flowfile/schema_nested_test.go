package flowfile

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestLiteralMismatchForANestedMessageField: a field typed as one of the task's
// own messages is written as a mapping, and anything else is reported before the
// run rather than by the plugin's decode.
func TestLiteralMismatchForANestedMessageField(t *testing.T) {
	t.Parallel()

	field := (&v1.Workflow{}).ProtoReflect().Descriptor().Fields().ByName("triggers")
	require.NotNil(t, field)

	require.Empty(t, literalMismatch(field, v1.NewValue(map[string]any{"webhooks": []any{}}).GetLiteral()))
	require.Contains(t, literalMismatch(field, v1.NewValue("nope").GetLiteral()), "expected a mapping for flowstate.v1.Triggers")
}
