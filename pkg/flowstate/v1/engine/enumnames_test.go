package engine_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestEnumNamesDurable is the second caller of [conformance.EnumNameCases]: the
// identical cases the local driver runs in enumnames_local_test.go. A name is
// lowered to the number a task stores before the specification exists, so the
// durable interpreter compares the same integer the local one does.
func TestEnumNamesDurable(t *testing.T) {
	require.NoError(t, v1.DefaultRegistry().Register(conformance.EnumNameTaskDef()))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(conformance.EnumNameTaskName) })

	for _, test := range conformance.EnumNameCases() {
		t.Run(test.Name, func(t *testing.T) {
			runAuthorityCase(t, test)
		})
	}
}
