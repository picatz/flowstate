package flowstatev1_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// TestEnumNamesLocal is the local driver's half of the shared enum name cases.
// TestEnumNamesDurable in engine/enumnames_test.go runs the identical
// [conformance.EnumNameCases].
func TestEnumNamesLocal(t *testing.T) {
	require.NoError(t, v1.DefaultRegistry().Register(conformance.EnumNameTaskDef()))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(conformance.EnumNameTaskName) })

	cases := conformance.EnumNameCases()
	require.NotEmpty(t, cases)

	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			runAuthorityCase(t, test)
			require.NotEmpty(t, test.Workflow.GetSteps()[1].GetValue().GetExpr().GetSourceInfo().GetMacroCalls(),
				"the case was meant to keep the author's spelling beside the number")
		})
	}
}
