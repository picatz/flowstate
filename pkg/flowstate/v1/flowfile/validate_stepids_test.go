package flowfile

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func logNode(id string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
		Name:   "log",
		Inputs: map[string]*v1.Value{"message": v1.NewLiteral("hi")},
	}}}
}

// TestValidateHoldsNestedStepIDsToTheSubmitBoundarysRules proves the compiler-side
// walk and the submit boundary ask one question (#1430): a hand-built workflow the
// server's [v1.CheckStepIDs] refuses is reported by Validate too, at any nesting,
// where before only the top level was checked for a digit-first or dashed id.
func TestValidateHoldsNestedStepIDsToTheSubmitBoundarysRules(t *testing.T) {
	t.Parallel()

	each := func(body ...*v1.Node) *v1.Node {
		return &v1.Node{Id: "each", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
			Items: v1.NewExpr("[]"), Body: body,
		}}}
	}

	for name, tc := range map[string]struct {
		steps []*v1.Node
		want  string
	}{
		"a nested root name":    {steps: []*v1.Node{each(logNode("vars"))}, want: "is the root"},
		"a nested dash":         {steps: []*v1.Node{each(logNode("a-b"))}, want: "not a valid identifier"},
		"a nested duplicate":    {steps: []*v1.Node{logNode("x"), each(logNode("x"))}, want: "duplicate id"},
		"a top-level duplicate": {steps: []*v1.Node{logNode("x"), logNode("x")}, want: "duplicate id"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			wf := &v1.Workflow{Name: "w", Steps: tc.steps}
			require.Error(t, v1.CheckStepIDs(wf), "the submit boundary must refuse it")

			var found bool
			for _, d := range Validate(wf) {
				found = found || strings.Contains(d.Message, tc.want)
			}
			assert.True(t, found, "Validate did not report %q", tc.want)
		})
	}
}
