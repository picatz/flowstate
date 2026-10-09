package flowdebug

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

func TestRecordAtReadsOnlyPlainPathsOfTheExecutingWorkflow(t *testing.T) {
	t.Parallel()

	record := func(name string) *v1.Type { return &v1.Type{Kind: &v1.Type_Message{Message: name}} }
	wf := &v1.Workflow{
		Name: "main",
		DeclaredTypes: []*v1.TypeDeclaration{
			{Name: "Line"},
			{Name: "Order", Fields: []*v1.InputDeclaration{{Name: "lines", ValueType: &v1.Type{Kind: &v1.Type_List{List: record("Line")}}}}},
		},
		DeclaredInputs: []*v1.InputDeclaration{{Name: "order", ValueType: record("Order")}},
	}
	shapes := shapesOf(wf)

	assert.Equal(t, "Order", shapes.recordAt("inputs.order", ""))
	assert.Equal(t, "Order", shapes.recordAt("inputs.order", "main"))
	assert.Equal(t, "Line", shapes.recordAt(`inputs["order"].lines[0]`, ""))

	assert.Empty(t, shapes.recordAt("inputs.order", "callee"),
		"inside a callee, `inputs` is the callee's, so this declaration says nothing of it")
	assert.Empty(t, shapes.recordAt("inputs.order.lines[-1]", ""))
	assert.Empty(t, shapes.recordAt("inputs.order.missing", ""))
	assert.Empty(t, shapes.recordAt("inputs.order.lines[0].x", ""))
	assert.Empty(t, shapes.recordAt("vars.order", ""))
	assert.Empty(t, shapes.recordAt("inputs.order + 1", ""))
	assert.Empty(t, shapes.recordAt("inputs.?order", ""))
	assert.Empty(t, (*declaredShapes)(nil).recordAt("inputs.order", ""))
}

func TestRecordAtReadsAQuotedKeyWholeWhateverItHolds(t *testing.T) {
	t.Parallel()

	record := func(name string) *v1.Type { return &v1.Type{Kind: &v1.Type_Message{Message: name}} }
	shapes := shapesOf(&v1.Workflow{
		Name:          "main",
		DeclaredTypes: []*v1.TypeDeclaration{{Name: "Line"}},
		DeclaredInputs: []*v1.InputDeclaration{{
			Name: "rows", ValueType: &v1.Type{Kind: &v1.Type_Map_{Map: &v1.Type_Map{Value: record("Line")}}},
		}},
	})

	for _, key := range []string{"a]b", `say "hi"`, `back\slash`, "plain"} {
		assert.Equal(t, "Line", shapes.recordAt("inputs.rows["+strconv.Quote(key)+"]", ""), key)
	}
	assert.Empty(t, shapes.recordAt(`inputs.rows["a"]x`, ""))
	assert.Empty(t, shapes.recordAt(`inputs.rows["a"`, ""))
}
