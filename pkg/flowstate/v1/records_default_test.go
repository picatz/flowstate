package flowstatev1_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// defaultedWorkflow declares Ticket{id (required), status: enum default open, tags:
// list(string) default [], seen: timestamp default 2026-01-01T00:00:00Z, owner:
// Person default {name: nobody}} and Person{name (required), role: default guest}, and
// takes a Ticket.
func defaultedWorkflow() *v1.Workflow {
	return &v1.Workflow{
		Name:    "tickets",
		Profile: v1.CurrentProfile,
		DeclaredTypes: []*v1.TypeDeclaration{
			{Name: "Person", Fields: []*v1.InputDeclaration{
				recordStringField("name", true),
				{Name: "role", Type: v1.InputDeclaration_TYPE_STRING, Default: v1.NewLiteral("guest")},
			}},
			{Name: "Ticket", Fields: []*v1.InputDeclaration{
				recordStringField("id", true),
				{Name: "status", Type: v1.InputDeclaration_TYPE_ENUM, Values: []string{"open", "closed"}, Default: v1.NewLiteral("open")},
				{Name: "tags", Type: v1.InputDeclaration_TYPE_LIST, Default: v1.NewLiteralList(),
					ValueType: &v1.Type{Kind: &v1.Type_List{List: &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_SCALAR_STRING}}}}},
				{Name: "seen", Type: v1.InputDeclaration_TYPE_TIMESTAMP, Default: v1.NewLiteral("2026-01-01T00:00:00Z")},
				{Name: "owner", Type: v1.InputDeclaration_TYPE_STRUCT, ValueType: recordTypeOf("Person"),
					Default: v1.NewLiteralMap(map[string]any{"name": "nobody"})},
			}},
		},
		DeclaredInputs: []*v1.InputDeclaration{
			recordField("ticket", v1.InputDeclaration_TYPE_STRUCT, recordTypeOf("Ticket"), true),
		},
		Steps: []*v1.Node{{Id: "a", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}},
	}
}

func bindTicket(t *testing.T, wf *v1.Workflow, ticket map[string]any) (map[string]any, error) {
	t.Helper()

	bound, err := v1.BindRunInputs(wf, map[string]*v1.Value{"ticket": v1.NewLiteralMap(ticket)})
	if err != nil {
		return nil, err
	}

	got, err := v1.LiteralToGo(bound["ticket"].GetLiteral())
	require.NoError(t, err)

	return got.(map[string]any), nil
}

// A field a value leaves out takes its default where the value is bound, at any
// depth, as the value CEL reads and not as the text the file wrote.
func TestARecordFieldTakesItsDefaultWhenTheValueLeavesItOut(t *testing.T) {
	t.Parallel()

	wf := defaultedWorkflow()
	require.NoError(t, v1.CheckRecordDeclarations(wf))

	got, err := bindTicket(t, wf, map[string]any{"id": "t-1"})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"id":     "t-1",
		"status": "open",
		"tags":   []any{},
		"seen":   "2026-01-01T00:00:00Z",
		"owner":  map[string]any{"name": "nobody", "role": "guest"},
	}, got)
}

// A supplied value wins over a default, including the zero-shaped one, and a
// default inside a supplied nested record still fills what that record left out.
func TestASuppliedFieldBeatsItsDefaultAndANestedRecordIsFilled(t *testing.T) {
	t.Parallel()

	got, err := bindTicket(t, defaultedWorkflow(), map[string]any{
		"id":     "t-2",
		"status": "closed",
		"tags":   []any{},
		"owner":  map[string]any{"name": "ada"},
	})
	require.NoError(t, err)
	assert.Equal(t, "closed", got["status"])
	assert.Equal(t, map[string]any{"name": "ada", "role": "guest"}, got["owner"])
}

// Filling is idempotent: a value that has been through a bind is bound again, as a
// call boundary or a Temporal replay does, and comes out the same.
func TestFillingDefaultsIsIdempotent(t *testing.T) {
	t.Parallel()

	wf := defaultedWorkflow()
	table := v1.TypesOf(wf)
	declaration := wf.GetDeclaredInputs()[0]

	first := v1.NormalizeWireValue(table, declaration.DeclaredType(), v1.NewLiteralMap(map[string]any{"id": "t-3"}).GetLiteral())
	second := v1.NormalizeWireValue(table, declaration.DeclaredType(), first)
	assert.Same(t, first, second)
}

// A required field is never absent, so a default on one can never be used: the
// contradiction is refused, as it is on an input.
func TestARequiredFieldWithADefaultIsRefused(t *testing.T) {
	t.Parallel()

	wf := defaultedWorkflow()
	wf.DeclaredTypes[1].Fields[0].Default = v1.NewLiteral("x")

	err := v1.CheckRecordDeclarations(wf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required: true")
	assert.Contains(t, err.Error(), `field "id"`)
}

// A default is held to what the field promises when the record is declared, not
// discovered at the first run that leaves the field out.
func TestADefaultThatBreaksItsFieldIsRefusedAtDeclaration(t *testing.T) {
	t.Parallel()

	for name, mutate := range map[string]func(*v1.InputDeclaration){
		"not in the enum": func(f *v1.InputDeclaration) { f.Default = v1.NewLiteral("pending") },
		"wrong type":      func(f *v1.InputDeclaration) { f.Default = v1.NewLiteral(int64(1)) },
		"bad timestamp text": func(f *v1.InputDeclaration) {
			f.Type = v1.InputDeclaration_TYPE_TIMESTAMP
			f.Values = nil
			f.Default = v1.NewLiteral("soon")
		},
		"fails its must": func(f *v1.InputDeclaration) {
			f.Default = v1.NewLiteral("open")
			f.Must = new(`this == "closed"`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			wf := defaultedWorkflow()
			mutate(wf.DeclaredTypes[1].Fields[1])

			err := v1.CheckRecordDeclarations(wf)
			require.Error(t, err)
			assert.Contains(t, err.Error(), `type "Ticket" field "status" default`)
		})
	}
}

// An example is held to the same rules, and is never bound.
func TestAnExampleThatBreaksItsFieldIsRefusedAndNeverBound(t *testing.T) {
	t.Parallel()

	wf := defaultedWorkflow()
	wf.DeclaredTypes[1].Fields[1].Example = v1.NewLiteral("closed")
	require.NoError(t, v1.CheckRecordDeclarations(wf))

	got, err := bindTicket(t, wf, map[string]any{"id": "t-4"})
	require.NoError(t, err)
	assert.Equal(t, "open", got["status"], "an example is documentation, not a fallback")

	wf.DeclaredTypes[1].Fields[1].Example = v1.NewLiteral("pending")
	err = v1.CheckRecordDeclarations(wf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "example")
}

// A value still has to satisfy the type: a supplied field that breaks its type is
// refused and the default does not paper over it.
func TestADefaultDoesNotHideASuppliedValueThatIsWrong(t *testing.T) {
	t.Parallel()

	_, err := bindTicket(t, defaultedWorkflow(), map[string]any{"id": "t-5", "status": "pending"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status")
}

// A near-empty record is a few bytes on the wire and a field per default once
// filled, so the fill is bounded where it is spent: a map of such records is refused
// before it is filled, and a handful is not.
func TestAnInputThatWouldFillTooManyDefaultsIsRefusedBeforeItIsFilled(t *testing.T) {
	t.Parallel()

	wide := &v1.TypeDeclaration{Name: "Wide"}
	for i := range v1.MaxRecordFields {
		wide.Fields = append(wide.Fields, &v1.InputDeclaration{
			Name: fmt.Sprintf("f%02d", i), Type: v1.InputDeclaration_TYPE_INT, Default: v1.NewLiteral(int64(i)),
		})
	}
	wf := &v1.Workflow{
		Name:          "wide",
		Profile:       v1.CurrentProfile,
		DeclaredTypes: []*v1.TypeDeclaration{wide},
		DeclaredInputs: []*v1.InputDeclaration{{
			Name: "rows", Type: v1.InputDeclaration_TYPE_STRUCT, Required: true,
			ValueType: &v1.Type{Kind: &v1.Type_Map_{Map: &v1.Type_Map{
				Value: recordTypeOf("Wide"),
			}}},
		}},
		Steps: []*v1.Node{{Id: "a", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}},
	}
	require.NoError(t, v1.CheckRecordDeclarations(wf))

	rows := func(n int) map[string]*v1.Value {
		entries := make(map[string]any, n)
		for i := range n {
			entries[fmt.Sprintf("r%d", i)] = map[string]any{}
		}

		return map[string]*v1.Value{"rows": v1.NewLiteralMap(entries)}
	}

	_, err := v1.BindRunInputs(wf, rows(4))
	require.NoError(t, err)

	_, err = v1.BindRunInputs(wf, rows(2000))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fields that take a default")
}

// A default that is a record whose fields default to records multiplies down the
// type, so a type whose empty expansion is past the bound is refused where it is
// declared, before any value reaches it.
func TestATypeWhoseDefaultsExpandWithoutBoundIsRefusedAtDeclaration(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{Name: "fan", Profile: v1.CurrentProfile}
	for level := range 3 {
		declaration := &v1.TypeDeclaration{Name: fmt.Sprintf("L%d", level)}
		for i := range v1.MaxRecordFields {
			field := &v1.InputDeclaration{Name: fmt.Sprintf("f%02d", i)}
			if level == 0 {
				field.Type = v1.InputDeclaration_TYPE_INT
				field.Default = v1.NewLiteral(int64(i))
			} else {
				field.Type = v1.InputDeclaration_TYPE_STRUCT
				field.ValueType = recordTypeOf(fmt.Sprintf("L%d", level-1))
				field.Default = v1.NewLiteralMap(map[string]any{})
			}
			declaration.Fields = append(declaration.Fields, field)
		}
		wf.DeclaredTypes = append(wf.DeclaredTypes, declaration)
	}

	err := v1.CheckRecordDeclarations(wf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expands to more than")
}

// A narrow chain that fans out at every level is refused by counting, not by
// expanding: eight fields over twelve levels would write 8^12 entries if judging the
// default filled it first.
func TestADeepNarrowDefaultFanOutIsRefusedWithoutExpandingIt(t *testing.T) {
	t.Parallel()

	wf := &v1.Workflow{Name: "chain", Profile: v1.CurrentProfile}
	for level := range 12 {
		declaration := &v1.TypeDeclaration{Name: fmt.Sprintf("L%d", level)}
		for i := range 8 {
			field := &v1.InputDeclaration{Name: fmt.Sprintf("f%d", i)}
			if level == 0 {
				field.Type = v1.InputDeclaration_TYPE_INT
				field.Default = v1.NewLiteral(int64(i))
			} else {
				field.Type = v1.InputDeclaration_TYPE_STRUCT
				field.ValueType = recordTypeOf(fmt.Sprintf("L%d", level-1))
				field.Default = v1.NewLiteralMap(map[string]any{})
			}
			declaration.Fields = append(declaration.Fields, field)
		}
		wf.DeclaredTypes = append(wf.DeclaredTypes, declaration)
	}

	err := v1.CheckRecordDeclarations(wf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expands to more than")
}

// A default or an example is bounded by the same count the submitted value is, so an
// input whose own default would fill past the bound is refused whether or not the
// type declarations were judged first.
func TestAnInputDefaultThatWouldFillTooMuchIsRefusedBeforeItIsFilled(t *testing.T) {
	t.Parallel()

	wide := &v1.TypeDeclaration{Name: "Wide"}
	for i := range v1.MaxRecordFields {
		wide.Fields = append(wide.Fields, &v1.InputDeclaration{
			Name: fmt.Sprintf("f%02d", i), Type: v1.InputDeclaration_TYPE_INT, Default: v1.NewLiteral(int64(i)),
		})
	}
	rows := map[string]any{}
	for i := range 2000 {
		rows[fmt.Sprintf("r%d", i)] = map[string]any{}
	}
	declaration := &v1.InputDeclaration{
		Name: "rows", Type: v1.InputDeclaration_TYPE_STRUCT,
		ValueType: &v1.Type{Kind: &v1.Type_Map_{Map: &v1.Type_Map{Value: recordTypeOf("Wide")}}},
		Default:   v1.NewLiteralMap(rows),
	}
	table := v1.TypesOf(&v1.Workflow{DeclaredTypes: []*v1.TypeDeclaration{wide}})

	err := v1.CheckInputDefaultIn(table, v1.CurrentProfile, declaration)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fields that take a default")

	declaration.Example, declaration.Default = declaration.Default, nil
	err = v1.CheckInputExampleIn(table, v1.CurrentProfile, declaration)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fields that take a default")
}

// The work one normalization does is bounded where it is done: a literal that reaches
// the walk without anyone having counted it first (a literal output, a stub's answer)
// stops filling at the budget, rather than allocating without limit.
func TestNormalizingStopsFillingAtTheWorkBudget(t *testing.T) {
	t.Parallel()

	wide := &v1.TypeDeclaration{Name: "Wide"}
	for i := range v1.MaxRecordFields {
		wide.Fields = append(wide.Fields, &v1.InputDeclaration{
			Name: fmt.Sprintf("f%02d", i), Type: v1.InputDeclaration_TYPE_INT, Default: v1.NewLiteral(int64(i)),
		})
	}
	table := v1.TypesOf(&v1.Workflow{DeclaredTypes: []*v1.TypeDeclaration{wide}})

	var elements []any
	for range 4000 {
		elements = append(elements, map[string]any{})
	}
	list := v1.NewLiteralList(elements...).GetLiteral()
	listOf := &v1.Type{Kind: &v1.Type_List{List: recordTypeOf("Wide")}}

	got := v1.NormalizeWireValue(table, listOf, list)

	filled := 0
	for _, element := range got.GetListValue().GetValues() {
		filled += len(element.GetMapValue().GetEntries())
	}
	assert.LessOrEqual(t, filled, 1<<16, "fills past the budget are not written")
	assert.Positive(t, filled)
}

// The fill is charged for the size of the default it copies, not one unit per field:
// a record whose single default is a long list, left out of thousands of near-empty
// records, is refused before the lists are rebuilt.
func TestAFillIsChargedForTheSizeOfTheDefaultItCopies(t *testing.T) {
	t.Parallel()

	moments := make([]any, 5000)
	for i := range moments {
		moments[i] = "2026-01-01T00:00:00Z"
	}
	big := &v1.TypeDeclaration{Name: "Big", Fields: []*v1.InputDeclaration{{
		Name: "d", Type: v1.InputDeclaration_TYPE_LIST, Default: v1.NewLiteralList(moments...),
		ValueType: &v1.Type{Kind: &v1.Type_List{List: &v1.Type{Kind: &v1.Type_Scalar_{Scalar: v1.Type_SCALAR_TIMESTAMP}}}},
	}}}
	wf := &v1.Workflow{
		Name: "big", Profile: v1.CurrentProfile, DeclaredTypes: []*v1.TypeDeclaration{big},
		DeclaredInputs: []*v1.InputDeclaration{{
			Name: "rows", Type: v1.InputDeclaration_TYPE_LIST, Required: true,
			ValueType: &v1.Type{Kind: &v1.Type_List{List: recordTypeOf("Big")}},
		}},
		Steps: []*v1.Node{{Id: "a", Kind: &v1.Node_Value{Value: v1.NewExpr("1")}}},
	}
	require.Error(t, v1.CheckRecordDeclarations(wf), "a 5000-node default exceeds what one empty record may expand to")

	big.Fields[0].Default = v1.NewLiteralList(moments[:100]...)
	require.NoError(t, v1.CheckRecordDeclarations(wf))

	rows := make([]any, 2000)
	for i := range rows {
		rows[i] = map[string]any{}
	}
	_, err := v1.BindRunInputs(wf, map[string]*v1.Value{"rows": v1.NewLiteralList(rows...)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fields that take a default")
}
