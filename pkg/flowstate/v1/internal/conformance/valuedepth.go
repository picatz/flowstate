package conformance

import (
	"fmt"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// One depth bound for every value a specification carries (#1765).
//
// A `vars:` entry or a step `value:` written as a mapping compiles to a CEL
// map literal rather than a [v1.Value_Structure], and the submit boundary's
// depth check counted structures alone — so a hand-built specification with a
// literal nested 63 levels deep was admitted where an input's `default:` at 33
// was refused, and every walk that spends depth on it (an expression, the
// secret authority, compaction) met a value nothing had bounded. The bound is
// [v1.MaxStructureDepth] for a literal and a structure alike, checked by
// [v1.CheckStructureDepth] from [v1.BindRunInputs], which both drivers reach.

// nestedMapLiteral is a map literal nested depth levels deep — `{"k": {"k":
// … "leaf"}}` — the shape the issue measured in every position.
func nestedMapLiteral(depth int) *expr.Value {
	value := &expr.Value{Kind: &expr.Value_StringValue{StringValue: "leaf"}}
	for range depth {
		value = &expr.Value{Kind: &expr.Value_MapValue{MapValue: &expr.MapValue{
			Entries: []*expr.MapValue_Entry{{
				Key:   &expr.Value{Kind: &expr.Value_StringValue{StringValue: "k"}},
				Value: value,
			}},
		}}}
	}
	return value
}

// ValueDepthRefusalCases returns specifications both drivers must refuse at
// submit because a literal they carry nests past the depth bound, in the
// words a submitted input past the same bound is refused in.
func ValueDepthRefusalCases() []Refusal {
	past := v1.MaxStructureDepth + 1

	// The whole sentence, with the depth reached, exactly as an input past the
	// bound is refused: the case pins that the two paths share one function,
	// not two that happen to agree.
	sentence := fmt.Sprintf("nests %d levels deep, over the %d levels this server can walk cheaply while "+
		"evaluating an expression over it (`if:`, `for_each`, `must:`, `unique:`); a value nested this "+
		"deeply is not a cost this server bounds any other way", past, v1.MaxStructureDepth)

	return []Refusal{
		{
			Name: "a vars literal nested past the depth bound is refused at submit",
			Workflow: &v1.Workflow{
				Name:    "vars-literal-too-deep",
				Profile: v1.CurrentProfile,
				Vars: map[string]*v1.Value{
					"d": {Kind: &v1.Value_Literal{Literal: nestedMapLiteral(past)}},
				},
				Steps: []*v1.Node{says("a", "hello")},
			},
			Contains: `the workflow's vars.d ` + sentence,
		},
		{
			Name: "a step value literal nested past the depth bound is refused at submit",
			Workflow: &v1.Workflow{
				Name:    "value-literal-too-deep",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{{
					Id:   "deep",
					Kind: &v1.Node_Value{Value: &v1.Value{Kind: &v1.Value_Literal{Literal: nestedMapLiteral(past)}}},
				}},
			},
			Contains: `step "deep"'s value ` + sentence,
		},
	}
}
