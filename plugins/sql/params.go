package main

import (
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// paramsToArgs converts a statement's declared params into driver arguments,
// in order - the only path in this plugin from a workflow-computed value to
// something a query executes with, and the reason it is a conversion to a
// bound parameter rather than a conversion to text.
//
// This is the structural half of "parameterized only": there is no function
// anywhere in this plugin that takes a params entry and appends it to a SQL
// string, so an injection-shaped parameter (a value containing `'; DROP
// TABLE users; --`) can only ever arrive at the driver as a bound value the
// driver treats as data - see params_test.go's
// TestParamsToArgsNeverInterpolatesIntoSQLText, which proves this by
// running exactly such a value through a real database and asserting the
// table it names still exists.
func paramsToArgs(params []*flowstatev1.Value) ([]any, error) {
	if len(params) > maxParams {
		return nil, sdk.InvalidInput("params has %d entries, over the %d parameter ceiling this task enforces", len(params), maxParams)
	}

	args := make([]any, len(params))
	for i, p := range params {
		v, err := paramToArg(p)
		if err != nil {
			return nil, sdk.InvalidInput("params[%d]: %v", i, err)
		}
		args[i] = v
	}
	return args, nil
}

// paramToArg converts one parameter value to a driver argument. Only
// scalars and null convert: a list or a map has no single-placeholder SQL
// binding, and accepting one here would invite exactly the "helpfully"
// stringify-and-splice behavior this plugin's whole design refuses.
func paramToArg(v *flowstatev1.Value) (any, error) {
	if v == nil {
		return nil, nil
	}

	literal, ok := v.GetKind().(*flowstatev1.Value_Literal)
	if !ok {
		return nil, sdk.InvalidInput("must be a literal value, not %T", v.GetKind())
	}

	switch k := literal.Literal.GetKind().(type) {
	case nil, *expr.Value_NullValue:
		return nil, nil
	case *expr.Value_StringValue:
		if len(k.StringValue) > maxParamBytes {
			return nil, sdk.InvalidInput("string parameter is %d bytes, over the %d byte ceiling this task enforces", len(k.StringValue), maxParamBytes)
		}
		return k.StringValue, nil
	case *expr.Value_BytesValue:
		if len(k.BytesValue) > maxParamBytes {
			return nil, sdk.InvalidInput("bytes parameter is %d bytes, over the %d byte ceiling this task enforces", len(k.BytesValue), maxParamBytes)
		}
		return k.BytesValue, nil
	case *expr.Value_BoolValue:
		return k.BoolValue, nil
	case *expr.Value_Int64Value:
		return k.Int64Value, nil
	case *expr.Value_Uint64Value:
		return k.Uint64Value, nil
	case *expr.Value_DoubleValue:
		return k.DoubleValue, nil
	default:
		return nil, sdk.InvalidInput(
			"has type %T, which is not a value this task can bind as a single SQL parameter "+
				"(a list or a map has no single-placeholder binding)", k)
	}
}
