package flowstatev1

import (
	"fmt"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// checkLiteralShape refuses a literal that is not a value of the structural type
// t, and names the first element that is not: `[1]` where `list(string)` was
// declared is "an integer at [0]", which a caller finishes as "declared
// list(string) but was given an integer at [0]".
//
// It is the container half of [checkDeclaredLiteralType], which judges only the
// outermost kind through the legacy enum. A nil t, `dyn`, an enum (whose members
// are the constraints' business, see [StringShaped]) accept anything, as does a
// message when table is nil; with one, a message is a record and is judged by
// [TypeTable.checkRecord], because the type promises nothing a literal can
// break.
//
// Bounded by [MaxStructureDepth] in the type, so a hand-built type that points
// back at itself ends the walk instead of the stack. Work is bounded by the
// literal, which the caller has already sized.
func checkLiteralShape(table TypeTable, t *Type, literal *expr.Value) error {
	return checkLiteralShapeAt(table, t, literal, "", 0)
}

func checkLiteralShapeAt(table TypeTable, t *Type, literal *expr.Value, path string, depth int) error {
	if depth > MaxStructureDepth {
		return nil
	}

	mismatch := func() error {
		return fmt.Errorf("%s%s", literalKindName(literal), atPath(path))
	}

	switch kind := t.GetKind().(type) {
	case *Type_Scalar_:
		if !literalIsScalar(kind.Scalar, literal) {
			return mismatch()
		}
	case *Type_List:
		list, ok := literal.GetKind().(*expr.Value_ListValue)
		if !ok {
			return mismatch()
		}
		for i, element := range list.ListValue.GetValues() {
			if err := checkLiteralShapeAt(table, kind.List, element, fmt.Sprintf("%s[%d]", path, i), depth+1); err != nil {
				return err
			}
		}
	case *Type_Map_:
		m, ok := literal.GetKind().(*expr.Value_MapValue)
		if !ok {
			return mismatch()
		}
		for _, entry := range m.MapValue.GetEntries() {
			key, isString := entry.GetKey().GetKind().(*expr.Value_StringValue)
			if !isString {
				return fmt.Errorf("a map with %s key%s", literalKindName(entry.GetKey()), atPath(path))
			}
			if err := checkLiteralShapeAt(table, kind.Map.GetValue(), entry.GetValue(), path+"."+key.StringValue, depth+1); err != nil {
				return err
			}
		}
	case *Type_Message:
		return table.checkRecord(kind.Message, literal, path, depth)
	}

	return nil
}

func atPath(path string) string {
	if path == "" {
		return ""
	}

	return " at " + path
}

// literalIsScalar reports whether a literal is a value of scalar s. Unsigned
// literals are ints for the reason [inputTypeOf] gives. Timestamps and durations
// have no literal spelling, so no literal is one.
func literalIsScalar(s Type_Scalar, literal *expr.Value) bool {
	switch literal.GetKind().(type) {
	case *expr.Value_StringValue:
		return s == Type_SCALAR_STRING
	case *expr.Value_Int64Value, *expr.Value_Uint64Value:
		return s == Type_SCALAR_INT
	case *expr.Value_DoubleValue:
		return s == Type_SCALAR_DOUBLE
	case *expr.Value_BoolValue:
		return s == Type_SCALAR_BOOL
	case *expr.Value_BytesValue:
		return s == Type_SCALAR_BYTES
	case *expr.Value_NullValue:
		return s == Type_SCALAR_NULL_TYPE
	default:
		return false
	}
}
