package flowstatev1

// TypeAssignable reports whether every value of type from is also a value of
// type to, which is the one question `flow breaking` asks of a declared type:
// an input may only widen (every value an old caller sent must still be
// accepted, so the old type must be assignable to the new), and an output may
// only narrow (every value a caller was promised must still be delivered, so
// the new type must be assignable to the old).
//
// `dyn`, or no type at all, accepts every value and is promised by none: it is
// assignable to nothing but itself, and everything is assignable to it. An enum
// is a string with a closed set, so it is assignable to `string` and to an enum,
// but a string is not assignable to an enum, whose members it may not be. A list
// and a map are covariant in what they hold, which is sound here because a
// value is read, never written back through the type.
//
// Bounded by [MaxStructureDepth] like the other projections: past it the answer
// is the conservative one, false, so a hand-built type that points back at itself
// reads as a change and never as a proof of compatibility.
func TypeAssignable(from, to *Type) bool {
	return assignableAt(from, to, 0)
}

func assignableAt(from, to *Type, depth int) bool {
	if depth > MaxStructureDepth {
		return false
	}

	if IsDyn(to) {
		return true
	}
	if IsDyn(from) {
		return false
	}

	switch f := from.GetKind().(type) {
	case *Type_Scalar_:
		t, ok := to.GetKind().(*Type_Scalar_)
		return ok && f.Scalar == t.Scalar
	case *Type_Enum:
		if _, ok := to.GetKind().(*Type_Enum); ok {
			return true
		}
		t, ok := to.GetKind().(*Type_Scalar_)
		return ok && t.Scalar == Type_SCALAR_STRING
	case *Type_List:
		t, ok := to.GetKind().(*Type_List)
		return ok && assignableAt(f.List, t.List, depth+1)
	case *Type_Map_:
		t, ok := to.GetKind().(*Type_Map_)
		return ok && assignableAt(f.Map.GetValue(), t.Map.GetValue(), depth+1)
	case *Type_Message:
		t, ok := to.GetKind().(*Type_Message)
		return ok && f.Message == t.Message
	}

	return false
}
