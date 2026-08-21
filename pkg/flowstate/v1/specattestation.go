package flowstatev1

// The client half of [RunResponse.specification_as_submitted], written here
// rather than at the one call site that needs it today, because the mistake it
// prevents is one every future call site can make independently.
//
// `GetSpecificationAsSubmitted()` — the accessor protoc generates — answers
// false for a server that said false *and* for a server that said nothing at
// all, because a proto3 optional's absence and its zero value read the same way
// through a getter. Those are different claims: the first is "your copy is not
// what ran", the second is "this server has no opinion, because it predates the
// field". Both must fail closed, and a reader who trusted the getter's `true`
// would get that right by accident while a reader who inverted it would not.
//
// So the affirmative question gets its own name and the presence check lives
// inside it, once. There is deliberately no `SubstitutedSpecification()` beside
// it: the negation of "attested as submitted" is the union of "attested as
// substituted" and "not attested at all", and offering a second method would
// invite a caller to distinguish two answers that must be treated alike.

// RanSubmittedSpecification reports whether the server affirmatively attested
// that this run executes the specification the caller submitted, unchanged.
//
// False for a server that said the specification was substituted, and false for
// one that did not answer at all — see [RunResponse.specification_as_submitted]
// for why silence is not consent. A client deciding whether its own copy of a
// specification may be trusted to describe the run — which values it declared
// `sensitive: true`, above all — asks this and nothing else.
func (x *RunResponse) RanSubmittedSpecification() bool {
	// A nil response is silence too, and reads the same as an unset field: no
	// attestation, so no trust.
	if x == nil || x.SpecificationAsSubmitted == nil {
		return false
	}

	return x.GetSpecificationAsSubmitted()
}
