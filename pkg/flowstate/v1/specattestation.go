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
//
// "Once" is per response message rather than per repository, because the field
// belongs to three of them: `Run`, `SignalWithStart` and `CreateSchedule` each
// bring durable work into existence from a caller's specification and each
// substitutes the trusted copy for it (#844). Protobuf gives no way to share one
// method across three generated types short of an interface nothing would be
// obliged to implement, so the three bodies below are identical on purpose — the
// alternative is two of the three RPCs having no affirmative accessor and a
// caller reaching for `GetSpecificationAsSubmitted()` there, which is precisely
// the getter this file exists to keep out of client code.

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

// RanSubmittedSpecification reports whether the server affirmatively attested
// that the entity this call reached executes the specification the caller
// submitted, unchanged.
//
// The same method on the same field for the same reason as
// [RunResponse.RanSubmittedSpecification] — three RPCs bring durable work into
// existence from a caller's specification, all three substitute the trusted copy
// and pin plugins onto it, and a client that has to remember which of them
// answers this question is a client that will eventually read the wrong one.
//
// False additionally, and always, when this call did not create the entity: the
// specification a pre-existing run is executing was compared against nothing
// here. See [SignalWithStartResponse.specification_as_submitted].
func (x *SignalWithStartResponse) RanSubmittedSpecification() bool {
	if x == nil || x.SpecificationAsSubmitted == nil {
		return false
	}

	return x.GetSpecificationAsSubmitted()
}

// RanSubmittedSpecification reports whether the server affirmatively attested
// that the schedule it just created will fire the specification the caller
// submitted, unchanged.
//
// The same method on the same field for the same reason as
// [RunResponse.RanSubmittedSpecification], and a claim about the creation rather
// than a standing property of the schedule — see
// [CreateScheduleResponse.specification_as_submitted].
func (x *CreateScheduleResponse) RanSubmittedSpecification() bool {
	if x == nil || x.SpecificationAsSubmitted == nil {
		return false
	}

	return x.GetSpecificationAsSubmitted()
}
