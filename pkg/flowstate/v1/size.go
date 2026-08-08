package flowstatev1

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// What a run may weigh, and why there are two numbers rather than one.
//
// Temporal refuses to write a payload past a blob limit — 2 MiB by default. That
// is not a limit Flowstate can raise, and it is not one a run discovers politely:
// a Continue-As-New whose state is too large fails the *workflow task*, and a
// failed workflow task is retried. Forever. The run reports RUNNING, occupies a
// worker's attention on every attempt, and never finishes. It was measured doing
// exactly that — a 1.2 MiB specification submitted successfully, ran its first
// step, and sat on attempt 5 of a workflow task forty-five seconds later, with a
// status any listing would show as healthy.
//
// A wedged run is worse than a failed one in every way that matters. A failure is
// visible, is reported, closes the run, and says what to do. So both of these
// bounds exist to convert that hang into an answer, and they answer at different
// moments because they know different things:
//
//   - [MaxSpecBytes] is checked when a specification is submitted, which is the
//     only moment an author is present to be told. It cannot predict what a run
//     will accumulate, so it reserves room rather than cutting things fine.
//   - [MaxRunStateBytes] is checked at Continue-As-New, where the state is no
//     longer a prediction. It is the backstop, and it fails the run.
const (
	// MaxSpecBytes bounds a submitted workflow specification.
	//
	// One mebibyte, which is deliberately the same number the Flowfile parser
	// bounds *source* at: a file `flow validate` will compile is a specification
	// the server will accept, and an author never meets one limit by satisfying
	// the other. It leaves the rest of the blob limit for what executing the
	// workflow adds — step outputs, carried signals, the resume position — since
	// the specification is only the part that does not grow.
	MaxSpecBytes = 1 << 20

	// MaxRunStateBytes bounds the state carried across a Continue-As-New.
	//
	// Under Temporal's default blob limit with room for the envelope around the
	// payload: proto.Size measures the message, and what the server weighs is that
	// message inside a payload with its metadata and encoding headers. Failing a
	// little early produces a diagnosis; failing a little late produces the hang
	// this exists to prevent, so the headroom is spent in the safe direction.
	//
	// A constant rather than configuration, and that is not a shortcut. This is
	// checked inside workflow code, so its value is a determinism input: a limit
	// read from an environment variable would be a different number on a worker
	// started later, and a run replaying against it could take a different branch
	// than the history says it took. Invariant 4 makes that a compiled-in number
	// by construction, which is the same reason the interpreter itself is pinned.
	//
	// A deployment that has raised its blob limit is therefore not served by
	// raising this to match. History that large is a problem of its own, and the
	// fix for a run that reaches it is almost always a workload that should carry
	// less.
	MaxRunStateBytes = 2<<20 - 64<<10

	// MaxSignalPayloadBytes bounds one signal's payload, at the server's door.
	//
	// The payload is the one part of a run's carried state a party *other than
	// the run's owner* chooses the size of. A signal that arrives before its
	// gate is carried across Continue-As-New (see [MaxPendingSignals]), and the
	// carry is weighed by [CheckRunStateSize] — whose outcome is failing the
	// run. Without a bound of its own, an authorized sender's payloads become a
	// way to push someone else's run over that limit: the sender gets a success
	// response at send time, and the run dies later with a state-size diagnosis
	// pointing at nothing the operator can see the cause of. Bounding the
	// payload where it is chosen puts the refusal on the party who can act on
	// it, at the moment they act.
	//
	// The arithmetic this buys, stated rather than implied: [MaxPendingSignals]
	// alone caps a hostile carry at 128 payloads of whatever Temporal's blob
	// limit admits — hundreds of mebibytes attempted against a two-mebibyte
	// budget. With this bound the worst-case product is 128 × 64 KiB = 8 MiB,
	// still more than a run can carry, so [CheckRunStateSize] remains the
	// backstop for a carry that is pathological in *count* — but a realistic
	// carry of a handful of maximal payloads now fits, and no single sender's
	// single send can be the surprise.
	//
	// 64 KiB is generous for what a payload is: a signal's payload becomes the
	// waiting step's outputs — an approval, an entity mutation, a callback's
	// result — not a document. Raising it later is compatible; lowering it
	// breaks senders, so it starts at the small end of plausible.
	//
	// Enforced at the server RPCs, deliberately not inside workflow code: the
	// door is where the sender is, and a constant only workflow code reads
	// would put the refusal back at Continue-As-New. It is still a constant
	// rather than configuration for the same reason its siblings are — one
	// number every deployment agrees on is one an author can design against.
	MaxSignalPayloadBytes = 64 << 10
)

// CheckSpecSize reports whether a specification is small enough to run.
//
// Separate from [Validate] because it answers a different question. Validation
// asks whether a workflow is well formed, which is about the schema; this asks
// whether it will fit, which is about the substrate underneath. A specification
// can be perfectly legal and still be one nothing can execute.
func CheckSpecSize(wf *Workflow) error {
	size := proto.Size(wf)
	if size <= MaxSpecBytes {
		return nil
	}

	return fmt.Errorf(
		"the workflow is %d bytes compiled, over the %d byte limit; "+
			"a run also carries its step outputs, so the whole of it has to fit in what Temporal will "+
			"store for one run. Move large values out of the specification — fetch them in a step, or "+
			"reference them — rather than writing them into it",
		size, MaxSpecBytes)
}

// CheckSignalPayloadSize reports whether a signal's payload is small enough to
// deliver.
//
// Called at the server's Signal and SignalWithStart handlers, before any round
// trip to Temporal: an oversized payload is refused synchronously, to the party
// who chose its size, with the number they need to act on it. See
// [MaxSignalPayloadBytes] for why the bound lives at the door rather than in
// the carry.
func CheckSignalPayloadSize(payload *Node_Outputs) error {
	size := proto.Size(payload)
	if size <= MaxSignalPayloadBytes {
		return nil
	}

	return fmt.Errorf(
		"the signal payload is %d bytes, over the %d byte limit; "+
			"a payload becomes the waiting step's outputs and is carried with the run, "+
			"so send a reference to something large rather than the thing itself",
		size, MaxSignalPayloadBytes)
}

// CheckRunStateSize reports whether a run's state can be carried forward.
//
// Called where a run suspends, which is the one place the answer is a fact rather
// than an estimate. A run that cannot be carried forward has to fail here: the
// alternative is not "carry on", it is Temporal refusing the Continue-As-New and
// retrying the workflow task until somebody notices.
func CheckRunStateSize(st *RunState) error {
	size := proto.Size(st)
	if size <= MaxRunStateBytes {
		return nil
	}

	return fmt.Errorf(
		"the run carries %d bytes of state, over the %d byte limit, so it cannot continue as new: "+
			"the workflow is %d bytes and its carried step outputs are the rest. "+
			"A step whose output is large should write it somewhere and pass a reference, "+
			"since every output a later step can still reach is carried across every suspension",
		size, MaxRunStateBytes, proto.Size(st.GetWorkflow()))
}
