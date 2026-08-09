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
	// TemporalDefaultBlobLimitBytes is the payload size Temporal refuses past,
	// at its default configuration.
	//
	// Named rather than left as arithmetic inside [MaxRunStateBytes] because it
	// is not only that constant's business any more. A deployment may configure
	// a payload codec, and an encrypting codec expands what it is handed, so the
	// question "does a maximal run state still fit once it is encoded" has to be
	// asked somewhere, by something that can name the limit it is asking about.
	// See `payloadcodec.Config.Validate`, which asks it at startup: the answer
	// is a property of the deployment's configuration, never of a running
	// workflow, so nothing here may be read inside workflow code.
	TemporalDefaultBlobLimitBytes = 2 << 20

	// RunStateReserveBytes is what [MaxRunStateBytes] leaves unspent under
	// [TemporalDefaultBlobLimitBytes], and it has two claimants.
	//
	// [PayloadEnvelopeReserveBytes] is the part that is not anybody's to spend:
	// the run state travels inside a payload, with metadata and encoding
	// headers, and proto.Size measures the message rather than the envelope. The
	// rest, [MaxCodecExpansionBytes], is what a configured codec may add.
	//
	// Splitting a reserve that was one number is what lets the codec check be
	// exact instead of guessing at somebody else's headroom, and the split is
	// arithmetic on this constant rather than a second literal so the parts
	// cannot come to disagree with the whole.
	RunStateReserveBytes = 64 << 10

	// PayloadEnvelopeReserveBytes is the part of [RunStateReserveBytes] kept for
	// the payload wrapped around a run state, before any codec sees it.
	//
	// Generous for what it holds. A payload's envelope is its metadata map (a
	// converter name, a message's full name) plus the field framing around the
	// data, which is tens of bytes, not thousands. It is four kibibytes because
	// spending headroom in the safe direction is the same judgement
	// [MaxRunStateBytes] already makes: what this reserve buys is a diagnosis
	// instead of a hang, and what it costs is nothing anyone can measure.
	PayloadEnvelopeReserveBytes = 4 << 10

	// ContinueAsNewFramingReserveBytes is headroom above what the codec
	// produces, for what the substrate wraps around it before the blob check
	// runs.
	//
	// The encoded payload is not what Temporal weighs. It is carried inside a
	// Payloads message, inside a Continue-As-New command, inside a history
	// event, each adding tags and length prefixes, and the size check applies
	// to the serialized whole. A ceiling set exactly at the blob limit would
	// therefore admit a codec whose output wedges anyway, over by precisely
	// the framing nobody counted. Kibibytes for what is tens of bytes of
	// framing, because this is the same judgement every reserve here makes:
	// failing a little early is a diagnosis, failing a little late is the
	// hang.
	ContinueAsNewFramingReserveBytes = 4 << 10

	// MaxCodecExpansionBytes is what a payload codec may add to a maximal run
	// state and still be allowed to start: the reserve, less the envelope
	// under the codec and the framing above it.
	//
	// A nonce, an authentication tag, and a key id are tens of bytes per
	// payload, so this is not a tight budget for anything that encrypts one
	// payload as one payload. It is tight for a codec that expands per byte:
	// armouring the ciphertext in base64 costs a third of two mebibytes, which
	// no reserve carved out of the blob limit could ever cover. That is the
	// answer such a codec should get, and it should get it at startup.
	MaxCodecExpansionBytes = RunStateReserveBytes - PayloadEnvelopeReserveBytes - ContinueAsNewFramingReserveBytes

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
	// Spelled as the blob limit minus its reserve, rather than as the same
	// subtraction written out, because the reserve now has to be divisible: a
	// deployment with a payload codec spends part of it on ciphertext overhead,
	// and the startup check that decides whether a codec fits has to be reading
	// the same number this bound was cut from. One constant cannot disagree with
	// itself. The value is unchanged and must stay unchanged: it is written into
	// the history of every run that has already suspended.
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
	MaxRunStateBytes = TemporalDefaultBlobLimitBytes - RunStateReserveBytes

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

// The parts still spell the whole, checked by the compiler.
//
// [MaxRunStateBytes] is a determinism input, so its value is not a preference:
// runs that suspended under 2 MiB minus 64 KiB replay against that number, and a
// worker that computes a different one from the same history can take a branch
// the history does not record. Now that the value is assembled from parts, a
// change to any part is a change to it, and the parts are edited for reasons
// that have nothing to do with replay. So the literal it has always been is
// written down once more, here, where its only job is to refuse to compile if it
// ever stops agreeing.
//
// Conversion of a negative constant to an unsigned type is a compile-time error,
// and both differences are taken so that drift in either direction is caught.
const _ = uint(MaxRunStateBytes-(2<<20-64<<10)) + uint((2<<20-64<<10)-MaxRunStateBytes)

// And the reserves' shares leave a codec something, which is the only reading
// of the split under which a deployment can configure a codec at all. Growing
// [PayloadEnvelopeReserveBytes] or [ContinueAsNewFramingReserveBytes] to
// swallow the whole reserve would refuse every codec that adds a single byte,
// silently and at startup, on every deployment.
const _ = uint(MaxCodecExpansionBytes - 1)

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
