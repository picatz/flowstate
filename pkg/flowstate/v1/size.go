package flowstatev1

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
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

	// MaxTaskOutputBytes bounds one step's outputs — the payload a task's
	// result becomes, which on the durable driver is an activity result
	// Temporal weighs against the same blob limit as everything else here.
	//
	// This closes the one payload seam the other bounds miss (#787). A task's
	// result is produced by a party the caller does not control — a remote
	// endpoint, a plugin — and the admission bounds upstream of it admit more
	// than Temporal will store: a plugin response may be
	// [plugin.DefaultMaxResponseBytes] (4 MiB) on the wire, and the http
	// task's default outputs carry a parsed JSON body twice (Body and Json).
	// Without this bound an oversized result is refused by the server at
	// activity completion, the activity retries against the same refusal, and
	// the step dies ten minutes later as a ScheduleToClose *timeout* — a
	// misdiagnosis, because the step finished repeatedly and its answer was
	// too big to write down. The local driver, with no server to refuse,
	// admitted it silently, which is the two drivers disagreeing in the worst
	// direction: the rehearsal passing what production wedges on.
	//
	// Cut from [TemporalDefaultBlobLimitBytes] by the same reserve as
	// [MaxRunStateBytes] rather than pinned to that constant, because the two
	// answer different questions that happen to share their arithmetic today:
	// an activity result travels in the same payload envelope, under the same
	// codec, with the same framing as a carried run state, so the same
	// claimants spend the same reserve. A result that fits here can still push
	// the *carry* over [MaxRunStateBytes] once it sits beside every other
	// step's outputs — [CheckRunStateSize] remains the backstop for the sum.
	MaxTaskOutputBytes = TemporalDefaultBlobLimitBytes - RunStateReserveBytes

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
	// The arithmetic this buys, stated rather than implied: an acknowledged
	// signal is carried unconditionally, however many accumulate (see
	// [MaxPendingSignals]), so nothing here caps the *count* — only
	// [CheckRunStateSize] does, by failing the run once the carry no longer
	// fits. What this bound caps is the other factor in that product: without
	// it, a sender's payload could be whatever Temporal's own blob limit
	// admits, and one oversized delivery — not a flood, a single send — could
	// be most of a two-mebibyte budget by itself. At 64 KiB a carry has to
	// genuinely grow in count before it is pathological, and a realistic
	// backlog of ordinary payloads fits with room to spare.
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

// encodedPayloadSize reports the byte length flowstate's payload converter
// actually produces for m — not proto.Size's binary-protobuf estimate.
//
// The SDK's default DataConverter (payloadcodec.Config.DataConverter, when no
// codec is configured) is a composite whose own comment says the order is
// deliberate: ProtoJSONPayloadConverter is checked before the binary one and
// wins the match for every proto.Message flowstate hands it, so every RunState
// and every completed run's Workflow_StepOutputs is serialized as ProtoJSON,
// never as binary protobuf. proto.Size measures a payload nothing here ever
// writes.
//
// The gap is not academic. Field names are spelled out per occurrence instead
// of a one-or-two-byte tag, map entries carry a key and a value name each, and
// every number and bytes value becomes text — bytes as base64, one third
// larger before the framing around it. A transcript with many small values or
// many map entries, the exact shape a run with many steps produces, can
// measure comfortably under the binary bound and exceed it as JSON. Measured
// against real transcripts, ProtoJSON output ran 1.03x-1.32x of proto.Size's
// estimate — against a bound that reserved 3.1% over the blob limit, so at the
// low end of that range the reserve was already roughly half spent by the
// measurement error alone (#716).
//
// This calls protojson.Marshal directly with zero-value MarshalOptions rather
// than going through go.temporal.io/sdk/converter: that is byte-for-byte what
// ProtoJSONPayloadConverter.ToPayload does for a standard proto.Message (see
// its source — `c.protoMarshalOptions.Marshal(valueProto)` with
// protojson.MarshalOptions{}), so the measurement matches the encoding
// exactly, without this determinism-sensitive package taking on a dependency
// on the Temporal SDK or on whatever payload codec a deployment has
// configured. A codec's own expansion is a separate, already-reserved budget —
// see [MaxCodecExpansionBytes] — because this function measures the payload a
// codec would be handed, not what the codec does to it.
//
// A marshal failure is treated as over the bound rather than propagated or
// ignored: fail closed, per the invariant every other bound in this package
// follows. proto.Size cannot fail, which is why every caller below still
// checks it for context even when protojson does the enforcing.
func encodedPayloadSize(m proto.Message) int {
	// Refuse to materialize an encoding the answer cannot need. Marshaling
	// allocates the whole output before any caller compares it to a bound, and
	// the party who shaped the message controls how big that is: an `outputs:`
	// map of thousands of fields referencing one 1 MiB response body holds its
	// copies as shared Go strings — cheap in memory — while the encoding spells
	// every copy out, so measuring an attacker-shaped result by marshaling it
	// is itself the memory explosion the bound exists to prevent. proto.Size
	// walks the message without allocating its encoding, and ProtoJSON output
	// is never smaller than the binary encoding for these message shapes —
	// field names versus one-or-two-byte tags, base64 versus raw bytes; #716
	// measured 1.03x-1.32x — so a message already past the blob limit in
	// binary is past every bound this package cuts under it, and can be
	// refused on the cheap walk alone. Anything that passes the walk encodes
	// to at most ~1.32x the blob limit, which bounds the marshal below.
	//
	// The binary size is returned rather than a sentinel so the diagnosis
	// still reports a real measurement; it understates what ProtoJSON would
	// produce, which only makes the sentence conservative, never wrong.
	if binary := proto.Size(m); binary > TemporalDefaultBlobLimitBytes {
		return binary
	}

	b, err := protojson.Marshal(m)
	if err != nil {
		// Past the blob limit itself, which every bound in this package is
		// cut under — so the fail-closed reading holds for every caller
		// ([MaxRunStateBytes] and [MaxTaskOutputBytes] alike), not only the
		// one whose constant happened to be named here first.
		return TemporalDefaultBlobLimitBytes + 1
	}
	return len(b)
}

// CheckRunStateSize reports whether a run's state can be carried forward.
//
// Called where a run suspends, which is the one place the answer is a fact rather
// than an estimate. A run that cannot be carried forward has to fail here: the
// alternative is not "carry on", it is Temporal refusing the Continue-As-New and
// retrying the workflow task until somebody notices.
//
// Measured by [encodedPayloadSize], not proto.Size — see its doc comment. This
// is also the Continue-As-New path #716 asked whether the fix covered:
// [CheckRunStateSize] is the only check on that path, so fixing it here is the
// whole of that answer.
func CheckRunStateSize(st *RunState) error {
	size := encodedPayloadSize(st)
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

// CheckRunResultSize reports whether a completed run's transcript is small
// enough for Temporal to record as the workflow result.
//
// Completion needs its own check: a short run never reaches the
// Continue-As-New check above, and repeated outputs can make its transcript
// larger than the inputs from which they were computed. Letting Temporal find
// that out would fail and retry the workflow task instead of closing the run.
//
// Measured by [encodedPayloadSize], not proto.Size — see its doc comment. A
// completed run's result is a Workflow_StepOutputs handed to the same
// DataConverter as a suspended run's RunState, so it is serialized the same
// way and has to be measured the same way.
func CheckRunResultSize(outputs *Workflow_StepOutputs) error {
	size := encodedPayloadSize(outputs)
	if size <= MaxRunStateBytes {
		return nil
	}

	return fmt.Errorf(
		"the completed run produced %d bytes of outputs, over the %d byte limit; "+
			"write large results somewhere and return references to them instead",
		size, MaxRunStateBytes)
}

// CheckTaskOutputSize reports whether one step's outputs fit in what Temporal
// will store as an activity result.
//
// Called from [Task.EvalInScope], where the outputs become a fact — the same
// choke point [checkTaskOutputElementBound] polices the element count at, and
// for the same reason: every task's result, built-in or plugin, returns
// through that one place on both drivers, so both refuse identically by
// construction. That placement is activity-side on the durable driver, which
// is what the rule at [TemporalDefaultBlobLimitBytes] requires: nothing in
// workflow code may read the blob limit, and an activity has no determinism
// exposure.
//
// Measured by [encodedPayloadSize], not proto.Size — the #716 lesson; an
// activity result is handed to the same DataConverter as everything else and
// is serialized as ProtoJSON, so a check measuring the binary encoding would
// measure a payload nothing writes. A refusal is classified
// [ErrorKindLimitExceeded] by the caller, so it is non-retryable: a too-large
// output is the same output on attempt two.
func CheckTaskOutputSize(out *Node_Outputs) error {
	size := encodedPayloadSize(out)
	if size <= MaxTaskOutputBytes {
		return nil
	}

	return fmt.Errorf(
		"the step produced %d bytes of outputs, over the %d byte limit; "+
			"write large results somewhere and return a reference, or select "+
			"fields with the task's outputs: input rather than carrying the whole response",
		size, MaxTaskOutputBytes)
}

// WorkerDeadlockDetectionTimeout is the workflow-task deadlock budget every
// flowstate worker runs with, production and test alike.
//
// The SDK's default panics a workflow task whose goroutine has not yielded for
// a second. Flowstate's documented bounds admit inputs whose workflow-side
// processing legitimately approaches that second (a task output at the
// element bound, a for_each at its trip ceiling), and a contended host turns
// that second into more. A budget the rehearsal passes and production fails
// would make local runs lie about what production will do, so there is one
// value and every worker reads it: large enough that work at a documented
// bound fits with margin on a busy host, small enough that a genuinely
// wedged workflow goroutine is still caught quickly (#431).
const WorkerDeadlockDetectionTimeout = 5 * time.Second

// DefaultWorkerStopTimeout is how long `flow worker` gives the Temporal SDK to
// drain in-flight activities and workflow tasks after a shutdown signal before
// it returns from Stop, overridden by `--worker-stop-timeout`/
// FLOWSTATE_WORKER_STOP_TIMEOUT.
//
// The SDK's own zero value is 0s: Stop's internal wait races a timer against
// the in-flight WaitGroup, and a zero timer fires immediately, so an unset
// value does not mean "wait forever" — it means "don't wait at all," which is
// silent data loss dressed up as a default. Two minutes is generous rather
// than tight because this repository's activities are documented as
// legitimately long-running (see the heartbeat discussion in CLAUDE.md); an
// operator whose deployment's own grace period is shorter than this (Docker's
// default stop grace is 10s) has to raise it or the container's SIGKILL will
// still land before the drain finishes — see docs/DEPLOYMENT.md.
const DefaultWorkerStopTimeout = 2 * time.Minute
