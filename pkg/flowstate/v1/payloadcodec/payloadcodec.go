// Package payloadcodec is the one seam where a run's payloads are encrypted
// before they are durable, and the one place both drivers are configured with
// the same answer.
//
// # Why this is a package and not a proto type
//
// A [Codec] holds, or reaches, key material. It is therefore exactly the type
// CLAUDE.md's proto-first rule carves out: a value defined by a boundary it
// refuses to cross. Nothing here may be serialized, put in a Flowfile, sent to
// a plugin as data, or written into a run's state. What travels is ciphertext;
// the thing that produced it stays in the process that holds the key. Do not
// "fix" this into the schema under `proto/flowstate/v1/`.
//
// # Where the seam actually is
//
// The substrate never encrypts payloads server-side. The only place a payload
// can be encrypted without trusting the cluster is the client's data-converter
// chain, which runs in the worker and the API server, outside the workflow
// sandbox, and may therefore call a KMS. The Go SDK spells that chain
// [converter.PayloadCodec] wrapped by [converter.NewCodecDataConverter], set on
// [client.Options.DataConverter] (go.temporal.io/sdk@v1.47.0
// converter/codec.go:22 and :146, internal/client.go:915).
//
// Errors are the composition hazard. The SDK's default failure converter does
// not encode a failure's message or stack trace: they are written into history
// as plain strings unless [temporal.DefaultFailureConverterOptions.EncodeCommonAttributes]
// is set (go.temporal.io/sdk@v1.47.0 internal/failure_converter.go:32-41,
// default false). An encrypted history whose error strings quote the values
// that caused the error is the fail-open composition this repo already knows
// by name. So [Config.FailureConverter] turns that option on whenever, and only
// whenever, a codec is configured: the operator never gets to configure one
// without the other.
//
// # Memos are ciphertext, and every read goes through this converter
//
// A memo is not exempt. The Go SDK encodes memo values with the *user's*
// converter and falls back to the default only if that fails
// (go.temporal.io/sdk@v1.47.0 internal/internal_workflow_client.go's
// encodeMemoValue, gated on SDKFlagMemoUserDCEncode, which defaults to true), so
// on a deployment that configures a codec here every memo the API server writes
// is encrypted at rest. That is the policy rather than an accident: a run's
// tenant, its starter, and its declared signal policy are exactly the fields
// worth encrypting.
//
// The consequence is that reading them is not optional plumbing. Whatever
// [Config.DataConverter] returns has to reach every read site too, or the reads
// decode nothing and the server answers "no such run" to the tenant that owns
// it. `server.WithDataConverter` is that half, and `flow server` passes it the
// same resolved config the Temporal client got.
//
// Search attributes are the exception and cannot be covered: the SDK always
// encodes those with the default converter, because the cluster has to index
// them. Nothing payload-derived may ever be projected into one.
//
// # Ciphertext has to fit where the plaintext did
//
// A run's carried state is bounded in plaintext, inside workflow code, because
// that bound is a determinism input (`v1.MaxRunStateBytes`). Temporal's blob
// limit applies to what comes out of this seam. A codec that expands therefore
// moves the real ceiling, and the failure it moves it into is a hang rather
// than an error: Continue-As-New fails the workflow task, the workflow task is
// retried forever, and the run reports RUNNING. So [Codec] declares its
// worst-case expansion and [Config.Validate] checks it at startup, against a
// maximal run state, before any payload exists. A codec that does not fit does
// not start.
//
// # Every ciphertext says which key wrote it
//
// A payload a real codec encodes carries the id of the key that encrypted it,
// in payload metadata, under [KeyIDMetadataKey]. Two capabilities are
// impossible without it, and neither can be retrofitted onto history already
// written:
//
//   - Rotation. Encode uses the current key; Decode selects by the id it finds,
//     from whatever ring the implementation still trusts. Without the id,
//     rotation is trial decryption or a flag day.
//   - Crypto-erasure, which is what `flow shred` will be. Destroying a key by id
//     makes every payload written under it permanently undecodable, without
//     touching a byte of history, and the destruction can be recorded as an
//     administrative event that names what it orphaned. Without the id there is
//     nothing to name.
//
// Metadata is plaintext, necessarily: it is how Decode knows what it is holding
// before it has chosen a key. So the projection rule the search-attribute guard
// enforces applies here verbatim, and for the same structural reason. A key id
// is deployment- or identity-derived. It is never derived from a payload, and it
// is never itself a secret: a codec whose key id reveals key material has put
// the key in history beside the ciphertext. Deriving the id from the key through
// a one-way function is the usual answer, and is what the toy codec does.
//
// [Codec.CurrentKeyID] is that id, declared rather than discovered, for the same
// reason [Codec.MaxEncodedSize] is: it is checked at startup, against the
// grammar [ValidateKeyID] pins, before any payload exists. An id that cannot be
// named in an error message, or that is long enough to matter against the
// expansion budget, is refused by the process rather than met by a run.
//
// # The null codec is the default, and is not a placeholder
//
// [Null] is what an unconfigured deployment runs, and it is deliberately a real
// value rather than a nil check spread across call sites: a nil codec that some
// paths test for and others do not is how a payload escapes a seam. Every
// construction point takes a [Config], and the zero [Config] is the null codec.
package payloadcodec

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// KeyIDMetadataKey is the payload metadata entry carrying the id of the key a
// payload was encrypted under. One name, owned here, written by every codec and
// read by every codec.
//
// # Why it is namespaced
//
// Payload metadata is one flat map, shared with whoever else touches the
// payload: the SDK reserves the bare names it uses for itself ("encoding",
// "messageType"), a codec chain is a list rather than a single codec, and
// nothing anywhere arbitrates the space. A bare "keyId" would therefore be a
// name two independent codecs could both pick, and the collision is silent and
// catastrophic in exactly the way this package exists to prevent: Decode would
// select a key by an id another codec wrote. Namespacing removes the
// possibility rather than making it unlikely.
//
// The spelling follows the one this project already uses where it puts a member
// in a map it does not own, the MCP `_meta` key
// "picatz.github.io/flowstate.contentDigest" (cmd/flow/mcpui.go). Same rule,
// same prefix, so there is one answer in the tree to "how do we name a key in
// somebody else's map" rather than one per protocol. The SDK's own convention
// is the one thing it cannot be: bare lowercase names are precisely the space
// the SDK reserves.
const KeyIDMetadataKey = "picatz.github.io/flowstate.keyId"

// MaxKeyIDBytes bounds a key id.
//
// The bound is not cosmetic. The id is stamped on every payload the codec
// writes, so it is inside the expansion the codec declares through
// [Codec.MaxEncodedSize] and inside the budget [checkRunStateFits] allots: an
// unbounded id is an unbounded expansion, checked nowhere. Sixty-four bytes
// holds a truncated hash, a UUID, or a KMS key version, which is every shape a
// key id takes in practice.
const MaxKeyIDBytes = 64

// ValidateKeyID reports whether an id meets the grammar every Flowstate key id
// meets: one to [MaxKeyIDBytes] bytes of ASCII letters, digits, '.', '_' or '-'.
//
// The grammar is chosen for where an id ends up rather than for what a key store
// finds convenient. An id is quoted back in a decode failure, printed in a
// startup line, and will be an argument to `flow shred`, so it may not carry
// whitespace, control bytes, quotes, or the '/' and ':' that other naming
// schemes use as structure. An id that cannot be shown to an operator verbatim
// is an id the operator cannot act on.
//
// Two callers, in two directions. [Config.Validate] checks the id a codec
// declares, at startup, before any payload exists. A codec's Decode checks the
// id it reads off a payload, which is input an outside party chose, before doing
// anything with it, including quoting it: the errors returned here therefore
// never echo the id itself, only its length or the one byte that was wrong.
func ValidateKeyID(id string) error {
	if id == "" {
		return fmt.Errorf("a key id must not be empty")
	}
	if len(id) > MaxKeyIDBytes {
		return fmt.Errorf(
			"a key id is at most %d bytes and this one is %d: the id is stamped on every payload "+
				"the codec writes, so it is spent out of the same expansion budget the ciphertext is",
			MaxKeyIDBytes, len(id))
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == '-':
		default:
			return fmt.Errorf(
				"a key id holds only ASCII letters, digits, '.', '_' or '-', and this one holds %q "+
					"at byte %d: an id is quoted back in a decode failure and named on a `flow shred` "+
					"command line, so it must survive being shown to an operator verbatim",
				string(rune(c)), i)
		}
	}
	return nil
}

// Codec encodes and decodes payload bytes on their way to and from the
// substrate.
//
// Encode and Decode are [converter.PayloadCodec]'s, unchanged and deliberately
// so: a Flowstate codec is usable anywhere the SDK takes a codec, without an
// adapter that could get the direction wrong. The other direction no longer
// holds, and that is the point of [Codec.MaxEncodedSize]: an SDK codec has to
// be wrapped in something that states its expansion before it can be configured
// here, because an expansion nobody has stated is one nobody has checked
// against the blob limit. Name is the other addition, and it earns its place at
// the diagnostic surface: "which codec was this history written with" is the
// first question asked about a payload that will not decode, and a %T of a
// wrapper answers it badly.
//
// Implementations must be safe for concurrent use: one codec serves every
// worker goroutine.
type Codec interface {
	// Name identifies the codec in diagnostics and startup logs. It must never
	// contain key material, a key id that is itself a secret, or anything
	// derived from a payload.
	Name() string

	// CurrentKeyID is the id of the key Encode is using now, which Encode
	// stamps on every payload it writes under [KeyIDMetadataKey].
	//
	// It is the *current* key, singular, because that is the only one anybody
	// outside the codec has a use for: it is what will appear on new payloads,
	// so it is what has to fit the grammar and the size budget at startup. The
	// ring Decode selects from is wider, holding every key still trusted, and
	// stays the implementation's business. Nothing here enumerates it, because
	// nothing here would do anything with the enumeration: `flow shred`
	// destroys a key in custody, which is `flow keys`' surface rather than this
	// one.
	//
	// Only the null codec answers with the empty string, and it means what it
	// says: this codec encrypts nothing, so there is no key, so a payload it
	// wrote names none. [Config.Validate] refuses any other codec that answers
	// that way, because a codec that stamps no id writes ciphertext that can
	// never be attributed to a key and therefore can never be shredded, which
	// is a promise broken silently, years later, by a deletion that deletes
	// nothing.
	//
	// This is a method on the interface rather than an optional one a codec may
	// implement, for the reason [Codec.MaxEncodedSize] is: an optional contract
	// is one an implementation omits by accident and nothing catches. The
	// compiler asking every codec the question is the enforcement.
	//
	// It must be stable for the life of the process: it is read once at startup
	// to validate, and read again on every Encode. A codec that rotated
	// underneath itself would declare one id and stamp another.
	CurrentKeyID() string

	// MaxEncodedSize reports the largest encoded size Encode may produce for
	// one payload of plain bytes, where both sides are measured as proto.Size
	// of a payload: what goes in is the payload the data converter built, and
	// what comes out is the payload the substrate will store.
	//
	// This is a promise the implementation makes about itself, and it is
	// checked once, at startup, against what Temporal will store for a maximal
	// run state (see [Config.Validate]). It is never called on a payload path
	// and never inside workflow code: it takes a size rather than a payload
	// precisely so that no implementation can be tempted to answer by encoding
	// something.
	//
	// Three requirements, each of which a wrong answer here would break:
	//
	//   - It must be an upper bound. Encode producing more than this is a run
	//     that wedges at a Continue-As-New, which is the hang
	//     [v1.MaxRunStateBytes] exists to convert into an answer.
	//   - It must be tight enough to be worth declaring. A codec that answers
	//     with a gibibyte satisfies "upper bound" and refuses to start, so an
	//     implementation states the overhead it actually adds, and its tests
	//     encode payloads and check that the declaration is approached rather
	//     than merely respected.
	//   - It must be monotone in plain, and never below it. A compressing
	//     codec's worst case is incompressible input, so its bound is still at
	//     least its input; a declaration under its input is a bound that is not
	//     one, and [Config.Validate] refuses it rather than trusting it.
	//
	// A batch is the sum of its payloads, not this: a codec with per-payload
	// overhead expands k payloads by k times that overhead, and k is the SDK's
	// choice rather than a size. The one call site is the run state carried
	// across a Continue-As-New, which is one payload.
	MaxEncodedSize(plain int) int

	// Encode is called on the way out, with payloads that are never nil, and
	// must not mutate its argument.
	//
	// Every payload it writes carries [Codec.CurrentKeyID] under
	// [KeyIDMetadataKey]. A codec that encrypts and does not stamp is writing
	// history nothing can rotate off and nothing can shred.
	Encode([]*commonpb.Payload) ([]*commonpb.Payload, error)

	// Decode is called on the way in, with payloads that are never nil, and
	// must not mutate its argument.
	//
	// It selects the key by the id the payload names, never by which key is
	// current: falling back to the current key when the id does not match is
	// how a rotated deployment turns a decode failure into a garbled success,
	// and how a shredded payload comes back to life.
	//
	// Three inputs, three answers, all fail-closed:
	//
	//   - A payload this codec wrote, naming an id it holds: decoded.
	//   - A payload naming an id it does not hold: an error naming the id and
	//     nothing else, never the ciphertext and never a guess. This is the
	//     read path of a payload whose key was destroyed, so the error text is
	//     a product surface: it should read as "this was destroyed", because
	//     that is usually what happened, and reading as corruption sends an
	//     operator to look for a bug in place of the shred they performed.
	//   - A payload this codec never wrote: tolerated only when it carries no
	//     mark of this codec at all, which is history written before the
	//     deployment turned a codec on. A payload marked as this codec's but
	//     carrying no key id is not that: it is a payload claiming an origin it
	//     does not have, and it is refused.
	//
	// The id read off a payload is input an outside party chose. Check it
	// against [ValidateKeyID] before using it as a map key or putting it in an
	// error.
	Decode([]*commonpb.Payload) ([]*commonpb.Payload, error)
}

// nullCodec is the identity codec: what a deployment that has configured
// nothing runs.
//
// It is a codec rather than an absence so that the presence of a codec is never
// the thing a call site branches on. The one place enablement is asked about is
// [Config.Enabled], and it is asked for exactly two reasons, whether to turn
// failure encoding on, and what to say at startup.
type nullCodec struct{}

func (nullCodec) Name() string { return "none" }

// CurrentKeyID is empty, and is the one codec allowed to answer that way: there
// is no key, nothing is encrypted, and the payloads that come out are the ones
// that went in, unmarked. Slice 1's guarantee that pre-codec history stays
// readable is the same statement seen from the other side.
func (nullCodec) CurrentKeyID() string { return "" }

func (nullCodec) Encode(p []*commonpb.Payload) ([]*commonpb.Payload, error) { return p, nil }

func (nullCodec) Decode(p []*commonpb.Payload) ([]*commonpb.Payload, error) { return p, nil }

// MaxEncodedSize is the identity, because Encode is. A deployment that has
// configured nothing must pass the startup check by arithmetic rather than by
// exemption: there is no branch in [Config.Validate] that skips the null codec,
// so the check itself is exercised on every process that starts.
func (nullCodec) MaxEncodedSize(plain int) int { return plain }

// A Flowstate codec is still an SDK codec, checked by the compiler rather than
// asserted in prose: [Config.DataConverter] hands one straight to
// [converter.NewCodecDataConverter], and the day that stops compiling is the day
// this package needs an adapter instead of a doc comment.
var _ converter.PayloadCodec = Codec(nil)

// Null returns the identity codec, which is the default for every deployment
// that has not configured one.
func Null() Codec { return nullCodec{} }

// IsNull reports whether c is the identity codec.
func IsNull(c Codec) bool {
	_, ok := c.(nullCodec)
	return ok
}

// Config is the codec slot, resolved once per process and handed identically to
// every client, worker, and local run.
//
// The zero value is the null codec, so a deployment that configures nothing
// behaves exactly as it did before this existed, including its failure
// converter, which stays the SDK default.
type Config struct {
	// Codec encrypts payloads. Nil means [Null].
	Codec Codec
}

// codec answers with the null codec rather than nil, so no caller here has to.
func (c Config) codec() Codec {
	if c.Codec == nil {
		return Null()
	}
	return c.Codec
}

// Enabled reports whether a codec that actually transforms payloads is
// configured.
func (c Config) Enabled() bool { return !IsNull(c.codec()) }

// Name is what a startup line and a diagnostic should say.
func (c Config) Name() string { return c.codec().Name() }

// serializer is the composite payload converter every flowstate process
// serializes with, and the one place the ProtoJSON-versus-binary question is
// answered (#911).
//
// # What differs from the SDK default, and why
//
// The list is the SDK's own (go.temporal.io/sdk@v1.47.0
// converter/default_data_converter.go:5-17) with exactly one change: the binary
// [converter.NewProtoPayloadConverter] is registered *ahead of*
// [converter.NewProtoJSONPayloadConverter] rather than behind it.
//
// The SDK's comment on that pair explains why the order is the whole decision:
// both converters match the same `proto.Message` interface, and
// [converter.CompositeDataConverter.ToPayload] walks its converters in
// registration order and takes the first one that returns a non-nil payload
// (composite_data_converter.go:80-99). Flowstate hands this converter nothing
// but proto messages — every RunState, every completed run's
// Workflow_StepOutputs, every activity argument and result — so on the SDK's
// order every one of them was stored as ProtoJSON, which runs 1.03x to 1.32x of
// the binary encoding on real transcripts, against a blob budget
// [v1.MaxRunStateBytes] argues can never be raised. The tax is worst on many
// small map entries, which is the shape a run with many steps produces.
//
// # Why this is a two-way door
//
// Decoding does not consult the order at all.
// [converter.CompositeDataConverter.FromPayload] reads the payload's own
// `encoding` metadata and looks the converter up in a map keyed by encoding
// (composite_data_converter.go:101-125), so a history holding both encodings
// decodes fine and a run written by an older worker keeps replaying. That is
// true only while *both* converters stay registered: this is a reorder, and
// must never become a replace. A chain that drops ProtoJSON strands every
// payload already written.
//
// # What deliberately did not change
//
// `v1.CheckRunStateSize` still measures ProtoJSON. It is called from workflow
// code, so its arithmetic is a determinism input; see the comment on
// `encodedPayloadSize` in size.go for the whole argument. The bound therefore
// over-counts what is now written, which is safety margin and fails in the safe
// direction.
var serializer = converter.NewCompositeDataConverter(
	converter.NewNilPayloadConverter(),
	converter.NewByteSlicePayloadConverter(),

	converter.NewProtoPayloadConverter(),
	converter.NewProtoJSONPayloadConverter(),

	converter.NewJSONPayloadConverter(),
)

// Serializer returns the converter flowstate serializes values with before any
// codec sees them.
//
// Exported for the read paths that need to decode a payload without a codec
// configured — and for tests that need to assert the decode-both property
// directly. A caller wiring a client or worker wants [Config.DataConverter] or
// [Config.Apply] instead, which pair it with the right codec and failure
// converter.
func Serializer() converter.DataConverter { return serializer }

// DataConverter returns the converter every client and worker in the process
// must be built with.
//
// The parent is [Serializer], which is what decides how a value becomes bytes;
// the codec decides what happens to those bytes afterwards. That order matters
// and is the SDK's, not a choice made here: the codec sees the serialized
// payload and nothing about the Go type it came from, which is what lets one
// codec serve a schema that changes.
func (c Config) DataConverter() converter.DataConverter {
	if !c.Enabled() {
		// The serializer itself, rather than a codec converter wrapping the
		// identity codec. They behave identically, and this way an unconfigured
		// deployment's payload path is byte-for-byte the one a codec-configured
		// deployment hands its codec.
		return serializer
	}
	return converter.NewCodecDataConverter(serializer, c.codec())
}

// FailureConverter returns the failure converter that must accompany
// [Config.DataConverter].
//
// Message and stack trace encoding is on exactly when a codec is configured.
// The SDK's default leaves both in plaintext in history
// (go.temporal.io/sdk@v1.47.0 internal/failure_converter.go:41), and an error
// string is where the value that caused the error usually ends up, so an
// encrypted history with plaintext failures is not "mostly encrypted", it is a
// seam with a hole in the shape of every bad input.
//
// Note the DataConverter passed through: the encoded attributes are themselves
// a payload, so they go through the codec too. A failure converter built on the
// default converter would move the message into a payload and leave it in the
// clear, which looks like it worked.
func (c Config) FailureConverter() converter.FailureConverter {
	if !c.Enabled() {
		return temporal.GetDefaultFailureConverter()
	}
	return temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{
		DataConverter:          c.DataConverter(),
		EncodeCommonAttributes: true,
	})
}

// Apply sets both converters on client options.
//
// One function rather than two fields set by each caller, because the pairing is
// the invariant: a client with the codec converter and the default failure
// converter is the fail-open configuration this package exists to make
// unrepresentable.
func (c Config) Apply(opts *client.Options) {
	if opts == nil {
		return
	}
	opts.DataConverter = c.DataConverter()
	opts.FailureConverter = c.FailureConverter()
}

// Validate reports whether the configuration can be used.
//
// Called where configuration is loaded rather than where a payload is encoded,
// which is the rule this repo applies to every policy surface: a codec that
// cannot come up must stop the process, not fail the first run that reaches it.
func (c Config) Validate() error {
	codec := c.codec()
	if codec.Name() == "" {
		return fmt.Errorf("payload codec: a codec must name itself, for diagnostics")
	}
	if err := checkKeyID(c); err != nil {
		return err
	}
	return checkRunStateFits(codec)
}

// checkKeyID refuses a codec whose current key id is missing or unspellable,
// before it has written a payload nobody can rotate off or shred.
//
// It runs before [checkRunStateFits] rather than after, because the id is a term
// in the size the size check is about: an id of unbounded length is an expansion
// of unbounded size, and the codec's own declaration is the thing the size check
// takes on trust.
//
// The null codec is the one exemption, and it is not really one: it is asked the
// question and answers "no key", which is true, and any other codec giving that
// answer is refused here. The rule is not "codecs that opt in declare an id", it
// is "a codec that transforms payloads has a key, and says which".
func checkKeyID(c Config) error {
	codec := c.codec()
	id := codec.CurrentKeyID()

	if !c.Enabled() {
		return nil
	}

	if id == "" {
		return fmt.Errorf(
			"payload codec %q names no current key id: every payload a codec writes carries the id "+
				"of the key that encrypted it, under the %q metadata entry, because that id is what "+
				"rotation selects by and what `flow shred` erases by. Ciphertext written without one "+
				"cannot be attributed to a key, so it can never be shredded: the erasure would report "+
				"success and destroy nothing",
			codec.Name(), KeyIDMetadataKey)
	}

	if err := ValidateKeyID(id); err != nil {
		return fmt.Errorf("payload codec %q declares a current key id it cannot use: %w", codec.Name(), err)
	}

	return nil
}

// checkRunStateFits refuses a codec whose ciphertext would not fit where its
// plaintext does.
//
// # What this is defending
//
// [v1.MaxRunStateBytes] is measured on the plaintext, with proto.Size, inside
// workflow code, because it is a determinism input and cannot be anything else.
// Temporal's blob limit is enforced on what the data converter chain produced,
// which on a deployment with a codec is ciphertext nobody measured. So a run
// state that passes [v1.CheckRunStateSize] can still be refused by the server,
// and the refusal lands on a Continue-As-New: the workflow task fails, is
// retried forever, and the run reports RUNNING while doing nothing. Encryption
// must not put back the hang the bound was written to remove.
//
// # Why here, and only here
//
// This asks a codec how much it expands, which is a question about the
// deployment's configuration, not about a run. Asking it inside workflow code
// would make replay depend on which codec a worker was started with. Asking it
// at the first Continue-As-New would be a diagnosis produced by the run that
// dies of it. So it is asked once, where configuration is loaded, by the one
// function every construction point already calls: fail closed, at startup,
// before a single payload exists.
//
// # The arithmetic
//
// A maximal run state is [v1.MaxRunStateBytes] of message inside a payload,
// which is why the envelope is added on the input side rather than subtracted
// from the limit: the codec is handed the payload, not the message, and a codec
// that expands per byte expands the envelope too.
func checkRunStateFits(codec Codec) error {
	const plain = v1.MaxRunStateBytes + v1.PayloadEnvelopeReserveBytes

	encoded := codec.MaxEncodedSize(plain)

	// A bound below its own input is not a bound, and the likeliest way to
	// produce one is arithmetic that overflowed. Refusing beats trusting: an
	// implementation that answers this way has not thought about the question,
	// and the whole check rests on the answer.
	if encoded < plain {
		return fmt.Errorf(
			"payload codec %q declares a worst-case encoded size of %d bytes for %d bytes of input, "+
				"which is smaller than the input: MaxEncodedSize is an upper bound on what Encode may "+
				"produce, and even a compressing codec's worst case is input it cannot compress. "+
				"Check the declaration for arithmetic that overflowed",
			codec.Name(), encoded, plain)
	}

	// The ceiling is under the blob limit by the framing reserve, because the
	// blob check weighs the payload inside the Payloads message, the command,
	// and the history event wrapped around it, and none of that framing is the
	// codec's to know about.
	if encoded > v1.TemporalDefaultBlobLimitBytes-v1.ContinueAsNewFramingReserveBytes {
		ceiling := v1.TemporalDefaultBlobLimitBytes - v1.ContinueAsNewFramingReserveBytes
		return fmt.Errorf(
			"payload codec %q expands a maximal run state to %d bytes, which is %d over the %d bytes "+
				"available under Temporal's %d byte blob limit once the framing the substrate wraps "+
				"around the payload is reserved: a run that reached it would fail its Continue-As-New, "+
				"and a failed workflow task is retried forever, so the run would report RUNNING and never finish. "+
				"A codec has %d bytes to spend on top of the %d byte run state and the %d byte payload "+
				"envelope around it, and this one declares %d. Raising the cluster's blob limit is not the "+
				"fix, since flowstate's run state bound is compiled in and would not move with it: the "+
				"codec has to be leaner, with less per-payload metadata, a shorter key id, or no per-byte "+
				"growth such as base64 armour",
			codec.Name(), encoded, encoded-ceiling, ceiling, v1.TemporalDefaultBlobLimitBytes,
			v1.MaxCodecExpansionBytes, v1.MaxRunStateBytes, v1.PayloadEnvelopeReserveBytes, encoded-plain)
	}

	return nil
}
