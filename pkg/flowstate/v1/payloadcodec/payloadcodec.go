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
// "fix" this into `proto/flowstate/v1/flowstate.proto`.
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
// the diagnostic surface , "which codec was this history written with" is the
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
	Encode([]*commonpb.Payload) ([]*commonpb.Payload, error)

	// Decode is called on the way in, with payloads that are never nil, and
	// must not mutate its argument. It must tolerate payloads this codec never
	// encoded: a deployment that turns a codec on has history written before
	// it did.
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

// DataConverter returns the converter every client and worker in the process
// must be built with.
//
// The parent is the SDK default converter, which is what decides how a value
// becomes bytes; the codec decides what happens to those bytes afterwards. That
// order matters and is the SDK's, not a choice made here: the codec sees the
// serialized payload and nothing about the Go type it came from, which is what
// lets one codec serve a schema that changes.
func (c Config) DataConverter() converter.DataConverter {
	if !c.Enabled() {
		// The default converter itself, rather than a codec converter wrapping
		// the identity codec. They behave identically, and this way an
		// unconfigured deployment's payload path is byte-for-byte the one it had
		// before this package existed.
		return converter.GetDefaultDataConverter()
	}
	return converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), c.codec())
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
	return checkRunStateFits(codec)
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

	if encoded > v1.TemporalDefaultBlobLimitBytes {
		return fmt.Errorf(
			"payload codec %q expands a maximal run state to %d bytes, which is %d over the %d bytes "+
				"Temporal will store for one payload: a run that reached it would fail its Continue-As-New, "+
				"and a failed workflow task is retried forever, so the run would report RUNNING and never finish. "+
				"A codec has %d bytes to spend on top of the %d byte run state and the %d byte payload "+
				"envelope around it, and this one declares %d. Raising the cluster's blob limit is not the "+
				"fix, since flowstate's run state bound is compiled in and would not move with it: the "+
				"codec has to be leaner, with less per-payload metadata, a shorter key id, or no per-byte "+
				"growth such as base64 armour",
			codec.Name(), encoded, encoded-v1.TemporalDefaultBlobLimitBytes, v1.TemporalDefaultBlobLimitBytes,
			v1.MaxCodecExpansionBytes, v1.MaxRunStateBytes, v1.PayloadEnvelopeReserveBytes, encoded-plain)
	}

	return nil
}
