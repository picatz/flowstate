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
// whenever, a codec is configured — the operator never gets to configure one
// without the other.
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
)

// Codec encodes and decodes payload bytes on their way to and from the
// substrate.
//
// The two methods are [converter.PayloadCodec]'s, unchanged and deliberately
// so: a Flowstate codec is usable anywhere the SDK takes a codec, and the SDK's
// own codecs are usable here, without an adapter that could get the direction
// wrong. Name is the addition, and it earns its place at the diagnostic surface
// — "which codec was this history written with" is the first question asked
// about a payload that will not decode, and a %T of a wrapper answers it badly.
//
// Implementations must be safe for concurrent use: one codec serves every
// worker goroutine.
type Codec interface {
	// Name identifies the codec in diagnostics and startup logs. It must never
	// contain key material, a key id that is itself a secret, or anything
	// derived from a payload.
	Name() string

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
// [Config.Enabled], and it is asked for exactly two reasons — whether to turn
// failure encoding on, and what to say at startup.
type nullCodec struct{}

func (nullCodec) Name() string { return "none" }

func (nullCodec) Encode(p []*commonpb.Payload) ([]*commonpb.Payload, error) { return p, nil }

func (nullCodec) Decode(p []*commonpb.Payload) ([]*commonpb.Payload, error) { return p, nil }

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
// behaves exactly as it did before this existed — including its failure
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
// string is where the value that caused the error usually ends up — so an
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
	return nil
}
