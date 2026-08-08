package main

import (
	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
)

// Where the payload codec is resolved, once, for every command in this binary.
//
// # One resolution point, both drivers
//
// `flow server` and `flow worker` reach this through [temporalConfig], which is
// the single place a Temporal client's options are built, including the pool
// that dials one client per mapped namespace, so a codec cannot end up covering
// some tenants and not others. `flow run local` reaches it through
// [localPayloadCodec] below. Both call this, so a deployment cannot rehearse
// under a different codec configuration than it runs under, and neither can
// forget to call it without the other noticing at review time: the function is
// the seam.
//
// # What it resolves today
//
// The null codec, and only the null codec. That is the whole of the prototype's
// claim: the slot exists, is threaded identically, and defaults to the behavior
// every deployment already has. What belongs here in the real slice is the
// plugin lookup, a codec is a plugin process, like a secrets backend, with key
// custody unified with `flow keys` and the issuer material rather than a second
// key story, and that lookup is the part this spike deliberately does not
// invent.
func payloadCodecConfig() (payloadcodec.Config, error) {
	cfg := payloadcodec.Config{}

	// Validated at resolution rather than at the first payload: a codec that
	// cannot come up must stop the command, not fail the first run that reaches
	// it. Cheap now, and it is the check that will matter when this reaches a
	// KMS.
	if err := cfg.Validate(); err != nil {
		return payloadcodec.Config{}, err
	}

	return cfg, nil
}

// localPayloadCodec resolves the codec for a local run, and applies it nowhere.
//
// # Why a local run does not encrypt anything, on purpose
//
// A codec is a boundary transform: it runs where a payload stops being a Go
// value in this process and becomes bytes somebody else stores. The local driver
// has no such boundary. `flow run local` calls [v1.RunWithInputs] in process;
// step outputs, `vars:`, and a signal's payload are live protobuf messages
// passed between function calls, never serialized, never persisted, and gone
// when the process exits. There is nothing for a codec to encrypt, and a codec
// invoked on a value that is about to be handed to the next function call would
// be encrypting and immediately decrypting for no reader.
//
// So the parity claim is not "the local driver encrypts too". It is the one both
// drivers can actually keep: *the same configuration is resolved, validated, and
// refused in the same way*, and a codec that cannot come up fails
// `flow run local` exactly as it fails `flow worker`. An author whose codec is
// misconfigured learns it at the rehearsal, which is what a local run is for.
//
// This is a no-op by design and by argument, not by omission. If the local
// driver ever grows durable state, a local history, a resumable run, a
// `flow test` fixture written to disk, that state is a boundary, and the codec
// belongs on it. That is the moment to change this function, and the reason to
// come looking for it.
func localPayloadCodec() (payloadcodec.Config, error) {
	return payloadCodecConfig()
}
