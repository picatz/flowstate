package engine

import (
	"sync/atomic"

	"go.temporal.io/sdk/converter"

	"github.com/picatz/flowstate/pkg/flowstate/v1/payloadcodec"
)

// The interpreter's copy of the process's data converter, and why it needs one.
//
// # The hole this closes
//
// [withSignalDeliveryCompat] wraps the converter a signal channel decodes with.
// It used to wrap [converter.GetDefaultDataConverter] unconditionally, which was
// correct while every deployment used the default converter and becomes a
// payload-codec bypass the moment one does not: the server encodes a signal's
// payload with the client's codec converter, history holds ciphertext, and the
// interpreter then hands those bytes to a converter that knows nothing about the
// codec. The decode fails, and a failed signal decode is not loud —
// channelImpl.Receive logs a corrupted signal and keeps waiting, exactly as
// [withSignalDeliveryCompat]'s own comment warns. An approval would be silently
// lost on every encrypted deployment.
//
// # Why a process value rather than a parameter
//
// The workflow-side converter is worker configuration, and the Go SDK carries it
// on the workflow context — but it exposes no public getter, only
// [workflow.WithDataConverter] to replace it (go.temporal.io/sdk@v1.47.0
// workflow/workflow_options.go:48). So the wrapper cannot ask the context what
// it is about to override. Until the SDK offers a getter, or the interpreter is
// restructured so the converter reaches [runWorkflow] some other way, the honest
// options are a process value set where the worker is built, or a bypass. This
// is the first.
//
// # Why this is replay-safe
//
// It is not a branch. Nothing here decides what a run does; it decides how bytes
// already in history are read back into a value, which is the same job the SDK's
// own worker-level converter does. A worker configured with a different codec
// than the one that wrote a payload cannot decode it — but that is true of the
// SDK's converter too, and it fails loudly at the decode rather than quietly at a
// different branch. Invariant 4's rule is about the interpreter's decisions being
// a pure function of history; a decoder is upstream of that.
var configuredConverter atomic.Pointer[converter.DataConverter]

// UseDataConverter tells the interpreter which data converter the worker is
// built with.
//
// Called once, at worker construction, before [Register], and from nowhere else.
// A deployment that never calls it gets [converter.GetDefaultDataConverter],
// which is what every deployment had before payload codecs existed.
func UseDataConverter(dc converter.DataConverter) {
	if dc == nil {
		configuredConverter.Store(nil)
		return
	}
	configuredConverter.Store(&dc)
}

// UseCodec is [UseDataConverter] spelled in terms of the codec slot, so a caller
// wiring a worker never has to build the converter itself and never has a chance
// to pair the codec converter with a plain failure converter.
func UseCodec(cfg payloadcodec.Config) { UseDataConverter(cfg.DataConverter()) }

// interpreterDataConverter is what workflow-side code decodes history with.
func interpreterDataConverter() converter.DataConverter {
	if dc := configuredConverter.Load(); dc != nil {
		return *dc
	}
	return converter.GetDefaultDataConverter()
}
