package engine

import (
	"go.temporal.io/sdk/converter"
)

// orDefaultConverter is the converter workflow-side code decodes history with,
// given the one a worker was registered with.
//
// # The hole this closes
//
// [withSignalDeliveryCompat] wraps the converter a signal channel decodes with.
// It used to wrap [converter.GetDefaultDataConverter] unconditionally, which was
// correct while every deployment used the default converter and becomes a
// payload-codec bypass the moment one does not: the server encodes a signal's
// payload with the client's codec converter, history holds ciphertext, and the
// interpreter then hands those bytes to a converter that knows nothing about the
// codec. The decode fails, and a failed signal decode is not loud:
// channelImpl.Receive logs a corrupted signal and keeps waiting, exactly as
// [withSignalDeliveryCompat]'s own comment warns. An approval would be silently
// lost on every encrypted deployment.
//
// # Why it is bound at registration
//
// The workflow-side converter is worker configuration, and the Go SDK carries it
// on the workflow context, but it exposes no public getter, only
// [workflow.WithDataConverter] to replace it (go.temporal.io/sdk@v1.48.0
// workflow/workflow_options.go). So the wrapper cannot ask the context what it is
// about to override. It is told instead, by [Register] binding the worker's
// converter into the workflow function it registers under the name "Run"
// ([TaskRuntimeConfig.WithDataConverter]). An earlier version kept it in a
// process global, which two embedded workers with different codecs overwrote.
//
// # Why this is replay-safe
//
// It is not a branch. Nothing here decides what a run does; it decides how bytes
// already in history are read back into a value, which is the same job the SDK's
// own worker-level converter does. A worker configured with a different codec
// than the one that wrote a payload cannot decode it, but that is true of the
// SDK's converter too, and it fails loudly at the decode rather than quietly at a
// different branch. Invariant 4's rule is about the interpreter's decisions being
// a pure function of history; a decoder is upstream of that.
func orDefaultConverter(dc converter.DataConverter) converter.DataConverter {
	if dc != nil {
		return dc
	}
	return converter.GetDefaultDataConverter()
}
