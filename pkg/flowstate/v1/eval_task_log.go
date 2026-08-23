package flowstatev1

import (
	"context"
	"log/slog"
	"maps"
	"slices"
)

// taskFuncLog emits a message and returns nothing.
//
// Returning an empty outputs message rather than nil, because nil means "this step
// contributed no entry" to the executors and an empty one means "this step ran and
// produced nothing". A `log:` step did run, and a run record that cannot tell the two
// apart cannot show that it did.
func taskFuncLog(ctx context.Context, input map[string]*Value, scope *Scope) (*Node_Outputs, error) {
	taskInputs := &Task_Log_Inputs{}
	if err := populateProtoMessageFromValueMap(ctx, input, taskInputs, scope); err != nil {
		return nil, NewTaskError("log", ErrorKindInvalidInput, err)
	}

	// The declared bounds, enforced rather than decorative.
	//
	// A `log:` step's fields are chosen by the workflow — count, key length and value
	// length all — and they are written to a worker's logs and into durable history.
	// Bounding the resource an author controls is the rule this repo states; a bound
	// nothing checks is a comment. This is also where a level outside the enum is
	// caught for a specification that reached a worker without passing `flow validate`.
	if err := Validate(taskInputs); err != nil {
		return nil, NewTaskError("log", ErrorKindInvalidInput, err)
	}

	// Sorted, because a map's order is not one and a log line whose fields shuffle
	// between two runs of the same workflow is a diff nobody can read.
	attrs := make([]any, 0, len(taskInputs.GetFields()))
	for _, name := range slices.Sorted(maps.Keys(taskInputs.GetFields())) {
		attrs = append(attrs, slog.String(name, taskInputs.GetFields()[name]))
	}

	LoggerFrom(ctx).LogAttrs(LogContextFrom(ctx), slogLevel(taskInputs.GetLevel()), taskInputs.GetMessage(),
		attrsOf(attrs)...)

	return nodeOutputsFromProtoMessage(&Task_Log_Outputs{})
}

// attrsOf narrows a slice built as []any back to the attribute type LogAttrs wants.
//
// LogAttrs is the allocation-free entry point and takes []slog.Attr; the slice is
// assembled as []any only because that is what reads naturally above. Anything that is
// not an Attr is dropped rather than coerced — there is nothing else in it, and a
// silent drop beats a panic in the one task whose whole job is to be visible.
func attrsOf(values []any) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(values))
	for _, v := range values {
		if attr, ok := v.(slog.Attr); ok {
			attrs = append(attrs, attr)
		}
	}

	return attrs
}

// maxHTTPResponseBytes bounds how much of an HTTP response body the http task
// will read.
//
// The binding reason is memory safety: a worker must not let a remote endpoint
// decide how much it allocates. The value is also chosen to sit within
// Temporal's default per-payload limit, so a body that reads successfully can
// actually flow through a workflow.
//
// This is a default, not a ceiling on what the system can handle. Genuinely
// large payloads are a solved problem on this substrate — a custom payload
// codec can offload the blob to external storage and carry a reference through
// history (the claim-check pattern), which is the coherent way to raise this
// rather than simply buffering more in the worker. Until that codec exists,
// workflows needing only part of a large response should select fields with the
// outputs input.
