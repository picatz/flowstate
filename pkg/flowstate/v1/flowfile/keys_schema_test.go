package flowfile

import (
	"slices"
	"testing"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestDocumentKeysCoverWorkflowSchema keeps the parser's root vocabulary tied
// to the Workflow descriptor rather than to a fixture that must remember to set
// every optional field. The aliases below are the deliberate YAML boundary:
// declarations lose their schema-oriented prefix, plugin requirements use the
// block's author-facing name, and edition belongs to the file rather than the
// compiled Workflow. Runtime-resolved fields have no Flowfile spelling.
func TestDocumentKeysCoverWorkflowSchema(t *testing.T) {
	t.Parallel()

	var schema []string
	fields := (&v1.Workflow{}).ProtoReflect().Descriptor().Fields()
	for i := range fields.Len() {
		name := string(fields.Get(i).Name())
		switch name {
		case "profile", "resolved_plugins", "resolved_task_capabilities":
			// Compiler- or submit-time facts, never author input.
		case "declared_inputs":
			schema = append(schema, "inputs")
		case "declared_outputs":
			schema = append(schema, "outputs")
		case "plugin_requirements":
			schema = append(schema, "plugins")
		default:
			schema = append(schema, name)
		}
	}
	schema = append(schema, "edition")

	assert.ElementsMatch(t, schema, DocumentKeys(),
		"the Workflow schema and the root Flowfile grammar disagree; map a new schema field to its author-facing key, or record why it is runtime-only")
}

// TestStepGrammarKeysCoverNodeSchema is the step-level counterpart. Node owns
// ordinary properties and kind arms, StepPolicy owns the flattened execution
// controls, Wait's oneof owns the four author-facing wait spellings, and Call
// owns the two file-only companion keys. Task names stay registry-derived.
func TestStepGrammarKeysCoverNodeSchema(t *testing.T) {
	t.Parallel()

	var schema []string
	node := (&v1.Node{}).ProtoReflect().Descriptor()
	for i := range node.Fields().Len() {
		name := string(node.Fields().Get(i).Name())
		switch name {
		case "task":
			// A dynamic key supplied by the active task registry.
		case "condition":
			schema = append(schema, "if")
		case "policy":
			schema = append(schema, descriptorFieldNames((&v1.StepPolicy{}).ProtoReflect().Descriptor())...)
		case "wait":
			schema = append(schema, waitGrammarKeys(t)...)
		case "call":
			schema = append(schema, "call")
			schema = append(schema, callCompanionKeys(t)...)
		default:
			schema = append(schema, name)
		}
	}

	assert.ElementsMatch(t, schema, StepGrammarKeys(),
		"the Node schema and step grammar disagree; map a new schema field to its author-facing key, or record why it has no independent spelling")
}

func waitGrammarKeys(t *testing.T) []string {
	t.Helper()

	var keys []string
	fields := (&v1.Wait{}).ProtoReflect().Descriptor().Fields()
	for i := range fields.Len() {
		name := string(fields.Get(i).Name())
		switch name {
		case "duration", "duration_expr":
			keys = append(keys, "sleep")
		case "until":
			keys = append(keys, "wait_until")
		case "signal":
			keys = append(keys, "wait_for_signal")
		case "signal_batch":
			keys = append(keys, "wait_for_signals")
		case "timeout", "timeout_expr":
			// Nested under a signal or until mapping, not a second step key.
		default:
			t.Fatalf("Wait gained schema field %q with no Flowfile grammar mapping", name)
		}
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

func callCompanionKeys(t *testing.T) []string {
	t.Helper()

	var keys []string
	fields := (&v1.Call{}).ProtoReflect().Descriptor().Fields()
	for i := range fields.Len() {
		name := string(fields.Get(i).Name())
		switch name {
		case "workflow":
			// The compiled callee embedded behind the path an author wrote.
		case "source":
			// The scalar value of the `call:` kind key itself.
		case "arguments":
			keys = append(keys, "with")
		case "source_digest":
			keys = append(keys, "digest")
		default:
			t.Fatalf("Call gained schema field %q with no Flowfile grammar mapping", name)
		}
	}
	return keys
}

func descriptorFieldNames(message protoreflect.MessageDescriptor) []string {
	fields := message.Fields()
	names := make([]string, 0, fields.Len())
	for i := range fields.Len() {
		names = append(names, string(fields.Get(i).Name()))
	}
	return names
}

// The public vocabulary functions return copies. A completion caller sorting or
// filtering its answer must not mutate what the compiler accepts next.
func TestGrammarKeySnapshotsAreIndependent(t *testing.T) {
	t.Parallel()

	document := DocumentKeys()
	step := StepGrammarKeys()
	require.NotEmpty(t, document)
	require.NotEmpty(t, step)
	document[0] = "changed"
	step[0] = "changed"
	assert.NotEqual(t, "changed", DocumentKeys()[0])
	assert.NotEqual(t, "changed", StepGrammarKeys()[0])
}
