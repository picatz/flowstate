package main

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The run document is a rendering of the schema and must never become a second
// description of it.
//
// That is this repository's most-repeated defect — a value with one meaning written
// down twice — and a renderer is exactly the shape it likes to take: a table of
// field names that agrees with the schema on the day it is written and quietly
// stops. So the tests here are in two groups.
//
// The first group pins the *rules* against the schema itself, by walking the
// descriptors: every rename still names a field, and the set of messages the
// collapse rule applies to is stated so that a schema change which adds or removes
// one fails here rather than silently changing what a `jq` expression reads.
//
// The second group pins the *fidelity*: everything the rendering does not touch is
// byte-for-byte what protojson wrote. That is the property that makes a field added
// to the schema appear in the document without this file changing, which is the
// whole reason the rendering is allowed to exist.

// TestRunDocumentRenamesResolve is the drift pin for [runDocumentNames].
//
// A rename keyed by a full field name that no longer exists is a rename that
// silently stops applying: the document goes back to the schema's spelling, every
// `jq` expression written against the documented path stops resolving, and nothing
// else in the tree notices. So each key is resolved against the descriptors, and
// the result is checked for the one way a rename can do damage rather than merely
// stop working — landing on a name a sibling field already renders as, which would
// put two fields on one JSON key.
func TestRunDocumentRenamesResolve(t *testing.T) {
	t.Parallel()

	require.NotEmpty(t, runDocumentNames,
		"the rename table is empty, so either the rendering changed or this pin lost its subject")

	for name, rendered := range runDocumentNames {
		descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
		require.NoErrorf(t, err,
			"the run document renames %s, which the schema no longer has: the rename has "+
				"silently stopped applying and every documented jq path through it has moved", name)

		field, ok := descriptor.(protoreflect.FieldDescriptor)
		require.Truef(t, ok, "%s names a %T rather than a field", name, descriptor)

		message := field.ContainingMessage()

		for i := range message.Fields().Len() {
			sibling := message.Fields().Get(i)
			if sibling.FullName() == field.FullName() {
				continue
			}

			assert.NotEqualf(t, rendered, renderedName(sibling),
				"renaming %s to %q collides with %s, so one of the two fields would be "+
					"written over the other", name, rendered, sibling.FullName())
		}
	}
}

// TestTheCollapsedWrappersAreTheOnesTheSchemaHas states the set of messages rule 1
// applies to, so that gaining or losing one is a decision somebody makes rather
// than a document that changes shape underneath its readers.
//
// The rule itself is structural and needs no table — a message whose only field is
// a map is that map — which is what keeps it from drifting. What this pins is the
// *consequence*: which nouns disappear from the document. A new single-map message
// somewhere in a run's answer would silently start collapsing, and a reader indexing
// through its wrapper would find the key gone; a second field added to one of these
// would silently bring a wrapper back. Both are visible here, before they are
// visible to somebody's pipeline.
func TestTheCollapsedWrappersAreTheOnesTheSchemaHas(t *testing.T) {
	t.Parallel()

	var collapsed []string

	// The schema is twelve files rather than one (#658), so there is no single
	// File_flowstate_v1_..._proto to walk any more — every generated file in
	// the package has to be visited, which is what RangeFiles over the global
	// registry does rather than naming each one and going stale the next time
	// a file is added or split further.
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		if fd.Package() != flowstatePackage {
			return true
		}
		walkMessages(fd.Messages(), func(descriptor protoreflect.MessageDescriptor) {
			if _, ok := soleMapField(descriptor); ok {
				collapsed = append(collapsed, string(descriptor.FullName()))
			}
		})
		return true
	})

	assert.ElementsMatch(t, []string{
		// The transcript's per-step entry: `named_values` and nothing else.
		"flowstate.v1.Node.Outputs",

		// What a run computed for its declared outputs: `values` and nothing else.
		"flowstate.v1.RunOutputs",

		// Not in a run's answer, and collapsed anyway because the rule is
		// structural rather than a list of exceptions. It is reached only through
		// a [v1.Value] whose kind is `structure`, which the rendering hands to
		// protojson whole — so this entry records that the rule sees it, not that
		// a document is affected by it.
		"flowstate.v1.Value.Structure.Map",
	}, collapsed,
		"the set of wrapper messages the run document collapses has changed. That moves "+
			"or restores a level in every jq expression that reads through it, so it is a "+
			"breaking change to the documented shape: decide it deliberately and say so in "+
			"the release notes")
}

// TestTheRenderingStaysOutOfWellKnownTypes is the negative direction of rule 1,
// and it is a data-loss guard rather than a tidiness one.
//
// The collapse rule is structural: a message whose only field is a map is that
// map. [structpb.Struct] is exactly that message — one field, `fields`, a map —
// and protojson renders it as a bare JSON object with no `fields` key at all,
// because that is what a Struct means. A structural rule applied to protojson's
// output for one would look for a key that is not there, find nothing, and write
// `{}` over the whole of somebody's data, reporting success.
//
// Nothing in flowstate.v1 imports Struct today. This is the test that decides what
// happens on the day something does, and the answer it pins is "nothing": the
// rendering enters this repository's own messages and CEL's value, and passes
// everything else through as protojson wrote it.
func TestTheRenderingStaysOutOfWellKnownTypes(t *testing.T) {
	t.Parallel()

	held, err := structpb.NewStruct(map[string]any{"region": "eu-west-1", "replicas": 3})
	require.NoError(t, err)

	assert.False(t, projects(held.ProtoReflect().Descriptor()),
		"the rendering would enter a google.protobuf.Struct, whose protojson form has no "+
			"field names to project through: it would write {} over the whole of it")

	rendered, err := marshalRunDocument(held, false, false)
	require.NoError(t, err)

	expected, err := marshalJSON(held, false)
	require.NoError(t, err)

	assert.JSONEq(t, string(expected), string(rendered),
		"a well-known type came back changed")
	assert.Contains(t, string(rendered), "eu-west-1", "the contents were dropped")
}

// TestTheRunDocumentIsProtojsonWhereItDoesNotProject is the fidelity pin, and the
// reason the rendering is allowed to be a rendering.
//
// Everything outside the three rules is not re-derived here — it is the bytes
// protojson produced, carried through. So a message holding nothing to project has
// to come out identical, field for field, including the parts nobody would think to
// assert: unpopulated fields emitted, a 64-bit schema field written as a string, an
// enum written by name, a timestamp in RFC 3339.
//
// Byte-for-byte after compaction, because protojson injects a space after a comma
// at random per binary and that is not a difference in the document.
func TestTheRunDocumentIsProtojsonWhereItDoesNotProject(t *testing.T) {
	t.Parallel()

	for _, message := range []proto.Message{
		&v1.RunSummary{
			WorkflowId: "flowstate-workflow-3f7c",
			Status:     v1.RunResponse_STATUS_TIMED_OUT,
			StartTime:  timestamppb.New(mustTime(t, "2026-08-15T09:41:02.5Z")),
		},
		&v1.ListResponse{
			Runs:          []*v1.RunSummary{{WorkflowId: "a"}, {WorkflowId: "b"}},
			NextPageToken: "more",
		},
		&v1.MutationResult{Verb: "cancel", WorkflowId: "flowstate-workflow-3f7c", Result: resultRequested},
		&v1.GetResponse{
			WorkflowId: "flowstate-workflow-3f7c",
			RunId:      "0198f1e2-0000-7000-8000-000000000000",
			Status:     v1.RunResponse_STATUS_FAILED,
			Kind: &v1.GetResponse_Error{Error: &v1.RunResponse_Error{
				Message: "step \"fetch\": denied by egress policy",
				Kind:    v1.ErrorKindPolicyDenied.String(),
			}},
			StartTime: timestamppb.New(mustTime(t, "2026-08-15T09:41:02Z")),
		},
		v1.Catalog(),
	} {
		t.Run(string(message.ProtoReflect().Descriptor().Name()), func(t *testing.T) {
			expected, err := marshalJSON(message, false)
			require.NoError(t, err)

			rendered, err := marshalRunDocument(message, false, false)
			require.NoError(t, err)

			assert.JSONEq(t, string(expected), string(rendered),
				"a document with nothing to project came out different from protojson, so "+
					"the rendering is re-deriving something it should be passing through")
			assert.Equal(t, compactJSON(t, expected), compactJSON(t, rendered),
				"the document parses the same but is not the same bytes: a key moved, which "+
					"protojson's field order decides and this must not")
		})
	}
}

// TestTheRunDocumentSpeaksTheLanguageOfTheFile is the change, stated as the paths a
// caller writes.
//
// One assertion per documented path, against the document `flow run local | jq`
// receives, because those paths are the contract the help makes and this is where
// it is kept.
func TestTheRunDocumentSpeaksTheLanguageOfTheFile(t *testing.T) {
	t.Parallel()

	rendered, err := marshalRunDocument(richRunOutputs(), false, false)
	require.NoError(t, err)

	document := decodeDocument(t, rendered)

	steps, ok := document["steps"].(map[string]any)
	require.True(t, ok, "`.steps` is the transcript, spelled the way the file spells it: %s", rendered)

	greet, ok := steps["greet"].(map[string]any)
	require.True(t, ok, "`.steps.greet` is the step, addressed by the id the file gave it")

	assert.Equal(t, "hello", greet["result"],
		"`.steps.greet.result` is what `${steps.greet.result}` reads, and it is the value "+
			"itself rather than a tagged union")

	outputs, ok := document["runOutputs"].(map[string]any)
	require.True(t, ok, "`.runOutputs` is what the workflow declared it would report")

	assert.Equal(t, json.Number("3"), outputs["count"],
		"a declared int output is a JSON number, so `.runOutputs.count == 3` is the "+
			"expression somebody writes")
	assert.Equal(t, "https://example.com/a?b=1&c=2", outputs["url"],
		"an ampersand in a URL was HTML-escaped, which protojson does not do")
	assert.Equal(t, []any{json.Number("1"), "two"}, outputs["mixed"],
		"a list output is a JSON list")
	assert.Equal(t, map[string]any{"ok": true}, outputs["nested"],
		"a map output is a JSON object rather than a list of key/value entries")
}

// TestTheRunDocumentKeepsAValueItCannotSpellPlainly is the honest half of rule 2.
//
// A [v1.Value] is a oneof and only one arm of it is a value. An error is *about* a
// value, and flattening it would make a failure indistinguishable from a map
// somebody computed — so it keeps the schema's own spelling, where the arm names
// itself. The same applies to anything JSON cannot hold: a NaN has no spelling, and
// protojson's `"NaN"` is a better answer than a guess.
func TestTheRunDocumentKeepsAValueItCannotSpellPlainly(t *testing.T) {
	t.Parallel()

	outputs := &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			"step": {NamedValues: map[string]*v1.Value{
				"failed": {Kind: &v1.Value_Error_{Error: &v1.Value_Error{
					Message: "no",
					Code:    v1.Value_Error_CODE_INTERNAL,
				}}},
				"unspellable": {Kind: &v1.Value_Literal{Literal: &expr.Value{
					Kind: &expr.Value_DoubleValue{DoubleValue: math.NaN()},
				}}},
			}},
		},
	}

	rendered, err := marshalRunDocument(outputs, false, false)
	require.NoError(t, err)

	document := decodeDocument(t, rendered)
	values := document["steps"].(map[string]any)["step"].(map[string]any)

	failure, ok := values["failed"].(map[string]any)
	require.True(t, ok, "an error value was flattened into something a reader cannot tell "+
		"from a computed map: %s", rendered)
	assert.Contains(t, failure, "error", "the arm no longer names itself")

	unspellable, ok := values["unspellable"].(map[string]any)
	require.True(t, ok, "a NaN was written as a JSON number, which no parser accepts: %s", rendered)
	assert.Contains(t, unspellable, "literal",
		"a value JSON cannot hold must keep protojson's spelling rather than be guessed at")
}

// TestRawWritesTheSchemasOwnDocument is the escape hatch, and the promise that the
// wire shape is still reachable from every verb that renders one.
//
// Compared against [marshalJSON] rather than against a literal, because the promise
// is "exactly what every other document this CLI writes says", not "this shape,
// today".
func TestRawWritesTheSchemasOwnDocument(t *testing.T) {
	t.Parallel()

	outputs := richRunOutputs()

	expected, err := marshalJSON(outputs, false)
	require.NoError(t, err)

	raw, err := marshalRunDocument(outputs, false, true)
	require.NoError(t, err)

	assert.Equal(t, string(expected), string(raw),
		"--raw is not the schema's own protojson, so a consumer generated against "+
			"flowstate.v1 has nowhere to go")

	assert.Contains(t, string(raw), "stepValues", "the schema's own nouns are gone from --raw")
	assert.Contains(t, string(raw), "namedValues", "the schema's own nouns are gone from --raw")
}

// TestARunDocumentIsStableAcrossRuns is #544's requirement for the machine surface,
// and the one place it disagrees with #328.
//
// #328 asked for empty maps and nulls to be elided. They are not, and must not be:
// a `jq` expression that resolves against one run has to resolve against the next,
// and a workflow that declared no outputs is a stable answer to a stable question
// where a missing key is a second question. The eliding belongs to the human
// surface, which is a different stream.
func TestARunDocumentIsStableAcrossRuns(t *testing.T) {
	t.Parallel()

	rendered, err := marshalRunDocument(&v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{"quiet": {}},
	}, false, false)
	require.NoError(t, err)

	document := decodeDocument(t, rendered)

	require.Contains(t, document, "runOutputs",
		"a workflow that declares no outputs dropped the key rather than answering null, "+
			"so `.runOutputs` resolves on some runs and not others: %s", rendered)
	assert.Nil(t, document["runOutputs"])

	steps, ok := document["steps"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, map[string]any{}, steps["quiet"],
		"a step that produced nothing was dropped rather than named with nothing under it")
}

// richRunOutputs is a finished run's transcript and answer, with one of every value
// shape the rendering has an opinion about.
func richRunOutputs() *v1.Workflow_StepOutputs {
	return &v1.Workflow_StepOutputs{
		StepValues: map[string]*v1.Node_Outputs{
			"greet": {NamedValues: map[string]*v1.Value{
				"result": v1.NewLiteral("hello"),
			}},
			"quiet": {},
		},
		RunOutputs: &v1.RunOutputs{Values: map[string]*v1.Value{
			"count":  v1.NewLiteral(int64(3)),
			"url":    v1.NewLiteral("https://example.com/a?b=1&c=2"),
			"mixed":  v1.NewLiteralList(int64(1), "two"),
			"nested": v1.NewLiteralMap(map[string]any{"ok": true}),
		}},
	}
}

// decodeDocument reads a rendered document the way a caller would, keeping numbers
// exactly as they were written so `3` and `3.0` stay distinguishable.
func decodeDocument(tb testing.TB, rendered []byte) map[string]any {
	tb.Helper()

	decoder := json.NewDecoder(strings.NewReader(string(rendered)))
	decoder.UseNumber()

	var document map[string]any
	require.NoError(tb, decoder.Decode(&document), "the document did not decode: %s", rendered)

	return document
}

// compactJSON removes protojson's per-binary randomized whitespace, which is not a
// difference in the document. See [marshalJSON].
func compactJSON(tb testing.TB, encoded []byte) string {
	tb.Helper()

	var buffer bytes.Buffer
	require.NoError(tb, json.Compact(&buffer, encoded))

	return buffer.String()
}

// walkMessages visits every message in a file, nested ones included.
func walkMessages(messages protoreflect.MessageDescriptors, visit func(protoreflect.MessageDescriptor)) {
	for i := range messages.Len() {
		message := messages.Get(i)
		if message.IsMapEntry() {
			continue
		}

		visit(message)
		walkMessages(message.Messages(), visit)
	}
}

// renderedName is the JSON key a field is written under, rename included.
func renderedName(field protoreflect.FieldDescriptor) string {
	if renamed, ok := runDocumentNames[field.FullName()]; ok {
		return renamed
	}

	return field.JSONName()
}
