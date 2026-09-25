package reference

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/protodoc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSourceInfoBehaviorIsExplicit(t *testing.T) {
	linked := (&flowstatev1.GetRequest{}).ProtoReflect().Descriptor()
	_, linkedHasComment := protodoc.CommentOf(linked)
	assert.False(t, linkedHasComment, "linked generated descriptors unexpectedly retained SourceCodeInfo")

	comment, sourceHasComment := protodoc.Comment(linked.FullName())
	assert.True(t, sourceHasComment)
	assert.Contains(t, comment, "request message for getting a workflow run")
	_, fieldHasComment := protodoc.Comment(linked.FullName().Append("run_id"))
	assert.False(t, fieldHasComment, "the pilot must not invent schema field prose")
}

func TestGeneratedReferenceIsCurrentAndHasNoInventedProse(t *testing.T) {
	var docs bytes.Buffer
	require.NoError(t, GenerateGet(&docs))
	committedDocs, err := os.ReadFile("../testdata/get-fields.md")
	require.NoError(t, err)
	assert.Equal(t, string(committedDocs), docs.String())
	assert.Contains(t, docs.String(), "UUID")
	assert.Contains(t, docs.String(), "Command-owned usage")
	assert.Equal(t, 2, strings.Count(docs.String(), " | — | "))
}

func TestPresenceLabelSeparatesRequirednessFromProtobufPresence(t *testing.T) {
	request := (&flowstatev1.GetRequest{}).ProtoReflect().Descriptor()
	assert.Equal(t, "required", presenceLabel(request.Fields().ByName("workflow_id")))
	assert.Equal(t, "optional; unset stays absent", presenceLabel(request.Fields().ByName("run_id")))

	response := (&flowstatev1.GetResponse{}).ProtoReflect().Descriptor()
	assert.Equal(t, "optional; unset uses the scalar zero value", presenceLabel(response.Fields().ByName("workflow_id")),
		"a plain proto3 scalar is not required merely because it lacks presence")
}

func TestReferencePropagatesWriterFailure(t *testing.T) {
	want := errors.New("disk full")
	err := GenerateGet(failingWriter{err: want})
	require.ErrorIs(t, err, want)
}

func TestEscapeTablePreservesRegexAlternationInOneCell(t *testing.T) {
	assert.Equal(t, `matching ^(GET\|POST)$`, escapeTable(`matching ^(GET|POST)$`))
}

func BenchmarkGeneration(b *testing.B) {
	for b.Loop() {
		var out bytes.Buffer
		if err := GenerateGet(&out); err != nil {
			b.Fatal(err)
		}
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }
