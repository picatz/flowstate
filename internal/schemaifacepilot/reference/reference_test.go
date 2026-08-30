package reference

import (
	"bytes"
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

func BenchmarkGeneration(b *testing.B) {
	for b.Loop() {
		var out bytes.Buffer
		if err := GenerateGet(&out); err != nil {
			b.Fatal(err)
		}
	}
}
