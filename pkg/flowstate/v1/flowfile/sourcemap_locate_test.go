package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

const locateFlowfile = `edition: v2026.4
name: main
steps:
  - id: pages
    for_each:
      items: ${[1, 2]}
      as: n
      steps:
        - id: page
          log:
            message: one
`

// locateFixture compiles locateFlowfile and maps it, the shape a producer other
// than the caller hands over.
func locateFixture(t *testing.T) (*v1.Workflow, *v1.DebugSourceMap) {
	t.Helper()

	workflow, positions, err := flowfile.ParseAt([]byte(locateFlowfile), "/work/main.yaml")
	require.NoError(t, err)

	return workflow, flowfile.SourceMap("/work/main.yaml", []byte(locateFlowfile), workflow, positions)
}

func TestLocatePlacesADiagnosticWhereTheMapSaysItsStepIs(t *testing.T) {
	t.Parallel()

	workflow, sourceMap := locateFixture(t)
	ds := flowfile.Diagnostics{
		{Step: "page", Path: []string{"pages", "page"}, Message: "found here"},
		{Step: "page", Path: []string{"pages", "nowhere"}, Message: "the map does not locate this"},
		{Message: "names no step"},
		{Step: "page", Path: []string{"pages", "page"}, Line: 3, Column: 1, Message: "already placed"},
	}

	placed, err := ds.Locate(workflow, sourceMap, nil)
	require.NoError(t, err)

	assert.Equal(t, 1, placed)
	assert.Equal(t, "/work/main.yaml", ds[0].File)
	assert.Equal(t, 9, ds[0].Line, "the line is the step's, not a guessed one")
	assert.Positive(t, ds[0].Column)
	assert.Zero(t, ds[1].Line, "a step the map does not locate keeps only its address")
	assert.Empty(t, ds[1].File)
	assert.Empty(t, ds[2].File)
	assert.Equal(t, 3, ds[3].Line, "a diagnostic with a position of its own is not moved")
}

func TestLocateRefusesAMapOfAnotherProgram(t *testing.T) {
	t.Parallel()

	workflow, sourceMap := locateFixture(t)
	other := proto.CloneOf(workflow)
	other.Description = proto.String("edited after the map was made")
	ds := flowfile.Diagnostics{{Step: "page", Path: []string{"pages", "page"}, Message: "x"}}

	placed, err := ds.Locate(other, sourceMap, nil)

	require.ErrorIs(t, err, flowfile.ErrSourceMapMismatch)
	assert.Zero(t, placed)
	assert.Zero(t, ds[0].Line, "a stale map must not move anything")
}

func TestLocateWithholdsADocumentThatChanged(t *testing.T) {
	t.Parallel()

	workflow, sourceMap := locateFixture(t)
	ds := flowfile.Diagnostics{{Step: "page", Path: []string{"pages", "page"}, Message: "x"}}

	var asked int
	placed, err := ds.Locate(workflow, sourceMap, func(document *v1.DebugSourceDocument) bool {
		asked++
		assert.Equal(t, "/work/main.yaml", document.GetUri())
		return true
	})

	require.NoError(t, err)
	assert.Zero(t, placed)
	assert.Zero(t, ds[0].Line, "the source changed, so the line is withheld rather than shifted")
	assert.Empty(t, ds[0].File)
	assert.Equal(t, 1, asked)
}

func TestLocateAsksOncePerDocument(t *testing.T) {
	t.Parallel()

	workflow, sourceMap := locateFixture(t)
	ds := flowfile.Diagnostics{
		{Step: "page", Path: []string{"pages", "page"}, Message: "a"},
		{Step: "pages", Path: []string{"pages"}, Message: "b"},
	}

	var asked int
	placed, err := ds.Locate(workflow, sourceMap, func(*v1.DebugSourceDocument) bool { asked++; return false })

	require.NoError(t, err)
	assert.Equal(t, 2, placed)
	assert.Equal(t, 1, asked)
}

// A frontend with no text, a canvas, is a producer too: its locator is a node id,
// the diagnostic gets the document and keeps its address, and no line is made up.
func TestLocateSourceFreeProducer(t *testing.T) {
	t.Parallel()

	workflow := &v1.Workflow{Name: "canvas", Steps: []*v1.Node{{Id: "greet"}}}
	sourceMap := &v1.DebugSourceMap{
		IrDigest:  flowfile.IRDigest(workflow),
		Documents: []*v1.DebugSourceDocument{{Uri: "canvas://board-7", Language: "canvas"}},
		Entries: []*v1.DebugSourceEntry{{
			Site:     &v1.DebugSite{Workflow: "canvas", Path: []string{"greet"}},
			Location: &v1.DebugSourceLocation{NodeId: "node-3"},
		}},
	}
	ds := flowfile.Diagnostics{{Step: "greet", Path: []string{"greet"}, Message: "x"}}

	placed, err := ds.Locate(workflow, sourceMap, nil)

	require.NoError(t, err)
	assert.Equal(t, 1, placed)
	assert.Equal(t, "canvas://board-7", ds[0].File)
	assert.Zero(t, ds[0].Line)
	assert.Equal(t, []string{"greet"}, ds[0].Path)
}

func TestLocateBoundsWhatTheMapAuthorChooses(t *testing.T) {
	t.Parallel()

	workflow := &v1.Workflow{Name: "w", Steps: []*v1.Node{{Id: "a"}}}
	entry := func(uri string) *v1.DebugSourceMap {
		return &v1.DebugSourceMap{
			IrDigest:  flowfile.IRDigest(workflow),
			Documents: []*v1.DebugSourceDocument{{Uri: uri, Digest: "sha256:00"}},
			Entries: []*v1.DebugSourceEntry{{
				Site:     &v1.DebugSite{Workflow: "w", Path: []string{"a"}},
				Location: &v1.DebugSourceLocation{Range: &v1.SourceRange{StartLine: 2, StartColumn: 3}},
			}},
		}
	}

	ds := flowfile.Diagnostics{{Step: "a", Path: []string{"a"}, Message: "x"}}
	_, err := ds.Locate(workflow, entry(strings.Repeat("a", 5000)), nil)
	require.Error(t, err, "a document name over the schema's bound is refused")
	assert.NotErrorIs(t, err, flowfile.ErrSourceMapMismatch)

	// A name that passes the schema still reaches a terminal or a log: control
	// characters, which can rewrite what a reader sees, are replaced.
	ds = flowfile.Diagnostics{{Step: "a", Path: []string{"a"}, Message: "x"}}
	_, err = ds.Locate(workflow, entry("a\x1b[2Jb\nc"), nil)
	require.NoError(t, err)
	assert.Equal(t, "a?[2Jb?c", ds[0].File)
	assert.Equal(t, 2, ds[0].Line)

	ds = flowfile.Diagnostics{{Step: "a", Path: []string{"a"}, Message: "x"}}
	_, err = ds.Locate(workflow, entry(strings.Repeat("é", 400)), nil)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(ds[0].File), 512)
	assert.True(t, strings.HasSuffix(ds[0].File, "é"), "cut on a rune boundary")
}

func TestLocateWithholdsALineInADocumentWithNoDigest(t *testing.T) {
	t.Parallel()

	workflow, sourceMap := locateFixture(t)
	sourceMap.Documents[0].Digest = ""
	ds := flowfile.Diagnostics{{Step: "page", Path: []string{"pages", "page"}, Message: "x"}}

	placed, err := ds.Locate(workflow, sourceMap, nil)

	require.NoError(t, err)
	assert.Zero(t, placed, "a line bound to no bytes could be any version of the text")
	assert.Zero(t, ds[0].Line)
}
