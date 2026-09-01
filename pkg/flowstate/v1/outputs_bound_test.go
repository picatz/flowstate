package flowstatev1_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// maxRefusalBytes is the ceiling the tests below hold a run-failure sentence to.
//
// Not derived from `truncateForError`'s own constant, deliberately: that value
// is unexported, and this is the claim a reader of the failure actually cares
// about — the sentence stays a sentence, whatever the trim is set to. Generous
// enough that the declared set, the predicate and the trimmed value all fit, and
// orders of magnitude below [v1.MaxTaskOutputBytes], which is what a task may
// legitimately answer with and therefore what an untrimmed sentence would carry.
const maxRefusalBytes = 2048

// TestEvalRunOutputsBoundsARejectedValueInItsRefusal is the regression for the
// P1 review finding on 5aafa67e.
//
// A declared output's value is whatever a task answered with, up to
// [v1.MaxTaskOutputBytes]. Both refusals rendered it whole, and the enum one
// rendered it through [strconv.Quote], which expands a control byte to six
// characters — so a task-controlled result could produce a failure several times
// its own size. That is invariant 5 (unbounded work at a seam another party
// controls) and invariant 3 at once: Temporal has a blob limit, so a sentence
// too large to persist fails the durable driver where the local driver simply
// returns it.
//
// The value is built from a control character on purpose, so this fails against
// a quote-then-trim ordering as well as against no trim at all.
func TestEvalRunOutputsBoundsARejectedValueInItsRefusal(t *testing.T) {
	t.Parallel()

	// Twice what a task may answer with, which is the size this sentence must
	// not be able to reach even before Quote's expansion.
	huge := strings.Repeat("\x01", 2*v1.MaxTaskOutputBytes)

	for _, test := range []struct {
		name        string
		declaration *v1.OutputDeclaration
		contains    []string
	}{
		{
			name: "an enum output outside its declared set",
			declaration: &v1.OutputDeclaration{
				Name:   "channel",
				Type:   v1.InputDeclaration_TYPE_ENUM,
				Values: []string{"stable", "beta"},
			},
			// The output and the declared set survive the trim: they are what
			// the author needs, and neither is sized by the run.
			contains: []string{`output "channel"`, `"stable", "beta"`},
		},
		{
			name: "an output that fails its own must",
			declaration: &v1.OutputDeclaration{
				Name: "channel",
				Must: strPtr(`this == "expected"`),
			},
			contains: []string{`output "channel"`, "must satisfy"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			test.declaration.Value = v1.NewLiteral(huge)
			wf := &v1.Workflow{Name: "wf", DeclaredOutputs: []*v1.OutputDeclaration{test.declaration}}

			_, err := v1.EvalRunOutputs(t.Context(), wf, v1.NewScope("", &v1.Workflow_StepOutputs{}))
			require.Error(t, err)

			assert.Less(t, len(err.Error()), maxRefusalBytes,
				"a refusal about a task-sized value must be bounded before it becomes the run's failure")
			for _, want := range test.contains {
				assert.Contains(t, err.Error(), want,
					"the trim must keep what the file declared, which is what an author reads")
			}

			// A failure travels in a proto3 string field, which will not hold
			// invalid UTF-8, so the trim has to cut at a rune boundary.
			assert.True(t, utf8.ValidString(err.Error()),
				"the trimmed refusal is not valid UTF-8, so it cannot be persisted or marshalled")
		})
	}
}

// TestARejectedValueIsTrimmedAtARuneBoundary is the multi-byte direction of the
// bound above, which a value of single-byte control characters cannot reach.
//
// A task answers with whatever it answers with, and a cut taken at a byte offset
// lands mid-sequence for any value that is not ASCII. The result is invalid
// UTF-8 in a proto3 string, which fails the whole response's marshalling rather
// than shortening one sentence.
func TestARejectedValueIsTrimmedAtARuneBoundary(t *testing.T) {
	t.Parallel()

	// Three bytes per rune, so the cut lands inside one.
	wf := &v1.Workflow{
		Name: "wf",
		DeclaredOutputs: []*v1.OutputDeclaration{
			{
				Name:   "channel",
				Value:  v1.NewLiteral(strings.Repeat("世", 4096)),
				Type:   v1.InputDeclaration_TYPE_ENUM,
				Values: []string{"stable", "beta"},
			},
		},
	}

	_, err := v1.EvalRunOutputs(t.Context(), wf, v1.NewScope("", &v1.Workflow_StepOutputs{}))
	require.Error(t, err)
	assert.Less(t, len(err.Error()), maxRefusalBytes)
	assert.True(t, utf8.ValidString(err.Error()),
		"the value was cut through a multi-byte sequence, producing invalid UTF-8")
}
