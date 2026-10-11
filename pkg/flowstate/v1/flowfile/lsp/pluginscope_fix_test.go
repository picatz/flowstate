package lsp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const shapingFixSource = `edition: v2026.4
name: shaping
steps:
  - id: call
    alpha.run:
      url: https://example.com
      outputs: '${ {"ref": response.json.ref} }'
`

// The migration a code action offers is the document's own lock's answer: the
// same text where the lock's alpha.run shapes its outputs is rewritten into the
// mapping form, and where another lock's alpha.run does not, it is left alone.
func TestACodeActionFixDependsOnTheDocumentsOwnLock(t *testing.T) {
	t.Parallel()

	shaping, ok := v1.LookupTask("http")
	require.True(t, ok)
	require.True(t, shaping.ShapesOutputs)
	shaping.Name, shaping.Summary = "alpha.run", "Task alpha.run."

	l := newLocks(t)
	l.set("a", "1")
	l.set("b", "1")
	l.by["a"].Tasks = []v1.TaskDef{shaping}
	l.by["b"].Tasks = []v1.TaskDef{lockTaskLike(t, "alpha.run", "http")}
	c := newClientFor(t, &FlowfileServer{Logger: discardLogger(), PluginCatalog: l.hook})
	c.initialize()

	rewritten := func(repo string) bool {
		uri := uriIn(repo)
		c.open(uri, shapingFixSource)
		for _, action := range c.codeAction(uri, wholeOf(shapingFixSource), nil, nil) {
			if action.Edit == nil {
				continue
			}
			for _, edit := range action.Edit.Changes[uri] {
				if strings.Contains(edit.NewText, "ref: ${response.json.ref}") {
					return true
				}
			}
		}

		return false
	}
	assert.True(t, rewritten("a"), "the document under the shaping lock was not migrated")
	assert.False(t, rewritten("b"), "the document under the other lock was migrated as if it shaped")
}
