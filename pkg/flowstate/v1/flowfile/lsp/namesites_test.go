package lsp

import (
	"strings"
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const namesSource = `edition: v2026.4
name: names
vars:
  greeting: hello
  other: ${vars.greeting + "!"}
steps:
  - id: each
    for_each:
      items: ${[1, 2]}
      as: n
      steps:
        - id: say
          log:
            message: ${vars.greeting} ${string(n)} ${[1].map(n, n + 1)}
        - id: later
          log:
            message: ${string(n) + vars.other}
  - id: outside
    log:
      message: ${vars.greeting}
`

func renameTo(t *testing.T, src, at string, off int, name string) (string, error) {
	t.Helper()
	doc := refsDoc(t, src)
	edit, err := renameAt(doc, positionOf(t, src, at, off), name)
	if err != nil {
		return "", err
	}
	return applyEdits(t, src, edit.Changes[string(doc.uri)]), nil
}

func TestRenameLoopIteratorMovesEveryRead(t *testing.T) {
	t.Parallel()

	// From the declaration and from a read: the same edit.
	for _, tc := range []struct {
		at  string
		off int
	}{{"as: n", len("as: ")}, {"${string(n)} ${[1]", len("${string(")}} {
		got, err := renameTo(t, namesSource, tc.at, tc.off, "item2")
		require.NoError(t, err, tc.at)
		assert.Contains(t, got, "as: item2")
		assert.Contains(t, got, "${string(item2)} ${[1].map(n, n + 1)}", "the comprehension's own n is a different binding")
		assert.Contains(t, got, "${string(item2) + vars.other}")
		assert.NotContains(t, got, "string(n)")
	}
}

func TestRenameVarMovesEveryRead(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		at  string
		off int
	}{{"greeting: hello", 0}, {"${vars.greeting} ${string", len("${vars.")}} {
		got, err := renameTo(t, namesSource, tc.at, tc.off, "salute")
		require.NoError(t, err, tc.at)
		assert.Contains(t, got, "  salute: hello")
		assert.Equal(t, 3, strings.Count(got, "vars.salute"), "the two step reads and the var that reads it")
		assert.NotContains(t, got, "vars.greeting")
	}
}

func TestRenameNameRefusals(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		at, to, want string
		off          int
	}{
		"not an identifier":       {"as: n", "1x", "not a valid", len("as: ")},
		"a root":                  {"as: n", "inputs", "", len("as: ")},
		"collides with a var":     {"greeting: hello", "other", "already named", 0},
		"var not an identifier":   {"greeting: hello", "a-b", "not a valid", 0},
		"another loop binds that": {"as: n", "n", "", len("as: ")}, // same name is a no-op, checked below
	} {
		_, err := renameTo(t, namesSource, tc.at, tc.off, tc.to)
		if name == "another loop binds that" {
			assert.NoError(t, err, name)
			continue
		}
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), tc.want, name)
	}
}

func TestRenameIteratorRefusesWhenAnotherLoopInScopeBindsTheName(t *testing.T) {
	t.Parallel()
	src := `edition: v2026.4
name: nested
steps:
  - id: outer
    for_each:
      items: ${[1]}
      as: a
      steps:
        - id: inner
          for_each:
            items: ${[2]}
            as: b
            steps:
              - id: use
                log:
                  message: ${string(a)}
`
	_, err := renameTo(t, src, "as: a", len("as: "), "b")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already binds")
}

func TestReferencesAndHighlightsOfAVarAndIterator(t *testing.T) {
	t.Parallel()
	doc := refsDoc(t, namesSource)

	refs := referencesAt(doc, positionOf(t, namesSource, "greeting: hello", 0), true)
	assert.Len(t, refs, 4, "the key, the var reading it, and two steps")
	refs = referencesAt(doc, positionOf(t, namesSource, "greeting: hello", 0), false)
	assert.Len(t, refs, 3)

	hl := highlightsAt(doc, positionOf(t, namesSource, "as: n", len("as: ")))
	var writes, reads int
	for _, h := range hl {
		if h.Kind == lsp.Write {
			writes++
		} else {
			reads++
		}
	}
	assert.Equal(t, 1, writes)
	assert.Equal(t, 2, reads, "string(n) in each of the two body steps, not the comprehension's n")
}

func TestPrepareRenameOfAnImplicitIteratorDeclines(t *testing.T) {
	t.Parallel()
	src := "edition: v2026.4\nname: d\nsteps:\n  - id: e\n    for_each:\n      items: ${[1]}\n      steps:\n        - id: s\n          log:\n            message: ${string(item)}\n"
	doc := refsDoc(t, src)
	_, _, ok := prepareRenameAt(doc, positionOf(t, src, "string(item)", len("string(")))
	assert.False(t, ok, "`item` is the default; no written declaration to rename")
	assert.NotEmpty(t, referencesAt(doc, positionOf(t, src, "string(item)", len("string(")), true), "references still list the read")
}

func TestRenameVarRefusesAReadItCannotSee(t *testing.T) {
	t.Parallel()
	// A fence in a field the model does not walk: renaming past it would leave a
	// stale read, so the rename is refused rather than partial.
	src := strings.Replace(namesSource, "name: names\n", "name: names\ndescription: says ${vars.greeting}\n", 1)
	_, err := renameTo(t, src, "greeting: hello", 0, "salute")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not track")
}

func TestRenameVarRefusesFormsItCannotAttribute(t *testing.T) {
	t.Parallel()
	for name, read := range map[string]string{
		"indexed":        `vars["greeting"]`,
		"spaced index":   `vars[ "greeting" ]`,
		"computed index": `vars[string("greeting")]`,
		"bare":           `size(vars)`,
		"method on vars": `vars.size()`,
	} {
		src := strings.Replace(namesSource, "name: names\n", "name: names\noutputs:\n  o:\n    value: ${"+read+"}\n", 1)
		_, err := renameTo(t, src, "greeting: hello", 0, "salute")
		require.Error(t, err, name)
	}
}

func TestRenameIteratorRefusesACaptureByABinder(t *testing.T) {
	t.Parallel()
	src := `edition: v2026.4
name: capture
steps:
  - id: each
    for_each:
      items: ${[1]}
      as: n
      steps:
        - id: say
          log:
            message: ${string([1, 2].exists(i, i == n))}
`
	_, err := renameTo(t, src, "as: n", len("as: "), "i")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already written")

	got, err := renameTo(t, src, "as: n", len("as: "), "j")
	require.NoError(t, err)
	assert.Contains(t, got, "i == j")
}

func TestRenameVarFollowsAnOptionalSelection(t *testing.T) {
	t.Parallel()
	src := strings.Replace(namesSource, "name: names\n", "name: names\noutputs:\n  o:\n    value: ${vars.?greeting.orValue(\"z\")}\n", 1)
	got, err := renameTo(t, src, "greeting: hello", 0, "salute")
	require.NoError(t, err)
	assert.Contains(t, got, `vars.?salute.orValue`)
	assert.NotContains(t, got, "greeting")
}
