package flowdebug_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

func TestAValueThatFitsALineIsItsCompactJSON(t *testing.T) {
	t.Parallel()

	// Fixed bytes, written out by hand: the compact form is the JSON every other
	// surface hands an author, so a change to it must be a change to this table.
	for name, tc := range map[string]struct {
		value any
		want  string
	}{
		"scalar":  {"hello", `"hello"`},
		"number":  {42, `42`},
		"record":  {map[string]any{"b": 2, "a": []any{1, 2}}, `{"a":[1,2],"b":2}`},
		"empty":   {map[string]any{}, `{}`},
		"escaped": {"line\nbreak \"quoted\" <tag>", `"line\nbreak \"quoted\" \u003ctag\u003e"`},
		"null":    {nil, `null`},
	} {
		assert.Equal(t, tc.want, flowdebug.RenderValue(tc.value, flowdebug.Layout{}), name)
	}
}

func TestALargeValueIsATree(t *testing.T) {
	t.Parallel()

	value := map[string]any{
		"total": 40,
		"customer": map[string]any{
			"name":  "ada",
			"notes": "a long note that does not fit",
			"tags":  []any{"a", "b"},
		},
		"lines": []any{1, map[string]any{"sku": "x-1", "qty": 2}, []any{}},
		"empty": map[string]any{},
	}

	assert.Equal(t, `customer:
  name: "ada"
  notes: "a long note that does not fit"
  tags:
    [0] "a"
    [1] "b"
empty: {}
lines:
  [0] 1
  [1]
    qty: 2
    sku: "x-1"
  [2] []
total: 40`, flowdebug.RenderValue(value, flowdebug.Layout{Width: 40}))
}

func TestATreeSaysWhatItLeftOut(t *testing.T) {
	t.Parallel()

	keys := map[string]any{}
	items := make([]any, 0, 10)
	for i := range 10 {
		keys[fmt.Sprintf("k%02d", i)] = i
		items = append(items, i)
	}
	deep := map[string]any{"a": map[string]any{"b": map[string]any{"c": map[string]any{"d": 1, "e": 2}}}}

	got := flowdebug.RenderValue(map[string]any{"keys": keys, "items": items}, flowdebug.Layout{Width: 10, MaxChildren: 3})
	assert.Contains(t, got, "  … 7 more keys")
	assert.Contains(t, got, "  … 7 more items")
	assert.Contains(t, got, "  k00: 0\n  k01: 1\n  k02: 2\n")

	assert.Equal(t, "a:\n  b:\n    c: {… 2 keys}",
		flowdebug.RenderValue(deep, flowdebug.Layout{Width: 10, Depth: 3}))

	one := flowdebug.RenderValue(map[string]any{"xs": []any{1, 2}}, flowdebug.Layout{Width: 5, MaxChildren: 1})
	assert.Contains(t, one, "… 1 more item")
}

func TestATreeNeverWritesAControlCharacterAsItself(t *testing.T) {
	t.Parallel()

	value := map[string]any{
		"\x1b[2Jkey":  "line\nbreak \x1b[31mred\x07",
		"fine_key-1.": strings.Repeat("x", 120),
	}

	got := flowdebug.RenderValue(value, flowdebug.Layout{})
	assert.NotContains(t, got, "\x1b")
	assert.NotContains(t, got, "\x07")
	assert.Equal(t, 2, strings.Count(got, "\n")+1, "a newline in data became a line of the layout")
	assert.Contains(t, got, `"\x1b[2Jkey":`)
	assert.Contains(t, got, "fine_key-1.:")
}

// TestWhatTheLayoutLeavesOutDoesNotDependOnWhatIsLeftOut: the elision is read
// off the tree's shape, so a secret beneath a cut says nothing about itself
// through how much room it took, and its text is not in the output.
func TestWhatTheLayoutLeavesOutDoesNotDependOnWhatIsLeftOut(t *testing.T) {
	t.Parallel()

	render := func(secret string) string {
		return flowdebug.RenderValue(map[string]any{
			"a": map[string]any{"b": map[string]any{"c": map[string]any{"token": secret, "other": 1}}},
			"z": strings.Repeat("y", 200),
		}, flowdebug.Layout{Depth: 3})
	}

	short, long := render("s3"), render(strings.Repeat("s3cr3t-", 500))
	assert.Equal(t, short, long)
	assert.NotContains(t, long, "s3cr3t")
	assert.Contains(t, long, "{… 2 keys}")
}

// TestInspectLaysOutAValueThatDoesNotFitALine: at the prompt a small answer is
// the one line it always was, and a large one is a tree rather than a wall.
func TestInspectLaysOutAValueThatDoesNotFitALine(t *testing.T) {
	t.Parallel()

	_, out, _, err := runDebuggedSession(t, "inspect 1 + 1\ninspect {'name': 'ada', 'orders': [{'sku': 'x-1', 'qty': 2, 'note': 'a long note to push this past a line'}, {'sku': 'y-2', 'qty': 1, 'note': 'and another long note for the second order'}]}\ncontinue\n", flowdebug.Options{})
	require.NoError(t, err)

	assert.Contains(t, out, "debug> 2\ndebug> ", "a scalar is one line")
	assert.Contains(t, out, "debug> name: \"ada\"\norders:\n  [0]\n    note: \"a long note to push this past a line\"\n    qty: 2\n    sku: \"x-1\"\n  [1]\n")
}

// TestATreeIsLaidOutAfterTheRedactor: a secret nested in a value too large for a
// line is withheld in the tree exactly as it is on one line.
func TestATreeIsLaidOutAfterTheRedactor(t *testing.T) {
	t.Parallel()

	const secret = "hunter2-correct-horse"
	var console strings.Builder
	session, err := flowdebug.New(flowdebug.Options{In: strings.NewReader("inspect {'user': 'ada', 'auth': {'token': '" + secret + "', 'scope': 'read'}, 'note': 'padding to push this value past one line of output width'}\ncontinue\n"), Out: &console})
	require.NoError(t, err)
	session.SetRedactor(func(text string) string { return strings.ReplaceAll(text, secret, "[redacted]") })

	ctx := v1.NewContextWithRegistry(t.Context(), debugRegistry(t, &ranSteps{}))
	ctx = v1.NewContextWithDebugger(ctx, session)
	ctx = v1.NewContextWithRunObserver(ctx, session)
	_, err = v1.Run(ctx, &v1.Workflow{Name: "debugged", Steps: []*v1.Node{markStep("build")}})
	require.NoError(t, err)

	out := console.String()
	assert.NotContains(t, out, secret)
	assert.Contains(t, out, "auth:\n  scope: \"read\"\n  token: \"[redacted]\"\n")
}
