package lsp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const lockFeaturesSource = `edition: v2026.4
name: lock-features
steps:
  - id: pre
    alpha.run:
      url: https://example.com
  - id: fan
    for_each:
      items: ${[steps.pre.status_code]}
      as: n
      steps:
        - id: greet
          alpha.run:
            url: https://example.com
        - id: shout
          log:
            message: ${n}
  - id: show
    log:
      message: ${string(steps.fan.results.filter(r, r.greet.status_code == 200).size())}
`

// The three readers that reach the registry through flowfile helpers rather than
// the compiler proper (a loop item's type in hover, in inlay hints, and a
// results entry's outputs) see their own lock's tasks, and not another
// document's.
func TestLoopTypesAndResultEntriesSeeOnlyTheirOwnLocksTasks(t *testing.T) {
	t.Parallel()

	l := newLocks(t)
	l.set("a", "1")
	l.by["a"].Tasks = []v1.TaskDef{lockTaskLike(t, "alpha.run", "http")}
	c := newClientFor(t, &FlowfileServer{Logger: discardLogger(), PluginCatalog: l.hook})
	c.initialize()

	features := func(uri string) (item, entry, hints string) {
		doc, ok := c.rawServerDoc(uri)
		require.True(t, ok)
		for _, h := range inlayHints(doc, wholeDocument()) {
			hints += h.Label + "\n"
		}
		p := positionOf(t, lockFeaturesSource, "${n}", 2)
		item = hoverText(c.hover(uri, p.Line, p.Character))
		p = positionOf(t, lockFeaturesSource, "r.greet.status_code", 0)
		entry = hoverText(c.hover(uri, p.Line, p.Character))

		return item, entry, hints
	}

	own := uriIn("a")
	got := c.open(own, lockFeaturesSource)
	require.False(t, unknownTask(got, "alpha.run"), "premise: the lock's task is known: %v", got.Diagnostics)
	item, entry, hints := features(own)
	assert.Contains(t, item, "Its type is `", "hover over a loop item lost the type its lock's task gives it")
	assert.Contains(t, hints, "n: ", "no inlay hint for a loop item typed by its lock's task")
	assert.Contains(t, entry, "status_code", "a results entry lost the outputs of its lock's task")

	// The same text where no lock is in force: alpha.run is unknown, so nothing
	// about it is typed.
	foreign := uriIn("b")
	require.True(t, unknownTask(c.open(foreign, lockFeaturesSource), "alpha.run"))
	item, entry, hints = features(foreign)
	assert.NotContains(t, item, "Its type is `", "another document's lock typed a loop item")
	assert.NotContains(t, hints, "n: ", "another document's lock produced an inlay hint")
	assert.NotContains(t, entry, "status_code", "another document's lock supplied a results entry's outputs")
}
