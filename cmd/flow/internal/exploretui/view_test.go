package exploretui

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui/tuitest"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Each screen is pinned in the three variants the debugger's are: styled, ASCII
// marks, and plain.

func TestTheScreenGolden(t *testing.T) {
	for _, v := range styles {
		t.Run(v.name, func(t *testing.T) {
			var b strings.Builder
			for _, size := range []tui.Size{{W: 120, H: 16}, {W: 60, H: 16}} {
				m, _ := started(t, fleet(), func(c *Config) { c.Style, c.Size = v.style, size })
				m = press(m, "j", "j", "enter", "j", "l")
				b.WriteString("=== " + size.String() + "\n" + view(m) + "\n")
			}
			tuitest.Golden(t, b.String())
		})
	}
}

func TestThePartialMarkAndTheFirstNoteAreShown(t *testing.T) {
	g := fleet()
	g.Partial, g.Notes = true, []string{"bad.yaml does not compile and was left out"}
	m, _ := started(t, g)

	out := view(m)
	assert.Contains(t, out, "partial")
	assert.Contains(t, out, "bad.yaml does not compile")
}

func TestAnEmptyGraphSaysWhatToDo(t *testing.T) {
	m, _ := started(t, &v1.Graph{})
	out := view(m)
	assert.Contains(t, out, "0 workflows")
	assert.Contains(t, out, "no workflows")
}

func TestHelpGolden(t *testing.T) {
	m, _ := started(t, fleet(), func(c *Config) { c.Size = tui.Size{W: 80, H: 20} })
	tuitest.Golden(t, view(press(m, "?")))
}
