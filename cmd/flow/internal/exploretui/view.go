package exploretui

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// Pane names, as the grid and the hits refer to them.
const (
	paneGraph   = "graph"
	paneDetails = "details"

	// rowPrefix is the id prefix the tree registers its rows under, and
	// panePrefix the one a pane's body and heading are registered under.
	rowPrefix  = "row/"
	panePrefix = "pane:"
)

// Screen limits. The screen is not drawn in a terminal smaller than these, and
// `flow explore` refuses to start in one.
const (
	MinWidth  = 40
	MinHeight = 10
)

// grid puts the details beside the tree in a wide terminal and under it in a
// narrow one.
var grid = tui.Grid{
	Min: tui.Size{W: MinWidth, H: 3},
	Rules: []tui.Rule{
		{MinWidth: 100, Root: tui.Divide(pane.Split{Percent: 55, Gap: 1}, tui.Leaf(paneGraph), tui.Leaf(paneDetails))},
		{MinWidth: MinWidth, Root: tui.Divide(pane.Split{Orientation: pane.Rows, Percent: 60}, tui.Leaf(paneGraph), tui.Leaf(paneDetails))},
	},
}

// Style is the colours and marks a screen is drawn with.
type Style struct {
	Theme   ui.Theme
	Symbols ui.SymbolSet
}

// Screen is everything [Screen.Draw] draws from. It holds no loader and reads no
// clock; [Model] owns one and changes it.
type Screen struct {
	Size tui.Size

	// Source says what the graph was read from, for the header.
	Source string

	// Index is the graph on show; nil before the first read has finished.
	Index *Index
	Tree  *pane.Tree

	// Problem is why the last read failed.
	Problem string
	Loading bool

	Toast tui.Toast
	Keys  tui.Keymap

	Help    bool
	HelpTop int
}

// geometry is where the parts of the screen are.
type geometry struct {
	header, body, toast, status pane.Rect
	layout                      tui.Layout
}

// geometry lays the screen out, or reports that the terminal is too small.
func (s Screen) geometry() (geometry, error) {
	var g geometry
	if s.Size.W < MinWidth || s.Size.H < MinHeight {
		return g, tui.ErrTooSmall{Have: s.Size, Need: tui.Size{W: MinWidth, H: MinHeight}}
	}
	w, h := s.Size.W, s.Size.H
	g.header = pane.Rect{W: w, H: 1}
	g.body = pane.Rect{Y: 1, W: w, H: h - 3}
	g.toast = pane.Rect{Y: h - 2, W: w, H: 1}
	g.status = pane.Rect{Y: h - 1, W: w, H: 1}

	var err error
	g.layout, err = grid.Resolve(g.body, paneGraph)

	return g, err
}

// Draw renders the screen in exactly Size cells, and the hits of what it drew.
// A screen too small to lay out is one sentence, wrapped to fit, and no hits.
func (s Screen) Draw(st Style) (string, *pane.Hits) {
	hits := &pane.Hits{}
	g, err := s.geometry()
	if err != nil {
		return strings.Join(pane.Fit(wrapWords(err.Error(), s.Size.W), s.Size.W, s.Size.H), "\n"), hits
	}

	parts := []pane.Placed{
		{Rect: g.header, Text: s.header(g.header.W, st)},
		{Rect: g.toast, Text: s.Toast.View(g.toast.W, st.Theme, st.Symbols)},
		{Rect: g.status, Text: tui.Bar{Left: s.Keys.Hints(g.status.W, st.Theme), Right: s.statusNote(st)}.View(g.status.W)},
	}

	if s.Help {
		o := s.options(g.body.W, g.body.H, st, true)
		parts = append(parts, pane.Placed{Rect: g.body, Text: s.helpView(o)})
		hits.Add(g.body, "help", pane.KindPane)

		return pane.Stitch(s.Size.W, s.Size.H, parts...), hits
	}

	for _, cell := range g.layout.Cells {
		o := s.options(cell.Rect.W, cell.Rect.H, st, cell.Pane == paneGraph)
		hits.Add(cell.Rect, panePrefix+cell.Pane, pane.KindPane)
		hits.Add(pane.Rect{X: cell.Rect.X, Y: cell.Rect.Y, W: cell.Rect.W, H: 1}, panePrefix+cell.Pane, pane.KindHeading)

		var text string
		switch cell.Pane {
		case paneGraph:
			text = s.graphView(cell.Rect, hits, st)
		case paneDetails:
			text = s.detailsView(o)
		}
		parts = append(parts, pane.Placed{Rect: cell.Rect, Text: text})
	}

	return pane.Stitch(s.Size.W, s.Size.H, parts...), hits
}

func (s Screen) options(w, h int, st Style, focused bool) pane.Options {
	return pane.Options{Width: w, Height: h, Theme: st.Theme, Symbols: st.Symbols, Focused: focused}
}

func (s Screen) header(width int, st Style) string {
	left := st.Theme.Strong.Render("flow explore")
	if s.Source != "" {
		left += " " + st.Theme.Muted.Render(ui.EscapeControl(s.Source))
	}
	right := ""
	if s.Index != nil && s.Index.Graph().GetPartial() {
		right = st.Theme.Warning.Render("partial")
	}

	return tui.Bar{Left: left, Right: right}.View(width)
}

func (s Screen) statusNote(st Style) string {
	if s.Loading {
		return st.Theme.Warning.Render("reading…")
	}

	return ""
}

// graphView is the tree pane: its heading and the rows that fit.
func (s Screen) graphView(cell pane.Rect, hits *pane.Hits, st Style) string {
	o := s.options(cell.W, cell.H, st, true)
	switch {
	case s.Index == nil && s.Problem != "":
		return pane.Heading(paneGraph, "", cell.W, o) + "\n" + st.Theme.Danger.Render(ui.EscapeControl(s.Problem))
	case s.Index == nil:
		return pane.Heading(paneGraph, "", cell.W, o) + "\n" + st.Theme.Muted.Render("reading…")
	}

	note := "1 workflow"
	if n := len(s.Index.Roots()); n != 1 {
		note = fmt.Sprintf("%d workflows", n)
	}
	body := o
	body.Height, body.Origin, body.Hits, body.Prefix = cell.H-1, pane.Rect{X: cell.X, Y: cell.Y + 1, W: cell.W, H: cell.H - 1}, hits, rowPrefix

	return pane.Heading(paneGraph, note, cell.W, o) + "\n" + s.Tree.View(body, "no workflows: name a Flowfile or a directory of them")
}

// detailsView is the inspector pane for the selected row.
func (s Screen) detailsView(o pane.Options) string {
	o.Focused = false
	heading := pane.Heading(paneDetails, "", o.Width, o)
	if s.Index == nil {
		return heading
	}
	body := o
	body.Height = o.Height - 1

	return heading + "\n" + s.Index.Details(s.Tree.Selected()).View(body, "nothing selected")
}

// helpView is the key help, scrolled to HelpTop.
func (s Screen) helpView(o pane.Options) string {
	body := s.Keys.Help(o.Width, o.Theme)
	room := max(0, o.Height-1)
	top := max(0, min(s.HelpTop, max(0, len(body)-room)))

	note := "? or esc closes"
	if len(body) > room {
		note = fmt.Sprintf("%d-%d of %d, up/down scrolls, ? or esc closes", top+1, min(top+room, len(body)), len(body))
	}
	lines := []string{pane.Heading("help", note, o.Width, o)}
	lines = append(lines, body[top:min(len(body), top+room)]...)
	for i, line := range lines {
		lines[i] = ui.Trim(line, o.Width)
	}

	return strings.Join(lines, "\n")
}

// wrapWords breaks text at spaces to fit width.
func wrapWords(text string, width int) string {
	var lines []string
	line := ""
	for word := range strings.FieldsSeq(text) {
		switch {
		case line == "":
			line = word
		case lipgloss.Width(line)+1+lipgloss.Width(word) <= width:
			line += " " + word
		default:
			lines = append(lines, line)
			line = word
		}
	}

	return strings.Join(append(lines, line), "\n")
}
