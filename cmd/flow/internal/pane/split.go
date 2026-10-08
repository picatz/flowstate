package pane

import (
	"cmp"
	"slices"
	"strings"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// Orientation is how a [Split] divides its space.
type Orientation uint8

const (
	// Columns puts the first pane left of the second.
	Columns Orientation = iota
	// Rows puts the first pane above the second.
	Rows
)

// Split divides a rectangle between two panes.
type Split struct {
	Orientation Orientation

	// Percent is the first pane's share of the divided length, from 1 to 99;
	// zero means half.
	Percent int

	// Gap is the cells left empty between the panes along the divided length.
	Gap int
}

// Rects divides r, giving each pane at least one cell where r has room for
// two. A width of zero or less is replaced by [ui.ClampWidth]'s fallback so a
// split of nothing is a split of something small rather than a division by it.
func (s Split) Rects(r Rect) (first, second Rect) {
	if r.W <= 0 {
		r.W = ui.ClampWidth(0)
	}
	r.H = max(r.H, 0)

	percent := s.Percent
	if percent <= 0 || percent >= 100 {
		percent = 50
	}

	switch s.Orientation {
	case Rows:
		gap := min(s.Gap, max(0, r.H-2))
		a := max(1, (r.H-gap)*percent/100)
		a = min(a, max(0, r.H-gap-1))

		return Rect{r.X, r.Y, r.W, a}, Rect{r.X, r.Y + a + gap, r.W, r.H - a - gap}
	default:
		gap := min(s.Gap, max(0, r.W-2))
		a := max(1, (r.W-gap)*percent/100)
		a = min(a, max(0, r.W-gap-1))

		return Rect{r.X, r.Y, a, r.H}, Rect{r.X + a + gap, r.Y, r.W - a - gap, r.H}
	}
}

// View draws two rendered panes into a w-by-h block.
func (s Split) View(w, h int, first, second string) string {
	if w <= 0 {
		w = ui.ClampWidth(0)
	}
	a, b := s.Rects(Rect{W: w, H: h})

	return Stitch(w, h, Placed{a, first}, Placed{b, second})
}

// Placed is rendered text and the rectangle it is to fill.
type Placed struct {
	Rect Rect
	Text string
}

// Stitch composes rendered panes into one w-by-h screen: each is cut and padded
// to its rectangle, and cells no pane covers are blank.
//
// Rectangles must not overlap. A pane that starts inside one already placed on
// its row is not drawn on that row, which keeps every line exactly w cells
// wide whatever it is handed.
func Stitch(w, h int, parts ...Placed) string {
	if w <= 0 || h <= 0 {
		return ""
	}

	type segment struct {
		x, w int
		text string
	}
	rows := make([][]segment, h)
	for _, p := range parts {
		r := p.Rect
		if r.Empty() || r.X < 0 || r.X >= w {
			continue
		}
		r.W = min(r.W, w-r.X)
		for i, line := range Fit(p.Text, r.W, r.H) {
			if y := r.Y + i; y >= 0 && y < h {
				rows[y] = append(rows[y], segment{x: r.X, w: r.W, text: line})
			}
		}
	}

	lines := make([]string, h)
	for y, segments := range rows {
		slices.SortStableFunc(segments, func(a, b segment) int { return cmp.Compare(a.x, b.x) })

		var b strings.Builder
		x := 0
		for _, seg := range segments {
			if seg.x < x {
				continue
			}
			b.WriteString(strings.Repeat(" ", seg.x-x))
			b.WriteString(seg.text)
			x = seg.x + seg.w
		}
		b.WriteString(strings.Repeat(" ", w-x))
		lines[y] = b.String()
	}

	return strings.Join(lines, "\n")
}
