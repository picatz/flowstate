package pane

import "slices"

// Rect is a rectangle of terminal cells, zero-based from the top left.
type Rect struct{ X, Y, W, H int }

// Contains reports whether the cell (x, y) is inside r.
func (r Rect) Contains(x, y int) bool {
	return x >= r.X && x < r.X+r.W && y >= r.Y && y < r.Y+r.H
}

// Empty reports whether r covers no cell.
func (r Rect) Empty() bool { return r.W <= 0 || r.H <= 0 }

// Kind says what sort of thing a [Hit] is, so the one resolving a click knows
// what to do without parsing an id.
type Kind string

const (
	// KindPane is the body of a pane; a click focuses it.
	KindPane Kind = "pane"
	// KindHeading is a pane's heading row.
	KindHeading Kind = "heading"
	// KindRow is a selectable row of a [Tree].
	KindRow Kind = "row"
	// KindMore is a tree's "… N more" row.
	KindMore Kind = "more"
	// KindTab is a tab that names a pane the screen has folded away.
	KindTab Kind = "tab"
	// KindPoint is a point of a timeline strip; a click travels to it.
	KindPoint Kind = "point"
	// KindInput is a line the person types into.
	KindInput Kind = "input"
)

// Hit is one thing a view drew and where.
type Hit struct {
	Rect Rect
	ID   string
	Kind Kind
}

// MaxHits bounds a registry. A screen has a few hundred cells of rows at the
// most; a registry that grew with the data being shown would be the data's to
// size.
const MaxHits = 4096

// Hits is the per-frame registry of what a view drew.
//
// A view builds a fresh one each time it draws and the model resolves a mouse
// message against it, so a click can only land on what is on screen. Where two
// hits overlap the later one wins, which lets a pane register its body first
// and its rows after. The zero value is ready to use, and every method is safe
// on a nil receiver so a view that was not asked for hits can pass nil.
type Hits struct {
	hits []Hit
}

// Add registers r under id. An empty rectangle, and anything past [MaxHits],
// is ignored.
func (h *Hits) Add(r Rect, id string, kind Kind) {
	if h == nil || r.Empty() || len(h.hits) >= MaxHits {
		return
	}

	h.hits = append(h.hits, Hit{Rect: r, ID: id, Kind: kind})
}

// At returns the topmost hit covering (x, y).
func (h *Hits) At(x, y int) (Hit, bool) {
	if h == nil {
		return Hit{}, false
	}

	for _, v := range slices.Backward(h.hits) {
		if v.Rect.Contains(x, y) {
			return v, true
		}
	}

	return Hit{}, false
}

// Len is how many hits are registered.
func (h *Hits) Len() int {
	if h == nil {
		return 0
	}

	return len(h.hits)
}
