package pane

import (
	"cmp"
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// Node is one item of a [Tree].
type Node struct {
	// ID names the node. It is unique within the tree, and it is what a click,
	// a selection and a [Request] refer to.
	ID string

	// Label is the node's name and Value what it holds, drawn beside it.
	Label string
	Value string

	// Children are the ones held. Total is how many the node has in all, so a
	// node with fewer Children than Total has a page still to load; zero means
	// the held ones are all of them.
	Children []Node
	Total    int
}

// total is how many children the node has.
func (n Node) total() int { return max(n.Total, len(n.Children)) }

// MaxChildren bounds the children held under one node, however many a
// [Loader] is willing to return.
const MaxChildren = 5000

// Request names a page of children to load: those of Parent, from Offset.
type Request struct {
	Parent string
	Offset int
}

// Loader answers a [Request]: the next children of a node, and how many it has
// in all. It is the callback a tree uses for children it does not hold, and it
// is allowed to be slow, which is why [Tree.Activate] hands back a [Request]
// instead of calling it: a view that blocked on its loader would freeze.
type Loader func(Request) (children []Node, total int, err error)

// RowKind says what a [Row] is.
type RowKind uint8

const (
	// RowNode is a node.
	RowNode RowKind = iota
	// RowMore is the "… N more" row under a node whose children are not all
	// held.
	RowMore
)

// Row is one visible line of a tree.
type Row struct {
	// ID is the node's, or "more:" and the parent's for a [RowMore].
	ID   string
	Kind RowKind

	// Depth is how far in the row is drawn.
	Depth int

	// Branch reports that the node has children, and Open that they are shown.
	Branch, Open bool

	Label, Value string

	// Parent and Offset say what activating a [RowMore] loads; Remaining is
	// how many it would still leave unseen.
	Parent    string
	Offset    int
	Remaining int
}

// Tree is a collapsible tree whose children may be paged in.
//
// It holds which nodes are open, which row is selected and how far it is
// scrolled, and nothing else: the rows are derived from the nodes each time,
// so replacing the nodes after a new read keeps what the person had open.
type Tree struct {
	roots    []Node
	open     map[string]bool
	selected string
	top      int
}

// NewTree returns a tree over roots with nothing open and the first row
// selected.
func NewTree(roots []Node) *Tree {
	t := &Tree{open: map[string]bool{}}
	t.SetRoots(roots)

	return t
}

// SetRoots replaces the nodes. What was open stays open where the node still
// exists, and the selection stays where its node does; otherwise it falls to
// the first row.
func (t *Tree) SetRoots(roots []Node) {
	if t.open == nil {
		t.open = map[string]bool{}
	}
	t.roots = roots

	ids := map[string]bool{}
	var walk func([]Node)
	walk = func(nodes []Node) {
		for _, n := range nodes {
			ids[n.ID] = true
			walk(n.Children)
		}
	}
	walk(roots)
	for id := range t.open {
		if !ids[id] {
			delete(t.open, id)
		}
	}

	rows := t.Rows()
	if !t.has(rows, t.selected) {
		t.selected = ""
		if len(rows) > 0 {
			t.selected = rows[0].ID
		}
	}
	t.top = min(t.top, max(0, len(rows)-1))
}

func (t *Tree) has(rows []Row, id string) bool {
	for _, r := range rows {
		if r.ID == id {
			return true
		}
	}

	return false
}

// Rows are the visible rows, top to bottom.
func (t *Tree) Rows() []Row {
	var rows []Row
	var walk func(nodes []Node, depth int, parent string)
	walk = func(nodes []Node, depth int, parent string) {
		for _, n := range nodes {
			open := t.open[n.ID]
			rows = append(rows, Row{
				ID: n.ID, Depth: depth, Branch: n.total() > 0, Open: open,
				Label: n.Label, Value: n.Value, Parent: parent,
			})
			if !open {
				continue
			}
			walk(n.Children, depth+1, n.ID)
			if remaining := n.total() - len(n.Children); remaining > 0 {
				rows = append(rows, Row{
					ID: moreID(n.ID), Kind: RowMore, Depth: depth + 1, Parent: n.ID,
					Offset: len(n.Children), Remaining: remaining,
				})
			}
		}
	}
	walk(t.roots, 0, "")

	return rows
}

func moreID(parent string) string { return "more:" + parent }

// Selected is the id of the selected row, or "".
func (t *Tree) Selected() string { return t.selected }

// Node returns the node with id.
func (t *Tree) Node(id string) (Node, bool) {
	var found Node
	var ok bool
	var walk func([]Node)
	walk = func(nodes []Node) {
		for _, n := range nodes {
			if n.ID == id {
				found, ok = n, true
			}
			if ok {
				return
			}
			walk(n.Children)
		}
	}
	walk(t.roots)

	return found, ok
}

// Select selects the row with id, and reports whether there is one.
func (t *Tree) Select(id string) bool {
	if !t.has(t.Rows(), id) {
		return false
	}
	t.selected = id

	return true
}

// Move selects the row delta rows from the selected one, stopping at the ends.
func (t *Tree) Move(delta int) {
	rows := t.Rows()
	if len(rows) == 0 {
		return
	}
	at := 0
	for i, r := range rows {
		if r.ID == t.selected {
			at = i
		}
	}
	t.selected = rows[max(0, min(len(rows)-1, at+delta))].ID
}

// Home and End select the first and last rows.
func (t *Tree) Home() { t.Move(-len(t.Rows())) }

// End selects the last row.
func (t *Tree) End() { t.Move(len(t.Rows())) }

// Open reports whether the node is open.
func (t *Tree) Open(id string) bool { return t.open[id] }

// Expand opens a branch and Collapse closes it. Expand returns the request for
// its first page when the node has children it does not hold yet.
func (t *Tree) Expand(id string) (Request, bool) {
	n, ok := t.Node(id)
	if !ok || n.total() == 0 {
		return Request{}, false
	}
	t.open[id] = true
	if len(n.Children) == 0 {
		return Request{Parent: id}, true
	}

	return Request{}, false
}

// Collapse closes the node. When the selection was inside it, the node is
// selected instead of the selection being lost.
func (t *Tree) Collapse(id string) {
	if !t.open[id] {
		return
	}
	delete(t.open, id)
	if !t.has(t.Rows(), t.selected) {
		t.selected = id
	}
}

// Toggle opens a closed branch and closes an open one.
func (t *Tree) Toggle(id string) (Request, bool) {
	if t.open[id] {
		t.Collapse(id)

		return Request{}, false
	}

	return t.Expand(id)
}

// Activate is what a click or enter does to the row with id: select it and, if
// it is a branch, toggle it; if it is a "… N more" row, ask for that page.
func (t *Tree) Activate(id string) (Request, bool) {
	for _, r := range t.Rows() {
		if r.ID != id {
			continue
		}
		t.selected = id
		if r.Kind == RowMore {
			return Request{Parent: r.Parent, Offset: r.Offset}, true
		}
		if r.Branch {
			return t.Toggle(id)
		}
	}

	return Request{}, false
}

// Parent selects the parent of the selected row, and reports whether it moved.
func (t *Tree) Parent() bool {
	for _, r := range t.Rows() {
		if r.ID == t.selected && r.Parent != "" {
			t.selected = r.Parent

			return true
		}
	}

	return false
}

// Fill appends a page of children to parent, which must be the next one: a page
// that does not start where the held ones end is an answer to a question the
// tree has since stopped asking, and is dropped. total replaces the node's
// count when it is positive.
func (t *Tree) Fill(parent string, offset int, children []Node, total int) bool {
	filled := false
	var walk func(nodes []Node)
	walk = func(nodes []Node) {
		for i := range nodes {
			if nodes[i].ID == parent {
				if offset != len(nodes[i].Children) {
					return
				}
				room := max(0, MaxChildren-len(nodes[i].Children))
				nodes[i].Children = append(nodes[i].Children, children[:min(len(children), room)]...)
				if total > 0 {
					nodes[i].Total = min(total, MaxChildren)
				}
				filled = true

				return
			}
			walk(nodes[i].Children)
		}
	}
	walk(t.roots)

	return filled
}

// Load is [Tree.Fill] driven by a loader, synchronously. A caller with a slow
// loader runs it itself and calls Fill with the answer.
func (t *Tree) Load(l Loader, r Request) error {
	children, total, err := l(r)
	if err != nil {
		return err
	}
	t.Fill(r.Parent, r.Offset, children, total)

	return nil
}

// Scroll moves the viewport by delta rows within a view of height rows.
func (t *Tree) Scroll(delta, height int) {
	t.top = max(0, min(t.top+delta, len(t.Rows())-max(1, height)))
	t.top = max(0, t.top)
}

// Reveal scrolls the least that brings the selected row into a view of height
// rows.
func (t *Tree) Reveal(height int) {
	height = max(1, height)
	for i, r := range t.Rows() {
		if r.ID != t.selected {
			continue
		}
		switch {
		case i < t.top:
			t.top = i
		case i >= t.top+height:
			t.top = i - height + 1
		}
	}
}

// Top is the first row of the viewport.
func (t *Tree) Top() int { return t.top }

// MaxValueRunes cuts a value in a row before the layout is handed it.
const maxCellRunes = 4096

// View draws the rows that fit in o.Height starting at the scroll position,
// and registers each with o.Hits under o.Prefix and the row's id. Empty is
// drawn instead when there are no rows.
func (t *Tree) View(o Options, empty string) string {
	rows := t.Rows()
	if len(rows) == 0 {
		return o.Theme.Muted.Render(ui.EscapeControl(empty))
	}

	top := max(0, min(t.top, len(rows)-1))
	end := min(len(rows), top+max(0, o.Height))
	visible := rows[top:end]

	// The label column is as wide as the widest label drawn, capped at half the
	// width, so one very long name does not push every value off the screen.
	labelWidth := 0
	for _, r := range visible {
		labelWidth = max(labelWidth, r.Depth*2+2+lipgloss.Width(cleaned(r.Label)))
	}
	labelWidth = min(labelWidth, max(8, o.Width/2))

	lines := make([]string, 0, len(visible))
	for i, r := range visible {
		lines = append(lines, t.line(r, labelWidth, o))
		kind := KindRow
		if r.Kind == RowMore {
			kind = KindMore
		}
		o.Hits.Add(Rect{X: o.Origin.X, Y: o.Origin.Y + i, W: o.Width, H: 1}, o.Prefix+r.ID, kind)
	}

	return strings.Join(lines, "\n")
}

func cleaned(s string) string {
	return ui.EscapeControl(strings.ReplaceAll(s, " ", " "))
}

// line draws one row.
func (t *Tree) line(r Row, labelWidth int, o Options) string {
	indent := strings.Repeat("  ", r.Depth)
	if r.Kind == RowMore {
		text := fmt.Sprintf("%s %d more", o.Symbols.Ellipsis, r.Remaining)

		return ui.Trim(o.Theme.Muted.Render(indent+"  "+text), o.Width)
	}

	mark := " "
	switch {
	case r.Branch && r.Open:
		mark = o.Symbols.Expanded
	case r.Branch:
		mark = o.Symbols.Collapsed
	}

	label := ui.Trim(cleaned(r.Label), max(1, labelWidth-r.Depth*2-2))
	pad := max(0, labelWidth-r.Depth*2-2-lipgloss.Width(label))
	value := ui.Trim(cleaned(r.Value), maxCellRunes)
	if o.PaintValue != nil {
		value = o.PaintValue(value)
	}

	name := o.Theme.Strong.Render(label)
	if r.ID == t.selected {
		gutter := cmp.Or(strings.TrimSpace(o.Symbols.Arrow), ">")
		if !o.Focused {
			gutter = o.Symbols.Bullet
		}
		line := gutter + indent + mark + " " + name + strings.Repeat(" ", pad) + "  " + value

		return ui.Trim(line, o.Width)
	}

	return ui.Trim(" "+indent+mark+" "+name+strings.Repeat(" ", pad)+"  "+value, o.Width)
}
