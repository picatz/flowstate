package tui

import (
	"fmt"
	"slices"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
)

// Node is one division of a [Rule]'s layout: either a named pane, or two nodes
// divided by a [pane.Split].
type Node struct {
	// Pane names the pane a leaf shows. It is empty for a division.
	Pane string

	Split         pane.Split
	First, Second *Node
}

// Leaf is a node that shows the named pane.
func Leaf(name string) *Node { return &Node{Pane: name} }

// Divide is a node that divides its space between two nodes.
func Divide(split pane.Split, first, second *Node) *Node {
	return &Node{Split: split, First: first, Second: second}
}

// Rule is the layout used from MinWidth columns up.
type Rule struct {
	MinWidth int

	// Root is how the panes are divided.
	Root *Node

	// Tabs, when set, shows one pane at a time instead of Root: the focused
	// one among Tabs, filling the space under a row of tab labels.
	Tabs []string
}

// Grid is a screen's responsive layout as data: the rules, widest first, and
// the smallest terminal the screen is drawn in at all.
type Grid struct {
	Rules []Rule

	// Min is the smallest size the grid lays out. A terminal under it is
	// answered with [ErrTooSmall] and no cells.
	Min Size
}

// ErrTooSmall is what [Grid.Resolve] returns for a terminal under the grid's
// minimum.
type ErrTooSmall struct{ Have, Need Size }

func (e ErrTooSmall) Error() string {
	return fmt.Sprintf("the terminal is %dx%d and this screen needs at least %dx%d", e.Have.W, e.Have.H, e.Need.W, e.Need.H)
}

// Cell is one pane and where it is drawn.
type Cell struct {
	Pane string
	Rect pane.Rect
}

// Layout is a grid resolved for one size.
type Layout struct {
	// Rule is the index of the rule that applied.
	Rule  int
	Cells []Cell

	// Tabs lists the panes folded into tabs, in order, or is nil when the
	// layout shows them all. The tab row is the first row of the area.
	Tabs []string

	// TabRow is where the tab labels are drawn, when there are tabs.
	TabRow pane.Rect
}

// Shown reports whether the pane is drawn in this layout.
func (l Layout) Shown(name string) bool {
	return slices.ContainsFunc(l.Cells, func(c Cell) bool { return c.Pane == name })
}

// Resolve lays the grid out in area. focus picks the pane a tabbed rule shows.
func (g Grid) Resolve(area pane.Rect, focus string) (Layout, error) {
	if area.W < g.Min.W || area.H < g.Min.H {
		return Layout{}, ErrTooSmall{Have: Size{area.W, area.H}, Need: g.Min}
	}

	for i, rule := range g.Rules {
		if area.W < rule.MinWidth {
			continue
		}

		layout := Layout{Rule: i}
		if len(rule.Tabs) > 0 {
			shown := rule.Tabs[0]
			if slices.Contains(rule.Tabs, focus) {
				shown = focus
			}
			layout.Tabs = rule.Tabs
			layout.TabRow = pane.Rect{X: area.X, Y: area.Y, W: area.W, H: 1}
			layout.Cells = []Cell{{Pane: shown, Rect: pane.Rect{X: area.X, Y: area.Y + 1, W: area.W, H: area.H - 1}}}

			return layout, nil
		}
		layout.Cells = place(rule.Root, area, nil)

		return layout, nil
	}

	return Layout{}, ErrTooSmall{Have: Size{area.W, area.H}, Need: g.Min}
}

func place(n *Node, r pane.Rect, cells []Cell) []Cell {
	if n == nil {
		return cells
	}
	if n.First == nil {
		return append(cells, Cell{Pane: n.Pane, Rect: r})
	}
	a, b := n.Split.Rects(r)

	return place(n.Second, b, place(n.First, a, cells))
}
