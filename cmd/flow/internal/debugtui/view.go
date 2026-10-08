package debugtui

import (
	"fmt"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/debugpane"
	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// Pane names, as the grid, the focus ring and the hits refer to them.
const (
	paneSteps     = "steps"
	paneScope     = "scope"
	paneInspector = "inspector"
	paneConsole   = "console"

	// scopePrefix is the id prefix the scope tree registers its rows under.
	scopePrefix = "scope/"
	// panePrefix is the id prefix a pane's body and heading are registered under.
	panePrefix = "pane:"
)

// Screen limits. The screen is not drawn in a terminal smaller than these, and
// `flow debug attach --tui` refuses to start in one.
const (
	MinWidth  = 60
	MinHeight = 12
)

// minBody is the least body a layout is resolved in: a heading and two rows.
// The screen as a whole is held to [MinWidth] and [MinHeight] before the grid
// is asked, which is what leaves the body this much.
const minBody = 3

// grid is how the screen folds as the terminal narrows: three columns with the
// selected name's detail on the right, the detail folded under the scope, the
// detail dropped, and below 80 columns one pane at a time under tabs.
var grid = tui.Grid{
	Min: tui.Size{W: MinWidth, H: minBody},
	Rules: []tui.Rule{
		{MinWidth: 120, Root: tui.Divide(pane.Split{Percent: 30, Gap: 1},
			tui.Leaf(paneSteps),
			tui.Divide(pane.Split{Percent: 55, Gap: 1}, tui.Leaf(paneScope), tui.Leaf(paneInspector)))},
		{MinWidth: 100, Root: tui.Divide(pane.Split{Percent: 40, Gap: 1},
			tui.Leaf(paneSteps),
			tui.Divide(pane.Split{Orientation: pane.Rows, Percent: 60}, tui.Leaf(paneScope), tui.Leaf(paneInspector)))},
		{MinWidth: 80, Root: tui.Divide(pane.Split{Percent: 40, Gap: 1}, tui.Leaf(paneSteps), tui.Leaf(paneScope))},
		{MinWidth: MinWidth, Tabs: []string{paneSteps, paneScope}},
	},
}

// Style is the colours and marks a screen is drawn with.
type Style struct {
	Theme   ui.Theme
	Symbols ui.SymbolSet
}

// Screen is everything [Screen.Draw] draws from. It holds no target and reads no
// clock; [Model] owns one and changes it.
type Screen struct {
	Size tui.Size

	// Frame is the last read of the run. Loaded is false before the first.
	Frame  flowdebug.Frame
	Loaded bool

	// Problem is why the last read failed, shown in place of the panes' content.
	Problem string

	Tree       *pane.Tree
	StepScroll int

	Console Console
	Toast   tui.Toast
	Keys    tui.Keymap
	Verbs   []flowdebug.Verb

	// Focus is the focused member of the ring, and Pane the content pane a
	// tabbed layout shows (the last of steps and scope that was focused).
	Focus string
	Pane  string
	Help  bool

	// Busy is the command line being run, or empty.
	Busy string
}

// geometry is where the parts of the screen are.
type geometry struct {
	header, toast, status, console, body pane.Rect
	layout                               tui.Layout
}

// consoleRows is the console's height, heading and input line included.
func consoleRows(height int) int {
	switch {
	case height >= 36:
		return 7
	case height >= 24:
		return 5
	default:
		return 4
	}
}

// geometry lays the screen out, or reports that the terminal is too small.
func (s Screen) geometry() (geometry, error) {
	var g geometry
	if s.Size.W < MinWidth || s.Size.H < MinHeight {
		return g, tui.ErrTooSmall{Have: s.Size, Need: tui.Size{W: MinWidth, H: MinHeight}}
	}

	w, h := s.Size.W, s.Size.H
	console := consoleRows(h)
	g.header = pane.Rect{X: 0, Y: 0, W: w, H: 1}
	g.body = pane.Rect{X: 0, Y: 1, W: w, H: h - 3 - console}
	g.console = pane.Rect{X: 0, Y: 1 + g.body.H, W: w, H: console}
	g.toast = pane.Rect{X: 0, Y: h - 2, W: w, H: 1}
	g.status = pane.Rect{X: 0, Y: h - 1, W: w, H: 1}

	shown := s.Pane
	if shown != paneSteps && shown != paneScope {
		shown = paneSteps
	}
	var err error
	g.layout, err = grid.Resolve(g.body, shown)

	return g, err
}

// Draw renders the screen in exactly Size cells, and the hits of what it drew.
//
// A screen too small to lay out is one sentence, wrapped to fit, and no hits.
func (s Screen) Draw(st Style) (string, *pane.Hits) {
	hits := &pane.Hits{}
	g, err := s.geometry()
	if err != nil {
		return strings.Join(pane.Fit(wrapWords(err.Error(), s.Size.W), s.Size.W, s.Size.H), "\n"), hits
	}

	parts := []pane.Placed{
		{Rect: g.header, Text: HeaderView(s.Frame, s.Loaded, g.header.W, st)},
		{Rect: g.toast, Text: s.Toast.View(g.toast.W, st.Theme, st.Symbols)},
		{Rect: g.status, Text: s.statusBar(g.status.W, st)},
	}

	if s.Help {
		o := s.options(g.body.W, g.body.H+g.console.H, st, true)
		parts = append(parts, pane.Placed{
			Rect: pane.Rect{X: 0, Y: 1, W: g.body.W, H: g.body.H + g.console.H},
			Text: HelpView(s.Keys, s.Verbs, o),
		})
		hits.Add(parts[len(parts)-1].Rect, "help", pane.KindPane)

		return pane.Stitch(s.Size.W, s.Size.H, parts...), hits
	}

	if len(g.layout.Tabs) > 0 {
		o := s.options(g.layout.TabRow.W, 1, st, false)
		parts = append(parts, pane.Placed{Rect: g.layout.TabRow, Text: TabsView(g.layout.Tabs, g.layout.Cells[0].Pane, g.layout.TabRow, hits, o)})
	}
	for _, cell := range g.layout.Cells {
		o := s.options(cell.Rect.W, cell.Rect.H, st, s.Focus == cell.Pane)
		o.Origin, o.Hits = cell.Rect, hits
		hits.Add(cell.Rect, panePrefix+cell.Pane, pane.KindPane)
		hits.Add(pane.Rect{X: cell.Rect.X, Y: cell.Rect.Y, W: cell.Rect.W, H: 1}, panePrefix+cell.Pane, pane.KindHeading)

		var text string
		switch cell.Pane {
		case paneSteps:
			text = StepsView(s.Frame, s.Loaded, s.StepScroll, o)
		case paneScope:
			o.Prefix = scopePrefix
			text = ScopeView(s.Tree, s.Frame, s.Loaded, s.Problem, o)
		case paneInspector:
			text = InspectorView(s.Tree, s.Frame, o)
		}
		parts = append(parts, pane.Placed{Rect: cell.Rect, Text: text})
	}

	co := s.options(g.console.W, g.console.H, st, s.Focus == paneConsole)
	parts = append(parts, pane.Placed{Rect: g.console, Text: ConsoleView(s.Console, s.Busy, co)})
	hits.Add(g.console, paneConsole, pane.KindInput)

	return pane.Stitch(s.Size.W, s.Size.H, parts...), hits
}

func (s Screen) options(w, h int, st Style, focused bool) pane.Options {
	return pane.Options{Width: w, Height: h, Theme: st.Theme, Symbols: st.Symbols, Focused: focused}
}

// statusBar is the bottom line: the keys that matter here, and what the screen
// is doing.
func (s Screen) statusBar(width int, st Style) string {
	right := ""
	if s.Busy != "" {
		right = st.Theme.Warning.Render("working: " + ui.EscapeControl(s.Busy))
	}

	return tui.Bar{Left: s.Keys.Hints(width, st.Theme), Right: right}.View(width)
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

// HeaderView is the top line: the program, the run, and where it stands.
func HeaderView(f flowdebug.Frame, loaded bool, width int, st Style) string {
	bar := tui.Bar{Right: st.Theme.Muted.Render("? help")}
	snapshot := f.Snapshot
	if !loaded || snapshot == nil {
		bar.Left = st.Theme.Strong.Render("flow debug") + st.Theme.Muted.Render(" · reading the run")

		return bar.View(width)
	}

	parts := []string{st.Theme.Strong.Render("flow debug")}
	run := snapshot.GetSession().GetRun()
	if id := ui.EscapeControl(run.GetWorkflowId()); id != "" {
		parts = append(parts, id)
	}
	if id := ui.EscapeControl(run.GetRunId()); id != "" {
		parts = append(parts, "run "+id[:min(len(id), 8)])
	}

	state := strings.ToLower(strings.TrimPrefix(snapshot.GetState().String(), "DEBUG_RUN_STATE_"))
	state = strings.ReplaceAll(state, "_", " ")
	if snapshot.GetState() == v1.DebugRunState_DEBUG_RUN_STATE_HELD {
		reason := strings.ToLower(strings.TrimPrefix(snapshot.GetReason().String(), "DEBUG_STOP_REASON_"))
		state += " " + strings.ReplaceAll(reason, "_", " ")
		if ids := snapshot.GetBreakpointIds(); len(ids) > 0 {
			state += " " + ui.EscapeControl(strings.Join(ids, ", "))
		}
	}
	parts = append(parts, state, fmt.Sprintf("rev %d", snapshot.GetRevision()))
	if f.Partial {
		parts = append(parts, "earlier steps not shown")
	}
	bar.Left = parts[0] + st.Theme.Muted.Render(" · "+strings.Join(parts[1:], " · "))

	return bar.View(width)
}

// TabsView is the row of tab labels a folded layout shows, the active one
// bracketed. Each label registers a hit naming its pane.
func TabsView(tabs []string, active string, row pane.Rect, hits *pane.Hits, o pane.Options) string {
	var b strings.Builder
	x := 0
	for _, tab := range tabs {
		label := " " + tab + " "
		style := o.Theme.Muted
		if tab == active {
			label, style = "["+tab+"]", o.Theme.Accent
		}
		b.WriteString(style.Render(label))
		hits.Add(pane.Rect{X: row.X + x, Y: row.Y, W: lipgloss.Width(label), H: 1}, tab, pane.KindTab)
		x += lipgloss.Width(label)
	}
	b.WriteString(o.Theme.Muted.Render("  tab switches"))

	return b.String()
}

// StepsView is the step pane: the heading and the run's steps, windowed to the
// height and kept around the held one. scroll moves the window from there.
func StepsView(f flowdebug.Frame, loaded bool, scroll int, o pane.Options) string {
	heading := pane.Heading(paneSteps, "", o.Width, o)
	if !loaded {
		return heading
	}

	frame, paused := debugpane.FromFrame(f)
	if !paused {
		return heading + "\n" + o.Theme.Muted.Render(notHeld(f))
	}

	lines, held := debugpane.StepLines(frame, o.Theme, o.Symbols)
	if len(lines) == 0 {
		return heading
	}
	rows := max(1, o.Height-1)
	start := stepStart(len(lines), held, rows, scroll)

	return heading + "\n" + strings.Join(lines[start:min(len(lines), start+rows)], "\n")
}

// stepStart is the first line of the window: centred on the held line, then
// moved by scroll, and never past either end.
func stepStart(n, held, rows, scroll int) int {
	if n <= rows {
		return 0
	}
	start := 0
	if held >= 0 {
		start = held - (rows-1)/2
	}

	return max(0, min(start+scroll, n-rows))
}

// StepScrollRange is how far the step window can be moved either way from its
// automatic place, for a client that clamps a scroll.
func StepScrollRange(f flowdebug.Frame, rows int, st Style) (lo, hi int) {
	frame, paused := debugpane.FromFrame(f)
	if !paused {
		return 0, 0
	}
	lines, held := debugpane.StepLines(frame, st.Theme, st.Symbols)
	if len(lines) <= rows {
		return 0, 0
	}
	auto := stepStart(len(lines), held, rows, 0)

	return -auto, len(lines) - rows - auto
}

// notHeld says why there is nothing to draw: the run is not stopped.
func notHeld(f flowdebug.Frame) string {
	state := strings.ToLower(strings.TrimPrefix(f.Snapshot.GetState().String(), "DEBUG_RUN_STATE_"))
	if f.Snapshot == nil {
		return "  no snapshot"
	}

	return "  not held (" + strings.ReplaceAll(state, "_", " ") + "); the steps come back at the next stop"
}

// maxCellRunes cuts a value before the layout is handed it, as debugpane does.
const maxCellRunes = debugpane.MaxValueRunes

func cutValue(text string) string {
	runes := []rune(text)
	if len(runes) <= maxCellRunes {
		return text
	}

	return string(runes[:maxCellRunes]) + " (cut)"
}

// ScopeNodes are the scope tree's nodes for a frame: one per group, with the
// bindings the frame resolved under it, and a value that has children of its
// own as a branch whose children are asked for when it is opened.
func ScopeNodes(f flowdebug.Frame) []pane.Node {
	var nodes []pane.Node
	for _, group := range f.Scope.GetGroups() {
		node := pane.Node{
			ID:    groupID(group.GetGroup()),
			Label: group.GetGroup(),
			Value: fmt.Sprintf("{%d}", group.GetTotal()),
			Total: int(group.GetTotal()),
		}
		for _, binding := range group.GetBindings() {
			value := f.Values[binding.GetExpression()]
			text := value.GetRendered()
			if binding.GetError() != "" {
				text = "(" + binding.GetError() + ")"
			}
			node.Children = append(node.Children, pane.Node{
				ID:    binding.GetExpression(),
				Label: binding.GetName(),
				Value: cutValue(text),
				Total: int(value.GetChildren()),
			})
		}
		nodes = append(nodes, node)
	}

	return nodes
}

func groupID(group string) string { return "g:" + group }

// ScopeView is the scope pane: the heading and the tree, or the reason there
// is none.
func ScopeView(tree *pane.Tree, f flowdebug.Frame, loaded bool, problem string, o pane.Options) string {
	if !loaded {
		return pane.Heading(paneScope, "", o.Width, o)
	}

	note := ""
	if f.Scope != nil {
		note = fmt.Sprintf("%d names", f.Scope.GetTotal())
	}
	heading := pane.Heading(paneScope, note, o.Width, o)

	empty := cmpOr(problem, f.ScopeNote, "  no scope: the run is not held")
	if f.Scope == nil || tree == nil {
		return heading + "\n" + o.Theme.Muted.Render(ui.EscapeControl("  "+strings.TrimSpace(empty)))
	}

	body := o
	body.Height = max(1, o.Height-1)
	body.Origin.Y++
	view := tree.View(body, "  no names in scope")
	if f.ScopeNote != "" {
		view += "\n" + o.Theme.Muted.Render(ui.EscapeControl("  "+f.ScopeNote))
	}

	return heading + "\n" + view
}

func cmpOr(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}

	return ""
}

// InspectorView is the detail pane: the selected row as key/value fields.
func InspectorView(tree *pane.Tree, f flowdebug.Frame, o pane.Options) string {
	heading := pane.Heading(paneInspector, "", o.Width, o)
	body := o
	body.Height = max(1, o.Height-1)

	return heading + "\n" + inspector(tree, f).View(body, "  select a row in the scope")
}

func inspector(tree *pane.Tree, f flowdebug.Frame) pane.Inspector {
	if tree == nil || tree.Selected() == "" {
		return pane.Inspector{}
	}
	id := tree.Selected()

	for _, group := range f.Scope.GetGroups() {
		if groupID(group.GetGroup()) == id {
			note := ""
			if int(group.GetTotal()) > len(group.GetBindings()) {
				note = "the rest are listed by opening the group; `scope` names them all"
			}

			return pane.Inspector{Fields: []pane.Field{
				{Key: "group", Value: group.GetGroup()},
				{Key: "root", Value: group.GetRoot()},
				{Key: "names", Value: fmt.Sprint(group.GetTotal())},
				{Key: "resolved", Value: fmt.Sprint(len(group.GetBindings()))},
			}, Note: note}
		}
	}

	node, ok := tree.Node(id)
	if !ok {
		return pane.Inspector{}
	}
	value := f.Values[id]
	fields := []pane.Field{{Key: "expression", Value: id}}
	if value != nil {
		fields = append(fields, pane.Field{Key: "type", Value: value.GetType()})
		if value.GetChildren() > 0 {
			fields = append(fields, pane.Field{Key: "children", Value: fmt.Sprint(value.GetChildren())})
		}
	}
	fields = append(fields, pane.Field{Key: "value", Value: node.Value})
	note := ""
	if value.GetTruncated() {
		note = "the target cut this value"
	}

	return pane.Inspector{Fields: fields, Note: note}
}

// SelectedExpression is the expression of the selected scope row, or "" when
// the selection is a group or nothing.
func SelectedExpression(tree *pane.Tree) string {
	if tree == nil || tree.Selected() == "" || strings.HasPrefix(tree.Selected(), "g:") || strings.HasPrefix(tree.Selected(), "more:") {
		return ""
	}

	return tree.Selected()
}

// HelpView is the help overlay: the keys, then the verbs that have none.
//
// Both halves come from the keymap and the verbs the front answers, so it
// teaches no key that does nothing and no verb the front refuses.
func HelpView(keys tui.Keymap, verbs []flowdebug.Verb, o pane.Options) string {
	lines := []string{pane.Heading("help", "? or esc closes", o.Width, o)}
	lines = append(lines, keys.Help(o.Width, o.Theme)...)

	bound := map[string]bool{}
	for _, b := range keys.Bindings() {
		if verb, ok := strings.CutPrefix(b.Name, verbBindPrefix); ok {
			bound[verb] = true
		}
	}
	var typed []string
	for _, verb := range verbs {
		if bound[verb.Name] {
			continue
		}
		spelling := verb.Name
		if verb.Argument != "" {
			spelling += " " + verb.Argument
		}
		typed = append(typed, "  "+o.Theme.Strong.Render(ui.EscapeControl(spelling))+"  "+ui.EscapeControl(verb.Help))
	}
	if len(typed) > 0 {
		lines = append(lines, "", o.Theme.Header.Render("Type in the console"))
		lines = append(lines, typed...)
	}

	lines = lines[:min(len(lines), o.Height)]
	for i, line := range lines {
		lines[i] = ui.Trim(line, o.Width)
	}

	return strings.Join(lines, "\n")
}
