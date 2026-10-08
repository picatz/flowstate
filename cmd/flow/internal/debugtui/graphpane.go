package debugtui

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// The flow pane draws the program's structure as a ladder, one row per step,
// with what the run has done at each step on the row.
//
// The structure is a function of the program alone and is computed once per
// program (see [Flow]); the state on it is a function of the frame's
// [flowdebug.Overlay], which is how a stop changes the picture without the
// picture being rebuilt. Both are text a viewer is shown, so every piece of
// remote or program text on a row is escaped and passed through the frame's
// redactor before it is drawn.

const (
	// paneFlow is the flow pane's name in the grid, the ring and the hits.
	paneFlow = "flow"

	// flowPrefix is the id prefix a node's hit is registered under; the rest is
	// the node's static address. foldPrefix is the same for the fold mark of a
	// group.
	flowPrefix = "flow/"
	foldPrefix = "fold/"

	// MaxFlowNodes bounds the steps one program's ladder draws. A program past it
	// is drawn as far as the bound reaches and the pane says how many are not.
	MaxFlowNodes = flowdebug.MaxOverlayNodes

	// maxFlowCallDepth is how deep a `call:` is followed into the callee's own
	// steps: as deep as the engine runs one.
	maxFlowCallDepth = v1.MaxCallDepth

	// levelWidth is the cells one level of nesting takes: the rail or corner of
	// the box, the fold mark, and a space.
	levelWidth = 3

	// doubleClickWindow is how close two clicks on one node are to be a double
	// click, by the clock the screen was given.
	doubleClickWindow = 400 * time.Millisecond
)

// NoProgramNote is what the flow pane says of a run it was given no program for.
const NoProgramNote = "no program; pass --program"

// ---- the structure ----

type rowKind uint8

const (
	// rowNode is one step.
	rowNode rowKind = iota
	// rowLabel names a branch of a parallel group or a case of a switch.
	rowLabel
)

// ladderRow is one row of a ladder, in the order the program is written.
type ladderRow struct {
	kind rowKind

	// addr is a step's static address (`pages/page`), and for a label the address
	// of the group it labels.
	addr string

	// id and what are a step's id and its kind as the program spells them; text
	// is a label's words.
	id, what, text string

	depth int

	// parent is the row of the group this one is inside, or -1.
	parent int

	// end is the last row inside this one, and size how many steps that is; a
	// row with end == its own index holds nothing. group reports that it holds
	// something.
	end, size int
	group     bool
}

// ladder is a program's structure flattened to rows.
type ladder struct {
	rows []ladderRow

	// byAddr is the row of each step by static address.
	byAddr map[string]int

	// steps is how many steps are drawn, skipped how many past [MaxFlowNodes] are
	// not, and cut how many calls nested past [maxFlowCallDepth] were not
	// followed.
	steps, skipped, cut int
}

// scope is where the steps of one sibling group are: how far in, what they are
// inside, and the address their own addresses continue.
type scope struct {
	depth, parent int
	prefix        string
	label         string
}

type builder struct {
	l *ladder

	// groups is the scope of a sibling group by its first step, set by the
	// parent that owns the group and consumed when the walk reaches it; of is the
	// scope of a step the walk has announced and not yet visited.
	groups map[*v1.Node]scope
	of     map[*v1.Node]scope
}

// buildLadder flattens a program with the shared walk. A nil program is an empty
// ladder. A `call:` is followed into its callee's steps, whose addresses continue
// the call's own, as the engine writes them without the call's callee name.
func buildLadder(wf *v1.Workflow) *ladder {
	b := &builder{
		l:      &ladder{byAddr: map[string]int{}},
		groups: map[*v1.Node]scope{},
		of:     map[*v1.Node]scope{},
	}
	b.walk(wf.GetSteps(), scope{parent: -1}, 0)
	b.finish()

	return b.l
}

func (b *builder) offer(nodes []*v1.Node, sc scope) {
	if first := firstNode(nodes); first != nil {
		b.groups[first] = sc
	}
}

func firstNode(nodes []*v1.Node) *v1.Node {
	for _, n := range nodes {
		if n != nil {
			return n
		}
	}

	return nil
}

func (b *builder) walk(nodes []*v1.Node, sc scope, calls int) {
	b.offer(nodes, sc)
	v1.WalkNodes(nodes, v1.Walk{
		Steps: func(group []*v1.Node) {
			first := firstNode(group)
			if first == nil {
				return
			}
			sc, ok := b.groups[first]
			if !ok {
				sc = scope{parent: -1}
			}
			delete(b.groups, first)
			for _, n := range group {
				if n != nil {
					b.of[n] = sc
				}
			}
			if sc.label != "" && b.l.steps < MaxFlowNodes {
				b.l.rows = append(b.l.rows, ladderRow{
					kind: rowLabel, addr: strings.TrimSuffix(sc.prefix, "/"), text: sc.label, depth: sc.depth, parent: sc.parent,
				})
			}
		},
		Node: func(n *v1.Node) { b.node(n, calls) },
	})
}

func (b *builder) node(n *v1.Node, calls int) {
	sc := b.of[n]
	delete(b.of, n)
	if b.l.steps >= MaxFlowNodes {
		b.l.skipped++

		return
	}

	addr := sc.prefix + n.GetId()
	index := len(b.l.rows)
	b.l.rows = append(b.l.rows, ladderRow{
		kind: rowNode, addr: addr, id: n.GetId(), what: v1.NodeKind(n), depth: sc.depth, parent: sc.parent,
	})
	b.l.steps++
	if _, seen := b.l.byAddr[addr]; !seen {
		b.l.byAddr[addr] = index
	}

	inside := scope{depth: sc.depth + 1, parent: index, prefix: addr + "/"}
	switch kind := n.GetKind().(type) {
	case *v1.Node_ForEach:
		b.offer(kind.ForEach.GetBody(), inside)
	case *v1.Node_Loop:
		b.offer(kind.Loop.GetBody(), inside)
	case *v1.Node_Parallel:
		branches := kind.Parallel.GetBranches()
		for i, branch := range branches {
			labelled := inside
			if len(branches) > 1 {
				labelled.label = fmt.Sprintf("branch %d", i+1)
			}
			b.offer(branch.GetSteps(), labelled)
		}
	case *v1.Node_Switch:
		cases := len(kind.Switch.GetCases())
		for i, body := range v1.SwitchBodies(kind.Switch) {
			labelled := inside
			labelled.label = "default"
			if i < cases {
				labelled.label = fmt.Sprintf("case %d", i+1)
			}
			b.offer(body, labelled)
		}
	case *v1.Node_Call:
		switch {
		case calls >= maxFlowCallDepth:
			b.l.cut++
		default:
			b.walk(kind.Call.GetWorkflow().GetSteps(), inside, calls+1)
		}
	}
}

// finish gives every row the extent of what it holds.
func (b *builder) finish() {
	rows := b.l.rows
	var open []int
	for i := range rows {
		for len(open) > 0 && rows[open[len(open)-1]].depth >= rows[i].depth {
			rows[open[len(open)-1]].end = i - 1
			open = open[:len(open)-1]
		}
		rows[i].end = i
		if rows[i].kind == rowNode {
			open = append(open, i)
		}
	}
	for _, i := range open {
		rows[i].end = len(rows) - 1
	}
	for i := range rows {
		if rows[i].kind != rowNode {
			continue
		}
		rows[i].group = rows[i].end > i
		for j := i + 1; j <= rows[i].end; j++ {
			if rows[j].kind == rowNode {
				rows[i].size++
			}
		}
	}
}

// visRow is one line of the ladder as drawn: a row, or the closing line of the
// group that row opens.
type visRow struct {
	index int
	close bool
}

// visible lists the lines the ladder has when the groups in folded are closed.
func (l *ladder) visible(folded map[string]bool) []visRow {
	var out []visRow
	var open []int
	closeTo := func(depth int) {
		for len(open) > 0 && l.rows[open[len(open)-1]].depth >= depth {
			out = append(out, visRow{index: open[len(open)-1], close: true})
			open = open[:len(open)-1]
		}
	}
	for i := 0; i < len(l.rows); {
		r := l.rows[i]
		closeTo(r.depth)
		out = append(out, visRow{index: i})
		if r.kind == rowNode && r.group {
			if folded[r.addr] {
				i = r.end + 1

				continue
			}
			open = append(open, i)
		}
		i++
	}
	closeTo(0)

	return out
}

// ---- the state of the pane ----

// Flow is the flow pane's state: the structure it draws, the selection, which
// groups are folded, and whether the person has taken the view from the run.
//
// The structure is built from the frame's program the first time it is drawn and
// again only when the program or its digest changes, never per stop. A Flow is
// used from one goroutine, the screen's.
type Flow struct {
	program *v1.Workflow
	digest  string
	ladder  *ladder

	// Selected is the static address of the selected step, or empty, which means
	// the held one.
	Selected string

	// Folded holds the groups drawn closed, by static address.
	Folded map[string]bool

	// Scrolled reports that the person moved the view, so it stays where Top puts
	// it instead of following the held step. Top is the first line drawn then.
	Scrolled bool
	Top      int

	heldSeen string

	clickedAddr string
	clickedAt   time.Time
}

// NewFlow returns an empty flow pane state.
func NewFlow() *Flow { return &Flow{Folded: map[string]bool{}} }

// ladderOf is the structure of the frame's program.
func (f *Flow) ladderOf(frame flowdebug.Frame) *ladder {
	digest := frame.Snapshot.GetIrDigest()
	if f.ladder == nil || f.program != frame.Program || f.digest != digest {
		f.program, f.digest = frame.Program, digest
		f.ladder = buildLadder(frame.Program)
	}

	return f.ladder
}

// Apply folds a new frame in. A selection the program no longer has is dropped,
// and while the person has not taken the view the selection follows the held
// step.
func (f *Flow) Apply(frame flowdebug.Frame) {
	l := f.ladderOf(frame)
	if _, ok := l.byAddr[f.Selected]; !ok {
		f.Selected = ""
	}
	held := frame.Overlay.Held
	if _, ok := l.byAddr[held]; !ok {
		held = ""
	}
	if held != "" && held != f.heldSeen && (f.Selected == "" || !f.Scrolled) {
		f.Selected = held
	}
	f.heldSeen = held
}

// Follow gives the view back to the run: it centres on the held step again.
func (f *Flow) Follow() { f.Scrolled = false }

// selected is the address of the step commands apply to.
func (f *Flow) selected(frame flowdebug.Frame) string {
	return cmp.Or(f.Selected, frame.Overlay.Held)
}

// flowView is the ladder as it is drawn right now.
type flowView struct {
	l      *ladder
	vis    []visRow
	folded map[string]bool

	// held and sel are the lines of the held and the selected step, or of the
	// closed group that hides it; -1 when there is none.
	held, sel int

	// start is the first line drawn.
	start int
}

// view lays the ladder out for a pane showing rows lines.
func (f *Flow) view(frame flowdebug.Frame, rows int) flowView {
	l := f.ladderOf(frame)
	v := flowView{l: l, vis: l.visible(f.Folded), folded: f.Folded, held: -1, sel: -1}
	v.held = v.lineOf(frame.Overlay.Held)
	v.sel = v.lineOf(f.selected(frame))

	rows = max(1, rows)
	switch last := max(0, len(v.vis)-rows); {
	case f.Scrolled:
		v.start = max(0, min(f.Top, last))
	case v.held >= 0:
		v.start = max(0, min(v.held-(rows-1)/2, last))
	}

	return v
}

// lineOf is the line the step at addr is drawn on, or the line of the outermost
// closed group around it, or -1.
func (v flowView) lineOf(addr string) int {
	index, ok := v.l.byAddr[addr]
	if !ok {
		return -1
	}
	shown := index
	for p := v.l.rows[index].parent; p >= 0; p = v.l.rows[p].parent {
		if v.folded[v.l.rows[p].addr] {
			shown = p
		}
	}

	return slices.IndexFunc(v.vis, func(r visRow) bool { return !r.close && r.index == shown })
}

// steps lists the lines that are steps.
func (v flowView) steps() []int {
	var out []int
	for i, r := range v.vis {
		if !r.close && v.l.rows[r.index].kind == rowNode {
			out = append(out, i)
		}
	}

	return out
}

// scrollBy moves the view by delta lines and takes it from the run.
func (f *Flow) scrollBy(frame flowdebug.Frame, rows, delta int) {
	v := f.view(frame, rows)
	if !f.Scrolled {
		f.Scrolled, f.Top = true, v.start
	}
	f.Top = max(0, min(f.Top+delta, max(0, len(v.vis)-max(1, rows))))
}

// reveal keeps the selected line in view, taking the view from the run.
func (f *Flow) reveal(frame flowdebug.Frame, rows int) {
	rows = max(1, rows)
	v := f.view(frame, rows)
	if !f.Scrolled {
		f.Scrolled, f.Top = true, v.start
	}
	switch {
	case v.sel < 0:
	case v.sel < f.Top:
		f.Top = v.sel
	case v.sel >= f.Top+rows:
		f.Top = v.sel - rows + 1
	}
	f.Top = max(0, min(f.Top, max(0, len(v.vis)-rows)))
}

// ---- drawing ----

// ladderRows is the lines of the ladder a pane h lines tall shows.
func ladderRows(h int, footer bool) int {
	rows := h - 1
	if footer {
		rows--
	}

	return max(0, rows)
}

// flowFooter is the one line under the ladder, or empty: what is not drawn and
// what could not be placed.
func flowFooter(f flowdebug.Frame, l *ladder) string {
	var notes []string
	if l.skipped > 0 {
		notes = append(notes, fmt.Sprintf("%d more not drawn", l.skipped))
	}
	if l.cut > 0 {
		notes = append(notes, fmt.Sprintf("%d call(s) nested past %d not followed", l.cut, maxFlowCallDepth))
	}
	unplaced := 0
	for address := range f.Overlay.States {
		if _, ok := l.byAddr[address]; !ok {
			unplaced++
		}
	}
	if unplaced > 0 {
		notes = append(notes, fmt.Sprintf("%d observed step(s) are not in this program", unplaced))
	}

	return strings.Join(notes, "; ")
}

// FlowView is the flow pane: the heading and the ladder, windowed to the height
// and kept around the held step unless the person moved the view. A frame with no
// program is one sentence saying so.
//
// Each step line registers a [pane.KindNode] hit under flowPrefix and its static
// address, and each group's fold mark one under foldPrefix.
func FlowView(flow *Flow, f flowdebug.Frame, loaded bool, o pane.Options) string {
	if flow == nil {
		flow = NewFlow()
	}
	if !loaded {
		return pane.Heading(paneFlow, "", o.Width, o)
	}
	if f.Program == nil {
		return pane.Heading(paneFlow, "", o.Width, o) + "\n" + o.Theme.Muted.Render(wrapWords(NoProgramNote, o.Width))
	}

	l := flow.ladderOf(f)
	footer := flowFooter(f, l)
	note := fmt.Sprintf("%d steps", l.steps)
	switch {
	case l.skipped > 0:
		note = fmt.Sprintf("%d more not drawn", l.skipped)
	case f.Partial:
		note = "earlier steps not shown"
	}
	heading := pane.Heading(paneFlow, note, o.Width, o)
	if len(l.rows) == 0 {
		return heading + "\n" + o.Theme.Muted.Render("  the program declares no steps")
	}

	rows := ladderRows(o.Height, footer != "")
	v := flow.view(f, rows)
	marks := breakpointMarks(f.Snapshot)
	selected := flow.selected(f)

	end := min(len(v.vis), v.start+rows)
	lines := make([]string, 0, o.Height)
	lines = append(lines, heading)
	for i := v.start; i < end; i++ {
		line, fold := ladderLine(v, i, flow, f, marks, selected, o)
		lines = append(lines, line)
		if r := v.l.rows[v.vis[i].index]; r.kind == rowNode && !v.vis[i].close {
			y := o.Origin.Y + len(lines) - 1
			o.Hits.Add(pane.Rect{X: o.Origin.X, Y: y, W: o.Width, H: 1}, flowPrefix+r.addr, pane.KindNode)
			if fold {
				o.Hits.Add(pane.Rect{X: o.Origin.X + 2 + r.depth*levelWidth, Y: y, W: levelWidth, H: 1}, foldPrefix+r.addr, pane.KindNode)
			}
		}
	}
	for len(lines) < 1+rows {
		lines = append(lines, "")
	}
	if footer != "" {
		lines = append(lines, o.Theme.Warning.Render(ui.Trim(ui.EscapeControl(footer), o.Width)))
	}

	return strings.Join(lines, "\n")
}

// ladderLine draws line i of the ladder, and reports whether it carries a fold
// mark.
func ladderLine(v flowView, i int, flow *Flow, f flowdebug.Frame, marks map[string]string, selected string, o pane.Options) (string, bool) {
	vr := v.vis[i]
	r := v.l.rows[vr.index]
	rail := o.Theme.Muted.Render(o.Symbols.Rail + "  ")
	rails := strings.Repeat(rail, r.depth)

	switch {
	case vr.close:
		return ui.Trim("  "+rails+o.Theme.Muted.Render(o.Symbols.Close+strings.Repeat(o.Symbols.Divider, levelWidth-1)), o.Width), false
	case r.kind == rowLabel:
		return ui.Trim("  "+rails+o.Theme.Muted.Render(ui.EscapeControl(r.text)), o.Width), false
	}

	state := f.Overlay.State(r.addr)
	glyph, style, tag := nodeMark(state, o)
	if state == flowdebug.NodePending && f.Partial && (v.held < 0 || i < v.held) {
		// Nothing was seen of a step that may have run before the observations
		// that were kept: not drawn as one that has not.
		glyph, style, tag = "?", o.Theme.Muted, "not shown"
	}
	if r.addr == f.Overlay.Held {
		if state != flowdebug.NodeHeld {
			tag = flowdebug.NodeHeld.String()
		}
		style = o.Theme.Strong
		if state == flowdebug.NodeFailed {
			style = o.Theme.Danger
		}
	}

	gutter := " "
	if r.addr == selected {
		gutter = cmp.Or(strings.TrimSpace(o.Symbols.Arrow), ">")
		if o.Focused {
			gutter = o.Theme.Accent.Render(gutter)
		} else {
			gutter = o.Theme.Muted.Render(gutter)
		}
	}
	armed := " "
	if _, ok := marks[r.addr]; ok {
		armed = o.Theme.Danger.Render(o.Symbols.Bullet)
	} else if _, ok := marks[r.id]; ok {
		armed = o.Theme.Danger.Render(o.Symbols.Bullet)
	}

	label := ui.EscapeControl(f.RedactText(r.id))
	if state == flowdebug.NodeHeld || r.addr == selected {
		label = o.Theme.Strong.Render(label)
	} else if state == flowdebug.NodePending || state == flowdebug.NodeSkipped {
		label = o.Theme.Muted.Render(label)
	}

	mark := "   "
	if r.group {
		fold := o.Symbols.Expanded
		if flow.Folded[r.addr] {
			fold = o.Symbols.Collapsed
		}
		mark = o.Theme.Muted.Render(o.Symbols.Open+fold) + " "
	}

	// The word comes before the kind: a line cut to a narrow pane loses the kind
	// first, and the word is the part that is not carried by colour.
	line := gutter + armed + rails + mark + style.Render(glyph) + " " + label
	if r.group && flow.Folded[r.addr] {
		line += o.Theme.Muted.Render(fmt.Sprintf(" (+%d)", r.size))
	}
	if tag != "" {
		line += " " + style.Render(tag)
	}
	if what := ui.EscapeControl(f.RedactText(r.what)); what != "" {
		line += " " + o.Theme.Muted.Render(what)
	}

	return ui.Trim(line, o.Width), r.group
}

// nodeMark is the glyph, colour and word a state is drawn with: the step pane's
// marks, with the held step an arrow-headed run mark in the strong role. The
// word is there because a pending and a waiting step share a glyph in ASCII, and
// colour is never the only carrier.
func nodeMark(state flowdebug.NodeState, o pane.Options) (string, lipgloss.Style, string) {
	switch state {
	case flowdebug.NodeHeld:
		return o.Symbols.Running, o.Theme.Strong, "held"
	case flowdebug.NodeRunning:
		return o.Symbols.Running, o.Theme.Info, "running"
	case flowdebug.NodeWaiting:
		return o.Symbols.Waiting, o.Theme.Info, "waiting"
	case flowdebug.NodeDone:
		return o.Symbols.Success, o.Theme.Success, ""
	case flowdebug.NodeTolerated:
		return o.Symbols.Warning, o.Theme.Warning, "tolerated"
	case flowdebug.NodeFailed:
		return o.Symbols.Failure, o.Theme.Danger, ""
	case flowdebug.NodeSkipped:
		return o.Symbols.Skipped, o.Theme.Muted, "skipped"
	default:
		return o.Symbols.Waiting, o.Theme.Muted, ""
	}
}

// breakpointMarks maps the static addresses and bare ids the session's armed
// stopping breakpoints cover to the name `delete` takes to remove them. A
// logpoint stops nothing and an unarmed breakpoint is not in force, so neither is
// marked.
func breakpointMarks(snapshot *v1.DebugSnapshot) map[string]string {
	marks := map[string]string{}
	// visits bounds the entries looked at, not only the entries kept: a snapshot
	// may report a thousand breakpoints of a thousand sites each.
	visits := 0
	set := func(key, name string) {
		if _, ok := marks[key]; !ok && key != "" && len(marks) < flowdebug.MaxOverlayNodes {
			marks[key] = name
		}
	}
	for _, bp := range snapshot.GetBreakpoints() {
		definition := bp.GetDefinition()
		if !bp.GetVerified() || definition.GetLogMessage() != "" {
			continue
		}
		name := cmp.Or(definition.GetStep(), bp.GetId())
		set(name, name)
		for _, site := range bp.GetSites() {
			if visits++; visits > flowdebug.MaxOverlayNodes {
				return marks
			}
			set(strings.Join(site.GetPath(), "/"), name)
		}
	}

	return marks
}

// ---- keys, clicks and commands ----

// flowRows is how many ladder lines the flow pane shows now, or zero when it is
// not drawn.
func (m Model) flowRows() int {
	rect, ok := m.cell(paneFlow)
	if !ok {
		return 0
	}
	footer := false
	if f := m.screen.Frame; m.screen.Loaded && f.Program != nil {
		footer = flowFooter(f, m.screen.Flow.ladderOf(f)) != ""
	}

	return ladderRows(rect.H, footer)
}

// navigateFlow handles a navigation key in the flow pane.
func (m Model) navigateFlow(name string) (tea.Model, tea.Cmd) {
	flow, frame := m.screen.Flow, m.screen.Frame
	if !m.screen.Loaded || frame.Program == nil {
		return m, nil
	}
	rows := max(1, m.flowRows())
	v := flow.view(frame, rows)
	steps := v.steps()
	if len(steps) == 0 {
		return m, nil
	}

	at := slices.Index(steps, v.sel)

	pick := func(i int) {
		i = max(0, min(i, len(steps)-1))
		flow.Selected = v.l.rows[v.vis[steps[i]].index].addr
		flow.reveal(frame, rows)
	}

	switch name {
	case bindUp:
		pick(at - 1)
	case bindDown:
		pick(at + 1)
	case bindPageUp:
		pick(at - rows)
	case bindPageDown:
		pick(at + rows)
	case bindHome:
		pick(0)
	case bindEnd:
		pick(len(steps) - 1)

	case bindCollapse:
		if v.sel < 0 {
			break
		}
		row := v.l.rows[v.vis[v.sel].index]
		switch {
		case row.group && !flow.Folded[row.addr]:
			flow.Folded[row.addr] = true
		case row.parent >= 0:
			flow.Selected = v.l.rows[row.parent].addr
		}
		flow.reveal(frame, rows)
	case bindExpand:
		if v.sel < 0 {
			break
		}
		if row := v.l.rows[v.vis[v.sel].index]; flow.Folded[row.addr] {
			delete(flow.Folded, row.addr)
		}
		flow.reveal(frame, rows)

	case bindToggle:
		return m.flowUntil()
	}

	return m, nil
}

// offers reports that the front answers a verb.
func (m Model) offers(verb string) bool {
	return slices.ContainsFunc(m.cfg.Verbs, func(v flowdebug.Verb) bool { return v.Name == verb })
}

// moves reports that a line resumes the run or steps it back, which is when the
// view of the run is given back to it.
func (m Model) moves(line string) bool {
	word, _, _ := strings.Cut(strings.TrimSpace(line), " ")
	if word == "" {
		return true
	}

	return slices.ContainsFunc(m.cfg.Verbs, func(v flowdebug.Verb) bool {
		return v.Moves && (v.Name == word || slices.Contains(v.Aliases, word))
	})
}

// flowStep is the selected step as a command names it. It refuses, with a toast,
// where there is none, where the front does not answer verb, and where the step's
// name is one the session withholds or one a command line cannot carry: the line
// is echoed to the console and a withheld name must not be.
func (m *Model) flowStep(verb string) (ladderRow, bool) {
	frame, flow := m.screen.Frame, m.screen.Flow
	if !m.offers(verb) {
		m.toast(ui.ToneWarning, "this front does not answer "+verb)

		return ladderRow{}, false
	}
	if !m.screen.Loaded || frame.Program == nil {
		m.toast(ui.ToneWarning, NoProgramNote)

		return ladderRow{}, false
	}
	l := flow.ladderOf(frame)
	index, ok := l.byAddr[flow.selected(frame)]
	if !ok {
		m.toast(ui.ToneWarning, "select a step in the flow first")

		return ladderRow{}, false
	}
	row := l.rows[index]
	if frame.RedactText(row.addr) != row.addr || strings.ContainsFunc(row.addr, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) }) {
		m.toast(ui.ToneWarning, "that step's name is withheld or cannot be typed; name it in the console")

		return ladderRow{}, false
	}

	return row, true
}

// flowUntil runs to the selected step.
func (m Model) flowUntil() (tea.Model, tea.Cmd) {
	row, ok := m.flowStep("until")
	if !ok {
		return m, nil
	}
	cmd := m.run("until " + row.addr)

	return m, cmd
}

// flowBreak sets a breakpoint on the selected step, or removes the one that is
// there.
func (m Model) flowBreak() (tea.Model, tea.Cmd) {
	row, ok := m.flowStep("break")
	if !ok {
		return m, nil
	}
	marks := breakpointMarks(m.screen.Frame.Snapshot)
	name, armed := marks[row.addr]
	if !armed {
		name, armed = marks[row.id]
	}
	if !armed {
		cmd := m.run("break " + row.addr)

		return m, cmd
	}
	if !m.offers("delete") {
		m.toast(ui.ToneWarning, "this front does not answer delete")

		return m, nil
	}
	if strings.ContainsFunc(name, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) }) {
		m.toast(ui.ToneWarning, "that breakpoint's name cannot be typed; delete it in the console")

		return m, nil
	}
	cmd := m.run("delete " + name)

	return m, cmd
}

// clickNode handles a click on a step or on a group's fold mark. Two clicks on
// one step within [doubleClickWindow] of the screen's clock run to it; a screen
// given no clock never sees a double click.
func (m Model) clickNode(id string, right bool) (tea.Model, tea.Cmd) {
	flow := m.screen.Flow
	m.setFocus(paneFlow)

	if addr, ok := strings.CutPrefix(id, foldPrefix); ok && !right {
		flow.Selected = addr
		if flow.Folded[addr] {
			delete(flow.Folded, addr)
		} else {
			flow.Folded[addr] = true
		}

		return m, nil
	}
	addr := strings.TrimPrefix(strings.TrimPrefix(id, foldPrefix), flowPrefix)
	flow.Selected = addr
	if right {
		return m.flowBreak()
	}

	double := false
	if m.cfg.Now != nil {
		now := m.cfg.Now()
		double = flow.clickedAddr == addr && !flow.clickedAt.IsZero() &&
			now.Sub(flow.clickedAt) >= 0 && now.Sub(flow.clickedAt) <= doubleClickWindow
		flow.clickedAddr, flow.clickedAt = addr, now
		if double {
			flow.clickedAddr, flow.clickedAt = "", time.Time{}
		}
	}
	if double {
		return m.flowUntil()
	}

	return m, nil
}
