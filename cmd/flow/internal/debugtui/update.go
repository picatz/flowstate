package debugtui

import (
	"slices"
	"strings"
	"unicode"

	tea "charm.land/bubbletea/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// setFocus moves focus, and remembers which content pane a folded layout shows.
func (m *Model) setFocus(name string) {
	m.ring = m.ring.Set(name)
	m.screen.Focus = m.ring.Current()
	if slices.Contains(contentPanes, name) {
		m.screen.Pane = name
	}
}

// cell is where a pane is drawn now, if it is.
func (m Model) cell(name string) (pane.Rect, bool) {
	g, err := m.screen.geometry()
	if err != nil {
		return pane.Rect{}, false
	}
	for _, c := range g.layout.Cells {
		if c.Pane == name {
			return c.Rect, true
		}
	}

	return pane.Rect{}, false
}

// scopeRows is how many tree rows the scope pane shows, or zero when it is not
// drawn.
func (m Model) scopeRows() int {
	rect, ok := m.cell(paneScope)
	if !ok {
		return 0
	}
	rows := rect.H - 1
	if m.screen.Frame.ScopeNote != "" {
		rows--
	}

	return max(0, rows)
}

// revealSelection keeps the selected row on screen.
func (m Model) revealSelection() {
	if rows := m.scopeRows(); rows > 0 {
		m.screen.Tree.Reveal(rows)
	}
}

// key handles a key press.
func (m Model) key(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	// A toast lasts until the next key, so what is on screen is a function of
	// the messages and not of how long ago one arrived.
	m.screen.Toast = m.screen.Toast.Clear()
	name := tui.KeyName(msg)

	if _, err := m.screen.geometry(); err != nil && name != "ctrl+c" && name != "ctrl+d" {
		// Only the message "too small" is on screen; a key that acted would act
		// on something nobody can see.
		return m, nil
	}

	if m.screen.Help {
		switch name {
		case "esc", "?":
			m.screen.Help, m.screen.HelpTop = false, 0
		case "ctrl+c":
			return m.leave(OutcomeInterrupt)
		case "ctrl+d":
			return m.leave(OutcomeLeave)
		case "q":
			// The exit keys the overlay teaches do what it says they do.
			m.screen.Help, m.screen.HelpTop = false, 0
			cmd := m.quit()

			return m, cmd
		case "up", "k":
			m.scrollHelp(-1)
		case "down", "j":
			m.scrollHelp(1)
		case "pgup":
			m.scrollHelp(-m.helpPage())
		case "pgdown", "space":
			m.scrollHelp(m.helpPage())
		case "home", "g":
			m.scrollHelp(-1 << 20)
		case "end", "G":
			m.scrollHelp(1 << 20)
		}

		return m, nil
	}

	if m.screen.Focus == paneConsole {
		return m.consoleKey(name, msg)
	}

	binding, ok := m.keys.Match(name)
	if !ok {
		return m, nil
	}

	return m.act(binding)
}

// act performs a binding.
func (m Model) act(b tui.Binding) (tea.Model, tea.Cmd) {
	if verb, ok := strings.CutPrefix(b.Name, verbBindPrefix); ok {
		if verb == "goto" {
			// A point is an argument no key supplies: the console opens with
			// the verb written and the point left to type.
			m.screen.Console.Text = "goto "
			m.setFocus(paneConsole)

			return m, nil
		}
		cmd := m.run(verb)
		return m, cmd
	}

	switch b.Name {
	case bindFocusNext:
		m.setFocus(m.ring.Next().Current())
	case bindFocusPrev:
		m.setFocus(m.ring.Prev().Current())
	case bindConsole:
		m.setFocus(paneConsole)
	case bindInspect:
		expression := SelectedExpression(m.screen.Tree)
		if expression == "" {
			m.toast(ui.ToneWarning, "select a name in the scope first")

			break
		}
		if strings.ContainsFunc(expression, unicode.IsControl) {
			// The expression is the target's text; a control character in it
			// is not something to put on an input line.
			m.toast(ui.ToneWarning, "that name has a control character; type it to inspect it")

			break
		}
		m.screen.Console.Text = "inspect " + expression
		m.setFocus(paneConsole)
	case bindUntil:
		return m.flowUntil()
	case bindBreak:
		if m.screen.Focus == paneSource {
			return m.sourceBreak(m.screen.Source.Selected)
		}

		return m.flowBreak()
	case bindHelp:
		m.screen.Help = true
	case bindQuit:
		cmd := m.quit()
		return m, cmd
	case bindInterrupt:
		return m.leave(OutcomeInterrupt)
	case bindLeave:
		return m.leave(OutcomeLeave)
	default:
		return m.navigate(b.Name)
	}

	return m, nil
}

// scrollHelp moves the help overlay by delta lines, within the lines it has.
func (m *Model) scrollHelp(delta int) {
	g, err := m.screen.geometry()
	if err != nil {
		return
	}
	lines := len(helpLines(m.screen.Keys, m.screen.Verbs, pane.Options{Width: g.body.W}))
	room := max(0, g.overlayRows()-1)
	m.screen.HelpTop = max(0, min(m.screen.HelpTop+delta, max(0, lines-room)))
}

// helpPage is how far a page key moves the overlay.
func (m Model) helpPage() int { return max(1, m.screen.Size.H-4) }

// quit detaches the run, or ends the screen if the run is already over. The
// outcome is set when the driver reports the detach was taken.
func (m *Model) quit() tea.Cmd {
	if m.ended() {
		m.quitting, m.outcome = true, OutcomeEnded

		return tea.Quit
	}

	return m.run("detach")
}

// navigate moves within the focused pane.
func (m Model) navigate(name string) (tea.Model, tea.Cmd) {
	switch m.screen.Focus {
	case paneFlow:
		return m.navigateFlow(name)

	case paneSource:
		return m.navigateSource(name)

	case paneSteps:
		step := map[string]int{bindUp: -1, bindDown: 1, bindPageUp: -m.stepRows(), bindPageDown: m.stepRows()}[name]
		m.scrollSteps(step)

		return m, nil

	case paneScope:
		tree, rows := m.screen.Tree, max(1, m.scopeRows())
		var request pane.Request
		var load bool
		switch name {
		case bindUp:
			tree.Move(-1)
		case bindDown:
			tree.Move(1)
		case bindPageUp:
			tree.Move(-rows)
		case bindPageDown:
			tree.Move(rows)
		case bindHome:
			tree.Home()
		case bindEnd:
			tree.End()
		case bindToggle:
			request, load = tree.Activate(tree.Selected())
		case bindExpand:
			request, load = tree.Expand(tree.Selected())
		case bindCollapse:
			if tree.Open(tree.Selected()) {
				tree.Collapse(tree.Selected())
			} else {
				tree.Parent()
			}
		}
		tree.Reveal(rows)
		if load {
			cmd := m.pageCmd(request)
			return m, cmd
		}
	}

	return m, nil
}

// stepRows is how many step lines the steps pane shows.
func (m Model) stepRows() int {
	rect, _ := m.cell(paneSteps)

	return max(1, rect.H-1)
}

// scrollSteps moves the step window, within the range the frame allows.
func (m *Model) scrollSteps(delta int) {
	lo, hi := StepScrollRange(m.screen.Frame, m.stepRows(), m.cfg.Style)
	m.screen.StepScroll = max(lo, min(hi, m.screen.StepScroll+delta))
}

// consoleKey handles a key while the console has focus: it is typing, except
// for the few keys that leave.
func (m Model) consoleKey(name string, msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	con := &m.screen.Console
	switch name {
	case "esc":
		m.setFocus(m.screen.Pane)
	case "ctrl+c":
		return m.leave(OutcomeInterrupt)
	case "ctrl+d":
		if con.Text == "" {
			return m.leave(OutcomeLeave)
		}
	case "enter":
		line := con.Submit()
		if line == "" {
			return m, nil
		}

		return m.submit(line)
	case "backspace":
		con.Backspace()
	case "ctrl+u":
		con.Clear()
	case "ctrl+w":
		con.DeleteWord()
	case "up":
		con.Older()
	case "down":
		con.Newer()
	case "tab":
		if con.Text == "" {
			m.setFocus(m.ring.Next().Current())

			break
		}
		if m.screen.Busy == "" {
			m.screen.Busy = busyCompleting

			cmd := m.completeCmd(con.Text)

			return m, cmd
		}
	case "shift+tab":
		m.setFocus(m.ring.Prev().Current())
	default:
		if msg.Text != "" {
			con.Insert(msg.Text)
		}
	}

	return m, nil
}

// submit runs a typed line. The words that leave are the ones `flow debug
// attach` gives the same meaning at its prompt.
func (m Model) submit(line string) (tea.Model, tea.Cmd) {
	switch line {
	case "disconnect":
		return m.leave(OutcomeDisconnect)
	case "quit", "q", "exit", "detach":
		cmd := m.quit()
		return m, cmd
	}

	cmd := m.run(line)

	return m, cmd
}

// click handles a mouse click. It is resolved against what the screen draws
// right now, so a click can only land on something that is on screen; one
// outside every hit is ignored.
func (m Model) click(mouse tea.Mouse) (tea.Model, tea.Cmd) {
	if mouse.Button != tea.MouseLeft && mouse.Button != tea.MouseRight {
		return m, nil
	}
	_, hits := m.screen.Draw(m.cfg.Style)
	hit, ok := hits.At(mouse.X, mouse.Y)
	if !ok {
		return m, nil
	}
	if hit.Kind == pane.KindNode {
		return m.clickNode(hit.ID, mouse.Button == tea.MouseRight)
	}
	if mouse.Button != tea.MouseLeft {
		// A right click means something only on a step.
		return m, nil
	}
	if id, ok := strings.CutPrefix(hit.ID, gutterPrefix); ok {
		return m.clickSource(id, true)
	}
	if id, ok := strings.CutPrefix(hit.ID, sourcePrefix); ok {
		return m.clickSource(id, false)
	}

	switch hit.Kind {
	case pane.KindTab:
		m.setFocus(hit.ID)

	case pane.KindInput:
		m.setFocus(paneConsole)

	case pane.KindPoint:
		point, ok := strings.CutPrefix(hit.ID, pointPrefix)
		if !ok {
			break
		}
		cmd := m.run("goto " + point)

		return m, cmd

	case pane.KindPane, pane.KindHeading:
		if hit.ID == "help" {
			m.screen.Help = false

			break
		}
		if name, ok := strings.CutPrefix(hit.ID, panePrefix); ok && name != paneInspector {
			m.setFocus(name)
		}

	case pane.KindRow, pane.KindMore:
		id, ok := strings.CutPrefix(hit.ID, scopePrefix)
		if !ok {
			break
		}
		m.setFocus(paneScope)
		request, load := m.screen.Tree.Activate(id)
		m.screen.Tree.Reveal(max(1, m.scopeRows()))
		if load {
			cmd := m.pageCmd(request)
			return m, cmd
		}
	}

	return m, nil
}

// wheel scrolls the pane under the pointer.
func (m Model) wheel(mouse tea.Mouse) (tea.Model, tea.Cmd) {
	delta := wheelRows
	switch mouse.Button {
	case tea.MouseWheelUp:
		delta = -wheelRows
	case tea.MouseWheelDown:
	default:
		return m, nil
	}

	_, hits := m.screen.Draw(m.cfg.Style)
	hit, ok := hits.At(mouse.X, mouse.Y)
	if !ok {
		return m, nil
	}

	switch {
	case hit.ID == "help":
		m.scrollHelp(delta)
	case strings.HasPrefix(hit.ID, scopePrefix), hit.ID == panePrefix+paneScope:
		m.screen.Tree.Scroll(delta, max(1, m.scopeRows()))
	case strings.HasPrefix(hit.ID, sourcePrefix), strings.HasPrefix(hit.ID, gutterPrefix), hit.ID == panePrefix+paneSource:
		if rows := m.sourceRows(); rows > 0 && m.screen.Loaded {
			m.screen.Source.scrollBy(m.screen.Frame, rows, delta)
		}
	case hit.ID == panePrefix+paneSteps:
		m.scrollSteps(delta)
	case strings.HasPrefix(hit.ID, flowPrefix), strings.HasPrefix(hit.ID, foldPrefix), hit.ID == panePrefix+paneFlow:
		if rows := m.flowRows(); rows > 0 && m.screen.Loaded {
			m.screen.Flow.scrollBy(m.screen.Frame, rows, delta)
		}
	}

	return m, nil
}
