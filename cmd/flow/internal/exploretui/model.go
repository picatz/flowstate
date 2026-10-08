package exploretui

import (
	"context"
	"errors"
	"strings"

	tea "charm.land/bubbletea/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Config is what a screen is opened over.
type Config struct {
	// Load reads the graph. It is called for the first screen and again for each
	// refresh, and it is allowed to be slow: the screen asks it from a command,
	// never from a key press.
	Load func(context.Context) (*v1.Graph, error)

	// Source says what Load reads, for the header.
	Source string

	Style Style

	// Size is the terminal's size until the first resize message says otherwise.
	Size tui.Size
}

// wheelRows is how far one notch of the wheel scrolls.
const wheelRows = 3

// graphMsg is a finished read.
type graphMsg struct {
	seq   uint64
	graph *v1.Graph
	err   error
}

// Model is the explorer screen.
type Model struct {
	cfg  Config
	ctx  context.Context
	keys tui.Keymap

	screen Screen

	// seq numbers reads; only the latest is applied, so a slow read cannot
	// overwrite a newer one.
	seq uint64

	quitting bool
}

// New returns the screen over cfg. ctx bounds every read it starts.
func New(ctx context.Context, cfg Config) (Model, error) {
	if cfg.Load == nil {
		return Model{}, errors.New("exploretui: a screen needs a loader")
	}
	keys, err := NewKeymap()
	if err != nil {
		return Model{}, err
	}

	return Model{
		cfg: cfg, ctx: ctx, keys: keys, seq: 1,
		screen: Screen{Size: cfg.Size, Source: cfg.Source, Tree: pane.NewTree(nil), Keys: keys, Loading: true},
	}, nil
}

// Done reports that the screen has asked to quit.
func (m Model) Done() bool { return m.quitting }

// Screen is the state the screen draws.
func (m Model) Screen() Screen { return m.screen }

// Init starts the first read.
func (m Model) Init() tea.Cmd { return m.read(m.seq) }

// View draws the screen.
func (m Model) View() tea.View {
	text, _ := m.screen.Draw(m.cfg.Style)
	view := tea.NewView(text)
	view.AltScreen = true
	view.MouseMode = tea.MouseModeCellMotion
	view.WindowTitle = "flow explore"

	return view
}

// Update folds one message in.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.screen.Size = tui.Size{W: msg.Width, H: msg.Height}
		m.reveal()

		return m, nil
	case tea.KeyPressMsg:
		return m.key(msg)
	case tea.MouseClickMsg:
		return m.click(tea.Mouse(msg))
	case tea.MouseWheelMsg:
		return m.wheel(tea.Mouse(msg))
	case tea.InterruptMsg:
		return m.leave()
	case graphMsg:
		return m.loaded(msg)
	}

	return m, nil
}

func (m Model) leave() (tea.Model, tea.Cmd) {
	m.quitting = true

	return m, tea.Quit
}

// toast shows a refusal or a problem until the next key press.
func (m *Model) toast(tone ui.Tone, text string) {
	m.screen.Toast = m.screen.Toast.Show(tone, text)
}

// read starts a read of the graph.
func (m Model) read(seq uint64) tea.Cmd {
	ctx, load := m.ctx, m.cfg.Load

	return func() tea.Msg {
		g, err := load(ctx)

		return graphMsg{seq: seq, graph: g, err: err}
	}
}

// loaded folds a finished read in. The rows that were open stay open and the
// selection stays where its node does, so a refresh is the same view with newer
// facts; a failed read leaves what was on screen and says so.
func (m Model) loaded(msg graphMsg) (tea.Model, tea.Cmd) {
	if msg.seq != m.seq {
		return m, nil
	}
	m.screen.Loading = false
	if msg.err != nil {
		if m.ctx.Err() != nil {
			return m, nil
		}
		m.screen.Problem = msg.err.Error()
		m.toast(ui.ToneDanger, "cannot read the graph: "+msg.err.Error())

		return m, nil
	}
	m.screen.Problem = ""

	tree := m.screen.Tree
	rows, selected := tree.Rows(), tree.Selected()
	// Rows come parents first, which is the order they must be opened again in.
	var opened []string
	for _, r := range rows {
		if r.Open && r.Kind == pane.RowNode {
			opened = append(opened, r.ID)
		}
	}

	m.screen.Index = NewIndex(msg.graph)
	tree.SetRoots(m.screen.Index.Roots())
	load := m.screen.Index.Loader()
	for _, id := range opened {
		if request, ok := tree.Expand(id); ok {
			_ = tree.Load(load, request)
		}
	}
	tree.Select(selected)
	m.reveal()

	return m, nil
}

// graphRect is where the tree pane is drawn now, if it is.
func (m Model) graphRect() (pane.Rect, bool) {
	g, err := m.screen.geometry()
	if err != nil {
		return pane.Rect{}, false
	}
	for _, c := range g.layout.Cells {
		if c.Pane == paneGraph {
			return c.Rect, true
		}
	}

	return pane.Rect{}, false
}

// treeRows is how many tree rows the graph pane shows, or zero when it is not
// drawn.
func (m Model) treeRows() int {
	rect, ok := m.graphRect()
	if !ok {
		return 0
	}

	return max(0, rect.H-1)
}

// reveal keeps the selected row on screen.
func (m Model) reveal() {
	if rows := m.treeRows(); rows > 0 {
		m.screen.Tree.Reveal(rows)
	}
}

// key handles a key press.
func (m Model) key(msg tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	// A toast lasts until the next key, so what is on screen is a function of
	// the messages and not of how long ago one arrived.
	m.screen.Toast = m.screen.Toast.Clear()
	name := tui.KeyName(msg)

	if _, err := m.screen.geometry(); err != nil && name != "ctrl+c" && name != "ctrl+d" && name != "q" {
		// Only the message "too small" is on screen; a key that acted would act
		// on something nobody can see.
		return m, nil
	}

	if m.screen.Help {
		switch name {
		case "esc", "?":
			m.screen.Help, m.screen.HelpTop = false, 0
		case "ctrl+c", "ctrl+d", "q":
			return m.leave()
		case "up", "k":
			m.scrollHelp(-1)
		case "down", "j":
			m.scrollHelp(1)
		case "pgup":
			m.scrollHelp(-m.helpPage())
		case "pgdown", "space":
			m.scrollHelp(m.helpPage())
		case "home":
			m.scrollHelp(-1 << 20)
		case "end", "G":
			m.scrollHelp(1 << 20)
		}

		return m, nil
	}

	binding, ok := m.keys.Match(name)
	if !ok {
		return m, nil
	}
	switch binding.Name {
	case bindQuit, bindInterrupt:
		return m.leave()
	case bindHelp:
		m.screen.Help = true
	case bindRefresh:
		if m.screen.Loading {
			m.toast(ui.ToneWarning, "still reading")

			break
		}
		m.screen.Loading = true
		m.seq++

		return m, m.read(m.seq)
	default:
		m.navigate(binding.Name)
	}

	return m, nil
}

// navigate moves within the tree.
func (m *Model) navigate(name string) {
	tree, rows := m.screen.Tree, max(1, m.treeRows())
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
		m.open(tree.Activate(tree.Selected()))
	case bindExpand:
		m.open(tree.Expand(tree.Selected()))
	case bindCollapse:
		if tree.Open(tree.Selected()) {
			tree.Collapse(tree.Selected())
		} else {
			tree.Parent()
		}
	}
	tree.Reveal(rows)
}

// open answers a request to load children. The index holds them all, so the
// answer is immediate.
func (m *Model) open(request pane.Request, load bool) {
	if !load || m.screen.Index == nil {
		return
	}
	if err := m.screen.Tree.Load(m.screen.Index.Loader(), request); err != nil {
		m.toast(ui.ToneWarning, err.Error())
	}
}

// scrollHelp moves the help overlay by delta lines, within the lines it has.
func (m *Model) scrollHelp(delta int) {
	g, err := m.screen.geometry()
	if err != nil {
		return
	}
	lines := len(m.screen.Keys.Help(g.body.W, m.cfg.Style.Theme))
	m.screen.HelpTop = max(0, min(m.screen.HelpTop+delta, max(0, lines-max(0, g.body.H-1))))
}

// helpPage is how far a page key moves the overlay.
func (m Model) helpPage() int { return max(1, m.screen.Size.H-4) }

// click handles a mouse click, resolved against what the screen draws right now.
func (m Model) click(mouse tea.Mouse) (tea.Model, tea.Cmd) {
	if mouse.Button != tea.MouseLeft {
		return m, nil
	}
	_, hits := m.screen.Draw(m.cfg.Style)
	hit, ok := hits.At(mouse.X, mouse.Y)
	if !ok {
		return m, nil
	}

	switch hit.Kind {
	case pane.KindPane, pane.KindHeading:
		if hit.ID == "help" {
			m.screen.Help = false
		}
	case pane.KindRow, pane.KindMore:
		if id, ok := strings.CutPrefix(hit.ID, rowPrefix); ok {
			m.open(m.screen.Tree.Activate(id))
			m.screen.Tree.Reveal(max(1, m.treeRows()))
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
	case hit.ID == panePrefix+paneGraph, strings.HasPrefix(hit.ID, rowPrefix):
		m.screen.Tree.Scroll(delta, max(1, m.treeRows()))
	}

	return m, nil
}
