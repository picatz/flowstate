package exploretui

import (
	"context"
	"errors"
	"maps"
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

	// Runs reads a workflow's recent runs by its declared name, for the rows
	// under its "runs" row. Nil leaves those rows out, which is what a screen
	// over files alone wants. It is called from a command, and is allowed to be
	// slow. more says the workflow has runs the answer leaves out.
	Runs func(ctx context.Context, workflow string) (runs []*v1.RunSummary, more bool, err error)

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

// runsMsg is a finished read of a workflow's runs, for the "runs" row Parent.
type runsMsg struct {
	parent string
	// gen numbers the reads of this parent; only the latest is applied.
	gen  uint64
	runs []*v1.RunSummary
	more bool
	err  error
}

// runsAnswer is one finished read of a "runs" row.
type runsAnswer struct {
	rows []pane.Node
	by   map[string]*v1.RunSummary
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

	// gens numbers the reads of each "runs" row, so an answer that is not to the
	// latest question asked of that row (a refresh, or the row opened again)
	// changes nothing. pending is a selection a refresh could not restore yet
	// because its row is read afresh; the read that fills the row restores it.
	gens    map[string]uint64
	pending string

	// answers holds what each "runs" row was last read as, so narrowing the
	// workflows shows an open row again from what was read and sends nothing to
	// the server; only a refresh reads again.
	answers map[string]runsAnswer
	// reuse says rows are being opened again by a rebuild, which uses answers; a
	// person opening a row asks the server afresh.
	reuse bool

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
		cfg: cfg, ctx: ctx, keys: keys, seq: 1, gens: map[string]uint64{}, answers: map[string]runsAnswer{},
		screen: Screen{Size: cfg.Size, Source: cfg.Source, Tree: pane.NewTree(nil), Keys: keys, Loading: true, Runs: map[string]*v1.RunSummary{}},
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
	case runsMsg:
		return m.ran(msg)
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

	m.answers = map[string]runsAnswer{}

	return m, m.rebuild(NewIndex(msg.graph))
}

// rebuild shows the workflows of x that pass the filter. The rows that were
// open stay open where their workflows are still shown, and the selection stays
// where its row does. It returns the commands the reopened rows start.
func (m *Model) rebuild(x *Index) tea.Cmd {
	tree := m.screen.Tree
	rows, selected := tree.Rows(), tree.Selected()
	// Rows come parents first, which is the order they must be opened again in.
	var opened []string
	for _, r := range rows {
		if r.Open && r.Kind == pane.RowNode {
			opened = append(opened, r.ID)
		}
	}

	// Summaries belong to rows the new tree is about to rebuild; the rows that
	// are open again are filled from what was read, or read afresh after a
	// refresh emptied the answers.
	m.screen.Runs = map[string]*v1.RunSummary{}
	m.screen.Index = x
	if m.cfg.Runs != nil {
		m.screen.Index = m.screen.Index.WithRunRows()
	}
	tree.SetRoots(m.screen.Index.RootsNamed(m.screen.Filter))
	var cmds []tea.Cmd
	m.reuse = true
	for _, id := range opened {
		if request, ok := tree.Expand(id); ok {
			if cmd := m.fill(request); cmd != nil {
				cmds = append(cmds, cmd)
			}
		}
	}
	m.reuse = false
	m.pending = ""
	if !tree.Select(selected) {
		m.pending = selected
	}
	m.reveal()

	return tea.Batch(cmds...)
}

// fill answers a request for the children of a row. The index holds a graph
// node's, so those arrive at once; a "runs" row's are read from the server, so
// that is a command and the answer comes back as a message.
func (m *Model) fill(request pane.Request) tea.Cmd {
	x := m.screen.Index
	if x == nil {
		return nil
	}
	if workflow, ok := x.RunsOf(request.Parent); ok {
		if answer, ok := m.answers[request.Parent]; ok && m.reuse {
			m.screen.Tree.Fill(request.Parent, 0, answer.rows, len(answer.rows))
			maps.Copy(m.screen.Runs, answer.by)

			return nil
		}
		ctx, runs := m.ctx, m.cfg.Runs
		m.gens[request.Parent]++
		gen := m.gens[request.Parent]

		return func() tea.Msg {
			rows, more, err := runs(ctx, workflow)

			return runsMsg{parent: request.Parent, gen: gen, runs: rows, more: more, err: err}
		}
	}
	if err := m.screen.Tree.Load(x.Loader(), request); err != nil {
		m.toast(ui.ToneWarning, err.Error())
	}

	return nil
}

// ran folds a finished read of runs into the row it was asked for.
func (m Model) ran(msg runsMsg) (tea.Model, tea.Cmd) {
	if msg.gen != m.gens[msg.parent] {
		return m, nil
	}
	if msg.err != nil {
		if m.ctx.Err() != nil {
			return m, nil
		}
		// The row stays open and empty, so the person sees where it failed; closing
		// and opening it asks again.
		m.screen.Tree.Collapse(msg.parent)
		m.toast(ui.ToneDanger, "cannot read the runs: "+msg.err.Error())

		return m, nil
	}
	rows, byRow := RunRows(msg.parent, msg.runs, msg.more)
	if !m.screen.Tree.Fill(msg.parent, 0, rows, len(rows)) {
		return m, nil
	}
	maps.Copy(m.screen.Runs, byRow)
	m.answers[msg.parent] = runsAnswer{rows: rows, by: byRow}
	if m.pending != "" && m.screen.Tree.Select(m.pending) {
		m.pending = ""
	}
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
	m.pending = ""
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

	if m.screen.Filtering {
		return m.typeFilter(msg, name)
	}
	if name == "esc" && m.screen.Filter != "" {
		return m.setFilter("", false)
	}

	binding, ok := m.keys.Match(name)
	if !ok {
		return m, nil
	}
	switch binding.Name {
	case bindFilter:
		if m.screen.Index == nil {
			m.toast(ui.ToneWarning, "nothing to filter yet")

			break
		}
		m.screen.Filtering = true
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
		return m, m.navigate(binding.Name)
	}

	return m, nil
}

// maxFilter bounds what can be typed, so a held key cannot grow the prompt
// without end.
const maxFilter = 80

// typeFilter folds a key into the filter being typed. Every change narrows the
// rows at once; enter keeps the filter and returns the keys to the tree, and
// esc drops it.
func (m Model) typeFilter(msg tea.KeyPressMsg, name string) (tea.Model, tea.Cmd) {
	filter := m.screen.Filter
	switch name {
	case "ctrl+c", "ctrl+d":
		return m.leave()
	case "enter":
		m.screen.Filtering = false

		return m, nil
	case "esc":
		return m.setFilter("", false)
	case "backspace":
		runes := []rune(filter)
		filter = string(runes[:max(0, len(runes)-1)])
	case "ctrl+u":
		filter = ""
	default:
		if msg.Text == "" || len([]rune(filter)) >= maxFilter {
			return m, nil
		}
		filter += msg.Text
	}

	return m.setFilter(filter, true)
}

// setFilter shows the workflows that match filter.
func (m Model) setFilter(filter string, filtering bool) (tea.Model, tea.Cmd) {
	m.screen.Filter, m.screen.Filtering = filter, filtering
	if m.screen.Index == nil {
		return m, nil
	}

	return m, m.rebuild(m.screen.Index)
}

// navigate moves within the tree, and returns the command a row that needs a
// read starts.
func (m *Model) navigate(name string) tea.Cmd {
	tree, rows := m.screen.Tree, max(1, m.treeRows())
	var cmd tea.Cmd
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
		cmd = m.open(tree.Activate(tree.Selected()))
	case bindExpand:
		cmd = m.open(tree.Expand(tree.Selected()))
	case bindCollapse:
		if tree.Open(tree.Selected()) {
			tree.Collapse(tree.Selected())
		} else {
			tree.Parent()
		}
	}
	tree.Reveal(rows)

	return cmd
}

// open answers a request to load children, if there is one.
func (m *Model) open(request pane.Request, load bool) tea.Cmd {
	if !load {
		return nil
	}

	return m.fill(request)
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
	m.pending = ""
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
			cmd := m.open(m.screen.Tree.Activate(id))
			m.screen.Tree.Reveal(max(1, m.treeRows()))

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
	case hit.ID == panePrefix+paneGraph, strings.HasPrefix(hit.ID, rowPrefix):
		m.screen.Tree.Scroll(delta, max(1, m.treeRows()))
	}

	return m, nil
}
