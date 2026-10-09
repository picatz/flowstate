package debugtui

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	tea "charm.land/bubbletea/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// inspected shows what an `inspect` or `expand` typed at the console answered,
// as a tree in the scope pane under "result". The transcript has the same answer
// as text; the tree is the part that can be opened and paged.
//
// Only one inspection is held: asking again replaces it, and so does the run
// moving, since its values are of the stop they were asked at.
func (m *Model) inspected(line string, result *flowdebug.DriveResult) {
	answer := result.Inspect
	if answer == nil || answer.GetError() != "" {
		return
	}
	verb, rest, _ := strings.Cut(line, " ")
	rest = strings.TrimSpace(rest)

	switch verb {
	case "inspect":
		if value := answer.GetValue(); value != nil && rest != "" {
			m.setResult(rest, value, nil, int(value.GetChildren()))
		}

	case "expand":
		expr, offset := flowdebug.ExpandPage(rest)
		root := resultPrefix + expr
		switch _, held := m.screen.Tree.Node(root); {
		case offset > 0 && held && m.screen.Result != nil:
			// The next page of the inspection already shown. A page that does not
			// start where the held ones end is dropped by the tree.
			m.screen.Tree.Fill(root, offset, childNodes(answer, root), int(answer.GetTotal()))
			m.keepResult(root)
			m.screen.Tree.Select(root)
			m.revealSelection()
		case offset == 0:
			m.setResult(expr, answer.GetValue(), childNodes(answer, root), int(answer.GetTotal()))
		}
	}
}

// setResult makes the inspection of expr the result, opened to the children it
// has so far.
func (m *Model) setResult(expr string, value *v1.DebugValue, children []pane.Node, total int) {
	text := fmt.Sprintf("{%d}", total)
	if value != nil {
		text = valueText(value)
	}
	root := pane.Node{ID: resultPrefix + expr, Label: expr, Value: text, Children: children, Total: total}
	group := pane.Node{ID: groupResult, Label: "result", Value: "{1}", Children: []pane.Node{root}}
	m.screen.Result = &group

	m.syncTree()
	m.screen.Tree.Expand(groupResult)
	if len(children) > 0 {
		m.screen.Tree.Expand(root.ID)
	}
	m.screen.Tree.Select(root.ID)
	m.revealSelection()
}

// keepResult copies the result back from the tree after a page was added to a
// row under it, since the tree owns the children it was given.
func (m *Model) keepResult(parent string) {
	if m.screen.Result == nil || !strings.HasPrefix(parent, resultPrefix) {
		return
	}
	if group, ok := m.screen.Tree.Node(groupResult); ok {
		m.screen.Result = &group
	}
}

// fill adds the page an `expand` answered to the row it was started from.
func (m *Model) fill(req pane.Request, answer *v1.DebugInspectResponse) {
	m.screen.Tree.Fill(req.Parent, req.Offset, childNodes(answer, req.Parent), int(answer.GetTotal()))
	m.keepResult(req.Parent)
	m.revealSelection()
}

// load is what opening a row, or asking for its next page, does when the tree
// does not hold the children: a scope group is listed by the screen itself, and
// the children of a value are the console's `expand` command, run through the
// driver so the transcript, the recording and the paging are the ones a typed
// `expand` has.
func (m *Model) load(req pane.Request) tea.Cmd {
	line := "expand " + expressionOf(req.Parent)
	if req.Offset > 0 {
		line += " from " + strconv.Itoa(req.Offset)
	}
	if strings.HasPrefix(req.Parent, "g:") || strings.ContainsFunc(line, unicode.IsControl) || len(line) > flowdebug.MaxCommandBytes {
		// A group is a listing and not a value, and a name that cannot be put on a
		// line is read by the screen rather than typed.
		return m.pageCmd(req)
	}

	rev, driver := m.frameRev, m.cfg.Driver
	cmd := m.start(line, false, func(ctx context.Context) (*flowdebug.DriveResult, error) {
		return driver.Do(ctx, line)
	})
	if cmd == nil {
		return nil
	}

	return func() tea.Msg {
		msg := cmd()
		if done, ok := msg.(doneMsg); ok {
			done.fill, done.fillRev = &req, rev
			return done
		}

		return msg
	}
}
