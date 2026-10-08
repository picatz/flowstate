package debugtui

import (
	"fmt"
	"strings"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// PaintValue colours a rendered value by the kind of each run
// [flowdebug.ValueTokens] finds in it: keys and elisions recede, literals take
// the product's accent, and the withheld marker takes the warning style so it
// cannot be read as data. Strings stay in the base style, which is how a theme
// with no colours (a pipe, NO_COLOR) loses emphasis and no information: the
// bytes are the same either way.
func PaintValue(text string, theme ui.Theme) string {
	var b strings.Builder
	for kind, run := range flowdebug.ValueTokens(text) {
		switch kind {
		case flowdebug.TokenKey, flowdebug.TokenElision:
			run = theme.Muted.Render(run)
		case flowdebug.TokenLiteral:
			run = theme.Accent.Render(run)
		case flowdebug.TokenRedacted:
			run = theme.Warning.Render(run)
		}
		b.WriteString(run)
	}

	return b.String()
}

// Ids of the nodes the scope tree holds besides the frame's own scope. Each of
// them names an expression the target is asked about, but none is the scope's
// row for it, so each has an id of its own: the tree keeps one open state and one
// selection per id, and `steps.a` seen as a scope row, as a watch and as an
// inspection are three rows.
const (
	// groupWatches is the group the watches are listed under.
	groupWatches = "x:watches"
	// groupResult is the group the last `inspect` or `expand` is listed under.
	groupResult = "x:result"

	// watchPrefix and resultPrefix begin the id of a watch's row and of an
	// inspection's root. A descendant of either is the root's id, idSep and the
	// descendant's expression, so one expression under two roots is two rows.
	watchPrefix  = "w:"
	resultPrefix = "r:"
	idSep        = "\x1f"
)

// expressionOf is the expression a row stands for: the id of a scope row, and
// what a watch or an inspection was asked about.
func expressionOf(id string) string {
	for _, prefix := range [...]string{resultPrefix, watchPrefix} {
		if rest, ok := strings.CutPrefix(id, prefix); ok {
			if _, child, nested := strings.Cut(rest, idSep); nested {
				return child
			}

			return rest
		}
	}

	return id
}

// childID is the id of a child of the row parent whose expression is expr.
func childID(parent, expr string) string {
	root, _, _ := strings.Cut(parent, idSep)
	if !strings.HasPrefix(root, watchPrefix) && !strings.HasPrefix(root, resultPrefix) {
		return expr
	}

	return root + idSep + expr
}

// valueText is what a row shows of a value: a value that fits a line as it is,
// and a value the renderer laid out as a tree by its type and size, since its
// children are what opening the row shows.
func valueText(v *v1.DebugValue) string {
	text := v.GetRendered()
	if !strings.Contains(text, "\n") {
		return cutValue(text)
	}
	if n := v.GetChildren(); n > 0 {
		return fmt.Sprintf("%s (%d)", v.GetType(), n)
	}
	first, _, _ := strings.Cut(text, "\n")

	return cutValue(first) + " …"
}

// childNodes are the rows of a page of children under the row parent.
func childNodes(answer *v1.DebugInspectResponse, parent string) []pane.Node {
	nodes := make([]pane.Node, 0, len(answer.GetChildren()))
	for _, child := range answer.GetChildren() {
		value := child.GetValue()
		nodes = append(nodes, pane.Node{
			ID:    childID(parent, value.GetExpression()),
			Label: child.GetName(),
			Value: valueText(value),
			Total: int(value.GetChildren()),
		})
	}

	return nodes
}
