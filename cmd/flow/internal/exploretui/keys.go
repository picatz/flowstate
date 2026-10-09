package exploretui

import "github.com/picatz/flowstate/cmd/flow/internal/tui"

// Binding names the model switches on.
const (
	bindUp        = "nav:up"
	bindDown      = "nav:down"
	bindToggle    = "nav:toggle"
	bindExpand    = "nav:expand"
	bindCollapse  = "nav:collapse"
	bindPageUp    = "nav:pgup"
	bindPageDown  = "nav:pgdown"
	bindHome      = "nav:home"
	bindEnd       = "nav:end"
	bindFilter    = "filter"
	bindRefresh   = "refresh"
	bindHelp      = "help"
	bindQuit      = "quit"
	bindInterrupt = "interrupt"
)

// NewKeymap binds the explorer's keys. The movement keys are the debugger's, so
// a person who has learned one screen has learned the other.
func NewKeymap() (tui.Keymap, error) {
	return tui.NewKeymap(
		tui.Binding{Name: bindUp, Keys: []string{"up", "k"}, Help: "move up a row", Group: "Move"},
		tui.Binding{Name: bindDown, Keys: []string{"down", "j"}, Help: "move down a row", Group: "Move"},
		tui.Binding{Name: bindToggle, Keys: []string{"enter"}, Help: "open or close the row (click does the same)", Group: "Move"},
		tui.Binding{Name: bindExpand, Keys: []string{"right", "l"}, Help: "open the row", Group: "Move"},
		tui.Binding{Name: bindCollapse, Keys: []string{"left", "h"}, Help: "close the row, or go to its parent", Group: "Move"},
		tui.Binding{Name: bindPageUp, Keys: []string{"pgup"}, Help: "up a page", Group: "Move"},
		tui.Binding{Name: bindPageDown, Keys: []string{"pgdown"}, Help: "down a page", Group: "Move"},
		tui.Binding{Name: bindHome, Keys: []string{"home"}, Help: "first row", Group: "Move"},
		tui.Binding{Name: bindEnd, Keys: []string{"end", "G"}, Help: "last row", Group: "Move"},
		tui.Binding{Name: bindFilter, Keys: []string{"f"}, Help: "filter workflows by name; enter keeps it, esc clears it",
			Group: "Screen", Hint: true, Short: "filter"},
		tui.Binding{Name: bindRefresh, Keys: []string{"r"}, Help: "read the files and the server again, keeping what is open",
			Group: "Screen", Hint: true, Short: "refresh"},
		tui.Binding{Name: bindHelp, Keys: []string{"?"}, Help: "show or hide this help", Group: "Screen", Hint: true, Short: "help"},
		tui.Binding{Name: bindQuit, Keys: []string{"q", "ctrl+d"}, Help: "leave", Group: "Screen", Hint: true, Short: "quit"},
		tui.Binding{Name: bindInterrupt, Keys: []string{"ctrl+c"}, Help: "leave at once", Group: "Screen"},
	)
}
