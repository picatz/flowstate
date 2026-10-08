package debugtui

import (
	"slices"

	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// verbKeys are the verbs that have a key of their own, and the keys. A verb is
// bound only when the front offers it.
var verbKeys = []struct {
	verb  string
	keys  []string
	short string
	hint  bool
}{
	{"step", []string{"s", "space"}, "step", true},
	{"next", []string{"n"}, "next", true},
	{"finish", []string{"f"}, "finish", false},
	{"continue", []string{"c"}, "continue", true},
	{"back", []string{"b"}, "back", false},
	{"reverse-continue", []string{"r"}, "reverse", false},
	{"goto", []string{"g"}, "goto", false},
	{"pause", []string{"p"}, "pause", false},
}

// consoleOnly are the verbs left to the console on purpose: they take an
// argument a key cannot supply, or are bound to something other than a key of
// their own. A verb in the command table that is in neither list fails
// TestKeymapIsTheCommandTable, so adding one forces the decision.
var consoleOnly = []string{
	"until", "break", "log", "catch", "delete", "clear", "breakpoints",
	"expand", "scope", "complete", "status", "backtrace", "help",
	// inspect has the `i` key, which opens the console with the selected name;
	// detach is the `q` key.
	"inspect", "detach",
}

// Binding names the client switches on, apart from "verb:<name>".
const (
	bindFocusNext  = "focus-next"
	bindFocusPrev  = "focus-prev"
	bindConsole    = "console"
	bindInspect    = "inspect-selected"
	bindUntil      = "flow-until"
	bindBreak      = "flow-break"
	bindHelp       = "help"
	bindQuit       = "quit"
	bindInterrupt  = "interrupt"
	bindLeave      = "leave"
	bindUp         = "nav:up"
	bindDown       = "nav:down"
	bindToggle     = "nav:toggle"
	bindExpand     = "nav:expand"
	bindCollapse   = "nav:collapse"
	bindPageUp     = "nav:pgup"
	bindPageDown   = "nav:pgdown"
	bindHome       = "nav:home"
	bindEnd        = "nav:end"
	verbBindPrefix = "verb:"
)

// NewKeymap binds the keys for a front that answers verbs.
//
// A key for a verb exists only when verbs lists it; the navigation, focus and
// screen keys are the screen's own and are always there, except that `i` needs
// `inspect` and `q` needs `detach`, the verbs they end in.
func NewKeymap(verbs []flowdebug.Verb) (tui.Keymap, error) {
	offered := func(name string) bool {
		return slices.ContainsFunc(verbs, func(v flowdebug.Verb) bool { return v.Name == name })
	}

	var bindings []tui.Binding
	for _, key := range verbKeys {
		if !offered(key.verb) {
			continue
		}
		help := ""
		for _, verb := range verbs {
			if verb.Name == key.verb {
				help = verb.Help
			}
		}
		bindings = append(bindings, tui.Binding{
			Name: verbBindPrefix + key.verb, Keys: key.keys, Help: help, Group: "Run the program",
			Hint: key.hint, Short: key.short,
		})
	}

	bindings = append(bindings,
		tui.Binding{Name: bindUp, Keys: []string{"up", "k"}, Help: "move up a row", Group: "Move"},
		tui.Binding{Name: bindDown, Keys: []string{"down", "j"}, Help: "move down a row", Group: "Move"},
		tui.Binding{Name: bindToggle, Keys: []string{"enter"}, Help: "open or close the row (click too); in the flow, run until it", Group: "Move"},
		tui.Binding{Name: bindExpand, Keys: []string{"right", "l"}, Help: "open the row; in the flow, unfold it", Group: "Move"},
		tui.Binding{Name: bindCollapse, Keys: []string{"left", "h"}, Help: "close the row, or go to its parent; in the flow, fold it", Group: "Move"},
		tui.Binding{Name: bindPageUp, Keys: []string{"pgup"}, Help: "up a page", Group: "Move"},
		tui.Binding{Name: bindPageDown, Keys: []string{"pgdown"}, Help: "down a page", Group: "Move"},
		tui.Binding{Name: bindHome, Keys: []string{"home"}, Help: "first row", Group: "Move"},
		tui.Binding{Name: bindEnd, Keys: []string{"end", "G"}, Help: "last row", Group: "Move"},
		tui.Binding{Name: bindFocusNext, Keys: []string{"tab"}, Help: "focus the next pane (click a pane to focus it)", Group: "Screen",
			Hint: true, Short: "focus"},
		tui.Binding{Name: bindFocusPrev, Keys: []string{"shift+tab"}, Help: "focus the previous pane", Group: "Screen"},
		tui.Binding{Name: bindConsole, Keys: []string{":", "/"}, Help: "type a command; every verb below works there", Group: "Screen",
			Hint: true, Short: "console"},
	)
	if offered("inspect") {
		bindings = append(bindings, tui.Binding{Name: bindInspect, Keys: []string{"i"},
			Help: "inspect the selected scope row in the console", Group: "Screen"})
	}
	if offered("until") {
		bindings = append(bindings, tui.Binding{Name: bindUntil, Keys: []string{"u"},
			Help: "run until the selected flow step (double click too)", Group: "Flow"})
	}
	if offered("break") {
		bindings = append(bindings, tui.Binding{Name: bindBreak, Keys: []string{"B"},
			Help: "toggle a breakpoint on the selected flow step (right click too), or on the selected source line (click its number too)", Group: "Flow"})
	}
	bindings = append(bindings,
		tui.Binding{Name: bindHelp, Keys: []string{"?"}, Help: "show or hide this help", Group: "Screen", Hint: true, Short: "help"},
	)
	if offered("detach") {
		bindings = append(bindings, tui.Binding{Name: bindQuit, Keys: []string{"q"},
			Help: "detach and let the run go on unattended", Group: "Screen", Hint: true, Short: "quit"})
	}
	bindings = append(bindings,
		tui.Binding{Name: bindInterrupt, Keys: []string{"ctrl+c"}, Help: "leave at once and release the run, like quit", Group: "Screen"},
		tui.Binding{Name: bindLeave, Keys: []string{"ctrl+d"}, Help: "leave and release the run (an empty console line)", Group: "Screen"},
	)

	return tui.NewKeymap(bindings...)
}
