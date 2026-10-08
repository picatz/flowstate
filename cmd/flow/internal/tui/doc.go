// Package tui is the bubbletea shell a full-screen view of this CLI is built
// from: the parts of a screen that are about the terminal and not about what is
// being shown.
//
// What is here is deliberately small and free of any particular client. A
// [Grid] says, as data, how a set of named panes folds as the terminal narrows.
// A [Ring] is the focus order. A [Keymap] is the one place keys are named, and
// renders the help overlay and the hint line from the same bindings it matches
// against, so the help cannot describe a key that does nothing. A [Toast] is
// the one-line refusal area and a [Bar] a status line. Clients assemble these
// with the pure components in [github.com/picatz/flowstate/cmd/flow/internal/pane],
// which this package imports and which never imports it.
//
// # No clocks
//
// Nothing here reads the time. A toast lasts until the next key press instead
// of until a timer, so a screen is a function of the messages it was sent and a
// golden of it is a test. A test holds this package, and the clients, to that.
//
// # What it does not know
//
// It imports no debugger and no graph: a client brings its own panes, and a
// need of this package's that a second client shares is added here, once,
// rather than copied.
package tui

import (
	"fmt"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
)

// Size is a terminal's size in cells.
type Size struct{ W, H int }

// String is the size as "80x24", which is how a test names a case.
func (s Size) String() string { return fmt.Sprintf("%dx%d", s.W, s.H) }

// Rect is the rectangle that starts at the top left and is s big.
func (s Size) Rect() pane.Rect { return pane.Rect{W: s.W, H: s.H} }
