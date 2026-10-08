// Package pane holds the pure components a full-screen view is assembled from.
//
// Every component here is a value that turns its state and a size into a
// string: no terminal read, no clock, no message loop, and no bubbletea. That
// is the rule that makes a pane testable by comparing bytes, and it is why the
// event-loop shell lives one package over in [github.com/picatz/flowstate/cmd/flow/internal/tui],
// which imports this package and is never imported by it. A test holds both
// halves of that line.
//
// What is here:
//
//   - [Hits], the registry a view fills with what it drew so a click is
//     resolved against the screen as it was painted;
//   - [Tree], a collapsible, paged tree with a [Loader] for children that are
//     not held yet;
//   - [Inspector], key/value rows describing one selected item;
//   - [Split], two panes side by side or stacked, and [Stitch], which places
//     rendered panes into one screen.
//
// Any text a pane draws that another party chose is passed through
// [ui.EscapeControl] at the point it is drawn, so a pane cannot be made to emit
// a terminal sequence by the data it shows.
package pane
