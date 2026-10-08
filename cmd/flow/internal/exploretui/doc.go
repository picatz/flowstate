// Package exploretui is the full-screen system explorer: `flow explore`.
//
// It draws one [v1.Graph], the same message `flow graph --output json` writes
// and the MCP and LSP surfaces read, as a tree a person opens one level at a
// time and an inspector for the row under the cursor. It reads nothing but that
// message and asks nothing of it but a reload, so what it shows can be pinned
// by a golden and what it knows is what any other client of the graph knows.
//
// # Tree mode
//
// Every workflow is a root. A node's children are the nodes its edges reach,
// calls first, then signals it waits for, then tasks it runs, each row saying
// how it is related and, for a workflow, how many of its runs are in each
// status. The tree opens lazily, so a cycle of workflows is a tree a person can
// keep opening and never one that is built without end.
//
// # Shared parts
//
// The panes, the grid, the keymap, the toast and the bar are the debugger's
// ([github.com/picatz/flowstate/cmd/flow/internal/pane] and
// [github.com/picatz/flowstate/cmd/flow/internal/tui]); this package adds only
// what is about a graph. Like them it reads no clock: a screen is a function of
// the messages it was sent.
package exploretui
