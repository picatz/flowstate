// Package debugtui is the debugger's full-screen view: a bubbletea screen over
// one [flowdebug.Target], opened by `flow debug attach --tui`.
//
// It is a client of three things and nothing else. The target's own answers,
// read as a [flowdebug.Frame] by [flowdebug.ReadFrame], are everything it
// draws, so it shows exactly what the line editor and the panes show and no
// value a target's redactor withheld. The [flowdebug.Driver] carries every
// command, so a key, a click and a typed line all end in the same call, answered
// with the same refusals. And [github.com/picatz/flowstate/cmd/flow/internal/pane]
// and [github.com/picatz/flowstate/cmd/flow/internal/tui] supply the components
// and the shell.
//
// # Pure views, one owner of state
//
// Drawing is [Screen.Draw], a function of the screen's state and a [Style]: it
// reads no clock, no terminal and no target. [Model] owns the state, changes it
// in Update, and talks to the target only through commands, so a test drives a
// model with messages and compares bytes.
//
// # What refreshes the screen
//
// The screen re-reads the frame when a command finishes and when the target's
// [flowdebug.Target.WaitSnapshot] reports a newer revision, never on a timer. A
// target that cannot be waited on leaves the screen as of its last read, which
// the next command refreshes.
//
// # Keys come from the command table
//
// [NewKeymap] binds keys only to verbs [flowdebug.DriverVerbs] lists, so a verb
// a front does not answer has no key and no help line, and a verb added to the
// table must be given a key or named as console-only before the tests pass.
package debugtui
