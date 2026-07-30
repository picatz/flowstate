// Package ui decides, in one place, how the flow command line looks.
//
// A command line has two audiences that want opposite things. A person reading a
// terminal wants a status to be findable at a glance, which is what colour and
// weight are for. A program reading a pipe wants bytes it can parse, and an escape
// sequence in the middle of them is corruption. Almost every defect in this area
// comes from a decision made per call site, where the call site cannot see which
// audience it has.
//
// So the decision is made once, per stream, from what that stream can actually do,
// and everything else asks. [Detect] answers "what is this stream", [New] builds
// the writers and styles that follow from the answer, and nothing else in the CLI
// consults the environment or checks for a terminal.
//
// Three properties are load-bearing, and each of them is a way people get burned:
//
//   - A pipe receives no escape sequences at all. Not fewer — none. Detection is
//     per stream, so `flow get x | jq` can be plain on stdout while the status line
//     on stderr is styled, in one invocation.
//   - Every style survives its own removal. Meaning is carried by the words and the
//     layout; colour and weight only make the meaning faster to find. That is what
//     a log file, a screen reader, and a colour-blind reader all receive.
//   - The palette is declared for both backgrounds. A terminal's background is the
//     user's choice, and a palette that assumes one is unreadable for half its
//     audience.
//
// See docs/CLI.md for the reasoning at length.
package ui

import (
	"io"
	"os"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/charmbracelet/x/term"
)

// SymbolsEnv names the variable that overrides symbol selection, for the case
// where the detection below guesses wrong on somebody's terminal.
const SymbolsEnv = "FLOWSTATE_SYMBOLS"

// Capabilities is what one output stream can do.
//
// Every field is derived, never configured: a person does not tell us their
// terminal supports 256 colours, they have a terminal that does or does not.
type Capabilities struct {
	// Profile is how much colour the stream carries, and it is the value the
	// writer degrades against. colorprofile resolves NO_COLOR, CLICOLOR_FORCE,
	// TERM=dumb, tmux and terminfo itself, so those are deliberately not
	// re-implemented here — one implementation of that logic is the point.
	Profile colorprofile.Profile

	// TTY reports whether the stream is a terminal. Distinct from Profile,
	// because CLICOLOR_FORCE asks for colour through a pipe and a person who
	// asked for that should get it.
	TTY bool

	// Dark reports whether the terminal background is dark, deciding which half
	// of every colour pair is used.
	Dark bool

	// Width is the usable columns, already bounded. Zero-width terminals and
	// pipes both report the fallback, so a caller never divides by nothing.
	Width int

	// Height is the rows, or zero where there are none to count.
	//
	// Zero rather than a fallback, deliberately, and the asymmetry with Width is the
	// point: a caller laying out text always needs *some* measure to wrap against,
	// so guessing 80 columns is better than nothing. Nothing needs a guessed number
	// of rows — a stream with no height is one nobody is scrolling — and a fallback
	// would let a full-screen view believe it had 24 rows of a pipe to fill.
	Height int

	// Unicode reports whether restrained typographic marks are safe to emit.
	Unicode bool
}

// Fallback layout, for a stream that cannot be measured.
const (
	fallbackWidth = 80

	// maxWidth keeps prose readable on a terminal somebody has made very wide.
	// Long measures are harder to read, not easier, and a paragraph spanning 300
	// columns is a paragraph nobody's eye can track back to the start of.
	maxWidth = 100
)

// Detect answers what a stream is.
//
// in is needed as well as out because asking a terminal for its background colour
// is a question written to the terminal and an answer read back from it, so it
// needs both halves. It is only asked when out is genuinely a terminal: against a
// pipe the query has nobody to answer it and waits out its own two-second timeout,
// which would put a two-second pause in front of every piped command.
func Detect(in, out *os.File, environ []string) Capabilities {
	caps := Capabilities{
		Profile: colorprofile.Detect(out, environ),
		Width:   fallbackWidth,
	}

	if out != nil {
		caps.TTY = term.IsTerminal(out.Fd())
	}

	if caps.TTY {
		if in != nil {
			caps.Dark = lipgloss.HasDarkBackground(in, out)
		} else {
			// Unknowable without a channel to ask on. Dark is the safer guess:
			// most terminals default to it, and light-on-dark misread is a dim
			// line rather than an invisible one.
			caps.Dark = true
		}

		if w, h, err := term.GetSize(out.Fd()); err == nil {
			caps.applySize(w, h)
		}
	}

	caps.Unicode = wantsUnicode(caps, environ)

	return caps
}

// applySize folds a terminal's reported size in.
//
// Split out of [Detect] because it is the part with rules — a clamp, two fallbacks,
// and a deliberate asymmetry between them — and the part [Detect] itself cannot be
// tested on, since reaching the branch needs a pty that a Go test does not have. The
// rules are testable here; what is left in Detect is the call that measures.
func (c *Capabilities) applySize(columns, rows int) {
	if columns > 0 {
		c.Width = ClampWidth(columns)
	}

	if rows > 0 {
		// Not clamped the way Width is. maxWidth exists because long measures are
		// harder to read, which is a fact about prose; there is no equivalent reason
		// to refuse to use a tall terminal's rows.
		c.Height = rows
	}
}

// ClampWidth bounds a measured terminal width the way this package bounds every
// other one.
//
// Exported because a full-screen view is told its size by its own event loop rather
// than by [Detect], and a resize that escaped the clamp would let the one surface
// that repaints grow to 300 columns while every surface that prints stayed at 100.
// Two answers to "how wide is the text" in one program is one too many.
func ClampWidth(columns int) int {
	if columns <= 0 {
		return fallbackWidth
	}

	return min(columns, maxWidth)
}

// Trim cuts a rendered string to a width, measuring what will be displayed.
//
// Display width rather than bytes, which is the whole reason this exists rather than
// slicing: a styled string carries escape sequences that occupy no columns, and a line
// that has been through a theme is mostly them. lipgloss measures correctly underneath;
// naming it once keeps every surface trimming the same way.
//
// Trim the *whole* line, once, at the end. A line is usually several parts — a pill, a
// message, some fields — and trimming each to the full width puts the line over it by
// however wide the other parts are.
func Trim(text string, width int) string {
	return lipgloss.NewStyle().MaxWidth(width).Render(text)
}

// wantsUnicode decides between the typographic marks and their ASCII fallbacks.
//
// The conservative direction is deliberate. A mark that does not render is a
// replacement glyph in the middle of a status line, and one that renders at an
// unexpected width breaks the alignment of every column after it — so this asks
// for positive evidence rather than assuming.
//
// The evidence is a terminal that carries colour: anything answering ANSI or
// better is a terminal from this millennium. A pipe gets ASCII, which is also what
// makes a CI log and a redirected file readable everywhere. FLOWSTATE_SYMBOLS
// overrides in either direction, because detection can be wrong and a person who
// can see their own terminal should win.
func wantsUnicode(caps Capabilities, environ []string) bool {
	switch strings.ToLower(lookup(environ, SymbolsEnv)) {
	case "unicode", "utf8", "utf-8":
		return true
	case "ascii":
		return false
	}

	return caps.TTY && caps.Profile >= colorprofile.ANSI
}

// lookup reads one variable out of an environment slice.
//
// Taken as a slice rather than read from the process so that the whole of this
// package is testable without setting variables on the running process, which is
// global state two parallel tests would fight over.
func lookup(environ []string, key string) string {
	prefix := key + "="
	for _, entry := range environ {
		if after, found := strings.CutPrefix(entry, prefix); found {
			return after
		}
	}
	return ""
}

// UI is the CLI's rendering surface: two writers that degrade to what their
// stream can carry, and the styles that go with them.
type UI struct {
	// Out carries the answer — a table, a JSON document, the thing a pipe reads.
	Out io.Writer

	// Err carries the account of it — status, warnings, what to do next.
	Err io.Writer

	// Caps is what the *answer* stream can do. Styling decisions about Err use
	// ErrCaps, because the two are genuinely independent: `flow get x | jq` has a
	// piped stdout and a terminal stderr.
	Caps    Capabilities
	ErrCaps Capabilities

	// Theme styles what goes to Out, and ErrTheme what goes to Err.
	//
	// Two themes rather than one for the same reason there are two Capabilities:
	// the streams are independent, and a single theme has to be resolved against
	// one of them. Resolved against stdout, `flow get x | jq` writes an *unstyled*
	// status to a terminal stderr, because the palette collapsed to plain for the
	// pipe that was never going to receive it. Resolved against stderr, the
	// opposite: escape sequences into `jq`.
	//
	// So the rule is the package's own, applied one level further down than it was:
	// the decision is made once *per stream*, and a call site picks the theme
	// belonging to the writer it is about to write to.
	Theme    Theme
	ErrTheme Theme
}

// New builds the rendering surface for a pair of streams.
func New(in, out, errOut *os.File, environ []string) *UI {
	return ForCapabilities(
		colorprofile.NewWriter(out, environ),
		colorprofile.NewWriter(errOut, environ),
		Detect(in, out, environ),
		Detect(in, errOut, environ),
	)
}

// ForCapabilities builds a surface for streams whose capabilities are already
// known.
//
// The one place the wiring lives, so [New] and [Plain] differ only in where the
// answers come from — detected from real files, or asserted. That matters for the
// asserted case in particular: a test describing a terminal it does not have is
// testing the same construction a terminal gets, rather than a hand-built struct
// that will still compile after this one changes.
func ForCapabilities(out, errOut io.Writer, caps, errCaps Capabilities) *UI {
	// Both streams belong to one terminal in every ordinary case, so the background
	// is read once. Where they differ — a piped stdout — the styles written to
	// stderr still have to be legible, and the terminal's own background is the one
	// that decides that. The colour *depth* is per stream, because that is the part
	// that differs between a pipe and a terminal.
	dark := caps.Dark || errCaps.Dark

	return &UI{
		Out:      out,
		Err:      errOut,
		Caps:     caps,
		ErrCaps:  errCaps,
		Theme:    NewTheme(dark, caps),
		ErrTheme: NewTheme(dark, errCaps),
	}
}

// Plain returns a surface that writes to the given writers and styles nothing.
//
// For tests, and for the case where a caller already holds writers rather than
// files. It is the same code path as a pipe, which is the point: the unstyled
// rendering is not a separate implementation that can drift from the styled one.
func Plain(out, errOut io.Writer) *UI {
	caps := Capabilities{Width: fallbackWidth}

	return ForCapabilities(out, errOut, caps, caps)
}
