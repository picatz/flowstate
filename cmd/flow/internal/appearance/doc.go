// Package appearance holds the CLI's appearance pins: charmbracelet/vhs tape
// files under testdata, one per styled surface, each paired with a golden of
// the terminal screen the tape produces.
//
// The rest of the test suite reads the CLI's output for substrings, which is
// exactly the wrong instrument for the thing this package holds. Alignment,
// wrap points, hanging indents and the blank line before a NEXT block are all
// carried by whitespace no substring assertion looks at, so they can drift
// through a fully green tree. A recorded screen is the only artifact that
// notices.
//
// # What a golden covers, and what it does not
//
// The golden is the visible terminal grid, read back through xterm.js, which
// hands over plain text: layout, wrapping, alignment and which symbols were
// chosen are all in it, and colour is not. Colour has its own pins in the ui
// package, where the role palette is defined. So a diff here means the shape
// of a surface changed, never its palette.
//
// # Why the width comes from stty
//
// vhs sizes a browser window and lets xterm.js fit a grid into it, which makes
// the column count a function of font metrics on the machine doing the
// recording. That is the wrong thing to hang a golden on. So each tape pins
// the pty instead, with `stty cols 80 rows 45`: the CLI asks the pty how wide
// it is, wraps at 80, and xterm renders lines that are already short enough to
// need no wrapping of its own. The grid only has to be big enough to hold the
// frame, and the recording no longer depends on a font.
//
// A grid too small to hold the frame scrolls the top away, which the test
// reports as its own failure rather than as a confusing diff.
//
// # Running
//
//	make appearance          # verify the goldens (needs vhs, ttyd, ffmpeg)
//	make appearance-update   # re-record after an intended change
//
// Without vhs on PATH the test skips, loudly, naming what is missing. CI
// installs the three binaries and runs the same target, which is what makes a
// styled surface something CI can hold.
package appearance
