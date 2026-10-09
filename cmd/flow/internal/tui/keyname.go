package tui

import (
	"unicode"
	"unicode/utf8"

	tea "charm.land/bubbletea/v2"
)

// KeyName is the name a [Keymap] matches a key press by: the character typed
// for a printable key ("s", "S", "?", ":"), and the keystroke for everything
// else ("space", "ctrl+c", "shift+tab", "pgdown").
//
// A shifted letter is its own character and not "shift+" a lowercase one, so
// `B` and `b` are two keys on every terminal, whether or not it reports the
// shift as a modifier.
func KeyName(msg tea.KeyPressMsg) string {
	if text := msg.Text; text != "" {
		if r, size := utf8.DecodeRuneInString(text); size == len(text) && r != ' ' && unicode.IsPrint(r) {
			return text
		}
	}

	return msg.Keystroke()
}
