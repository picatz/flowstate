package tui

import (
	"fmt"
	"slices"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/picatz/flowstate/cmd/flow/internal/pane"
	"github.com/picatz/flowstate/cmd/flow/internal/ui"
)

// Binding is one named action and the keys that perform it.
type Binding struct {
	// Name is what the client switches on.
	Name string

	// Keys are spelled as bubbletea spells a key press: "s", "space",
	// "ctrl+c", "shift+tab", "pgdown".
	Keys []string

	// Help is the sentence the help overlay shows.
	Help string

	// Group is the heading the help overlay files the binding under.
	Group string

	// Hint reports that the binding is shown in the one-line hint bar, and
	// Short is the word it is shown with there (Name when empty).
	Hint  bool
	Short string
}

// Keymap is the one place a screen's keys are named. It matches a key press and
// it renders the help overlay and the hint bar, from the same bindings, so
// neither can describe a key that does nothing or leave one out.
type Keymap struct {
	bindings []Binding
}

// NewKeymap returns a keymap over bindings. A key bound twice is an error: the
// second binding would be unreachable and its help a lie.
func NewKeymap(bindings ...Binding) (Keymap, error) {
	seen := map[string]string{}
	for _, b := range bindings {
		for _, key := range b.Keys {
			if other, dup := seen[key]; dup {
				return Keymap{}, fmt.Errorf("tui: key %q is bound to both %s and %s", key, other, b.Name)
			}
			seen[key] = b.Name
		}
	}

	return Keymap{bindings: slices.Clone(bindings)}, nil
}

// Bindings are the bindings, in the order they were given.
func (k Keymap) Bindings() []Binding { return slices.Clone(k.bindings) }

// Match finds the binding a key press performs.
func (k Keymap) Match(key string) (Binding, bool) {
	for _, b := range k.bindings {
		if slices.Contains(b.Keys, key) {
			return b, true
		}
	}

	return Binding{}, false
}

// Named finds a binding by name.
func (k Keymap) Named(name string) (Binding, bool) {
	for _, b := range k.bindings {
		if b.Name == name {
			return b, true
		}
	}

	return Binding{}, false
}

// keyLabel is how a key is written in help: the shorthand a person reads.
func keyLabel(key string) string {
	switch key {
	case "space":
		return "space"
	case "pgup":
		return "PgUp"
	case "pgdown":
		return "PgDn"
	}

	return key
}

func labels(keys []string) string {
	out := make([]string, len(keys))
	for i, key := range keys {
		out[i] = keyLabel(key)
	}

	return strings.Join(out, " ")
}

// Hints is the one-line bar of the bindings marked as hints, as many as fit.
func (k Keymap) Hints(width int, theme ui.Theme) string {
	var parts []string
	used := 0
	for _, b := range k.bindings {
		if !b.Hint || len(b.Keys) == 0 {
			continue
		}
		short := b.Short
		if short == "" {
			short = b.Name
		}
		part := theme.Strong.Render(keyLabel(b.Keys[0])) + " " + theme.Muted.Render(short)
		if used+lipgloss.Width(part)+2 > width {
			break
		}
		parts = append(parts, part)
		used += lipgloss.Width(part) + 2
	}

	return strings.Join(parts, "  ")
}

// Help renders the overlay body: bindings by group, keys then sentence, one
// line each, wrapped to width.
func (k Keymap) Help(width int, theme ui.Theme) []string {
	var groups []string
	byGroup := map[string][]Binding{}
	for _, b := range k.bindings {
		if _, ok := byGroup[b.Group]; !ok {
			groups = append(groups, b.Group)
		}
		byGroup[b.Group] = append(byGroup[b.Group], b)
	}

	keyWidth := 0
	for _, b := range k.bindings {
		keyWidth = max(keyWidth, lipgloss.Width(labels(b.Keys)))
	}
	keyWidth = min(keyWidth, max(6, width/3))

	var lines []string
	for _, group := range groups {
		if group != "" {
			if len(lines) > 0 {
				lines = append(lines, "")
			}
			lines = append(lines, theme.Header.Render(ui.EscapeControl(group)))
		}
		for _, b := range byGroup[group] {
			keys := ui.Trim(labels(b.Keys), keyWidth)
			pad := strings.Repeat(" ", max(0, keyWidth-lipgloss.Width(keys)))
			text := pane.Fit(ui.EscapeControl(b.Help), max(1, width-keyWidth-4), 1)[0]
			lines = append(lines, "  "+theme.Strong.Render(keys)+pad+"  "+strings.TrimRight(text, " "))
		}
	}

	return lines
}
