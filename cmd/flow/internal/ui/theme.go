package ui

import (
	"image/color"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/charmbracelet/x/exp/charmtone"
)

// Colour here is a role, never a hue.
//
// Call sites say "this is a warning" and never "this is amber", which is what
// makes the palette changeable in one edit and what keeps two call sites from
// picking two different ambers for the same idea. It is the same reasoning the
// task registry applies to capability: one definition, everything derived.
//
// Every role is declared for both backgrounds, because a terminal's background is
// the user's choice. A palette that assumes one is unreadable for half its
// audience, and the half it fails is the half that cannot simply be told to change
// their terminal.
//
// No role means anything on its own. A status is a word first and a colour second,
// so removing every colour loses emphasis and no information — which is exactly
// what a pipe, a CI log, and a screen reader receive.

// Theme resolves colour roles against a background and a colour depth.
type Theme struct {
	// Muted is secondary text: a placeholder, a hint, a unit. Never the only
	// carrier of anything.
	Muted lipgloss.Style

	// Strong is emphasis within a line, for the token a reader is scanning for.
	Strong lipgloss.Style

	// Accent is the product's own voice: a heading, a command in an example.
	Accent lipgloss.Style

	// The four outcome roles. Info is deliberately distinct from Accent: one says
	// "this is us talking", the other says "this is how it went".
	Success lipgloss.Style
	Warning lipgloss.Style
	Danger  lipgloss.Style
	Info    lipgloss.Style

	// Header is a table's column row.
	Header lipgloss.Style

	// pill renders a label inside a filled background, which is what a status
	// reads as when it needs to be found rather than read.
	pillFor map[Tone]lipgloss.Style

	// plain reports whether styling was suppressed, so a caller can skip building
	// a decorated string it is about to have stripped anyway.
	plain bool
}

// Tone names the outcome roles, for the surfaces that pick one at runtime.
type Tone int

const (
	// ToneNeutral is the absence of an outcome: something in progress, or a fact
	// with no valence.
	ToneNeutral Tone = iota
	ToneSuccess
	ToneWarning
	ToneDanger
	ToneInfo
)

// NewTheme resolves the palette for a background and a stream's colour depth.
//
// The depth matters as well as the background: below ANSI there is no colour to
// resolve to, so every role collapses to weight — bold and faint — which the
// writer still carries. Each step down loses emphasis and no information.
func NewTheme(dark bool, caps Capabilities) Theme {
	pick := lipgloss.LightDark(dark)

	p := NewPalette(pick)
	muted, strong, accent := p.Muted, p.Strong, p.Accent
	success, warning, danger, info := p.Success, p.Warning, p.Danger, p.Info
	onFill := p.OnFill

	plain := caps.Profile < colorprofile.ANSI

	theme := Theme{
		Muted:   styleIf(plain, lipgloss.NewStyle().Foreground(muted).Faint(true)),
		Strong:  styleIf(plain, lipgloss.NewStyle().Foreground(strong).Bold(true)),
		Accent:  styleIf(plain, lipgloss.NewStyle().Foreground(accent)),
		Success: styleIf(plain, lipgloss.NewStyle().Foreground(success)),
		Warning: styleIf(plain, lipgloss.NewStyle().Foreground(warning)),
		Danger:  styleIf(plain, lipgloss.NewStyle().Foreground(danger)),
		Info:    styleIf(plain, lipgloss.NewStyle().Foreground(info)),
		Header:  styleIf(plain, lipgloss.NewStyle().Foreground(muted).Bold(true)),
		plain:   plain,
	}

	fills := map[Tone]color.Color{
		ToneNeutral: muted,
		ToneSuccess: success,
		ToneWarning: warning,
		ToneDanger:  danger,
		ToneInfo:    info,
	}

	theme.pillFor = make(map[Tone]lipgloss.Style, len(fills))
	for tone, fill := range fills {
		theme.pillFor[tone] = styleIf(plain,
			lipgloss.NewStyle().Foreground(onFill).Background(fill).Bold(true).Padding(0, 1))
	}

	return theme
}

// styleIf returns the style, or a bare one when styling is suppressed.
//
// Suppressing here rather than relying on the writer is belt and braces with a
// reason: a bold style still emits SGR on a profile that has no colour, and a
// plain-text surface that emits *some* escapes is harder to reason about than one
// that emits none.
func styleIf(plain bool, s lipgloss.Style) lipgloss.Style {
	if plain {
		return lipgloss.NewStyle()
	}
	return s
}

// Tone returns the style for an outcome role.
func (t Theme) Tone(tone Tone) lipgloss.Style {
	switch tone {
	case ToneSuccess:
		return t.Success
	case ToneWarning:
		return t.Warning
	case ToneDanger:
		return t.Danger
	case ToneInfo:
		return t.Info
	default:
		return t.Muted
	}
}

// Pill renders a label inside a filled background.
//
// For the one value on a line that a reader is scanning for — a run's status in a
// listing, an outcome in a report. It is deliberately rare: a line with three
// pills has none, because the point of a filled background is that the eye lands
// on it before it lands on anything else.
//
// The label is upper-cased rather than decorated, so that the pill still reads as
// a status once the background is gone.
func (t Theme) Pill(tone Tone, label string) string {
	label = strings.ToUpper(label)
	if t.plain {
		return label
	}

	return t.pillFor[tone].Render(label)
}

// Plain reports whether this theme renders anything at all.
func (t Theme) Plain() bool { return t.plain }

// Palette is each role resolved to a colour, before any style is built from it.
//
// Separated from [Theme] because the colours and the styles built from them have
// different lifetimes: a role is one decision, and the several styles that use it
// are not. Deriving everything from one palette is what keeps `flow --help` and
// `flow list` looking like one program rather than two that happen to ship in one
// binary — the help page having been drawn by a library, once, and by this palette
// only because the library was handed a translation of it.
type Palette struct {
	Muted   color.Color
	Strong  color.Color
	Accent  color.Color
	Success color.Color
	Warning color.Color
	Danger  color.Color
	Info    color.Color

	// OnFill is the text laid over a filled background. Not a role of its own: it
	// is whatever contrasts with a fill, and every fill here is saturated enough
	// that one answer serves all of them.
	OnFill color.Color

	// Surface is a filled background for a block of text rather than a token. Low
	// contrast on purpose: it groups, it does not announce.
	Surface color.Color
}

// NewPalette resolves every role against a background.
//
// Charm's palette supplies the hues, used for their design rather than their
// branding: the values are built as a set and hold contrast against both
// backgrounds, which is the hard part of choosing colours and not worth redoing by
// eye. Each pair is (light background, dark background) — the darker value first,
// because it is the one that has to be legible on a light terminal.
func NewPalette(pick lipgloss.LightDarkFunc) Palette {
	return Palette{
		Muted:   pick(charmtone.Oyster, charmtone.Squid),
		Strong:  pick(charmtone.Pepper, charmtone.Salt),
		Accent:  pick(charmtone.Grape, charmtone.Charple),
		Success: pick(charmtone.Guac, charmtone.Julep),
		Warning: pick(charmtone.Cumin, charmtone.Mustard),
		Danger:  pick(charmtone.Sriracha, charmtone.Coral),
		Info:    pick(charmtone.Oceania, charmtone.Malibu),
		OnFill:  pick(charmtone.Butter, charmtone.Pepper),
		Surface: pick(charmtone.Salt, charmtone.Charcoal),
	}
}
