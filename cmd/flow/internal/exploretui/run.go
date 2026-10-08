package exploretui

import (
	"context"
	"errors"
	"fmt"
	"io"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/colorprofile"
)

// Terminal is the terminal a screen runs on.
type Terminal struct {
	In  io.Reader
	Out io.Writer

	// Profile is the colour depth the output was detected to carry, so the
	// screen is not detected a second time and possibly differently.
	Profile colorprofile.Profile
}

// Run opens the screen on a terminal and returns when it ends. Leaving by a
// key, by ctrl-C or by the context being cancelled is not an error: the screen
// changes nothing, so there is nothing to release.
func Run(ctx context.Context, term Terminal, cfg Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	model, err := New(ctx, cfg)
	if err != nil {
		return err
	}
	if _, err := tea.NewProgram(model,
		tea.WithContext(ctx),
		tea.WithInput(term.In),
		tea.WithOutput(term.Out),
		tea.WithColorProfile(term.Profile),
	).Run(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, tea.ErrProgramKilled) || errors.Is(err, tea.ErrInterrupted) {
			return nil
		}

		return fmt.Errorf("explore screen: %w", err)
	}

	return nil
}
