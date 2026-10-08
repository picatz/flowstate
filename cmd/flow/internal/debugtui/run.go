package debugtui

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

// Run opens the screen on a terminal and returns how it ended.
//
// The program owns the terminal for as long as it runs: raw mode, the
// alternate screen and the mouse are bubbletea's, and are given back when it
// returns. A ctrl-C therefore arrives as a key to [Model.Update], which ends
// the screen and returns [OutcomeInterrupt] for the caller to release the run
// by, and the context being cancelled from outside is the same.
func Run(ctx context.Context, term Terminal, cfg Config) (Outcome, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	model, err := New(ctx, cfg)
	if err != nil {
		return OutcomeLeave, err
	}

	final, err := tea.NewProgram(model,
		tea.WithContext(ctx),
		tea.WithInput(term.In),
		tea.WithOutput(term.Out),
		tea.WithColorProfile(term.Profile),
	).Run()
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, tea.ErrProgramKilled) || errors.Is(err, tea.ErrInterrupted) {
			return OutcomeInterrupt, nil
		}

		return OutcomeLeave, fmt.Errorf("debug screen: %w", err)
	}

	done, ok := final.(Model)
	if !ok {
		return OutcomeLeave, fmt.Errorf("debug screen ended with a model of type %T, which is a bug", final)
	}
	if !done.Done() {
		return OutcomeLeave, nil
	}

	return done.Outcome(), nil
}
