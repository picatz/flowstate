package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/picatz/flowstate/cmd/flow/internal/exploretui"
	"github.com/picatz/flowstate/cmd/flow/internal/tui"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// newExploreCommand builds `flow explore`.
func newExploreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "explore [path]...",
		Short: "Explore how workflows connect, and what is running, on a screen",
		Long: "Open the graph `flow graph` writes as a screen you move around in: every " +
			"workflow is a row, and opening one shows what it calls, the signals it " +
			"waits for and the tasks it runs, to any depth. The pane beside it " +
			"describes the selected row, including what calls it.\n\n" +
			"It reads the same sources as `flow graph`, with the same flags: Flowfiles " +
			"under the paths, and with `--live` the runs on the server at `--address`, " +
			"counted by workflow and status. Press r to read them again; what is open " +
			"stays open. The screen changes nothing anywhere.\n\n" +
			"Keys are the debugger's: j and k move, enter or l opens, h closes or goes " +
			"to the parent, ? lists them all, q leaves. Rows can be clicked and the " +
			"wheel scrolls.\n\n" +
			"It needs a terminal at least 40 columns by 10 rows. For a script or an " +
			"agent, `flow graph --output json` is the same graph as data.",
		Args:          cobra.ArbitraryArgs,
		RunE:          runExplore,
		SilenceErrors: true,
		SilenceUsage:  true,
		Example: `# How the examples connect:
flow explore examples

# What is running, over what the files declare:
flow explore examples --live`,
	}
	addServerFlags(cmd)
	cmd.Flags().Bool("live", false, "also show the runs on the server at --address, counted by workflow and status")
	cmd.Flags().String("filter", "", "with --live, a CEL expression over runs, as `flow list --filter` takes")

	return cmd
}

func runExplore(cmd *cobra.Command, args []string) error {
	src, err := graphSourcesOf(cmd, args)
	if err != nil {
		return err
	}

	surface := newSurface(cmd)
	size := tui.Size{W: exploretui.MinWidth, H: exploretui.MinHeight}
	if why := terminalRefusal(cmd.InOrStdin(), surface.Out, size); why != "" {
		return fmt.Errorf("flow explore needs a terminal, and %s; `flow graph` writes the same graph as text or JSON", why)
	}

	// Both were checked by [terminalRefusal].
	stdin, _ := cmd.InOrStdin().(*os.File)
	sink, _ := terminalFile(surface.Out)
	width, height, _ := term.GetSize(int(sink.Fd()))

	return exploretui.Run(cmd.Context(), exploretui.Terminal{In: stdin, Out: sink, Profile: surface.Caps.Profile}, exploretui.Config{
		Load:   func(context.Context) (*v1.Graph, error) { return src.build(cmd) },
		Source: src.describe(),
		Style:  exploretui.Style{Theme: surface.Theme, Symbols: surface.Caps.Symbols()},
		Size:   tui.Size{W: width, H: height},
	})
}

// describe says what the sources are, for a screen's header.
func (s graphSources) describe() string {
	var parts []string
	for _, p := range s.paths {
		parts = append(parts, filepath.Base(filepath.Clean(p)))
	}
	if s.live {
		parts = append(parts, "live")
	}

	return strings.Join(parts, " ")
}
