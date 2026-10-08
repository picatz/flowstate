package main

import (
	"fmt"

	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/graph"
)

// newGraphCommand builds `flow graph`.
func newGraphCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "graph <path>...",
		Short: "Show how workflows, tasks and signals connect",
		Long: "Read Flowfiles and show how they connect: which workflows call which, " +
			"which tasks each one runs, and which signals each one waits for. A path " +
			"is a Flowfile, or a directory searched for the YAML files that are " +
			"Flowfiles, by shape rather than by name.\n\n" +
			"The graph is what the files declare, not what is running: it reads files " +
			"and contacts no server. `--output json` writes the `Graph` message, the " +
			"same document any other client of the graph reads, with nodes and edges " +
			"in a fixed order so two runs over the same files are the same bytes.\n\n" +
			"A file that does not compile is left out and named in the graph's notes, " +
			"which marks the graph partial; the command still succeeds, because the " +
			"rest of the graph is true. Use `flow validate` to learn what is wrong " +
			"with the file.",
		Args:          cobra.MinimumNArgs(1),
		RunE:          runGraph,
		SilenceErrors: true,
		SilenceUsage:  true,
		Example: `# How the examples connect:
flow graph examples

# One workflow and the workflows it calls:
flow graph examples/call-a-workflow/workflow.yaml

# The same graph for a program or an agent:
flow graph examples -o json | jq '.edges[] | select(.count > 1)'`,
	}
	addOutputFlag(cmd)

	return cmd
}

func runGraph(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}

	files, err := collectFlowfiles(args)
	if err != nil {
		return err
	}

	var (
		workflows []*v1.Workflow
		skipped   []string
	)
	for _, path := range files {
		// A suite file sits beside the workflow it tests and is not one.
		if isTestFilePath(path) {
			continue
		}
		wf, _, err := flowfile.ParseFile(path)
		if err != nil {
			skipped = append(skipped, fmt.Sprintf("%s does not compile and was left out", path))

			continue
		}
		workflows = append(workflows, wf)
	}

	g := graph.Static(workflows...)
	if len(skipped) > 0 {
		g.Partial = true
		g.Notes = append(skipped[:min(len(skipped), 100)], g.Notes...)
		g.Notes = g.Notes[:min(len(g.Notes), 100)]
	}

	surface := newSurface(cmd)
	if format != FormatText {
		return writeJSON(surface, format, g)
	}

	return graph.Text(surface.Out, g)
}
