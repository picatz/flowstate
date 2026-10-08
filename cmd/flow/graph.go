package main

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/graph"
)

// maxGraphFiles bounds how many Flowfiles one `flow graph` parses.
const maxGraphFiles = 1000

// maxGraphRunPages bounds how many pages of runs one `flow graph --live` reads,
// so a namespace another party fills cannot make the command read forever. At
// the largest page that is ten thousand runs; more is reported as partial.
const maxGraphRunPages = 10

// truncateNote cuts a path to a length the graph schema's notes can hold.
func truncateNote(path string) string {
	const limit = 512
	if len(path) <= limit {
		return path
	}

	return strings.ToValidUTF8(path[:limit], "") + "..."
}

// newGraphCommand builds `flow graph`.
func newGraphCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "graph [path]...",
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
			"with the file.\n\n" +
			"With `--live` the graph also shows what is running: how many runs each " +
			"workflow has in each status, read from the server at `--address`. A " +
			"workflow the server runs and no named file declares is still shown. " +
			"`--filter` narrows the runs with the same CEL expression `flow list` takes. " +
			"Paths are optional with `--live`.",
		Args:          cobra.ArbitraryArgs,
		RunE:          runGraph,
		SilenceErrors: true,
		SilenceUsage:  true,
		Example: `# How the examples connect:
flow graph examples

# One workflow and the workflows it calls:
flow graph examples/call-a-workflow/workflow.yaml

# The same graph for a program or an agent:
flow graph examples -o json | jq '.edges[] | select(.count > 1)'

# What is running now, over what the files declare:
flow graph examples --live

# Only the failures of one workflow:
flow graph --live --filter 'status == "FAILED" && name == "billing"'`,
	}
	addOutputFlag(cmd)
	addServerFlags(cmd)
	cmd.Flags().Bool("live", false, "also show the runs on the server at --address, counted by workflow and status")
	cmd.Flags().String("filter", "", "with --live, a CEL expression over runs, as `flow list --filter` takes")

	return cmd
}

func runGraph(cmd *cobra.Command, args []string) error {
	format, err := resolveOutputFormat(cmd)
	if err != nil {
		return err
	}
	src, err := graphSourcesOf(cmd, args)
	if err != nil {
		return err
	}
	g, err := src.build(cmd)
	if err != nil {
		return err
	}

	surface := newSurface(cmd)
	if format != FormatText {
		return writeJSON(surface, format, g)
	}

	return graph.Text(surface.Out, g)
}

// graphSources is what a graph is read from: the Flowfiles under paths and, with
// live, the runs on the server. `flow graph` reads it once; `flow explore` reads
// it again each time it is asked to refresh.
type graphSources struct {
	paths  []string
	live   bool
	filter string
}

// graphSourcesOf reads the sources the command line names, and refuses a
// combination that names none before anything is read or requested.
func graphSourcesOf(cmd *cobra.Command, paths []string) (graphSources, error) {
	live, _ := cmd.Flags().GetBool("live")
	filter, _ := cmd.Flags().GetString("filter")
	if len(paths) == 0 && !live {
		return graphSources{}, errors.New("name a Flowfile or a directory of them, or use --live to show what is running")
	}
	if filter != "" && !live {
		return graphSources{}, errors.New("--filter narrows the runs that --live reads; add --live")
	}
	if _, err := v1.NewRunFilter(filter); err != nil {
		return graphSources{}, err
	}

	return graphSources{paths: paths, live: live, filter: filter}, nil
}

// build reads the sources into one graph.
func (s graphSources) build(cmd *cobra.Command) (*v1.Graph, error) {
	var files []string
	if len(s.paths) > 0 {
		var err error
		if files, err = collectFlowfiles(s.paths); err != nil {
			return nil, err
		}
	}

	// Bounded before any file is parsed: a directory is another party's to fill,
	// and parsing is the expensive part.
	if len(files) > maxGraphFiles {
		return nil, fmt.Errorf("%d Flowfiles found, more than the %d one graph reads; name a narrower directory", len(files), maxGraphFiles)
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
			skipped = append(skipped, fmt.Sprintf("%s does not compile and was left out", truncateNote(path)))

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

	if s.live {
		return withLiveRuns(cmd, g, s.filter)
	}

	return g, nil
}

// withLiveRuns lays the runs on the server over g.
//
// A page that fails is an error and no graph is written: counts from the pages
// before it would read as the whole system. Only the page bound yields a partial
// graph, which says how many runs it counted.
func withLiveRuns(cmd *cobra.Command, g *v1.Graph, filter string) (*v1.Graph, error) {
	server := serverFlagsOf(cmd)
	client := newWorkflowServiceClient(server)

	var (
		runs     []*v1.RunSummary
		token    string
		more     bool
		excluded uint32
	)
	for range maxGraphRunPages {
		request := &v1.ListRequest{PageSize: 1000, PageToken: token, Filter: filter}
		if err := v1.Validate(request); err != nil {
			return nil, err
		}
		response, err := client.List(cmd.Context(), connect.NewRequest(request))
		if err != nil {
			return nil, refusedList(server, err)
		}
		if d := response.Msg.GetFilterDiagnostic(); d != "" {
			return nil, fmt.Errorf("the server could not evaluate --filter: %s", d)
		}
		runs = append(runs, response.Msg.GetRuns()...)
		excluded += response.Msg.GetExcludedByError()

		previous := token
		token = response.Msg.GetNextPageToken()
		more = token != ""
		if !more {
			break
		}
		// A token that has not moved is the same question again, and the far
		// side decides how long this loop runs.
		if token == previous {
			return nil, fmt.Errorf("the server returned the same page token twice; %d runs were read before stopping", len(runs))
		}
	}

	out := graph.WithRuns(g, runs)
	// Runs the filter could not be evaluated for are left out of the listing and
	// only counted, so a count that ignored them would read as complete.
	if excluded > 0 {
		out.Partial = true
		out.Notes = slices.Insert(out.Notes, 0, fmt.Sprintf("%d runs were left out because --filter could not be evaluated for them", excluded))
		out.Notes = out.Notes[:min(len(out.Notes), 100)]
	}
	if more {
		out.Partial = true
		// First, so the bound is the note a full list does not cut.
		out.Notes = slices.Insert(out.Notes, 0, fmt.Sprintf("the first %d runs were counted; more exist and were not read", len(runs)))
		out.Notes = out.Notes[:min(len(out.Notes), 100)]
	}

	return out, nil
}
