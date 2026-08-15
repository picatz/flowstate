package main

import (
	"fmt"

	"github.com/spf13/cobra"

	flowmcp "github.com/picatz/flowstate/cmd/flow/internal/mcp"

	"github.com/picatz/flowstate/cmd/flow/internal/docsgen"
)

// The reference generator's wiring: the command, and the live values it derives
// from.
//
// The generator itself lives in cmd/flow/internal/docsgen, which is where it
// can be tested as something other than its own output (#410). What stays here
// is what only this package can supply — the cobra tree, the two renderers
// `flow help` spells a command and a flag with, the MCP tool table, and the
// address default — assembled in one place so that "what the reference is
// derived from" is a list somebody can read rather than a set of reaches from
// inside the generator.

// newDocsCommand builds the hidden `docs` command.
//
// Hidden and registered beside `flow man` in execute.go rather than in
// [newRootCommand], for the same reason that one is: it is a build step, not
// something a user types. Keeping it out of the root constructor also keeps it
// out of the command tree the README's pin tests and the generated CLI reference
// walk, which is correct — `flow docs generate` documents the product, and is
// not part of it.
func newDocsCommand() *cobra.Command {
	docs := &cobra.Command{
		Use:                   "docs",
		Short:                 "Generate reference documentation",
		Hidden:                true,
		SilenceUsage:          true,
		DisableFlagsInUseLine: true,
	}

	generate := &cobra.Command{
		Use:          "generate",
		Short:        "Write the generated reference into docs/reference/",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			dir, err := cmd.Flags().GetString("dir")
			if err != nil {
				return err
			}

			generator, err := newReferenceGenerator()
			if err != nil {
				return err
			}

			written, err := generator.Generate(dir)
			if err != nil {
				return err
			}

			for _, path := range written {
				fmt.Fprintln(cmd.OutOrStdout(), path)
			}

			return nil
		},
	}
	generate.Flags().String("dir", docsgen.DefaultDir, "directory to write the generated reference into")

	docs.AddCommand(generate)

	return docs
}

// newReferenceGenerator hands the generator this binary's own live values.
//
// [newRootCommand] as a constructor rather than a built tree, because the
// environment-mirror probe rebuilds it once per documented variable: a flag
// default is read from the environment at construction, so one shared command
// would answer with whatever the environment held when it was made.
func newReferenceGenerator() (*docsgen.Generator, error) {
	return docsgen.New(docsgen.Sources{
		NewRoot:        newRootCommand,
		UseLine:        useLine,
		FlagName:       flagName,
		MCPTools:       mcpToolDocs(),
		DefaultAddress: defaultServerAddress,
	})
}

// mcpToolDocs describes every tool `flow mcp` registers, in the order it
// registers them.
//
// Here rather than in the generator because the derivation needs this package:
// [flowmcp.WorkflowServiceMethods] is the registration itself, asserted against
// the service descriptor in both directions by cmd/flow/internal/mcp's own
// tests, so walking it is what makes the reference document exactly what an
// agent connects to. Which side a tool answers on is [flowmcp.LocalTools],
// hand-kept there for the identical reason: the dispatch closure in
// [flowmcp.WorkflowServiceMethods] is a Go func value nothing can inspect from
// outside.
func mcpToolDocs() []docsgen.MCPTool {
	var tools []docsgen.MCPTool
	for _, method := range flowmcp.WorkflowServiceMethods() {
		tools = append(tools, docsgen.MCPTool{
			Name:        flowmcp.ToolName(method.Name),
			Description: flowmcp.ToolDescription(method.Name),
			Request:     string(method.Input.FullName()),
			Local:       flowmcp.LocalTools[method.Name],
		})
	}

	tools = append(tools, docsgen.MCPTool{
		Name:        flowmcp.RunLocalToolName,
		Description: flowmcp.RunLocalToolDescription,
		Local:       true,
	})
	tools = append(tools, docsgen.MCPTool{
		Name:        flowmcp.TestToolName,
		Description: flowmcp.TestToolDescription,
		Local:       true,
	})

	return tools
}
