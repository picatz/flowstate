package docsgen

import (
	"fmt"
	"strings"
)

// MCPTool is one tool `flow mcp` registers, as the reference describes it.
//
// Handed in rather than derived here, because the derivation needs the dispatch
// table itself: which side a tool answers on is carried by a Go func value that
// nothing outside can inspect, and the descriptions are assembled beside the
// tools they belong to. cmd/flow builds this list next to that table, where a
// tool added without a row is a compile-time neighbour rather than a document
// that quietly lost a line.
type MCPTool struct {
	// Name is the tool as an agent calls it, e.g. flowstate_run_local.
	Name string

	// Description is the sentence a model chooses the tool by.
	Description string

	// Request is the full name of the request message, where the tool is an RPC
	// and there is one. Empty for a tool with no RPC behind it.
	Request string

	// Response is the full name of the response message, where the tool is an
	// RPC and there is one. Empty for a tool with no RPC behind it.
	Response string

	// Local marks a tool that answers in the `flow mcp` process itself, with no
	// server and no Temporal.
	Local bool

	// Effect is what calling the tool does, rendered from the annotations it
	// advertises: read, idempotent write or write, with destructive and
	// open-world where they hold.
	Effect string
}

// reach is the "Answers" column.
func (t MCPTool) reach() string {
	if t.Local {
		return "locally"
	}

	return "via a server"
}

// renderMCPReference documents the agent surface.
//
// The tool set is derived: what cmd/flow hands over is what `flow mcp`
// registers, asserted against the service descriptor in both directions by its
// own tests, so walking it here documents exactly what an agent connects to.
func (g *Generator) renderMCPReference() string {
	var b strings.Builder

	b.WriteString(generatedNotice + "\n\n")
	b.WriteString("# MCP tool reference\n\n")
	b.WriteString("`flow mcp` serves the control plane to an agent over stdio. Every WorkflowService\n")
	b.WriteString("RPC becomes one tool, discovered by walking the service descriptor rather than\n")
	b.WriteString("kept in a list, so an RPC added to the schema is a tool the day the code is\n")
	b.WriteString("regenerated.\n\n")
	b.WriteString("**Answers locally** means the tool needs no server and no Temporal: authoring\n")
	b.WriteString("(validate, compile, read the catalog, rehearse a run) works with nothing else\n")
	b.WriteString("stood up. The rest address durable runs, which only a server has, and say so\n")
	b.WriteString("rather than failing opaquely when `--address` was not given.\n\n")
	b.WriteString("**Effect** is the tool's `annotations` as a client reads them, derived from the\n")
	b.WriteString("`method_rules` option on the RPC (and declared in Go for the tools that are not\n")
	b.WriteString("RPCs): whether it reads or writes, whether repeating a write changes nothing\n")
	b.WriteString("further, whether it destroys something, and whether it reaches beyond this\n")
	b.WriteString("deployment. They are hints for deciding whether to ask a person first and carry\n")
	b.WriteString("no authority; authorization is the caller's scope.\n\n")
	b.WriteString("Every proto-derived RPC tool advertises an `outputSchema` for its response and\n")
	b.WriteString("answers with two forms of one document. The text block is the document\n")
	b.WriteString("`--output json` prints, so a `jq` expression written against the CLI reads an\n")
	b.WriteString("agent's answer unchanged: a run's outputs are `steps.<id>.<output>` and a value\n")
	b.WriteString("is the value, not CEL's tagged encoding of one. `structuredContent` carries the\n")
	b.WriteString("same answer in the schema's own protojson. A schema-aware client can therefore\n")
	b.WriteString("pass a successful `flowstate_compile` `workflow` result directly to\n")
	b.WriteString("`flowstate_run`. Both forms are counted against the surface's size limit\n")
	b.WriteString("because both leave the process. Local-only tools keep their purpose-built text\n")
	b.WriteString("contracts and do not advertise a schema-owned result message.\n\n")

	b.WriteString("| Tool | Answers | Effect | Request message | Response message |\n|---|---|---|---|---|\n")
	for _, tool := range g.src.MCPTools {
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s | %s |\n",
			cell(tool.Name), tool.reach(), orDash(cell(tool.Effect)), orDash(codeOrEmpty(tool.Request)), orDash(codeOrEmpty(tool.Response)))
	}
	b.WriteString("\n")

	for _, tool := range g.src.MCPTools {
		fmt.Fprintf(&b, "## `%s`\n\n", tool.Name)
		fmt.Fprintf(&b, "%s\n\n", tool.Description)
	}

	return b.String()
}
