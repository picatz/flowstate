---
name: flowfile-author
description: Use when writing or changing a Flowstate Flowfile (workflow.yaml or *.flow.yaml); loops on the flow MCP tools until the file validates.
---

# Authoring a Flowfile

A Flowfile is YAML with CEL expressions that compiles to a typed Protobuf
specification. The grammar moves between editions, so do not write from memory.

1. Read the language guide from the `flowstate` MCP server at
   `flowstate://docs/language`, and a close example under
   `flowstate://docs/examples/`.
2. Find tasks with the `flowstate_get_catalog` tool (or the
   `flowstate://catalog/tasks` resource) instead of guessing names and inputs.
3. Write the file, then call `flowstate_validate` on its text. Fix every
   diagnostic; each carries the line it is on. `flow validate <path>` does the
   same from a shell.
4. `flow fmt <path>` puts the file in canonical form and `flow lint <path>`
   suggests idiomatic spellings.
5. Add a `*.test.yaml` beside it (see the `flowfile-test` skill) before calling
   the work done.

Do not edit generated files, and do not invent a step key the guide does not
list: validation rejects it.
