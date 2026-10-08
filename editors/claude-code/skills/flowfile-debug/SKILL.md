---
name: flowfile-debug
description: Use when a Flowstate run or test case misbehaves and you need to step through it, inspect values, or watch a durable run.
---

# Debugging a Flowfile

- `flowstate_debug` (MCP) steps through a test case under the same stubs and
  virtual clock, so a session is reproducible and reaches nothing.
- `flow run local <path>` runs the file in this process; `flow run <path>`
  runs it durably on a server, and `flow watch <id>`, `flow timeline <id>`
  and `flow get <id>` report what a run did.
- `flow dap` speaks the Debug Adapter Protocol: breakpoints on lines, or
  function breakpoints named after a step (`build`, `pages[2]/page`), with
  conditions and hit counts. `attach` debugs a durable run and needs the
  `workload.debug` permission.
- `flow graph --live` shows what is running across workflows.

Start from the failing step id in the timeline, reproduce it as a test case
with the same stubs, then step through that case rather than the live run.
