---
name: flowfile-test
description: Use when adding or running tests for a Flowstate Flowfile (*.test.yaml): stubbed tasks, scripted signals, virtual clock.
---

# Testing a Flowfile

`*.test.yaml` files declare a workflow, the arguments to run it with, task
responses to stub, scripted signals, and what the run must produce. Cases run
in process on a virtual clock, so a workflow that sleeps for a day finishes in
under a second and nothing reaches the network.

- `flow init <dir>` writes a starter Flowfile and its test file.
- `flow test <path>` runs the cases and reports branch coverage. Add
  `--coverage-required` to fail on steps and `switch` arms no case reaches.
- The `flowstate_test` MCP tool runs a case without a shell.

Write a case for the failure path (a stubbed task error, a timeout, a denied
approval) as well as the happy path; a test that only passes proves little.
A case's `ran:`, `skipped:` and `compensated:` must name real steps, and the
runner refuses a case that names one that does not exist.
