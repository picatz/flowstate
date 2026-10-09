---
description: Find why a Flowstate run or Flowfile failed and fix it
argument-hint: <run-id or Flowfile>
---

Debug: $ARGUMENTS

Delegate to the `flowfile-debugger` agent. Treat the argument as a Flowfile
path if such a file exists, otherwise as a run id. Finish with the first
failed step and its reason, the change made, and the validate, test, and
local-replay results, naming anything not verified. Do not start, signal, or
cancel a run on a server unless I say so.
