# Shared test defaults

This focused testing example shows the three-file relationship that a single
`workflow.test.yaml` cannot:

- `testdefaults.yaml` provides the workflow path, a safe `log` stub, a shared
  assertion, and a variable to every `*.test.yaml` in this directory.
- `workflow.test.yaml` consumes the shared variable.
- `edge.test.yaml` overrides only the input relevant to its case.

Run both suites from the repository root:

```console
$ go run ./cmd/flow test examples/testing-defaults/
PASS  examples/testing-defaults/edge.test.yaml: a sibling suite inherits the same directory fixture
PASS  examples/testing-defaults/workflow.test.yaml: shared defaults supply the workflow and task stub
```

Directory defaults are fixtures, not expressions evaluated separately for every
case. Keep scenario-specific inputs and expectations in each suite; move only
genuinely shared setup into `testdefaults.yaml`. The editor recognizes both file
shapes; see [editor setup](../../docs/EDITORS.md#what-the-server-provides-for-a-test-file).
