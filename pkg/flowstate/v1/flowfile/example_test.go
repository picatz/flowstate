package flowfile_test

import (
	"fmt"
	"strings"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// ExampleParse compiles a Flowfile from bytes into a workflow. Parse decides a
// step's task purely from its shape, so a well-formed file compiles here even
// if a task name is one this build does not register; that "does the build know
// this task" question belongs to [flowfile.Validate], not to Parse.
func ExampleParse() {
	workflow, _, err := flowfile.Parse([]byte(`
edition: v2026.3
name: pipeline
steps:
  - id: fetch
    http:
      url: https://api.example.com/data
  - id: notify
    log:
      message: done
`))
	if err != nil {
		fmt.Println("parse:", err)
		return
	}

	fmt.Println("name: ", workflow.GetName())
	fmt.Println("steps:", len(workflow.GetSteps()))
	for _, step := range workflow.GetSteps() {
		fmt.Println("  -", step.GetId())
	}

	// Output:
	// name:  pipeline
	// steps: 2
	//   - fetch
	//   - notify
}

// ExampleValidate reports the problems in a compiled workflow that would
// otherwise surface only at run time. Here a step's if: references a step that
// does not exist, which Parse accepts (it is well-formed) but Validate catches:
// the diagnostic names the step, the field, and what is wrong.
func ExampleValidate() {
	workflow, _, err := flowfile.Parse([]byte(`
edition: v2026.3
name: report
steps:
  - id: notify
    if: ${steps.fetch.status == 200}
    log:
      message: done
`))
	if err != nil {
		fmt.Println("parse:", err)
		return
	}

	diagnostics := flowfile.Validate(workflow)
	fmt.Println("problems:", len(diagnostics))
	for _, d := range diagnostics {
		// A diagnostic with no source position renders with a leading space
		// where the line:column would go; trim it for a clean one-line report.
		fmt.Println(strings.TrimSpace(d.Error()))
	}

	// Output:
	// problems: 1
	// step "notify" if: references unknown step "fetch"
}
