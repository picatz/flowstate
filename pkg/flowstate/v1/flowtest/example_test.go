package flowtest_test

import (
	"fmt"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// Example_runSource drives a workflow and its test file from Go, as bytes,
// which is the Go-facing side of the user-facing flow test command. A stub
// stands in for the http task, so the case exercises the workflow's shape with
// no real dependency ever having to answer: nothing here reaches the network.
//
// The report is the same structure flow test renders. Each case carries its
// name and whether it passed, which is what a caller driving this from Go reads
// back.
func Example_runSource() {
	workflow := []byte(`
edition: v2026.3
name: health-check
steps:
  - id: probe
    http:
      url: https://api.example.com/health
`)

	tests := []byte(`
edition: v2026.3
tests:
  - name: the probe reports healthy
    stubs:
      - task: http
        returns:
          status_code: 200
    expect:
      ran: [probe]
`)

	report := flowtest.RunSource("<submitted>", workflow, tests)

	for _, c := range report.GetCases() {
		fmt.Printf("%s: passed=%t\n", c.GetName(), c.GetPassed())
	}

	// Output:
	// the probe reports healthy: passed=true
}
