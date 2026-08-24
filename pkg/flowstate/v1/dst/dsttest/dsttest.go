// Package dsttest is the [testing]-shaped front door to the deterministic
// simulation tier: explore a workflow's schedule space and fail the test on the
// first schedule that disagrees with written order.
//
// A separate package, on the model of net/http/httptest and this repository's
// own [secretstest], rather than a function inside dst itself. The reason is the
// one secretstest states, arriving at dst for the first time with issue #800:
// dst used to be reached only from `go test`, and `flow test --seeds N` now
// drives [dst.Explore] over a Flowfile's own cases, so pkg/flowstate/v1/flowtest
// and through it cmd/flow reach dst on the ordinary path. A package a binary
// imports should not carry a [testing.TB]-shaped entry point.
//
// One honest correction to secretstest's version of the argument, since it is
// the reason to state the rule as a boundary rather than as a byte count: the
// `flow` binary already links "testing" transitively, through
// go.temporal.io/sdk/internal. So this move reclaims nothing measurable. What it
// keeps is the property that *our* packages say what they are for — dst
// describes a simulation, dsttest asserts on one — which is what stops the next
// person from reading a test helper in a production package as licence to add
// another. Nothing outside a test imports this package.
//
// [secretstest]: https://pkg.go.dev/github.com/picatz/flowstate/pkg/flowstate/v1/secrets/secretstest
package dsttest

import (
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/dst"
)

// CheckScheduleEquivalence explores one workflow's schedule space and fails tb
// on the first schedule whose observables differ from written order's.
//
// The failure names the seed and prints the command that replays it, because a
// seed nobody can act on is a random number.
func CheckScheduleEquivalence(tb testing.TB, run dst.RunFunc) *dst.Report {
	tb.Helper()

	budget, err := dst.DefaultBudget()
	if err != nil {
		tb.Fatalf("the schedule budget is not usable: %v", err)
	}

	report := dst.Explore(tb.Context(), budget, run)

	// Printed on every run, pass or fail: a search that explored no junctions is
	// a search that proved nothing, and a bound that was reached changes what a
	// green means. Both are facts about the *check*, so neither is allowed to be
	// silent.
	tb.Logf("schedule equivalence: %d schedules explored, up to %d scheduling decisions each, truncated=%t",
		report.Schedules(), report.Decisions(), report.Truncated())

	if report.Divergence == nil {
		return report
	}

	tb.Fatalf("%s", dst.FailureText(tb.Name(), report.Divergence))

	return report
}
