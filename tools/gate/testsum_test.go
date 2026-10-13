package main

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// TestTheTestLegsPipeThroughTheSameSummarizerMakeTestUses is the drift pin for
// #1727: `make test` and this gate's test legs are meant to print one shape,
// and they only do while both run `go test -json` into tools/testsum. The
// Makefile's recipe is read rather than restated, the same rule the CI tests
// in this package follow for ci.yml.
func TestTheTestLegsPipeThroughTheSameSummarizerMakeTestUses(t *testing.T) {
	data, err := os.ReadFile("../../Makefile")
	if err != nil {
		t.Fatal(err)
	}
	recipe := regexp.MustCompile(`(?m)^test:\n((?:\t.*\n)+)`).FindStringSubmatch(string(data))
	if recipe == nil {
		t.Fatal("the Makefile has no `test:` recipe on the line after the target")
	}
	body := strings.TrimSpace(recipe[1])
	goTest := regexp.MustCompile(`(?m)^GO_TEST_RACE = (.*)$`).FindStringSubmatch(string(data))
	if goTest == nil {
		t.Fatal("the Makefile has no GO_TEST_RACE, the go test command make test runs")
	}
	// The command holds a `$(if $(ARTIFACT_SWEEP),…,)` and the recipe a
	// `$(if $(TEST_JSON),tee …|,)` between the pipe and the summarizer, so
	// the pieces are asserted rather than one string. The shuffle is a
	// variable, on by default now that the tests which register into
	// cmd/flow's process-wide registry put it back (see the Makefile), and
	// the seed it prints is what testsum's rerun lines carry.
	for _, want := range []string{"go test -json", "-shuffle=$(TEST_SHUFFLE)", "-race", "-timeout 900s"} {
		if !strings.Contains(goTest[1], want) {
			t.Errorf("make test's go test command lacks %q: %s", want, goTest[1])
		}
	}
	if strings.Contains(goTest[1], "-p=") {
		t.Errorf("GO_TEST_RACE fixes -p for every package; only the Temporal owners run serially: %s", goTest[1])
	}
	// The split: the Temporal owners one at a time, everything else at the
	// default -p, and both halves into one summarizer.
	for _, want := range []string{"$(TEST_PACKAGES)", "$(TEMPORAL_TEST_PACKAGES)", "$(GO_TEST_RACE) $$parallel", "$(GO_TEST_RACE) -p=1 $$serial", "| ", "go run ./tools/testsum"} {
		if !strings.Contains(body, want) {
			t.Errorf("make test's recipe lacks %q:\n%s", want, body)
		}
	}
	if !strings.HasSuffix(body, "go run ./tools/testsum") {
		t.Errorf("make test's recipe does not end in the summarizer, so its status is not the summarizer's:\n%s", body)
	}
	if !strings.Contains(string(data), "test: .SHELLFLAGS := -o pipefail -c") {
		t.Error("make test runs a pipeline without pipefail, so a go test that died before printing a failure would be green")
	}

	// The gate's legs: same summarizer, same -json.
	for _, tc := range []struct {
		name string
		spec cmdSpec
	}{
		{"module-wide", moduleWideTestCommand()},
		{"narrow", affectedTestCommand([]string{modulePath + "/tools/gate"})},
		{"ordering", goTestSummarized([]string{"GOMEMLIMIT=1GiB"}, "-race", "-cpu=1", "-count=20", "-timeout", orderingTimeout, "./pkg/flowstate/v1/flowtest/")},
	} {
		display := tc.spec.display()
		if !strings.HasPrefix(display, "GOMEMLIMIT=") || !strings.Contains(display, " go test -json ") {
			t.Errorf("%s leg displays %q, which is not a bounded go test -json", tc.name, display)
		}
		if !strings.HasSuffix(display, " | go run ./tools/testsum") {
			t.Errorf("%s leg displays %q, which does not end in the summarizer", tc.name, display)
		}
		if tc.spec.verify == nil {
			t.Errorf("%s leg has no verify func, so the pipeline would be run as one argv", tc.name)
		}
		if tc.name != "ordering" && !hasFieldSequence(display, "-p=1", "-timeout", "900s") {
			t.Errorf("%s leg does not retain the exact package isolation and timeout tokens: %s", tc.name, display)
		}
	}
}

func hasFieldSequence(line string, want ...string) bool {
	fields := strings.Fields(line)
	for i := 0; i+len(want) <= len(fields); i++ {
		if slices.Equal(fields[i:i+len(want)], want) {
			return true
		}
	}
	return false
}

// TestASummarizedLegFailsWhenGoTestFails is the pipefail half: the step's
// status has to be red when `go test` is, not only when the summarizer is.
// A package that does not exist makes go test fail before any event, which
// is exactly the case a pipeline's last-command status would hide.
func TestASummarizedLegFailsWhenGoTestFails(t *testing.T) {
	t.Chdir("../..")
	spec := goTestSummarized(nil, "-run", "XXX", "./tools/gate/testdata/no-such-package/")
	if err := spec.verify(); err == nil {
		t.Fatal("go test on a package that does not exist passed through the summarized leg")
	} else if !strings.Contains(err.Error(), "go test") {
		t.Errorf("the failure %q does not say go test failed", err)
	}
}
