package main

import (
	"strings"
	"testing"

	flowstatev1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
	"github.com/picatz/flowstate/pkg/flowstate/v1/secrets"
)

func TestClampMaxOutputBytesDefaultsAndRefusesOverCeiling(t *testing.T) {
	got, err := clampMaxOutputBytes(0)
	if err != nil || got != defaultMaxOutputBytes {
		t.Fatalf("clampMaxOutputBytes(0) = (%d, %v), want (%d, nil)", got, err, defaultMaxOutputBytes)
	}

	if _, err := clampMaxOutputBytes(-1); err == nil {
		t.Error("clampMaxOutputBytes(-1): got no error, want one")
	}

	if _, err := clampMaxOutputBytes(maxMaxOutputBytes + 1); err == nil {
		t.Error("clampMaxOutputBytes(ceiling+1): got no error, want one - a value over the ceiling " +
			"must be refused, not silently clamped")
	}

	got, err = clampMaxOutputBytes(1024)
	if err != nil || got != 1024 {
		t.Fatalf("clampMaxOutputBytes(1024) = (%d, %v), want (1024, nil)", got, err)
	}
}

func TestClampMaxEventsDefaultsAndRefusesOverCeiling(t *testing.T) {
	got, err := clampMaxEvents(0)
	if err != nil || got != defaultMaxEvents {
		t.Fatalf("clampMaxEvents(0) = (%d, %v), want (%d, nil)", got, err, defaultMaxEvents)
	}

	if _, err := clampMaxEvents(-1); err == nil {
		t.Error("clampMaxEvents(-1): got no error, want one")
	}

	if _, err := clampMaxEvents(maxMaxEvents + 1); err == nil {
		t.Error("clampMaxEvents(ceiling+1): got no error, want one")
	}
}

// TestMaxOutputBytesCeilingFitsATaskOutput pins #2168: a run that tries to
// spend more than the ceiling in every field a run controls - the final
// message, the patch, the changed files and the events - comes back within
// flowstatev1.MaxTaskOutputBytes as the host measures a step output.
func TestMaxOutputBytesCeilingFitsATaskOutput(t *testing.T) {
	// Control characters are what JSON spells with the most bytes (\u0001),
	// the worst case for the encoding the host measures.
	text := func(n int) string { return strings.Repeat("\x01", n) }

	var files []fileChange
	for range maxDiffFiles {
		files = append(files, fileChange{Path: text(4096), OldPath: text(4096), ChangeType: text(16)})
	}

	events := make([]eventLine, maxMaxEvents)
	for i := range events {
		events[i] = eventLine{kind: text(32), summary: text(maxEventSummaryBytes)}
	}

	// Many tiny records, where the framing around each is what costs.
	var tinyFiles []fileChange
	for range maxDiffFiles {
		tinyFiles = append(tinyFiles, fileChange{Path: "a", OldPath: "b", ChangeType: "c"})
	}

	tinyEvents := make([]eventLine, maxMaxEvents)
	for i := range tinyEvents {
		tinyEvents[i] = eventLine{kind: "k", summary: "s"}
	}

	for name, tc := range map[string]struct {
		patch  string
		files  []fileChange
		events []eventLine
	}{
		"everything at once": {patch: text(maxMaxOutputBytes), files: files, events: events},
		"files only":         {files: files},
		"events only":        {events: events},
		"tiny records":       {files: tinyFiles, events: tinyEvents},
	} {
		t.Run(name, func(t *testing.T) {
			run := runResult{finalMessage: text(maxFinalMessageBytes * 2), threadID: text(4096), events: tc.events}

			out := spendOutputBudget(run, secrets.NewScrubber(), tc.patch, false, tc.files, maxMaxOutputBytes, maxMaxEvents)
			if name != "tiny records" && !out.GetTruncated() {
				t.Fatalf("a run asking for more than the ceiling came back untruncated")
			}

			encoded, err := sdk.EncodeOutputs(out)
			if err != nil {
				t.Fatalf("EncodeOutputs: %v", err)
			}
			if err := flowstatev1.CheckTaskOutputSize(encoded); err != nil {
				t.Fatalf("a codex result at its own ceiling (%d bytes) is refused by the host: %v", maxMaxOutputBytes, err)
			}
		})
	}
}

// TestBoundFilesSpendsTheBudgetOnPaths pins that changed files are charged for
// the text they carry and a non-positive budget keeps none.
func TestBoundFilesSpendsTheBudgetOnPaths(t *testing.T) {
	files := []fileChange{{Path: "aaaa", ChangeType: "add"}, {Path: "bbbb", ChangeType: "add"}}
	each := 7 + fileFramingBytes

	if got, truncated, spent := boundFiles(files, each); len(got) != 1 || !truncated || spent != each {
		t.Errorf("boundFiles(budget %d) = %d files, truncated %v, spent %d; want 1, true, %d", each, len(got), truncated, spent, each)
	}
	if got, truncated, _ := boundFiles(files, 0); len(got) != 0 || !truncated {
		t.Errorf("boundFiles(budget 0) kept %d files (truncated %v); want none, true", len(got), truncated)
	}
	if got, truncated, spent := boundFiles(files, 2*each); len(got) != 2 || truncated || spent != 2*each {
		t.Errorf("boundFiles(budget %d) = %d files, truncated %v, spent %d; want 2, false, %d", 2*each, len(got), truncated, spent, 2*each)
	}
}
