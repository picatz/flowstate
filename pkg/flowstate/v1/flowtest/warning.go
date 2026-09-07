package flowtest

import (
	"fmt"

	"github.com/picatz/flowstate/internal/textbound"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// These bounds are applied while a suite is assembled, before a TestReport is
// retained or handed to a renderer. Source-sized warning messages and the
// product of the per-file case and stub limits must not become a report-sized
// memory multiplier.
const (
	maxWarningMessageBytes = 4 << 10
	maxSuiteWarningBytes   = 64 << 10
	maxSuiteWarnings       = 256
	maxWarningMarkerBytes  = 256
)

type suiteWarningBudget struct {
	remaining int
	kept      int
	omitted   int
	marker    *v1.Diagnostic
}

func newSuiteWarningBudget() *suiteWarningBudget {
	// Reserve the marker up front so reporting an exhausted budget cannot be
	// what pushes the retained warning text beyond it.
	return &suiteWarningBudget{remaining: maxSuiteWarningBytes - maxWarningMarkerBytes}
}

// take keeps the first warnings that fit the suite budget. Once it is spent,
// one diagnostic records the aggregate omitted count instead of silently
// making a shortened report look complete.
//
// What is returned is what the report retains, so nothing here may be sized by
// what was thrown away: a case that produced ten thousand warnings must not
// leave a slice with room for ten thousand behind it.
func (b *suiteWarningBudget) take(warnings []*v1.Diagnostic) []*v1.Diagnostic {
	if len(warnings) == 0 {
		return nil
	}

	// The marker already exists, so every warning here is omitted and the
	// count is the whole of what is still reported. Bounding messages that are
	// about to be dropped, and allocating a slice to hold none of them, would
	// be work proportional to the amplification this budget exists to refuse.
	if b.marker != nil {
		b.omitted += len(warnings)
		b.markOmitted()
		return nil
	}

	// Capacity for the slots the suite can still fill — the remaining retained
	// warnings plus the marker — rather than for the warnings this case
	// produced.
	kept := make([]*v1.Diagnostic, 0, min(len(warnings), maxSuiteWarnings-b.kept))
	for i, warning := range warnings {
		warning.Message = boundedMessage(warning.GetMessage(), maxWarningMessageBytes)
		if b.kept >= maxSuiteWarnings-1 || len(warning.GetMessage()) > b.remaining {
			// The marker stands in for this warning and every one after it, in
			// this case and in every later one, so it takes the field of the
			// first warning it replaced and is placed in the file by the same
			// caller that places the warnings it kept.
			b.omitted += len(warnings) - i
			b.marker = &v1.Diagnostic{Field: warning.GetField()}
			b.markOmitted()
			kept = append(kept, b.marker)

			break
		}

		b.remaining -= len(warning.GetMessage())
		b.kept++
		kept = append(kept, warning)
	}
	return kept
}

// markOmitted rewrites the marker for the running omitted count, bounded by the
// same reservation newSuiteWarningBudget held back for it: the sentence is
// short today, and the budget stays honest if it is ever reworded or localized.
func (b *suiteWarningBudget) markOmitted() {
	b.marker.Message = boundedMessage(fmt.Sprintf(
		"%d additional warning(s) omitted because the suite warning budget was reached; rerun fewer cases (--run) to see them",
		b.omitted), maxWarningMarkerBytes)
}

func boundedMessage(message string, limit int) string {
	if len(message) <= limit {
		return message
	}
	suffix := fmt.Sprintf("... (truncated, exceeded %d bytes)", limit)
	return textbound.Cut(message, limit-len(suffix)) + suffix
}
