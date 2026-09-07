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
func (b *suiteWarningBudget) take(warnings []*v1.Diagnostic) []*v1.Diagnostic {
	kept := make([]*v1.Diagnostic, 0, len(warnings))
	for _, warning := range warnings {
		warning.Message = boundedWarningMessage(warning.GetMessage())
		if b.marker != nil || b.kept >= maxSuiteWarnings-1 || len(warning.GetMessage()) > b.remaining {
			b.omit(warning, &kept)
			continue
		}

		b.remaining -= len(warning.GetMessage())
		b.kept++
		kept = append(kept, warning)
	}
	return kept
}

func (b *suiteWarningBudget) omit(warning *v1.Diagnostic, into *[]*v1.Diagnostic) {
	b.omitted++
	if b.marker == nil {
		b.marker = &v1.Diagnostic{Field: warning.GetField()}
		*into = append(*into, b.marker)
	}
	b.marker.Message = fmt.Sprintf(
		"%d additional warning(s) omitted because the suite warning budget was reached; rerun fewer cases (--run) to see them",
		b.omitted)
}

func boundedWarningMessage(message string) string {
	if len(message) <= maxWarningMessageBytes {
		return message
	}
	suffix := fmt.Sprintf("... (truncated, exceeded %d bytes)", maxWarningMessageBytes)
	return textbound.Cut(message, maxWarningMessageBytes-len(suffix)) + suffix
}
