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
//
// maxSuiteWarnings and maxSuiteWarningBytes bound the author-influenced text a
// suite retains. The omission marker is outside both: it is a fixed-size
// sentence, one per case that had a warning dropped, so marker text is bounded
// by [MaxTestsPerFile] — the loader's own bound on cases — rather than by
// anything a document chooses. Bounding the markers along with the warnings
// would trade a bounded cost for a wrong verdict, because a case whose
// warnings are all dropped must still hold one, as below.
const (
	maxWarningMessageBytes = 4 << 10
	maxSuiteWarningBytes   = 64 << 10
	maxSuiteWarnings       = 256
	maxWarningMarkerBytes  = 256
)

// suiteWarningBudget doles the suite's warning text out across its cases, in
// case order — the earlier warning wins, matching [suiteTranscriptBudget] and
// every other first-N truncation here.
type suiteWarningBudget struct {
	remaining int
	kept      int
}

func newSuiteWarningBudget() *suiteWarningBudget {
	return &suiteWarningBudget{remaining: maxSuiteWarningBytes}
}

// take keeps the first warnings that fit the suite budget and replaces the rest
// with the one diagnostic saying how many this case lost, so a shortened report
// never reads as a complete one.
//
// Every case that warned keeps a warning. `--fail-on-warning` and the per-case
// PASS/FAIL line both read `len(TestCase.Warnings)`, so a case returning none
// is a case the CLI calls green: dropping the last diagnostic off a case that
// had one would let a memory bound decide a verdict, which is a different and
// worse defect than the amplification this budget exists to refuse (Codex,
// #1857). The marker is that case's own, like [suiteTranscriptBudget.take]'s
// dropped-account line, rather than a suite-wide one an earlier case holds.
//
// What is returned is what the report retains for its whole life, so nothing
// here is sized by what was dropped: a case that produced ten thousand
// warnings must not leave a slice with room for ten thousand behind it.
func (b *suiteWarningBudget) take(warnings []*v1.Diagnostic) []*v1.Diagnostic {
	if len(warnings) == 0 {
		return nil
	}

	// Capacity for the slots the suite can still fill plus this case's marker,
	// rather than for the warnings the case produced.
	kept := make([]*v1.Diagnostic, 0, min(len(warnings), maxSuiteWarnings-b.kept+1))
	for i, warning := range warnings {
		warning.Message = boundedMessage(warning.GetMessage(), maxWarningMessageBytes)
		if b.kept >= maxSuiteWarnings || len(warning.GetMessage()) > b.remaining {
			// The marker takes the field of the first warning it replaced, so
			// the caller that places this case's warnings places it too and an
			// editor can underline it where the dropped ones would have been.
			kept = append(kept, omissionMarker(warning.GetField(), len(warnings)-i))

			break
		}

		b.remaining -= len(warning.GetMessage())
		b.kept++
		kept = append(kept, warning)
	}
	return kept
}

// omissionMarker is the one diagnostic that stands in for a case's dropped
// warnings, bounded like any other message: the sentence is short today, and
// the budget stays honest if it is ever reworded, localized, or handed a count
// with more digits than expected.
func omissionMarker(field string, omitted int) *v1.Diagnostic {
	return &v1.Diagnostic{
		Field: field,
		Message: boundedMessage(fmt.Sprintf(
			"%d warning(s) omitted: the suite's warning budget is spent; rerun fewer cases (--run) to see them",
			omitted), maxWarningMarkerBytes),
	}
}

func boundedMessage(message string, limit int) string {
	if len(message) <= limit {
		return message
	}
	suffix := fmt.Sprintf("... (truncated, exceeded %d bytes)", limit)
	return textbound.Cut(message, limit-len(suffix)) + suffix
}
