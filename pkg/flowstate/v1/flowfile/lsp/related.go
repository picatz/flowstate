package lsp

import (
	"fmt"
	"regexp"

	"github.com/sourcegraph/go-lsp"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Some of the validator's complaints are about two places at once: a duplicate id
// is a problem *because of* the earlier step that already has it, and a reference
// to a step that runs later is a problem because of where that step is. The
// diagnostic sits on the place that is wrong; the other place is what an author
// has to look at to fix it, and without it they search the file by eye.
//
// LSP carries that as related information, which go-lsp's Diagnostic has no field
// for, so the published form below embeds it and adds the field. The embedded
// struct marshals flat, so a client that knows nothing of related information
// reads the same payload it always did.

// relatedInformation is a location that explains a diagnostic.
type relatedInformation struct {
	Location lsp.Location `json:"location"`
	Message  string       `json:"message"`
}

// publishedDiagnostic is a diagnostic as it goes on the wire.
type publishedDiagnostic struct {
	lsp.Diagnostic

	RelatedInformation []relatedInformation `json:"relatedInformation,omitempty"`
}

// publishDiagnosticsParams is lsp.PublishDiagnosticsParams over [publishedDiagnostic].
type publishDiagnosticsParams struct {
	URI         lsp.DocumentURI       `json:"uri"`
	Diagnostics []publishedDiagnostic `json:"diagnostics"`
}

// publishable attaches related information to each diagnostic that has some.
func publishable(doc *document, carried []carriedDiagnostic) []publishedDiagnostic {
	out := make([]publishedDiagnostic, 0, len(carried))
	for _, c := range carried {
		out = append(out, publishedDiagnostic{
			Diagnostic:         c.published,
			RelatedInformation: relatedFor(doc, c),
		})
	}

	return out
}

// The message shapes the validator writes for the two cases, held here with the
// code that depends on them; related_test.go pins each against the validator's
// own output, so rewording one there fails here and not silently.
var (
	duplicateIDMessage    = regexp.MustCompile(`^duplicate id "([^"]+)"`)
	referencedStepMessage = regexp.MustCompile(`^references step "([^"]+)"`)
)

// relatedFor finds the other place a diagnostic is about, if it names one.
//
// A duplicate id is reported on one declaration (the validator positions by id, so
// the first), which leaves the others as the places to look: each is listed.
func relatedFor(doc *document, c carriedDiagnostic) []relatedInformation {
	if doc.parsed == nil {
		return nil
	}

	msg := c.published.Message
	var out []relatedInformation
	add := func(s *parsedStep, label string) {
		rng, ok := idNameRange(doc, s)
		if !ok || overlaps(rng, c.published.Range) {
			return // the diagnostic is already on the declaration it would point at
		}
		out = append(out, relatedInformation{
			Location: lsp.Location{URI: doc.uri, Range: rng},
			Message:  fmt.Sprintf("step %q %s", s.id, label),
		})
	}

	switch {
	case duplicateIDMessage.MatchString(msg):
		id := duplicateIDMessage.FindStringSubmatch(msg)[1]
		for _, s := range doc.parsed.steps {
			if s.id == id {
				add(s, "also declared here")
			}
		}
	case c.source.Code == v1.DiagnosticCodeUnresolvedReference && referencedStepMessage.MatchString(msg):
		if target := doc.parsed.step(referencedStepMessage.FindStringSubmatch(msg)[1]); target != nil {
			add(target, "declared here")
		}
	}

	return out
}
