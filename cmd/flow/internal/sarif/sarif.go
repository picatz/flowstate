// Package sarif renders Flowstate diagnostic reports as SARIF 2.1.0, the Static
// Analysis Results Interchange Format, so a code-scanning consumer (GitHub code
// scanning, an IDE's problems panel) reads `flow validate` without a bespoke
// parser.
//
// It is a projection of [v1.DiagnosticReport], not a second source of truth: a
// diagnostic's code becomes the rule, its line and column the region, its step
// address a logical location, its [v1.RelatedLocation] a related location and its
// [v1.SuggestedEdit] a fix. Anything the report does not say is omitted rather than
// invented; SARIF lines and columns start at 1, so the schema's zero for "unknown"
// never reaches the output as a position.
//
// SARIF 2.1.0: https://docs.oasis-open.org/sarif/sarif/v2.1.0/sarif-v2.1.0.html
package sarif

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"slices"
	"strings"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

const (
	// Version is the SARIF version written, §3.13.2.
	Version = "2.1.0"

	// Schema locates the SARIF 2.1.0 JSON Schema, §3.13.3.
	Schema = "https://json.schemastore.org/sarif-2.1.0.json"

	// MaxResults bounds the results in one run. GitHub code scanning accepts at
	// most 25,000 per run, and a report is built from files another party wrote,
	// so the work and the output stop here; the run says how many were left out.
	MaxResults = 25000

	// MaxBytes bounds the text the results carry (messages, file names, edit
	// titles and replacements) before encoding. It keeps the log under the size
	// code-scanning uploads accept however large the diagnostics are; the run
	// says how many results were left out.
	MaxBytes = 16 << 20
)

// Tool names the program that produced the results, §3.19.
type Tool struct {
	Name           string
	Version        string
	InformationURI string
}

// Marshal renders reports as one SARIF log with a single run. The output is
// deterministic for the same reports: results are in report and diagnostic order
// and rules in the registry's order.
func Marshal(reports []*v1.DiagnosticReport, tool Tool) ([]byte, error) {
	doc := run{ColumnKind: "unicodeCodePoints", Results: []result{}, Tool: toolComponent{Driver: driver{
		Name: tool.Name, Version: tool.Version, InformationURI: tool.InformationURI,
	}}}

	used := map[string]bool{}
	var dropped, budget int
	full := false
	for _, report := range reports {
		diagnostics := report.GetDiagnostics()
		if full {
			// Nothing more is projected; the omitted count is just a length.
			dropped += len(diagnostics)
			continue
		}
		for i, d := range diagnostics {
			budget += weigh(report.GetFile(), d)
			if len(doc.Results) >= MaxResults || budget > MaxBytes {
				dropped += len(diagnostics) - i
				full = true
				break
			}
			code := d.GetCode()
			if code == "" {
				code = string(v1.DiagnosticCodeGeneral)
			}
			used[code] = true
			doc.Results = append(doc.Results, resultOf(report.GetFile(), code, d))
		}
	}

	rules, index := rulesFor(used)
	doc.Tool.Driver.Rules = rules
	for i := range doc.Results {
		doc.Results[i].RuleIndex = index[doc.Results[i].RuleID]
	}
	doc.Invocations = []invocation{{ExecutionSuccessful: true}}
	if dropped > 0 {
		doc.Invocations[0].ToolExecutionNotifications = []notification{{
			Level: "warning",
			Message: message{Text: fmt.Sprintf(
				"%d further results were left out: a run holds at most %d results and %d bytes of text", dropped, MaxResults, MaxBytes)},
		}}
	}

	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(log{Schema: Schema, Version: Version, Runs: []run{doc}}); err != nil {
		return nil, fmt.Errorf("rendering the SARIF log: %w", err)
	}

	return out.Bytes(), nil
}

// rulesFor describes the codes in use, registry order first, then any code the
// registry does not know (a newer producer's), so every ruleId resolves. The
// second result maps a rule id to its index in the first.
func rulesFor(used map[string]bool) ([]rule, map[string]int) {
	var rules []rule
	index := map[string]int{}
	for _, info := range v1.DiagnosticCodes() {
		if used[string(info.Code)] {
			index[string(info.Code)] = len(rules)
			rules = append(rules, rule{ID: string(info.Code), ShortDescription: &message{Text: info.Description}})
		}
	}
	var unknown []string
	for code := range used {
		if _, known := index[code]; !known {
			unknown = append(unknown, code)
		}
	}
	slices.Sort(unknown)
	for _, code := range unknown {
		index[code] = len(rules)
		rules = append(rules, rule{ID: code})
	}

	return rules, index
}

// weigh is the text one diagnostic adds to the log, an upper-bound estimate of
// what resultOf writes for it.
func weigh(file string, d *v1.Diagnostic) int {
	n := len(file) + len(d.GetMessage()) + len(d.GetCode())
	for _, id := range d.GetPath() {
		n += 2 * len(id)
	}
	for _, r := range d.GetRelated() {
		n += len(r.GetFile()) + len(r.GetMessage())
	}
	for _, e := range d.GetEdits() {
		n += len(e.GetTitle())
		for _, c := range e.GetChanges() {
			n += len(c.GetNewText())
		}
	}

	return n
}

func resultOf(file, code string, d *v1.Diagnostic) result {
	loc := location{PhysicalLocation: &physicalLocation{
		ArtifactLocation: artifactLocation{URI: uriOf(file)},
		Region:           regionOf(d.GetLine(), d.GetColumn(), 0, 0),
	}}
	if path := d.GetPath(); len(path) > 0 {
		last := len(path) - 1
		segments := make([]*v1.DebugSegment, 0, last)
		for _, id := range path[:last] {
			segments = append(segments, &v1.DebugSegment{StepId: id})
		}
		loc.LogicalLocations = []logicalLocation{{
			Name:               path[last],
			FullyQualifiedName: v1.FormatDebugAddress(segments, path[last]),
			Kind:               "member",
		}}
	}

	r := result{
		RuleID:    code,
		Level:     "error",
		Message:   message{Text: d.GetMessage()},
		Locations: []location{loc},
	}
	for i, related := range d.GetRelated() {
		r.RelatedLocations = append(r.RelatedLocations, relatedLocation{
			ID: i,
			PhysicalLocation: &physicalLocation{
				ArtifactLocation: artifactLocation{URI: uriOf(related.GetFile())},
				Region:           regionOf(related.GetLine(), related.GetColumn(), 0, 0),
			},
			Message: &message{Text: related.GetMessage()},
		})
	}
	for _, edit := range d.GetEdits() {
		change := artifactChange{ArtifactLocation: artifactLocation{URI: uriOf(file)}}
		for _, c := range edit.GetChanges() {
			rg := c.GetRange()
			deleted := regionOf(rg.GetStartLine(), rg.GetStartColumn(), rg.GetEndLine(), rg.GetEndColumn())
			if deleted == nil {
				// SARIF requires the region a replacement removes; a change with
				// no position cannot be stated, so it is not offered as a fix.
				change.Replacements = nil

				break
			}
			change.Replacements = append(change.Replacements, replacement{
				DeletedRegion: deleted, InsertedContent: &content{Text: c.GetNewText()},
			})
		}
		if len(change.Replacements) == 0 {
			continue
		}
		r.Fixes = append(r.Fixes, fix{Description: &message{Text: edit.GetTitle()}, ArtifactChanges: []artifactChange{change}})
	}

	return r
}

// regionOf is the region a 1-based position names, or nil when there is none: SARIF
// has no zero line, so an unknown position is no region rather than region zero.
func regionOf(line, column, endLine, endColumn uint32) *region {
	if line == 0 {
		return nil
	}
	r := &region{StartLine: line}
	if column > 0 {
		r.StartColumn = column
	}
	if endLine > 0 {
		r.EndLine = endLine
		if endColumn > 0 {
			r.EndColumn = endColumn
		}
	}

	return r
}

// uriOf spells a file name as the URI reference SARIF carries: a relative name stays
// relative (what code scanning resolves against the repository), slashes are
// forward, and reserved characters are escaped. An absolute path is a file URI.
func uriOf(name string) string {
	if name == "" {
		return ""
	}
	path := filepath.ToSlash(name)
	if filepath.IsAbs(name) || isWindowsAbs(path) {
		return fileURI(path)
	}

	return (&url.URL{Path: path}).String()
}

// isWindowsAbs reports whether a slash-form path starts with a drive letter or
// a UNC prefix, which [filepath.IsAbs] only recognises on Windows.
func isWindowsAbs(path string) bool {
	if strings.HasPrefix(path, "//") {
		return true
	}

	return len(path) >= 3 && path[1] == ':' && path[2] == '/' &&
		(path[0] >= 'a' && path[0] <= 'z' || path[0] >= 'A' && path[0] <= 'Z')
}

// fileURI is the file URI of an absolute slash-form path (RFC 8089 §2): a drive
// path keeps its drive in the path (`file:///C:/work/f.yaml`) and a UNC path's
// host is the URI authority (`file://host/share/f.yaml`).
func fileURI(path string) string {
	u := url.URL{Scheme: "file", Path: path}
	switch {
	case strings.HasPrefix(path, "//"):
		host, rest, _ := strings.Cut(strings.TrimPrefix(path, "//"), "/")
		u.Host, u.Path = host, "/"+rest
	case path[0] != '/':
		u.Path = "/" + path
	}

	return u.String()
}

// The SARIF object model, only the properties this package writes. Names and
// optionality follow the specification: an omitted property means "not known".
type (
	log struct {
		Schema  string `json:"$schema"`
		Version string `json:"version"`
		Runs    []run  `json:"runs"`
	}

	run struct {
		Tool toolComponent `json:"tool"`
		// ColumnKind says how columns count: Flowstate counts code points, SARIF's
		// default is UTF-16 code units, §3.14.13.
		ColumnKind  string       `json:"columnKind,omitempty"`
		Invocations []invocation `json:"invocations,omitempty"`
		Results     []result     `json:"results"`
	}

	toolComponent struct {
		Driver driver `json:"driver"`
	}

	driver struct {
		Name           string `json:"name"`
		Version        string `json:"version,omitempty"`
		InformationURI string `json:"informationUri,omitempty"`
		Rules          []rule `json:"rules,omitempty"`
	}

	rule struct {
		ID               string   `json:"id"`
		ShortDescription *message `json:"shortDescription,omitempty"`
	}

	invocation struct {
		ExecutionSuccessful        bool           `json:"executionSuccessful"`
		ToolExecutionNotifications []notification `json:"toolExecutionNotifications,omitempty"`
	}

	notification struct {
		Level   string  `json:"level"`
		Message message `json:"message"`
	}

	message struct {
		Text string `json:"text"`
	}

	result struct {
		RuleID           string            `json:"ruleId"`
		RuleIndex        int               `json:"ruleIndex"`
		Level            string            `json:"level"`
		Message          message           `json:"message"`
		Locations        []location        `json:"locations,omitempty"`
		RelatedLocations []relatedLocation `json:"relatedLocations,omitempty"`
		Fixes            []fix             `json:"fixes,omitempty"`
	}

	location struct {
		PhysicalLocation *physicalLocation `json:"physicalLocation,omitempty"`
		LogicalLocations []logicalLocation `json:"logicalLocations,omitempty"`
	}

	relatedLocation struct {
		ID               int               `json:"id"`
		PhysicalLocation *physicalLocation `json:"physicalLocation,omitempty"`
		Message          *message          `json:"message,omitempty"`
	}

	physicalLocation struct {
		ArtifactLocation artifactLocation `json:"artifactLocation"`
		Region           *region          `json:"region,omitempty"`
	}

	artifactLocation struct {
		URI string `json:"uri"`
	}

	logicalLocation struct {
		Name               string `json:"name"`
		FullyQualifiedName string `json:"fullyQualifiedName"`
		Kind               string `json:"kind"`
	}

	region struct {
		StartLine   uint32 `json:"startLine"`
		StartColumn uint32 `json:"startColumn,omitempty"`
		EndLine     uint32 `json:"endLine,omitempty"`
		EndColumn   uint32 `json:"endColumn,omitempty"`
	}

	fix struct {
		Description     *message         `json:"description,omitempty"`
		ArtifactChanges []artifactChange `json:"artifactChanges"`
	}

	artifactChange struct {
		ArtifactLocation artifactLocation `json:"artifactLocation"`
		Replacements     []replacement    `json:"replacements"`
	}

	replacement struct {
		DeletedRegion   *region  `json:"deletedRegion"`
		InsertedContent *content `json:"insertedContent,omitempty"`
	}

	content struct {
		Text string `json:"text"`
	}
)
