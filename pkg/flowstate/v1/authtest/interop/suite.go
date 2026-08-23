package interop

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// Adapter is the only provider-specific part of a black-box suite. External
// implementations must be opt-in; Environment never discovers or contacts one.
type Adapter interface {
	Name() string
	Run(context.Context, *Environment, Case) error
}

// Case ties an executable probe to an exact normative reference.
type Case struct {
	ID        string    `json:"id"`
	Protocol  string    `json:"protocol"`
	Reference string    `json:"reference"`
	Subset    string    `json:"subset"`
	Extension string    `json:"extension,omitempty"`
	Deviation Deviation `json:"deviation,omitempty"`
}

type Outcome string

const (
	Pass Outcome = "pass"
	Fail Outcome = "fail"
	Skip Outcome = "skip"
)

type Result struct {
	Case    Case    `json:"case"`
	Outcome Outcome `json:"outcome"`
	Detail  string  `json:"detail,omitempty"`
}

// ProviderExpectation documents a provider quirk separately from conformance.
type ProviderExpectation struct {
	Provider    string `json:"provider"`
	CaseID      string `json:"case_id"`
	Expectation string `json:"expectation"`
}

// Report is a granular capability record suitable for JSON serialization.
type Report struct {
	Provider             string                `json:"provider"`
	Results              []Result              `json:"results"`
	ProviderExpectations []ProviderExpectation `json:"provider_expectations,omitempty"`
	KnownDeviations      []string              `json:"known_deviations,omitempty"`
}

// Run executes identical cases through one adapter. Provider expectations never
// rewrite standards outcomes.
func Run(ctx context.Context, env *Environment, adapter Adapter, cases []Case, expectations []ProviderExpectation) Report {
	report := Report{Provider: adapter.Name(), ProviderExpectations: slices.Clone(expectations)}
	for _, test := range cases {
		if test.Reference == "" {
			report.Results = append(report.Results, Result{Case: test, Outcome: Fail, Detail: "missing exact RFC section or draft revision"})
			continue
		}
		env.Scenario(test.Deviation)
		if err := adapter.Run(ctx, env, test); err != nil {
			report.Results = append(report.Results, Result{Case: test, Outcome: Fail, Detail: err.Error()})
			continue
		}
		report.Results = append(report.Results, Result{Case: test, Outcome: Pass})
	}
	return report
}

// Markdown renders a capability matrix rather than an ambiguous compatibility badge.
func (r Report) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Interoperability capabilities: %s\n\n| Case | Protocol reference | Supported subset | Extension | Outcome |\n|---|---|---|---|---|\n", r.Provider)
	for _, result := range r.Results {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", result.Case.ID, result.Case.Reference, result.Case.Subset, result.Case.Extension, result.Outcome)
	}
	if len(r.KnownDeviations) > 0 {
		b.WriteString("\n## Known deviations\n")
		deviations := slices.Clone(r.KnownDeviations)
		sort.Strings(deviations)
		for _, d := range deviations {
			fmt.Fprintf(&b, "- %s\n", d)
		}
	}
	if len(r.ProviderExpectations) > 0 {
		b.WriteString("\n## Provider-specific expectations (not conformance)\n")
		for _, e := range r.ProviderExpectations {
			fmt.Fprintf(&b, "- **%s / %s:** %s\n", e.Provider, e.CaseID, e.Expectation)
		}
	}
	return b.String()
}
