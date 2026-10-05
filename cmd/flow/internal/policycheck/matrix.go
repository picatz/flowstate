package policycheck

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"
	"unicode"
	"unicode/utf8"

	"github.com/picatz/flowstate/internal/strictyaml"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// The bounds on a matrix file, enforced before anything is decided. It is a
// document a person wrote, but it may be checked out from a fork or generated,
// and every row is a full evaluation of every gate (invariant 5).
const (
	// MaxMatrixBytes is the largest matrix file accepted: far above a
	// hundred-row table, far below something worth holding in memory.
	MaxMatrixBytes = 256 << 10

	// MaxMatrixRows is the most identities one matrix may list.
	MaxMatrixRows = 256

	// MaxRowClaims and MaxRowInputs bound what one row may carry.
	MaxRowClaims = 64
	MaxRowInputs = 64

	// MaxRowNameRunes bounds a row's name, which is printed in a table.
	MaxRowNameRunes = 64
)

// Matrix is a file of named identities to ask every selected gate about:
//
//	identities:
//	  - name: sre-lead
//	    subject: sre-lead@example.com
//	    issuer: https://issuer.example.com
//	    claims: {team: release-managers}
//	    starter: {subject: dev@example.com, issuer: https://issuer.example.com}
//	    inputs: {expected_approver: sre-lead@example.com}
//	    expect: admitted
//
// The identity fields are those of a test file's `sender:` ([flowtest.ScriptedIdentity]),
// validated by the same rule. It is decoded strictly: a misspelled key is a
// refusal, because a misspelled `expect:` would otherwise assert nothing while
// reading as if it asserted something.
type Matrix struct {
	Identities []Row `yaml:"identities"`
}

// Row is one named identity in a [Matrix].
type Row struct {
	// Name labels the row in the table and in a mismatch. Required, unique, and
	// free of control characters.
	Name string `yaml:"name"`

	// The identity attempting each act: subject, issuer, namespace and claims.
	// All absent is an unauthenticated caller.
	flowtest.ScriptedIdentity `yaml:",inline"`

	// Starter is who started the hypothetical run. Absent leaves the starter
	// unknown (see [Subject.Starter]); `starter: {}` says it was started by
	// nobody authenticated.
	Starter *flowtest.ScriptedIdentity `yaml:"starter"`

	// Inputs are this row's arguments, read against the workflow's `inputs:`
	// declarations by whoever runs the matrix. They replace, name by name, any
	// arguments given for every row.
	Inputs map[string]any `yaml:"inputs"`

	// Expect is what this row must get: one outcome for every gate checked, or
	// a map from gate (`signals.deploy-approved`, `debug`, `triggers.manual`) to
	// the outcome that gate must give.
	Expect Expectation `yaml:"expect"`
}

// Expectation is a row's assertion, either one [Outcome] for every gate or one
// per named gate. The zero value asserts nothing.
type Expectation struct {
	// All applies to every gate checked, when set.
	All Outcome

	// ByGate applies to the named gates only; a gate not named asserts
	// nothing.
	ByGate map[string]Outcome
}

// UnmarshalYAML reads the scalar form and the map form, and refuses anything
// else, so an outcome is one of the two words wherever it is written.
func (e *Expectation) UnmarshalYAML(unmarshal func(any) error) error {
	var word string
	if err := unmarshal(&word); err == nil {
		outcome, err := ParseOutcome(word)
		if err != nil {
			return err
		}
		e.All = outcome

		return nil
	}

	var words map[string]string
	if err := unmarshal(&words); err != nil {
		return errors.New("expect is admitted, refused, or a map from gate to one of those")
	}

	e.ByGate = make(map[string]Outcome, len(words))
	for gate, word := range words {
		outcome, err := ParseOutcome(word)
		if err != nil {
			return fmt.Errorf("expect for %s: %w", gate, err)
		}
		e.ByGate[gate] = outcome
	}

	return nil
}

// For is what the expectation asserts for a gate, and whether it asserts
// anything. A gate-specific entry wins over All.
func (e Expectation) For(gate Gate) (Outcome, bool) {
	if outcome, ok := e.ByGate[gate.String()]; ok {
		return outcome, true
	}

	return e.All, e.All != ""
}

// ParseMatrix reads and validates a matrix document.
//
// Everything that can be refused without a workflow is refused here: size, row
// count, names, malformed identities (by [flowtest.ScriptedIdentity.Check], the
// rule a test file's identities are held to), and per-row bounds. What needs the
// workflow - a gate named in an expectation that is not being checked, an input
// the workflow does not take - is the caller's, with [Matrix.CheckGates].
func ParseMatrix(data []byte) (*Matrix, error) {
	if len(data) > MaxMatrixBytes {
		return nil, fmt.Errorf("the matrix is %d bytes, over the %d byte limit", len(data), MaxMatrixBytes)
	}

	var matrix Matrix
	if err := strictyaml.UnmarshalStrict(data, &matrix); err != nil {
		return nil, fmt.Errorf("the matrix is not a document of `identities:`: %w", err)
	}

	if len(matrix.Identities) == 0 {
		return nil, errors.New("the matrix lists no identities; add at least one under `identities:`")
	}
	if len(matrix.Identities) > MaxMatrixRows {
		return nil, fmt.Errorf("the matrix lists %d identities, over the limit of %d", len(matrix.Identities), MaxMatrixRows)
	}

	seen := make(map[string]struct{}, len(matrix.Identities))

	for i, row := range matrix.Identities {
		where := fmt.Sprintf("identity %d", i+1)

		if err := checkRowName(row.Name); err != nil {
			return nil, fmt.Errorf("%s: %w", where, err)
		}

		where = fmt.Sprintf("identity %q", row.Name)

		if _, dup := seen[row.Name]; dup {
			return nil, fmt.Errorf("%s is listed twice; names label the table, so they must be unique", where)
		}
		seen[row.Name] = struct{}{}

		if len(row.Claims) > MaxRowClaims || len(row.Inputs) > MaxRowInputs ||
			(row.Starter != nil && len(row.Starter.Claims) > MaxRowClaims) {
			return nil, fmt.Errorf("%s carries more than %d claims or %d inputs", where, MaxRowClaims, MaxRowInputs)
		}

		if err := row.ScriptedIdentity.Check(where); err != nil {
			return nil, err
		}
		if err := row.Starter.Check(where + " starter"); err != nil {
			return nil, err
		}
	}

	return &matrix, nil
}

func checkRowName(name string) error {
	switch {
	case name == "":
		return errors.New("has no `name:`")
	case utf8.RuneCountInString(name) > MaxRowNameRunes:
		return fmt.Errorf("has a name over %d characters", MaxRowNameRunes)
	case strings.ContainsFunc(name, unicode.IsControl):
		return errors.New("has a name containing a control character")
	}

	return nil
}

// CheckGates refuses an expectation that names a gate which is not among the
// gates being checked, which would otherwise assert nothing while reading as if
// it asserted something.
func (m *Matrix) CheckGates(gates []Gate) error {
	checked := make(map[string]struct{}, len(gates))
	for _, gate := range gates {
		checked[gate.String()] = struct{}{}
	}

	for _, row := range m.Identities {
		for _, name := range slices.Sorted(maps.Keys(row.Expect.ByGate)) {
			if _, ok := checked[name]; !ok {
				return fmt.Errorf("identity %q expects an outcome for %q, which this check does not decide; "+
					"the gates checked are %s", row.Name, name, listedGates(gates))
			}
		}
	}

	return nil
}

func listedGates(gates []Gate) string {
	names := make([]string, 0, len(gates))
	for _, gate := range gates {
		names = append(names, gate.String())
	}

	return fmt.Sprintf("%q", names)
}

// Result is one subject's decisions, with the expectation they were held to.
type Result struct {
	// Name labels the subject; empty for a single command-line identity.
	Name string

	Decisions []Decision

	// Expect is what the subject was held to. The zero value holds it to
	// nothing.
	Expect Expectation
}

// Mismatches lists the decisions that contradict the expectation.
func (r Result) Mismatches() []Decision {
	var wrong []Decision

	for _, decision := range r.Decisions {
		if want, asserted := r.Expect.For(decision.Gate); asserted && want != decision.Outcome() {
			wrong = append(wrong, decision)
		}
	}

	return wrong
}

// Matches reports whether no decision contradicts the expectation.
func (r Result) Matches() bool { return len(r.Mismatches()) == 0 }

// WriteLines writes one line per gate: the gate, admitted or refused, and for a
// refusal the engine's sentence, and for a contradicted expectation what was
// expected.
func WriteLines(w io.Writer, result Result) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)

	for _, decision := range result.Decisions {
		fmt.Fprintf(tw, "%s\t%s\n", decision.Gate, describe(decision, result.Expect))
	}

	return tw.Flush()
}

// describe is a decision's cell: its outcome, qualified by what a reader needs
// to act on it.
func describe(decision Decision, expect Expectation) string {
	text := string(decision.Outcome())

	if want, asserted := expect.For(decision.Gate); asserted && want != decision.Outcome() {
		text += " (expected " + string(want) + ")"
	}

	switch {
	case decision.Reason != "":
		text += ": " + decision.Reason
	case decision.Note != "":
		text += ": " + decision.Note
	}

	return text
}

// WriteTable writes a senders-by-gates table: a row per result, a column per
// gate, each cell admitted or refused, with a contradicted expectation marked in
// the cell. Reasons are not repeated per cell: each distinct sentence a gate
// refused with is written once beneath the table.
func WriteTable(w io.Writer, gates []Gate, results []Result) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)

	header := []string{"identity"}
	for _, gate := range gates {
		header = append(header, gate.String())
	}
	fmt.Fprintln(tw, strings.Join(header, "\t"))

	reasons := map[Gate][]string{}

	for _, result := range results {
		cells := []string{result.Name}

		for _, gate := range gates {
			decision, ok := decisionFor(result, gate)
			if !ok {
				cells = append(cells, "-")
				continue
			}

			cell := string(decision.Outcome())
			if want, asserted := result.Expect.For(gate); asserted && want != decision.Outcome() {
				cell += " (expected " + string(want) + ")"
			}
			cells = append(cells, cell)

			if decision.Reason != "" && !slices.Contains(reasons[gate], decision.Reason) {
				reasons[gate] = append(reasons[gate], decision.Reason)
			}
		}

		fmt.Fprintln(tw, strings.Join(cells, "\t"))
	}

	if err := tw.Flush(); err != nil {
		return err
	}

	// Each distinct sentence once per refused gate. A matrix of two hundred
	// rows must not repeat the engine's sentence two hundred times, and a gate
	// can refuse for more than one reason (the predicate said no, or it
	// errored), which a single remembered sentence would hide.
	for _, gate := range gates {
		for _, reason := range reasons[gate] {
			if _, err := fmt.Fprintf(w, "\n%s refuses with: %s\n", gate, reason); err != nil {
				return err
			}
		}
	}

	return nil
}

func decisionFor(result Result, gate Gate) (Decision, bool) {
	for _, decision := range result.Decisions {
		if decision.Gate == gate {
			return decision, true
		}
	}

	return Decision{}, false
}

// Report is the machine form of a check: the gates asked, each subject's
// decisions, and whether every asserted expectation held. A plain document
// rather than a schema message, as `flow lint`'s and `flow audit`'s are: nothing
// in it travels between components.
type Report struct {
	Gates   []string       `json:"gates"`
	Results []ReportResult `json:"results"`

	// Matches is false when any result contradicts what it was expected to get.
	Matches bool `json:"matches"`
}

// ReportResult is one subject in a [Report].
type ReportResult struct {
	// Name is the matrix row, absent for a single command-line identity.
	Name      string           `json:"name,omitempty"`
	Decisions []ReportDecision `json:"decisions"`
}

// ReportDecision is one gate's answer. Reason is the engine's refusal sentence
// and never quotes a value; Expected is set only when an expectation was
// asserted, and Matches then says whether it held.
type ReportDecision struct {
	Gate     string  `json:"gate"`
	Outcome  Outcome `json:"outcome"`
	Reason   string  `json:"reason,omitempty"`
	Note     string  `json:"note,omitempty"`
	Expected Outcome `json:"expected,omitempty"`
	Matches  *bool   `json:"matches,omitempty"`
}

// NewReport assembles the machine form of results over gates.
func NewReport(gates []Gate, results []Result) Report {
	report := Report{Gates: make([]string, 0, len(gates)), Results: make([]ReportResult, 0, len(results)), Matches: true}

	for _, gate := range gates {
		report.Gates = append(report.Gates, gate.String())
	}

	for _, result := range results {
		out := ReportResult{Name: result.Name, Decisions: make([]ReportDecision, 0, len(result.Decisions))}

		for _, decision := range result.Decisions {
			entry := ReportDecision{
				Gate:    decision.Gate.String(),
				Outcome: decision.Outcome(),
				Reason:  decision.Reason,
				Note:    decision.Note,
			}

			if want, asserted := result.Expect.For(decision.Gate); asserted {
				held := want == decision.Outcome()
				entry.Expected, entry.Matches = want, &held
				report.Matches = report.Matches && held
			}

			out.Decisions = append(out.Decisions, entry)
		}

		report.Results = append(report.Results, out)
	}

	return report
}
