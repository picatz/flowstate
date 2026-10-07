package policycheck

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"regexp"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/goccy/go-yaml"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/picatz/flowstate/internal/strictyaml"
	"github.com/picatz/flowstate/internal/textbound"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowtest"
)

// The bounds on a matrix file, enforced before anything is decided. It is a
// document a person wrote, but it may be checked out from a fork or generated,
// and every row is a full evaluation of every gate (invariant 5).
//
// The rows, names, claims and outcomes are bounded by the schema
// ([v1.PolicyCheckMatrix]'s protovalidate rules), which is where those numbers
// are written; the two constants here mirror it so a caller and a test can name
// them, and a test holds them to the schema's own boundary. What a schema rule
// cannot say is bounded here: the file's bytes, the YAML constructs that expand,
// and the size of a row's inputs once decoded.
const (
	// MaxMatrixBytes is the largest matrix file accepted: far above a
	// hundred-row table, far below something worth holding in memory.
	MaxMatrixBytes = 256 << 10

	// MaxMatrixRows mirrors the schema's limit on identities.
	MaxMatrixRows = 256

	// MaxRowInputNodes bounds the values (counting every nested element) one
	// row's `inputs:` may hold once decoded. The matrix refuses aliases, so
	// this equals what was written; it is the second bound, on what a decoder
	// produced, so that a decoder that began expanding references could not
	// reopen the memory a small file could cost.
	MaxRowInputNodes = 4096

	// MaxFlowOpeners bounds the `[` and `{` bytes in the whole file, counted
	// wherever they sit: in quotes, in comments, in a block scalar. Flow nesting
	// can never be deeper than the number of openers, so this bounds it without
	// modelling YAML at all, which is the point: a scan that decided what was
	// quoted or commented could disagree with the parser, and the parser's
	// memory is the cost being bounded. 4096 is a ceiling no table reaches: 256
	// rows of a claims map, a starter, an expectation and a few nested inputs
	// come to under two thousand.
	MaxFlowOpeners = 4096

	// MaxBlockIndent and MaxBlockTokens bound block nesting the same way. Block
	// depth on a line can never exceed its leading spaces plus the block
	// indicators written on it (`- `, `? `, `: `), so a line with more than
	// either is refused, whatever the line is.
	MaxBlockIndent = 128
	MaxBlockTokens = 64

	// MaxExactInteger is the magnitude from which a number in a row's
	// inputs is refused: the schema carries an input as a double, which holds
	// integers exactly only below 2^53, and a check that rounded an argument
	// could answer differently from the engine for the value actually passed.
	MaxExactInteger = 1 << 53

	// MaxRowNameRunes mirrors the schema's limit on a row's name, which is
	// printed in a table.
	MaxRowNameRunes = 64
)

// Matrix is a file of named identities to ask every selected gate about:
//
//	identities:
//	  - name: sre-lead
//	    principal:
//	      subject: sre-lead@example.com
//	      issuer: https://issuer.example.com
//	      claims: {team: release-managers}
//	    starter: {principal: {subject: dev@example.com, issuer: https://issuer.example.com}}
//	    inputs: {expected_approver: sre-lead@example.com}
//	    expect: admitted
//
// The file's shape is [v1.PolicyCheckMatrix]; this is that message read into the
// types the check works with. A row's `principal:` is a [v1.Principal]; the check
// reads the part of it a test file's `sender:` ([flowtest.ScriptedIdentity])
// carries, validated by the same rule, and refuses what it cannot (a claim that
// is not a string, `actions`, `issuer_entry`) rather than ignore it. A
// misspelled key is a refusal, because a misspelled `expect:` would otherwise
// assert nothing while reading as if it asserted something.
type Matrix struct {
	Identities []Row
}

// Row is one named identity in a [Matrix].
type Row struct {
	// Name labels the row in the table and in a mismatch. Required, unique, and
	// free of control characters.
	Name string

	// The identity attempting each act: the row's principal, read as a subject,
	// issuer, namespace, kind and claims. All absent is an unauthenticated
	// caller.
	flowtest.ScriptedIdentity

	// Starter is who started the hypothetical run. Absent leaves the starter
	// unknown (see [Subject.Starter]); `starter: {}` says it was started by
	// nobody authenticated.
	Starter *flowtest.ScriptedIdentity

	// Inputs are this row's arguments, read against the workflow's `inputs:`
	// declarations by whoever runs the matrix. They replace, name by name, any
	// arguments given for every row.
	Inputs map[string]any

	// Expect is what this row must get: one outcome for every gate checked, or
	// a map from gate (`signals.deploy-approved`, `debug`, `triggers.manual`) to
	// the outcome that gate must give.
	Expect Expectation
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
// Everything that can be refused without a workflow is refused here: size, the
// YAML constructs that expand, the schema's own rules ([v1.Validate] over
// [v1.PolicyCheckMatrix]: rows, names, claims, outcomes), duplicate names,
// malformed identities (by [flowtest.ScriptedIdentity.Check], the rule a test
// file's identities are held to), and the size of each row's inputs. What needs
// the workflow - a gate named in an expectation that is not being checked, an
// input the workflow does not take - is the caller's, with [Matrix.CheckGates].
//
// No error quotes the document: not a claim, not an input, not a line of source.
// A refusal says which field and which rule.
func ParseMatrix(data []byte) (*Matrix, error) {
	if len(data) > MaxMatrixBytes {
		return nil, fmt.Errorf("the matrix is %d bytes, over the %d byte limit", len(data), MaxMatrixBytes)
	}

	if err := refuseDeepNesting(data); err != nil {
		return nil, err
	}

	if err := refuseAliasedYAML(data); err != nil {
		return nil, err
	}

	var doc v1.PolicyCheckMatrix
	if err := readMatrix(data, &doc); err != nil {
		return nil, fmt.Errorf("the matrix is not a document of `identities:`: %w", decodeError(err))
	}

	if err := v1.Validate(&doc); err != nil {
		return nil, validationError(err)
	}

	matrix := &Matrix{Identities: make([]Row, 0, len(doc.GetIdentities()))}
	seen := make(map[string]struct{}, len(doc.GetIdentities()))

	for i, held := range doc.GetIdentities() {
		caller, err := scriptedFromPrincipal(fmt.Sprintf("identity %q", held.GetName()), held.GetPrincipal())
		if err != nil {
			return nil, err
		}

		row := Row{
			Name:             held.GetName(),
			ScriptedIdentity: caller,
			Inputs:           held.GetInputs().AsMap(),
			Expect:           Expectation{All: Outcome(held.GetExpect())},
		}

		if starter := held.GetStarter(); starter != nil {
			who, err := scriptedFromPrincipal(fmt.Sprintf("identity %q starter", held.GetName()), starter.GetPrincipal())
			if err != nil {
				return nil, err
			}
			row.Starter = &who
		}

		if len(held.GetExpectByGate()) > 0 {
			row.Expect.ByGate = make(map[string]Outcome, len(held.GetExpectByGate()))
			for gate, word := range held.GetExpectByGate() {
				row.Expect.ByGate[gate] = Outcome(word)
			}
		}

		where := fmt.Sprintf("identity %d", i+1)

		if _, dup := seen[row.Name]; dup {
			return nil, fmt.Errorf("%s is listed twice; names label the table, so they must be unique", where)
		}
		seen[row.Name] = struct{}{}

		where = fmt.Sprintf("identity %q", row.Name)

		if nodes := inputNodes(row.Inputs); nodes > MaxRowInputNodes {
			return nil, fmt.Errorf("%s carries inputs holding more than %d values", where, MaxRowInputNodes)
		}

		if inexactNumber(row.Inputs) {
			return nil, fmt.Errorf("%s carries a number of magnitude 2^53 or more in its inputs, which a matrix cannot "+
				"carry exactly; give it with --input, which applies to every row", where)
		}

		if err := row.ScriptedIdentity.Check(where); err != nil {
			return nil, err
		}
		if err := row.Starter.Check(where + " starter"); err != nil {
			return nil, err
		}

		matrix.Identities = append(matrix.Identities, row)
	}

	return matrix, nil
}

// scriptedFromPrincipal reads a row's wire [v1.Principal] as the scripted
// identity the gates are asked about.
//
// A scripted identity is what `flow test` and the command line spell, and a
// signal predicate's `claims` read strings, so what it cannot carry is refused
// rather than dropped: a check that ignored a list claim or granted actions would
// answer for an identity other than the one written. The refusal names the field
// and never a value.
func scriptedFromPrincipal(where string, who *v1.Principal) (flowtest.ScriptedIdentity, error) {
	if who.GetIssuerEntry() != "" {
		return flowtest.ScriptedIdentity{}, fmt.Errorf("%s: `issuer_entry` names a trust policy entry, which a check has none of", where)
	}

	if len(who.GetActions()) > 0 {
		return flowtest.ScriptedIdentity{}, fmt.Errorf("%s: `actions` are not read by the gates a check decides", where)
	}

	var claims map[string]string
	for _, name := range slices.Sorted(maps.Keys(who.GetClaims())) {
		text, ok := who.GetClaims()[name].GetKind().(*structpb.Value_StringValue)
		if !ok {
			return flowtest.ScriptedIdentity{}, fmt.Errorf("%s: claim %q is not a string, and a signal predicate reads claims as strings", where, textbound.Truncate(name, 64))
		}
		if claims == nil {
			claims = make(map[string]string, len(who.GetClaims()))
		}
		claims[name] = text.StringValue
	}

	return flowtest.ScriptedIdentity{
		Subject:   who.GetSubject(),
		Issuer:    who.GetIssuer(),
		Namespace: who.GetNamespace(),
		Kind:      v1.PrincipalKindName(who.GetKind()),
		Claims:    claims,
	}, nil
}

// readMatrix decodes the document into the schema's message, letting a row
// write `kind: human`, the spelling a trust policy and every other surface use,
// where the schema's own enum name is PRINCIPAL_KIND_HUMAN. Only the exact
// lowercase names are rewritten, so the spelling stays as strict as a trust
// policy's; anything else reaches the schema untouched and is refused there.
//
// The document is read once as a generic message, its kinds respelled, and then
// read into the schema, so both reads keep the strictness of
// [strictyaml.UnmarshalProto] and protojson.
func readMatrix(data []byte, into *v1.PolicyCheckMatrix) error {
	doc := &structpb.Struct{}
	if err := strictyaml.UnmarshalProto(data, doc); err != nil {
		return err
	}

	respell := func(identity *structpb.Value) {
		who := identity.GetStructValue().GetFields()["principal"].GetStructValue().GetFields()
		name, ok := who["kind"].GetKind().(*structpb.Value_StringValue)
		if !ok {
			return
		}
		if kind := v1.PrincipalKindNamed(name.StringValue); kind != v1.PrincipalKind_PRINCIPAL_KIND_UNSPECIFIED {
			who["kind"] = structpb.NewStringValue(kind.String())
		}
	}

	for _, row := range doc.GetFields()["identities"].GetListValue().GetValues() {
		respell(row)
		respell(row.GetStructValue().GetFields()["starter"])
	}

	encoded, err := protojson.Marshal(doc)
	if err != nil {
		return err
	}

	return protojson.Unmarshal(encoded, into)
}

// unknownField picks the name out of protojson's "unknown field" refusal, the
// one decode error worth saying more about: a misspelled key.
var unknownField = regexp.MustCompile(`unknown field "([^"]{1,64})"`)

// decodeError reduces what reading the document into the message said to what
// and, where the decoder knows it, where - never the source. protojson's own
// text for a value of the wrong type quotes the value, and a matrix's values
// are claims and inputs.
func decodeError(err error) error {
	if _, ok := errors.AsType[yaml.Error](err); ok {
		return withoutSource(err)
	}

	if found := unknownField.FindStringSubmatch(err.Error()); found != nil {
		return fmt.Errorf("unknown field %q", found[1])
	}

	return errors.New("a value has the wrong shape for its field (for example text where a mapping belongs)")
}

// validationError renders the schema's refusals as field and rule message,
// which protovalidate words without the value that failed it.
func validationError(err error) error {
	invalid, ok := errors.AsType[*v1.ValidationError](err)
	if !ok {
		return errors.New("the matrix does not satisfy its schema")
	}

	lines := make([]string, 0, len(invalid.Violations))
	for _, violation := range invalid.Violations[:min(len(invalid.Violations), 5)] {
		lines = append(lines, fmt.Sprintf("%s: %s", cmp.Or(violation.Field, "the matrix"), violation.Message))
	}

	return fmt.Errorf("the matrix does not satisfy its schema: %s", strings.Join(lines, "; "))
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
	notes := map[Gate][]string{}

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

			if decision.Note != "" && !slices.Contains(notes[gate], decision.Note) {
				notes[gate] = append(notes[gate], decision.Note)
			}

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
		// An admission no policy produced is not a gate that was passed, and a
		// bare `admitted` cell would read as one.
		for _, note := range notes[gate] {
			if _, err := fmt.Fprintf(w, "\n%s admits without a policy: %s\n", gate, note); err != nil {
				return err
			}
		}

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

// refuseAliasedYAML refuses a matrix that holds an anchor, an alias or a merge
// key, on the presence of the construct and before anything is decoded. They are
// how a few hundred bytes become gigabytes - nested aliases multiply - and a
// table of identities has no use for them, so the answer is the one the Flowfile
// grammar gives ([flowfile.StrictYAMLRefusals]) rather than a bound that fires
// after the cost is paid. The sentence is fixed and positions only: it quotes
// nothing the document says.
func refuseAliasedYAML(data []byte) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("the matrix could not be read as YAML")
		}
	}()

	file, parseErr := strictyaml.ParseBytes(data, 0)
	if parseErr != nil {
		return fmt.Errorf("the matrix is not YAML: %w", withoutSource(parseErr))
	}

	documents := 0
	for _, doc := range file.Docs {
		if doc.Body != nil {
			documents++
		}
	}
	if documents > 1 {
		return errors.New("a matrix is one document; a second, after `---`, would be silently ignored, so put every identity under one `identities:`")
	}

	if found := flowfile.StrictYAMLRefusals(file); len(found) > 0 {
		return fmt.Errorf("line %d, column %d: a matrix is a plain table; anchors (&), aliases (*) and merge keys (<<) "+
			"are not accepted, so write each value out", found[0].Line, found[0].Column)
	}

	return nil
}

// withoutSource reduces a decoder's error to its message and position. The
// decoder's own rendering annotates the offending line of the document, and in
// a matrix that line can be a claim or an input value.
func withoutSource(err error) error {
	if located, ok := errors.AsType[yaml.Error](err); ok {
		if tok := located.GetToken(); tok != nil && tok.Position != nil {
			return fmt.Errorf("line %d, column %d: %s", tok.Position.Line, tok.Position.Column, located.GetMessage())
		}

		return errors.New(located.GetMessage())
	}

	return err
}

// inputNodes counts the values a row's inputs hold, every nested element
// included, stopping as soon as the count passes [MaxRowInputNodes] (and at a
// fixed depth) so the count itself is bounded however the value was built.
func inputNodes(inputs map[string]any) int {
	count := 0

	var walk func(value any, depth int)
	walk = func(value any, depth int) {
		if count > MaxRowInputNodes {
			return
		}
		count++

		if depth > 32 {
			count = MaxRowInputNodes + 1
			return
		}

		switch v := value.(type) {
		case map[string]any:
			for _, held := range v {
				walk(held, depth+1)
			}
		case []any:
			for _, held := range v {
				walk(held, depth+1)
			}
		}
	}

	walk(inputs, 0)

	return count
}

// inexactNumber reports whether any value in inputs is a number of
// [MaxExactInteger] or more in magnitude, at any depth. The values arrive as
// doubles (a schema Struct), so one at or past 2^53 may already have been
// rounded from what the author wrote.
func inexactNumber(inputs map[string]any) bool {
	var walk func(value any) bool
	walk = func(value any) bool {
		switch v := value.(type) {
		case float64:
			// Any number, whole or not: at this magnitude every double is whole,
			// and `1e16` is the same value the engine would read as an integer.
			return math.Abs(v) >= MaxExactInteger
		case map[string]any:
			for _, held := range v {
				if walk(held) {
					return true
				}
			}
		case []any:
			for _, held := range v {
				if walk(held) {
					return true
				}
			}
		}

		return false
	}

	return walk(inputs)
}

// refuseDeepNesting refuses a document whose nesting the parser could be made
// to pay for, by counting bytes and modelling nothing.
//
// It runs before any parser does, because the cost it prevents is the parser's:
// goccy builds its tree recursively, so `[[[[...` of a few hundred kilobytes
// exhausts memory before a bound on the tree could be asked.
//
// # Why it does not read YAML
//
// An earlier scan tracked quotes and comments so that a bracket in text would
// not count, and a stray quote in a plain scalar (`don 't`) put it in quote mode
// while the parser read the same line as plain text and went on to build the
// tree. Any rule about what is quoted is a guess the parser can contradict. So
// this makes none. Flow depth is at most the number of `[` and `{` bytes, so
// every one counts, wherever it is. Block depth on a line is at most its leading
// spaces plus its block indicators, so a line with too many of either is
// refused. Both are over-approximations: they refuse some documents the parser
// would have read in bounded memory, and none it would not.
//
// A line ends at \n or \r, the two breaks the parser has. A tab in indentation
// is refused, since the parser does and an indentation that counts differently
// to the two is another way to disagree. The refusal is a fixed sentence with a
// position and quotes nothing.
func refuseDeepNesting(data []byte) error {
	refuse := func(line, col int, what string) error {
		return fmt.Errorf("line %d, column %d: %s; a table of identities needs none of that nesting", line, col, what)
	}

	data = bytes.TrimPrefix(data, []byte("\xef\xbb\xbf"))

	var (
		line, lineStart = 1, 0
		openers         int
		inIndent        = true
		spaces, tokens  int
	)

	for i := 0; i < len(data); i++ {
		c := data[i]
		col := i - lineStart + 1

		if c == '\n' || c == '\r' {
			// A trailing indicator, with nothing after it on the line. Counted
			// whatever precedes it: over-counting only refuses more.
			if i > lineStart && isIndicator(data[i-1]) {
				tokens++
			}
			if tokens > MaxBlockTokens {
				return refuse(line, col, "a line carries too many block indicators")
			}

			// \r\n is one break.
			if c == '\r' && i+1 < len(data) && data[i+1] == '\n' {
				i++
			}

			line++
			lineStart = i + 1
			inIndent, spaces, tokens = true, 0, 0

			continue
		}

		if inIndent {
			switch c {
			case ' ':
				spaces++
				if spaces > MaxBlockIndent {
					return refuse(line, col, "a line is indented too deeply")
				}

				continue
			case '\t':
				return refuse(line, col, "a tab is not allowed in indentation")
			}

			inIndent = false
		}

		switch {
		case c == '[' || c == '{':
			openers++
			if openers > MaxFlowOpeners {
				return refuse(line, col, "the document holds too many flow collections")
			}

		case isIndicator(c) && i+1 < len(data) && (data[i+1] == ' ' || data[i+1] == '\t'):
			// Checked where the line ends, below and after the loop.
			tokens++
		}
	}

	// A last line with no break after it ends the same way a broken one does.
	if len(data) > lineStart && isIndicator(data[len(data)-1]) {
		tokens++
	}
	if tokens > MaxBlockTokens {
		return refuse(line, len(data)-lineStart+1, "a line carries too many block indicators")
	}

	return nil
}

// isIndicator reports whether c can open a block entry when a space follows it:
// a sequence entry, an explicit key, or a mapping value.
func isIndicator(c byte) bool { return c == '-' || c == '?' || c == ':' }
