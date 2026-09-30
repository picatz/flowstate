package conformance

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// The completeness of [CapabilityCases] and the table docs/DEBUGGING.md derives
// from them.
//
// The corpus is only worth what it covers, and what it must cover is not a list
// somebody keeps: it is every field of [v1.DebugCapabilities] in `debug.proto`,
// read through protoreflect. A field added there is a capability a surface may
// now advertise, and with no case it is one no test holds either driver to.

// metadataCapabilities names the fields of [v1.DebugCapabilities] that describe
// rather than do, and so need no case, each with the reason. Empty today: every
// field is a behavior a driver has or refuses. An entry with no reason would be a
// way to make the walk quiet rather than a decision somebody made, which
// `TestEveryCapabilityFieldHasACase` refuses.
var metadataCapabilities = map[string]string{}

// TestEveryCapabilityFieldHasACase is the completeness rule over the real
// schema and the real corpus.
func TestEveryCapabilityFieldHasACase(t *testing.T) {
	fields := capabilityFields()
	names := make([]string, fields.Len())
	for i := range names {
		names[i] = string(fields.Get(i).Name())
	}
	require.GreaterOrEqual(t, len(names), 15, "the schema walk found too few capabilities; the walk is wrong, not the schema")

	for _, problem := range capabilityCoverageProblems(names, CapabilityCases(), metadataCapabilities) {
		t.Error(problem)
	}
}

// TestEveryCapabilityCaseIsWellFormed holds each case to what both drivers'
// tests take for granted: it names a bool field, has something to run and to
// read, and says of each driver that refuses it how that refusal reads.
func TestEveryCapabilityCaseIsWellFormed(t *testing.T) {
	cases := CapabilityCases()
	require.NotEmpty(t, cases, "the corpus is empty, so every claim below is vacuous")

	for _, c := range cases {
		t.Run(c.Field, func(t *testing.T) {
			field := capabilityField(c.Field)
			require.NotNil(t, field, "no such field of DebugCapabilities")
			assert.Equal(t, protoreflect.BoolKind, field.Kind(),
				"only a bool capability is read as advertised; teach AssertCapabilityCase the new kind")
			assert.NotEmpty(t, c.Exercise, "the docs table says what each case exercises")
			assert.NotNil(t, c.Workflow)
			assert.NotNil(t, c.Read)
			for driver, want := range map[string]CapabilityOutcome{"local": c.Local, "durable": c.Durable} {
				if !want.Applied {
					assert.NotEmpty(t, want.Says, "%s: a capability a driver lacks is named by that driver's own words", driver)
				}
			}
			if c.SourceMap != nil {
				assert.Equal(t, v1.WorkflowIRDigest(c.Workflow), c.SourceMap.GetIrDigest(),
					"a source map for another program is refused by the local driver, so this case would test that instead")
			}
		})
	}
}

// capabilityCoverageProblems is the completeness rule: every field is covered
// by exactly one case or exempt with a reason, and no case or exemption names a
// field the schema does not have.
//
// A function of its inputs so the demand is testable: the real corpus covers
// the real schema, so a test that only ran the two would pass whether the rule
// worked or not.
func capabilityCoverageProblems(fields []string, cases []CapabilityCase, exempt map[string]string) []string {
	var problems []string

	covered := map[string]int{}
	for _, c := range cases {
		covered[c.Field]++
	}
	for _, name := range fields {
		reason, exempted := exempt[name]
		switch {
		case exempted && strings.TrimSpace(reason) == "":
			problems = append(problems, fmt.Sprintf("%s is exempt with no reason; the reason is the record", name))
		case exempted && covered[name] > 0:
			problems = append(problems, fmt.Sprintf("%s is exempt as metadata and also has a case; delete the exemption", name))
		case !exempted && covered[name] == 0:
			problems = append(problems, fmt.Sprintf("DebugCapabilities.%s has no case: add one to CapabilityCases, so each driver "+
				"is held to advertising what it does, or exempt it in metadataCapabilities with the reason it describes rather than does", name))
		case covered[name] > 1:
			problems = append(problems, fmt.Sprintf("%s has %d cases; one case per capability keeps the docs table one row", name, covered[name]))
		}
	}
	for name := range covered {
		if !slices.Contains(fields, name) {
			problems = append(problems, fmt.Sprintf("a case names %q, which DebugCapabilities does not have", name))
		}
	}
	for name := range exempt {
		if !slices.Contains(fields, name) {
			problems = append(problems, fmt.Sprintf("metadataCapabilities lists %q, which DebugCapabilities does not have; delete the entry", name))
		}
	}
	slices.Sort(problems)

	return problems
}

// TestTheCompletenessRuleWorksInBothDirections exercises the demand against
// fixtures, since the tree itself always satisfies it.
func TestTheCompletenessRuleWorksInBothDirections(t *testing.T) {
	one := []CapabilityCase{{Field: "step_in"}}

	assert.Empty(t, capabilityCoverageProblems([]string{"step_in"}, one, nil), "a covered field was reported")

	problems := capabilityCoverageProblems([]string{"step_in", "added_later"}, one, nil)
	require.Len(t, problems, 1)
	assert.Contains(t, problems[0], "DebugCapabilities.added_later has no case",
		"a field added to the schema with no case was not demanded of the corpus")

	assert.Len(t, capabilityCoverageProblems([]string{"step_in"}, append(one, one...), nil), 1,
		"two cases for one capability were accepted")
	assert.Len(t, capabilityCoverageProblems([]string{"step_in"}, append(one, CapabilityCase{Field: "removed"}), nil), 1,
		"a case for a field the schema no longer has was accepted")

	assert.Empty(t, capabilityCoverageProblems([]string{"step_in", "label"}, one, map[string]string{"label": "a name, not a behavior"}),
		"an exempt field with a reason was reported")
	assert.Len(t, capabilityCoverageProblems([]string{"step_in", "label"}, one, map[string]string{"label": " "}), 1,
		"an exemption with no reason was accepted")
	assert.Len(t, capabilityCoverageProblems([]string{"step_in"}, one, map[string]string{"step_in": "not really"}), 1,
		"an exemption for a field that has a case was accepted")
	assert.Len(t, capabilityCoverageProblems([]string{"step_in"}, one, map[string]string{"gone": "was metadata"}), 1,
		"an exemption for a field the schema no longer has was accepted")
}

const (
	capabilityTableStart = "<!-- capabilities:start -->"
	capabilityTableEnd   = "<!-- capabilities:end -->"
)

// update rewrites the generated table in docs/DEBUGGING.md, the way the
// appearance goldens are rewritten.
var update = flag.Bool("update", false, "rewrite the capability table in docs/DEBUGGING.md from the corpus")

// capabilityTable renders the per-driver parity table from the corpus, in the
// order the schema declares the capabilities.
func capabilityTable(cases []CapabilityCase) string {
	byField := map[string]CapabilityCase{}
	for _, c := range cases {
		byField[c.Field] = c
	}

	cell := func(o CapabilityOutcome) string {
		if o.Applied {
			return "yes"
		}

		return fmt.Sprintf("no: says %q", o.Says)
	}

	var out strings.Builder
	out.WriteString("| Capability | What proves it | Local | Durable |\n| --- | --- | --- | --- |\n")
	fields := capabilityFields()
	for i := range fields.Len() {
		name := string(fields.Get(i).Name())
		c, ok := byField[name]
		if !ok {
			continue
		}
		fmt.Fprintf(&out, "| `%s` | %s | %s | %s |\n", name, c.Exercise, cell(c.Local), cell(c.Durable))
	}

	return out.String()
}

// spliceCapabilityTable returns doc with the text between the table's markers
// replaced by table.
func spliceCapabilityTable(doc, table string) (string, error) {
	start := strings.Index(doc, capabilityTableStart)
	end := strings.Index(doc, capabilityTableEnd)
	if start < 0 || end < start {
		return "", fmt.Errorf("docs/DEBUGGING.md needs %s then %s around the generated table", capabilityTableStart, capabilityTableEnd)
	}

	return doc[:start+len(capabilityTableStart)] + "\n\n" + table + "\n" + doc[end:], nil
}

// TestTheDebuggingDocCapabilityTableIsTheCorpus fails when the table in
// docs/DEBUGGING.md is not the one the corpus renders. The corpus is what both
// drivers are held to, so the document cannot say a driver does what its tests
// do not: regenerate with
// `go test ./pkg/flowstate/v1/internal/conformance -run TestTheDebuggingDocCapabilityTableIsTheCorpus -update`.
func TestTheDebuggingDocCapabilityTableIsTheCorpus(t *testing.T) {
	path := filepath.Join("..", "..", "..", "..", "..", "docs", "DEBUGGING.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Skip("not running from a checkout; nothing to compare the capability table against")
	}

	table := capabilityTable(CapabilityCases())
	want, err := spliceCapabilityTable(string(raw), table)
	require.NoError(t, err)

	if *update {
		require.NoError(t, os.WriteFile(path, []byte(want), 0o644))

		return
	}
	assert.Equal(t, want, string(raw),
		"the capability table in docs/DEBUGGING.md is not what the corpus renders; regenerate it with -update")
}

// TestTheCapabilityTableNamesEveryCapabilityAndBothDrivers pins what the table
// is a projection of, so a renderer that dropped a row or a refusal's words
// would fail here rather than as a quiet omission from the document.
func TestTheCapabilityTableNamesEveryCapabilityAndBothDrivers(t *testing.T) {
	table := capabilityTable(CapabilityCases())

	fields := capabilityFields()
	for i := range fields.Len() {
		assert.Contains(t, table, "| `"+string(fields.Get(i).Name())+"` |")
	}
	assert.Contains(t, table, `no: says "logpoints are not supported"`, "a driver's refusal is named in the table")
	assert.Contains(t, table, "| yes | yes |", "a capability both drivers do is shown as such")

	spliced, err := spliceCapabilityTable("before\n"+capabilityTableStart+"\nstale\n"+capabilityTableEnd+"\nafter", table)
	require.NoError(t, err)
	assert.NotContains(t, spliced, "stale")
	assert.True(t, strings.HasPrefix(spliced, "before\n"+capabilityTableStart) && strings.HasSuffix(spliced, capabilityTableEnd+"\nafter"),
		"the text outside the markers was touched")

	_, err = spliceCapabilityTable("no markers", table)
	assert.Error(t, err, "a document without markers was written to")
}
