package main

import (
	"bytes"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wallClock matches the two fields of a run document a clock chose.
//
// They are the only thing that legitimately differs between two runs of one
// source, so they are replaced rather than the comparison being loosened to
// JSONEq: everything else — field order, spelling, which fields were emitted at
// all — is what this test is for, and an order-insensitive comparison would pass
// on two documents no single `jq` expression reads the same way.
var wallClock = regexp.MustCompile(`"(startTime|closeTime)":"[^"]*"`)

// sameRunDocument compacts a document and blanks its timestamps, so what is left
// is what a reader would have to write an expression against.
func sameRunDocument(t *testing.T, document []byte) string {
	t.Helper()

	var compact bytes.Buffer
	require.NoError(t, json.Compact(&compact, document),
		"the answer is not a JSON document: %s", document)

	return string(wallClock.ReplaceAll(compact.Bytes(), []byte(`"$1":"<when>"`)))
}

// TestTheAgentAndTheAuthorReadTheSameRunDocument is #1553 stated as the property
// the fix has to hold.
//
// `flow run local -o json` and flowstate_run_local execute the same source
// through the same driver, so what they hand back has to be one document. It was
// not: the CLI projected the run — `steps.<id>.<output>`, and a value rather than
// CEL's tagged encoding of one — while the tool wrote the schema's own protojson,
// so an author and the agent helping them could not share a `jq` filter, a schema,
// or an example.
//
// Compared as documents rather than field by field, because a field-by-field test
// is what let the two drift in the first place: every earlier assertion about this
// answer passed on both dialects.
//
// The sources are chosen for the value shapes the issue names — an int (protojson
// spells it a string), a list, a map, a structural literal written as YAML rather
// than computed — because those are where the two spellings differ. A workflow
// whose only output is a string would pass this test against the bug.
func TestTheAgentAndTheAuthorReadTheSameRunDocument(t *testing.T) {
	t.Parallel()

	sources := map[string]string{
		"scalars a tagged encoding spells differently": `edition: v2026.3
name: scalars
steps:
  - id: count
    value: ${2}
  - id: ratio
    value: ${0.5}
  - id: ok
    value: ${true}
  - id: label
    value: ${"standard"}
outputs:
  count:
    value: ${steps.count.value}
  ratio:
    value: ${steps.ratio.value}
  ok:
    value: ${steps.ok.value}
  label:
    value: ${steps.label.value}
`,
		"collections whose entries are values too": `edition: v2026.3
name: collections
steps:
  - id: regions
    value: ${["eu-west-1", "us-east-1"]}
  - id: counts
    value: '${ {"eu-west-1": 2, "us-east-1": 3} }'
outputs:
  regions:
    value: ${steps.regions.value}
  counts:
    value: ${steps.counts.value}
`,
		"a literal written as YAML rather than computed": `edition: v2026.3
name: structured
steps:
  - id: noop
    value: ${true}
outputs:
  plan:
    value:
      tier: standard
      shards: 2
      regions:
        - eu-west-1
`,
	}

	for name, source := range sources {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			stdout, stderr, err := runLocal(t, source, "--output", "json")
			require.NoError(t, err, "the CLI could not run the source this test compares: %s", stderr)

			session := connectMCP(t, defaultLocalRunPosture())

			result, _ := callRunLocal(t, session, map[string]any{"source": source})
			require.False(t, result.IsError, "the tool could not run the source this test compares: %s",
				result.Content[0].(*mcp.TextContent).Text)

			var answer struct {
				Run json.RawMessage `json:"run"`
			}
			require.NoError(t, json.Unmarshal([]byte(result.Content[0].(*mcp.TextContent).Text), &answer))

			assert.Equal(t, sameRunDocument(t, []byte(stdout)), sameRunDocument(t, answer.Run),
				"`flow run local -o json` and flowstate_run_local answered with two different "+
					"documents for one run, which is the dialect split #1553 closed")
		})
	}
}
