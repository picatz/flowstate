package flowfile_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The `plugins:` block, exercised from the file rather than from the protobuf.
//
// CLAUDE.md's "a capability is not done until it is reachable from a Flowfile"
// is about exactly this feature's first cut: the resolver had tests that built
// `*v1.PluginRequirement` values in Go, which proves a resolver and says nothing
// about whether anyone can write one down. What follows goes through the parser
// and the validator, which is the path an author's editor takes.

// TestValidatePluginsBlock covers the block an author writes: what is accepted,
// what is refused, and what the refusal says.
func TestValidatePluginsBlock(t *testing.T) {
	tests := []struct {
		name string
		src  string
		// want is a substring the diagnostics must contain; empty means the
		// Flowfile must validate cleanly.
		want string
	}{
		{
			name: "a requirement is declared and the file validates",
			src: `
edition: v2026.3
name: needs-a-plugin
plugins:
  git: v0.1.0
steps:
  - id: a
    log:
      message: hi
`,
		},
		{
			// Several, because the block is a mapping and one entry would not catch
			// a compiler that kept only the last.
			name: "several requirements are declared together",
			src: `
edition: v2026.3
name: needs-three
plugins:
  git: v0.1.0
  github: v1.2.3
  sql: v10.0.4
steps:
  - id: a
    log:
      message: hi
`,
		},
		{
			// The version grammar is deliberately small, and a version missing its
			// v is the mistake somebody makes on their first try.
			name: "a version with no v prefix is refused",
			src: `
edition: v2026.3
name: bare-version
plugins:
  git: "0.1.0"
steps:
  - id: a
    log:
      message: hi
`,
			want: `plugin "git" requires a semantic version written as vMAJOR.MINOR.PATCH, but "0.1.0" was written here`,
		},
		{
			name: "a two-part version is refused",
			src: `
edition: v2026.3
name: short-version
plugins:
  git: "v1.2"
steps:
  - id: a
    log:
      message: hi
`,
			want: `but "v1.2" was written here`,
		},
		{
			name: "a range is refused, because the grammar has no ranges",
			src: `
edition: v2026.3
name: range-version
plugins:
  git: ">=1.0.0"
steps:
  - id: a
    log:
      message: hi
`,
			want: `but ">=1.0.0" was written here`,
		},
		{
			// The key is misspelled, which must be reported rather than ignored:
			// silently dropping it would leave an author believing their run is
			// pinned to a plugin when nothing about it was ever recorded.
			name: "a misspelled plugins key is reported with the spelling that works",
			src: `
edition: v2026.3
name: misspelled
plugin:
  git: v0.1.0
steps:
  - id: a
    log:
      message: hi
`,
			want: `unknown key "plugin"; did you mean "plugins"?`,
		},
		{
			// A version written unquoted is a YAML number, not a string, and the
			// diagnostic has to say so rather than complain about the grammar of a
			// version the author never wrote.
			name: "an unquoted numeric version is reported as the type mistake it is",
			src: `
edition: v2026.3
name: numeric
plugins:
  git: 0.1
steps:
  - id: a
    log:
      message: hi
`,
			want: "must be a string, but a number was written here",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Through the same helper the rest of this package's diagnostic tests
			// use, because a `plugins:` mistake is reported by the compiler rather
			// than by the semantic pass, since there is no workflow to validate once a
			// version cannot be read, and an author sees one stream either way.
			got := diagnose(t, tc.src)

			if tc.want == "" {
				require.Empty(t, got, "the file should have validated cleanly")

				return
			}

			require.Contains(t, got, tc.want)
		})
	}
}

// TestValidatePluginsBlockReportsPosition asserts the diagnostic lands on the
// requirement that is wrong rather than on the block that holds it.
//
// A `plugins:` block is a list of independent claims, and an author with six of
// them needs to be told which one to edit. The position is the whole difference
// between that and reading all six.
func TestValidatePluginsBlockReportsPosition(t *testing.T) {
	src := `edition: v2026.3
name: positions
plugins:
  git: v0.1.0
  github: "1.0.0"
steps:
  - id: a
    log:
      message: hi
`

	got := diagnose(t, src)

	// Line 5 is `github: "1.0.0"`. Line 4 is the requirement that is fine, and
	// line 3 is the block: both are findable and both are the wrong answer.
	require.True(t, strings.HasPrefix(got, "5:"),
		"the diagnostic should be positioned on the requirement that is wrong; got %q", got)
	require.Contains(t, got, "plugins.github",
		"the diagnostic should name the path of the requirement")
	require.NotContains(t, got, "plugins.git:",
		"the requirement that is fine should not be reported")
}

// TestParsePluginsBlockCompilesRequirements is the other half: the block does
// not merely validate, it reaches the specification a server is handed.
//
// Asserted through [flowfile.Parse] rather than through the validator, because a
// grammar that is checked and then dropped on the floor passes every diagnostic
// test there is.
func TestParsePluginsBlockCompilesRequirements(t *testing.T) {
	src := `edition: v2026.3
name: compiles
plugins:
  git: v0.1.0
  sql: v2.3.4
steps:
  - id: a
    log:
      message: hi
`

	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)

	require.Len(t, wf.GetPluginRequirements(), 2)
	require.Equal(t, "git", wf.GetPluginRequirements()[0].GetName())
	require.Equal(t, "v0.1.0", wf.GetPluginRequirements()[0].GetMinimumVersion())
	require.Equal(t, "sql", wf.GetPluginRequirements()[1].GetName())
	require.Equal(t, "v2.3.4", wf.GetPluginRequirements()[1].GetMinimumVersion())

	// Nothing pins itself. The selection is the control plane's, and a file that
	// arrived carrying one would be describing a deployment it has never seen.
	require.Empty(t, wf.GetResolvedPlugins())
}

// TestPluginsSurviveARoundTrip is the `flow fix` guard, and the same shape as
// TestStepVarsSurviveARoundTrip in vars_test.go: a key the parser reads but
// Marshal does not write is not a formatting bug, it is `flow fmt` deleting a
// section from a valid file. #455 was exactly this hole for `plugins:`.
//
// Checked as bytes rather than by parsing the result and asking whether it
// still validates, because a validating file is not the claim being tested -
// the claim is that Marshal writes the block back verbatim, in the position
// the parser reads it in and both examples in examples/plugins/ write it in:
// directly under the description, above everything the run computes.
func TestPluginsSurviveARoundTrip(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: needs-a-plugin
description: uses a plugin task
plugins:
  git: v0.1.0
  sql: v2.3.4
vars:
  region: eu-west-1
steps:
- id: a
  log:
    message: hi
`

	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)

	out, err := flowfile.Marshal(wf)
	require.NoError(t, err)

	// The whole file, not a substring: this source is already in Marshal's own
	// canonical layout, so a correct Marshal reproduces it exactly. Before the
	// fix, Marshal dropped the `plugins:` block entirely and this comparison
	// failed with the block missing from the output.
	require.Equal(t, src, string(out), "Marshal did not reproduce the plugins: block byte for byte")

	// And the order is preserved, not merely the presence: several requirements
	// went in, and a compiler that kept only one or reordered them would still
	// pass a substring check.
	require.Len(t, wf.GetPluginRequirements(), 2)

	again, err := flowfile.Unmarshal(out)
	require.NoError(t, err, "a marshalled file did not parse:\n%s", out)
	require.Equal(t, wf.GetPluginRequirements(), again.GetPluginRequirements())
}

// TestMarshalRefusesAnUnparseableVersion is the negative direction of the
// round trip above: Marshal serves hand-built specifications too, and a
// minimum version the parser rejects (a leading zero, here) must be refused
// with an error naming the plugin, never written into a document that
// Parse will then refuse to read back.
func TestMarshalRefusesAnUnparseableVersion(t *testing.T) {
	t.Parallel()

	src := `edition: v2026.3
name: needs-a-plugin
plugins:
  git: v0.1.0
steps:
- id: a
  log:
    message: hi
`

	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)

	// Corrupt the requirement the way only a hand-built spec can be: the
	// parser would have refused this spelling at the file boundary.
	wf.GetPluginRequirements()[0].MinimumVersion = "v01.2.3"

	out, err := flowfile.Marshal(wf)
	require.Error(t, err, "Marshal wrote a version the parser rejects:\n%s", out)
	require.ErrorContains(t, err, `plugin "git"`)
	require.ErrorContains(t, err, "v01.2.3")
	require.Nil(t, out)
}
