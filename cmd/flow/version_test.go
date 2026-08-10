package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runVersionInto runs `flow version` in-process, the same pattern
// keys_test.go's runKeysGenerateInto uses, so this exercises runVersion
// directly rather than only the ranked helpers underneath it.
func runVersionInto(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()

	var out, errOut bytes.Buffer

	cmd := newVersionCommand()
	for i := 0; i+1 < len(args); i += 2 {
		require.NoError(t, cmd.Flags().Set(args[i], args[i+1]))
	}
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetContext(t.Context())

	err = runVersion(cmd, nil)

	return out.String(), errOut.String(), err
}

// TestResolveVersionInfoIsHonestWithNothingStamped is #373's "honest devel
// when absent" requirement: with the package-level version var at its zero
// value, resolveVersionInfo never invents a number: it either reads what
// runtime/debug found on this build, or says "devel"/"unknown".
func TestResolveVersionInfoIsHonestWithNothingStamped(t *testing.T) {
	t.Cleanup(func() { version = "dev" })
	version = "dev"

	info := resolveVersionInfo()

	assert.NotEmpty(t, info.Version, "version must never be empty")
	assert.NotEmpty(t, info.Commit, "commit must never be empty")
	assert.NotEmpty(t, info.CommitDate, "date must never be empty")
	assert.NotEqual(t, "0", info.Version, "a made-up version is worse than an honest devel")

	// go test itself is a module-aware build, so debug.ReadBuildInfo always
	// succeeds here; this only pins that when it does, and the linker never
	// stamped a version, the answer says so rather than defaulting to the
	// literal string "dev" main.go's own var carries, which is not what a
	// person typing `flow version` was told to expect ("devel", matching `go
	// version -m`'s own "(devel)" convention, minus the parens this command
	// finds noisier than they're worth).
	assert.NotEqual(t, "dev", info.Version, `an unstamped build must report "devel", not the literal placeholder "dev"`)
}

// TestResolveVersionInfoHonorsAnExplicitVersion is the other half: when
// -ldflags stamped a version, that value wins over anything runtime/debug
// would otherwise have filled in, matching #373's "so -ldflags stamping
// stays optional" without making it stop working when present.
func TestResolveVersionInfoHonorsAnExplicitVersion(t *testing.T) {
	t.Cleanup(func() { version = "dev" })
	version = "1.2.3"

	info := resolveVersionInfo()

	assert.Equal(t, "1.2.3", info.Version)
}

// TestRunVersionTextIsAPlainLine is the default rendering #373 asks for.
func TestRunVersionTextIsAPlainLine(t *testing.T) {
	stdout, stderr, err := runVersionInto(t)
	require.NoError(t, err)

	assert.Empty(t, stderr, "the answer belongs on stdout, not narrated on stderr")

	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	assert.Len(t, lines, 1, "the default text form should be one plain line: %q", stdout)
	assert.Contains(t, stdout, "flow")
	assert.Contains(t, stdout, "commit")
	assert.Contains(t, stdout, "committed", "the text line speaks the commit's date, not a build time it does not know")
}

// versionJSONFields is the stable field set #373 asks -o json for. A test
// keyed on JSON tags rather than on field order, so reordering the struct's
// fields cannot pass this by accident the way a byte-for-byte comparison
// would let it.
var versionJSONFields = []string{"version", "commit", "commitDate", "goVersion", "os", "arch", "modified"}

// TestRunVersionJSONParsesWithDocumentedFields is #373's machine-readable
// requirement, proven the way a consumer would use it: parse the output and
// check the fields it gates on are actually there and correctly typed,
// rather than only checking the command exits zero.
func TestRunVersionJSONParsesWithDocumentedFields(t *testing.T) {
	for _, format := range []string{"json", "jsonl"} {
		t.Run(format, func(t *testing.T) {
			stdout, _, err := runVersionInto(t, "output", format)
			require.NoError(t, err)

			var document map[string]any
			require.NoError(t, json.Unmarshal([]byte(stdout), &document),
				"flow version -o %s did not produce parseable JSON:\n%s", format, stdout)

			for _, field := range versionJSONFields {
				assert.Contains(t, document, field, "missing documented field %q", field)
			}

			gotVersion, ok := document["version"].(string)
			assert.True(t, ok && gotVersion != "", "version must be a non-empty string")

			modified, ok := document["modified"].(string)
			assert.True(t, ok, "modified is tri-state and must be a string")
			assert.Contains(t, []string{"true", "false", "unknown"}, modified,
				"modified answers only true, false, or unknown: absence of the vcs "+
					"setting is a real answer and must never read as a clean tree")
		})
	}
}

// TestRunVersionJSONRoundTripsIntoVersionInfo is the stricter form of the
// same requirement: the documented shape is exactly versionInfo, not merely
// a superset of it.
func TestRunVersionJSONRoundTripsIntoVersionInfo(t *testing.T) {
	stdout, _, err := runVersionInto(t, "output", "json")
	require.NoError(t, err)

	var info versionInfo
	require.NoError(t, json.Unmarshal([]byte(stdout), &info))

	assert.NotEmpty(t, info.Version)
	assert.NotEmpty(t, info.Commit)
	assert.NotEmpty(t, info.CommitDate)
	assert.NotEmpty(t, info.GoVersion)
	assert.NotEmpty(t, info.OS)
	assert.NotEmpty(t, info.Arch)
}

// TestRunVersionRefusesAnUnknownOutputFormat holds `flow version` to the same
// rule as every other verb behind addOutputFlag: an --output value it does
// not accept is refused rather than silently downgraded.
func TestRunVersionRefusesAnUnknownOutputFormat(t *testing.T) {
	_, _, err := runVersionInto(t, "output", "yaml")
	require.Error(t, err)
	assert.True(t, isUsageError(err))
}

// TestNewVersionCommandDeclaresNoNetworkFlags proves the command has nothing
// that could make it reach a peer: its whole flag set is --output, added by
// addOutputFlag like every other answer-shaped verb, and none of --address,
// --namespace, --profile or any other flag that would give it somewhere to
// dial. That is what "offline always" comes down to for a cobra command:
// there is nothing here it could configure a connection with even by
// mistake, which resolveVersionInfo's own doc backs up: it reads only
// runtime.GOOS/GOARCH/Version and runtime/debug.ReadBuildInfo, none of which
// touch the network.
func TestNewVersionCommandDeclaresNoNetworkFlags(t *testing.T) {
	cmd := newVersionCommand()

	var names []string
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		names = append(names, f.Name)
	})

	assert.Equal(t, []string{"output"}, names,
		"flow version should declare exactly --output and nothing that could reach a peer")
}
