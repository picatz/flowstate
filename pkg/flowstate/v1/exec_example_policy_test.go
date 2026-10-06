package flowstatev1

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/picatz/flowstate/internal/strictyaml"
	"github.com/picatz/flowstate/pkg/flowstate/v1/execpolicy"
)

// TestShippedExecPolicyAllowsOnlyTheShapesItNames loads examples/exec-checks'
// own operator policy, rewriting only the machine-specific paths, and runs
// argv against it. git reads the repository's config, which an earlier step may
// have written, and core.fsmonitor, diff.external and a textconv driver each
// name a program git then runs; so status, diff and log must stay unlisted
// until the policy also neutralizes that config.
func TestShippedExecPolicyAllowsOnlyTheShapesItNames(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("exec is denied off Unix; there is no policy to load")
	}

	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "examples", "exec-checks", "exec-policy.yaml"))
	require.NoError(t, err)

	var doc ExecPolicy
	require.NoError(t, strictyaml.UnmarshalProto(raw, &doc))
	section := doc.GetExec()
	require.NotNil(t, section)
	for name := range section.GetExecutables() {
		found, err := exec.LookPath(name)
		require.NoError(t, err, "the shipped policy lists %q, which this machine lacks", name)
		section.Executables[name] = found
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	section.Roots = []string{root}

	rewritten, err := protojson.Marshal(&doc)
	require.NoError(t, err)
	policy, err := ParseExecPolicy(rewritten)
	require.NoError(t, err)

	check := func(argv ...string) error {
		_, err := policy.Check(t.Context(), execpolicy.Request{Argv: argv, Dir: root})
		return err
	}

	require.NoError(t, check("git", "--version"))
	require.NoError(t, check("git", "rev-parse", "--git-dir"))

	for _, argv := range [][]string{
		{"git", "status"},
		{"git", "diff"},
		{"git", "log"},
		{"git", "log", "--oneline"},
		{"git", "diff", "--no-ext-diff", "--no-textconv"},
		{"git", "rev-parse", "--git-dir", "extra"},
		{"git", "-c", "core.fsmonitor=true", "rev-parse", "--git-dir"},
	} {
		require.Error(t, check(argv...), "the shipped policy must not allow %v", argv)
	}
}
