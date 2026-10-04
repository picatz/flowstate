package conformance

import (
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// ExecPolicyFile is the operator policy an exec example ships beside its
// workflow.
const ExecPolicyFile = "exec-policy.yaml"

// execExampleInstall is the one exec policy the example harnesses share.
//
// The exec task is a single registry entry, and the example corpora run in
// parallel with each other, so each harness installing and restoring its own
// would restore the denied default under another that is still running. One
// reference-counted install serves them all: the first acquire installs, the
// last release restores.
var execExampleInstall struct {
	mu        sync.Mutex
	refs      int
	policy    string
	workspace string
	restore   func()
}

// ExecExampleWorkspace makes an example that runs programs runnable for real.
//
// The policy is the example's own [ExecPolicyFile], loaded through
// [v1.ParseExecPolicy] exactly as `--exec-policy` loads it, so its allow and
// deny rules apply unchanged. Only what is machine-specific is rewritten: each
// listed program is resolved on this machine by its bare name (the harness
// fails, loudly, when one is absent, rather than skipping the example), and the
// roots become a directory the harness creates. It returns the directory a
// run's `workspace` input should be pointed at, and installs the policy until
// the test (and every other user of it) has finished.
func ExecExampleWorkspace(tb testing.TB, workflowPath string) string {
	tb.Helper()

	policyPath := filepath.Join(filepath.Dir(workflowPath), ExecPolicyFile)

	install := &execExampleInstall
	install.mu.Lock()
	defer install.mu.Unlock()

	if install.refs > 0 && install.policy != policyPath {
		tb.Fatalf("%s and %s are both exec examples; the harness shares one exec policy and would "+
			"run one under the other's", install.policy, policyPath)
	}

	if install.refs == 0 {
		install.policy = policyPath
		install.workspace, install.restore = installExecExample(tb, policyPath)
	}
	install.refs++

	tb.Cleanup(func() {
		install.mu.Lock()
		defer install.mu.Unlock()

		install.refs--
		if install.refs == 0 {
			install.restore()
		}
	})

	return install.workspace
}

func installExecExample(tb testing.TB, policyPath string) (string, func()) {
	tb.Helper()

	raw, err := os.ReadFile(policyPath)
	require.NoError(tb, err, "an example that uses exec ships the operator policy it runs under")

	var doc map[string]any
	require.NoError(tb, yaml.Unmarshal(raw, &doc))
	section, ok := doc["exec"].(map[string]any)
	require.True(tb, ok, "%s has no exec: section", policyPath)

	programs, _ := section["executables"].(map[string]any)
	for name := range programs {
		found, err := exec.LookPath(name)
		require.NoErrorf(tb, err, "%s lists %q, which this machine does not have; the example "+
			"cannot run without it", policyPath, name)
		programs[name] = found
	}

	root, err := os.MkdirTemp("", "flowstate-exec-example-")
	require.NoError(tb, err)
	root, err = filepath.EvalSymlinks(root)
	require.NoError(tb, err)
	workspace := filepath.Join(root, "demo")
	require.NoError(tb, os.Mkdir(workspace, 0o755))
	section["roots"] = []string{root}

	rewritten, err := yaml.Marshal(doc)
	require.NoError(tb, err)

	policy, err := v1.ParseExecPolicy(rewritten)
	require.NoError(tb, err, "the example's exec policy must itself load")

	registry := v1.DefaultRegistry()
	original, existed := registry.Lookup("exec")
	require.NoError(tb, registry.Replace(v1.ExecTaskDef(policy)))

	return workspace, func() {
		if existed {
			_ = registry.Replace(original)
		}
		_ = os.RemoveAll(root)
	}
}

// WithWorkspace returns inputs with the `workspace` input pointed at dir. The
// bound map is copied, not changed: both drivers are handed the same one.
func WithWorkspace(inputs map[string]*v1.Value, dir string) map[string]*v1.Value {
	out := maps.Clone(inputs)
	if out == nil {
		out = map[string]*v1.Value{}
	}
	out["workspace"] = v1.NewLiteral(dir)

	return out
}
