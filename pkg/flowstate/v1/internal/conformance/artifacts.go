package conformance

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Shared cases for `workspace:` and `produce:`, run by both execution drivers
// with the exec cases' machinery: real programs under a real policy, because a
// stubbed task proves the plumbing and none of the claims worth making. That a
// tree one step builds is the tree the next step sees, byte for byte; that a
// workspace is empty when an attempt starts and gone when it ends; that a link
// in a produced tree fails the step rather than being skipped; that a worker
// with no store denies; and that the operator's roots still confine a
// workspace.
//
// They are [ExecCase]s and run through the same two runners as [ExecCases],
// plus the artifact runtime [NewArtifactRuntime] builds for each.

// ArtifactWorkspaceDir is the directory under the case's root where the
// artifact runtime creates per-attempt workspaces, so the exec policy's one
// root covers it.
const ArtifactWorkspaceDir = "workspaces"

// NewArtifactRuntime builds the artifact capability a case runs under, or nil
// for a case on a worker with none. The store is in memory and the workspace
// root is under root unless the case puts it outside.
func NewArtifactRuntime(tb testing.TB, root string, c ExecCase) *v1.ArtifactRuntime {
	tb.Helper()

	if c.NoArtifactStore {
		return nil
	}

	base := filepath.Join(root, ArtifactWorkspaceDir)
	if c.WorkspaceOutsideRoots {
		base = filepath.Join(ExecRoot(tb), "elsewhere")
	}

	runtime, err := v1.NewMemoryArtifactRuntime(base)
	require.NoError(tb, err)

	return runtime
}

// artifactStep is an exec step with no dir of its own: it runs in the
// workspace the step declares.
func artifactStep(id string, script string, workspace map[string]*v1.Value, produce map[string]string) *v1.Node {
	return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{
		Name:      "exec",
		Inputs:    map[string]*v1.Value{"argv": shArgv(script)},
		Workspace: workspace,
		Produce:   produce,
	}}}
}

// producedRef reads one produced artifact of a step as the map history holds.
func producedRef(tb testing.TB, out *v1.Workflow_StepOutputs, step, name string) map[string]any {
	tb.Helper()

	all, ok := execField(tb, out, step, v1.ArtifactsOutput).(map[string]any)
	require.True(tb, ok, "step %q has no artifacts map", step)
	ref, ok := all[name].(map[string]any)
	require.True(tb, ok, "step %q produced no artifact %q", step, name)

	return ref
}

// ArtifactCases returns the shared cases both drivers must agree on. root comes
// from [ExecRoot].
func ArtifactCases(root string) []ExecCase {
	mount := func(step, name string) *v1.Value { return v1.NewExpr("steps." + step + ".artifacts." + name) }

	build := `mkdir -p out/sub && printf built > out/app && printf data > out/sub/data`

	refWithDigest := func(digest string) map[string]*v1.Value {
		return map[string]*v1.Value{"ref": v1.NewLiteralMap(map[string]any{
			"digest": digest, "size_bytes": int64(1), "entry_count": int64(1),
		})}
	}

	return []ExecCase{
		{
			// The capability's reason to exist: what one step makes is what the
			// next step runs on, through nothing but an expression.
			Name: "a tree one step produces is the next step's workspace",
			Workflow: execWorkflow("artifacts-hand-off",
				artifactStep("build", build, nil, map[string]string{"bin": "out"}),
				artifactStep("test", `cat bin/app bin/sub/data`,
					map[string]*v1.Value{"bin": mount("build", "bin")}, map[string]string{"seen": "."}),
			),
			Check: func(tb testing.TB, out *v1.Workflow_StepOutputs) {
				require.Equal(tb, "builtdata", execField(tb, out, "test", "stdout"))

				ref := producedRef(tb, out, "build", "bin")
				require.Len(tb, ref["digest"], 64)
				require.EqualValues(tb, 3, ref["entry_count"], "sub, sub/data and app")
				require.EqualValues(tb, len("built")+len("data"), ref["size_bytes"])
				require.Equal(tb, []string{"digest", "entry_count", "size_bytes"}, slices.Sorted(maps.Keys(ref)),
					"history holds a digest and two numbers, never bytes")

				// What the second step saw is the first's tree one level down: the
				// same three entries under a bin/ directory of their own.
				seen := producedRef(tb, out, "test", "seen")
				require.EqualValues(tb, 4, seen["entry_count"])
				require.NotEqual(tb, ref["digest"], seen["digest"], "mounted at bin/, it is a different tree")
			},
		},
		{
			// A mount of "." is the workspace itself.
			Name: "a mount at the workspace root puts the files at the top",
			Workflow: execWorkflow("artifacts-root-mount",
				artifactStep("build", build, nil, map[string]string{"bin": "out"}),
				artifactStep("test", `cat app sub/data`,
					map[string]*v1.Value{".": mount("build", "bin")}, nil),
			),
			Check: func(tb testing.TB, out *v1.Workflow_StepOutputs) {
				require.Equal(tb, "builtdata", execField(tb, out, "test", "stdout"))
			},
		},
		{
			// Content addressing: the same bytes are the same artifact, which is
			// what lets a later run find an earlier one's output by value.
			Name: "the same tree built twice has one digest",
			Workflow: execWorkflow("artifacts-content-addressed",
				artifactStep("a", build, nil, map[string]string{"o": "out"}),
				artifactStep("b", build, nil, map[string]string{"o": "out"}),
			),
			Check: func(tb testing.TB, out *v1.Workflow_StepOutputs) {
				require.Equal(tb, producedRef(tb, out, "a", "o")["digest"], producedRef(tb, out, "b", "o")["digest"])
			},
		},
		{
			// Per attempt: empty on arrival, and gone when the step ends, with
			// nothing left in the workspace root for the next attempt to find.
			Name: "a workspace starts empty and is removed after the step",
			Workflow: execWorkflow("artifacts-fresh-workspace",
				artifactStep("one", `printf '%s|' "$(ls -A | wc -l | tr -d ' ')"; pwd; printf left > leftover`, nil, map[string]string{"x": "."}),
				artifactStep("two", `printf '%s|' "$(ls -A | wc -l | tr -d ' ')"; pwd`, nil, map[string]string{"x": "."}),
			),
			Check: func(tb testing.TB, out *v1.Workflow_StepOutputs) {
				first, _ := execField(tb, out, "one", "stdout").(string)
				second, _ := execField(tb, out, "two", "stdout").(string)
				require.True(tb, strings.HasPrefix(first, "0|"), "first workspace was not empty: %q", first)
				require.True(tb, strings.HasPrefix(second, "0|"), "the second step saw the first's leftover: %q", second)

				firstDir, secondDir := strings.TrimPrefix(first, "0|"), strings.TrimPrefix(second, "0|")
				require.NotEqual(tb, firstDir, secondDir, "each step gets its own workspace")
				for _, dir := range []string{firstDir, secondDir} {
					_, err := os.Stat(strings.TrimSpace(dir))
					require.ErrorIs(tb, err, os.ErrNotExist, "workspace %q outlived its step", dir)
				}

				left, err := os.ReadDir(filepath.Join(root, ArtifactWorkspaceDir))
				require.NoError(tb, err)
				require.Empty(tb, left, "the workspace root holds nothing once the run is over")
			},
		},
		{
			// Mounting a tree and not touching it yields the tree: the snapshot is
			// of the files, not of the step.
			Name: "a mounted tree can be passed along unchanged",
			Workflow: execWorkflow("artifacts-pass-through",
				artifactStep("build", build, nil, map[string]string{"bin": "out"}),
				artifactStep("relay", `true`, map[string]*v1.Value{".": mount("build", "bin")}, map[string]string{"same": "."}),
			),
			Check: func(tb testing.TB, out *v1.Workflow_StepOutputs) {
				require.Equal(tb, producedRef(tb, out, "build", "bin")["digest"], producedRef(tb, out, "relay", "same")["digest"])
			},
		},
		{
			// The default posture: a worker nobody gave a store to denies, and
			// the denial names the flag.
			Name:            "a worker with no artifact store denies",
			NoArtifactStore: true,
			Workflow: execWorkflow("artifacts-no-store",
				artifactStep("build", build, nil, map[string]string{"bin": "out"})),
			ExpectedKind:  v1.ErrorKindPolicyDenied,
			ExpectedError: []string{"no artifact store", v1.ArtifactStoreFlag},
		},
		{
			// The operator's roots still confine: a workspace the policy's roots
			// do not reach is not a place to run, and the denial says what to
			// change without naming the worker's directory.
			Name:                  "a workspace outside the exec policy's roots is denied",
			WorkspaceOutsideRoots: true,
			Workflow: execWorkflow("artifacts-outside-roots",
				artifactStep("build", build, nil, map[string]string{"bin": "out"})),
			ExpectedKind:  v1.ErrorKindPolicyDenied,
			ExpectedError: []string{"exec", "workspace", "roots"},
		},
		{
			// An explicit dir is no escape hatch: it is checked exactly as it
			// would be without a workspace.
			Name: "an explicit dir outside the roots is still denied",
			Workflow: execWorkflow("artifacts-explicit-dir",
				&v1.Node{Id: "run", Kind: &v1.Node_Task{Task: &v1.Task{
					Name: "exec",
					Inputs: map[string]*v1.Value{
						"argv": shArgv(`true`),
						"dir":  v1.NewLiteral(filepath.Dir(root)),
					},
					Produce: map[string]string{"x": "."},
				}}}),
			ExpectedKind:  v1.ErrorKindPolicyDenied,
			ExpectedError: []string{"not under any root"},
		},
		{
			Name: "a produced path the task never made fails the step",
			Workflow: execWorkflow("artifacts-missing-path",
				artifactStep("build", `true`, nil, map[string]string{"bin": "out"})),
			ExpectedKind:  v1.ErrorKindInvalidInput,
			ExpectedError: []string{`produce "bin"`, "did not create"},
		},
		{
			// There is no skip mode: a snapshot that left the link out would be a
			// different tree from the one the task made.
			Name: "a symbolic link inside a produced tree fails the step",
			Workflow: execWorkflow("artifacts-symlink",
				artifactStep("build", `mkdir out && ln -s /etc/passwd out/link`, nil, map[string]string{"bin": "out"})),
			ExpectedKind:  v1.ErrorKindInvalidInput,
			ExpectedError: []string{`produce "bin"`, "symlink"},
		},
		{
			// The path to the tree is checked as well as the tree: a link in a
			// parent component would otherwise have whatever it points at
			// snapshotted as the step's output.
			Name: "a produced path that is itself a symbolic link fails the step",
			Workflow: execWorkflow("artifacts-symlink-root",
				artifactStep("build", `ln -s /tmp out`, nil, map[string]string{"bin": "out"})),
			ExpectedKind:  v1.ErrorKindInvalidInput,
			ExpectedError: []string{`produce "bin"`, "symbolic link"},
		},
		{
			// A reference to something this worker's store for this tenant does
			// not hold is permanent: the same retry would find the same disk.
			Name: "a reference the store does not hold fails the step permanently",
			Workflow: &v1.Workflow{
				Name: "artifacts-unreachable", Profile: v1.CurrentProfile,
				Vars: refWithDigest(strings.Repeat("ab", 32)),
				Steps: []*v1.Node{artifactStep("use", `true`,
					map[string]*v1.Value{"src": v1.NewExpr("vars.ref")}, nil)},
			},
			ExpectedKind:  v1.ErrorKindInvalidInput,
			ExpectedError: []string{`workspace "src"`, "not in this worker's store"},
		},
		{
			// A workspace entry that is not an artifact reference is refused
			// before any directory is made.
			Name: "a workspace entry that is not an artifact reference fails the step",
			Workflow: &v1.Workflow{
				Name: "artifacts-malformed-ref", Profile: v1.CurrentProfile,
				Vars: map[string]*v1.Value{"ref": v1.NewLiteral("not-a-ref")},
				Steps: []*v1.Node{artifactStep("use", `true`,
					map[string]*v1.Value{"src": v1.NewExpr("vars.ref")}, nil)},
			},
			ExpectedKind:  v1.ErrorKindInvalidInput,
			ExpectedError: []string{`workspace "src"`},
		},
	}
}
