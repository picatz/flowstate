package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// The analysed packages are read as source, so nothing here needs them
	// compiled. The import is what makes the diff-scoped gate select this test:
	// it expands a changed package to the packages that import it, and a diff
	// that only touches the engine or v1 would otherwise never reach a tool
	// that reads them from disk.
	_ "github.com/picatz/flowstate/pkg/flowstate/v1/engine"
)

// repoRoot is the repository root, two directories above this package.
func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	return root
}

// TestEveryWorkflowSideEvaluationIsChargedOrExempt is the claim #1970 turned
// into a build failure: every uncharged CEL evaluation reachable from engine.Run
// carries a `charge:exempt` reason at the call. A new uncharged call in the
// engine, or in anything it calls, fails here and names the site and the path.
func TestEveryWorkflowSideEvaluationIsChargedOrExempt(t *testing.T) {
	t.Parallel()

	sites, err := Analyze(repoRoot(t), "Run")
	require.NoError(t, err)

	for _, s := range Unlabelled(sites) {
		t.Errorf("%s:%d: %s is uncharged and reachable from engine.Run via %v; route it through a WithCost entry point or declare `// %s <reason>`",
			s.File, s.Line, s.Callee, s.Path, exemptMarker)
	}
}

// TestTheRepositoryExemptionsAreTheThreeAudited keeps the exemption list from
// growing unnoticed: each entry is a claim about pacing a reviewer accepted, so
// adding one changes this table in the same diff. Removing one fails it too, so
// the table cannot keep a stale entry.
func TestTheRepositoryExemptionsAreTheThreeAudited(t *testing.T) {
	t.Parallel()

	sites, err := Analyze(repoRoot(t), "Run")
	require.NoError(t, err)

	var got []string
	for _, s := range sites {
		require.NotEmpty(t, s.Exempt, "%s:%d", s.File, s.Line)
		got = append(got, s.File)
	}
	assert.Equal(t, []string{
		"pkg/flowstate/v1/eval.go",  // stored output expressions, bounded by their own counter
		"pkg/flowstate/v1/nodes.go", // ResolveTaskInputs, paced by the activity that follows
		"pkg/flowstate/v1/wait.go",  // wait expressions, paced by the park that follows
	}, got)
}

// fixture writes a two-package tree: an evaluator with one charged and one
// uncharged entry point, a competing type that also has an Eval method, and the
// given extra engine and v1 source.
func fixture(t *testing.T, engineSrc, v1Src string) string {
	t.Helper()
	root := t.TempDir()
	write := func(rel, src string) {
		path := filepath.Join(root, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(src), 0o600))
	}
	write(corePkg+"/ev.go", `package v1

type Evaluator struct{}

func DefaultEvaluator() *Evaluator { return nil }

func (e *Evaluator) Eval() int                        { return 0 }
func (e *Evaluator) EvalParsedBase() int              { return 0 }
func (e *Evaluator) EvalParsedBaseWithCost() (int, uint64) { return 0, 0 }

type task struct{}

func (task) Eval() int { return 0 }
`+v1Src)
	write(enginePkg+"/run.go", `package engine

import v1 "example.com/v1"

`+engineSrc)
	return root
}

func TestAnUnchargedCallReachableFromRunIsReportedWithItsPath(t *testing.T) {
	t.Parallel()

	root := fixture(t,
		"func Run() { v1.Helper() }\n",
		"func Helper() { inner() }\nfunc inner() { DefaultEvaluator().EvalParsedBase() }\n")

	sites, err := Analyze(root, "Run")
	require.NoError(t, err)
	require.Len(t, sites, 1)
	assert.Equal(t, "EvalParsedBase", sites[0].Callee)
	assert.Equal(t, []string{"engine.Run", "v1.Helper", "v1.inner"}, sites[0].Path)
	assert.Empty(t, sites[0].Exempt)
	assert.Len(t, Unlabelled(sites), 1)
}

func TestALocalBoundToTheEvaluatorIsTrackedForTheAmbiguousEvalName(t *testing.T) {
	t.Parallel()

	root := fixture(t,
		"func Run() { v1.Helper() }\n",
		"func Helper() {\n\tev := DefaultEvaluator()\n\tev.Eval()\n}\n")

	sites, err := Analyze(root, "Run")
	require.NoError(t, err)
	require.Len(t, sites, 1)
	assert.Equal(t, "Eval", sites[0].Callee)
}

func TestADeclaredExemptionPassesAndKeepsItsReason(t *testing.T) {
	t.Parallel()

	root := fixture(t,
		"func Run() { v1.Helper() }\n",
		"func Helper() {\n\t// charge:exempt an activity follows\n\t// immediately.\n\tDefaultEvaluator().EvalParsedBase()\n}\n")

	sites, err := Analyze(root, "Run")
	require.NoError(t, err)
	require.Len(t, sites, 1)
	assert.Equal(t, "an activity follows immediately.", sites[0].Exempt)
	assert.Empty(t, Unlabelled(sites))
}

func TestAnExemptionDoesNotCoverTheNextStatementAfterABlankLine(t *testing.T) {
	t.Parallel()

	root := fixture(t,
		"func Run() { v1.Helper() }\n",
		"func Helper() {\n\t// charge:exempt elsewhere\n\n\tDefaultEvaluator().EvalParsedBase()\n}\n")

	sites, err := Analyze(root, "Run")
	require.NoError(t, err)
	assert.Len(t, Unlabelled(sites), 1)
}

func TestChargedUnreachableAndNonEvaluatorCallsAreNotReported(t *testing.T) {
	t.Parallel()

	root := fixture(t,
		"func Run() { v1.Helper() }\n",
		`func Helper() {
	DefaultEvaluator().EvalParsedBaseWithCost()
	var t task
	t.Eval()
}

// activitySide is not reachable from Run: an activity is correctly uncharged.
func activitySide() { DefaultEvaluator().EvalParsedBase() }
`)

	sites, err := Analyze(root, "Run")
	require.NoError(t, err)
	assert.Empty(t, sites)
}
