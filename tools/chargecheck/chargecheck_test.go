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

// TestTheRepositoryExemptionsAreTheAudited keeps the exemption list from
// growing unnoticed: each entry is a claim about pacing a reviewer accepted, so
// adding one changes this table in the same diff. Removing one fails it too, so
// the table cannot keep a stale entry.
func TestTheRepositoryExemptionsAreTheAudited(t *testing.T) {
	t.Parallel()

	sites, err := Analyze(repoRoot(t), "Run")
	require.NoError(t, err)

	var got []string
	for _, s := range sites {
		require.NotEmpty(t, s.Exempt, "%s:%d", s.File, s.Line)
		got = append(got, s.File)
	}
	assert.Equal(t, []string{
		"pkg/flowstate/v1/eval.go",               // stored output expressions, bounded by their own counter
		"pkg/flowstate/v1/eval_task_http.go",     // http task body, runs in the task's activity
		"pkg/flowstate/v1/eval_task_http_run.go", // http task body: outputs block
		"pkg/flowstate/v1/eval_task_http_run.go", // http task body: per-output expressions
		"pkg/flowstate/v1/nodes.go",              // ResolveTaskInputs, paced by the activity that follows
		"pkg/flowstate/v1/protoliterals.go",      // task input population, runs in the task's activity
		"pkg/flowstate/v1/wait.go",               // wait expressions, paced by the park that follows
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

// fixtureFile is fixture with the whole engine file supplied, imports included.
func fixtureFile(t *testing.T, engineFile, v1Src string) string {
	t.Helper()
	root := fixture(t, "", v1Src)
	require.NoError(t, os.WriteFile(filepath.Join(root, enginePkg, "run.go"), []byte(engineFile), 0o600))
	return root
}

// unlabelled analyses a fixture and returns what it would fail on.
func unlabelled(t *testing.T, root string) []Site {
	t.Helper()
	sites, err := Analyze(root, "Run")
	require.NoError(t, err)
	return Unlabelled(sites)
}

func TestAnEvaluatorParameterOrFieldIsSeenForTheAmbiguousEvalName(t *testing.T) {
	t.Parallel()

	byParam := fixture(t, "func Run() { v1.Helper(nil) }\n",
		"func Helper(ev *Evaluator) { ev.Eval() }\n")
	assert.Len(t, unlabelled(t, byParam), 1, "a parameter typed *Evaluator")

	byField := fixture(t, "func Run() { v1.Helper() }\n",
		"type holder struct{ ev *Evaluator }\n\nfunc Helper() { h := holder{}; h.ev.Eval() }\n")
	assert.Len(t, unlabelled(t, byField), 1, "a struct field typed *Evaluator")
}

func TestAWrapperMethodOnTheEvaluatorIsASink(t *testing.T) {
	t.Parallel()

	root := fixture(t, "func Run() { v1.Helper() }\n",
		"func (e *Evaluator) Wrapper() { e.EvalParsedBase() }\n\nfunc Helper() { DefaultEvaluator().Wrapper() }\n")
	got := unlabelled(t, root)
	require.Len(t, got, 1)
	assert.Equal(t, "EvalParsedBase", got[0].Callee)
}

func TestAPackageLevelFuncValueInitialiserIsScanned(t *testing.T) {
	t.Parallel()

	root := fixture(t, "func Run() { v1.Helper() }\n",
		"var hook = func() { DefaultEvaluator().EvalParsedBase() }\n\nfunc Helper() { hook() }\n")
	assert.Len(t, unlabelled(t, root), 1)
}

func TestAnAliasedV1ImportStillResolvesQualifiedCalls(t *testing.T) {
	t.Parallel()

	root := fixtureFile(t, "package engine\n\nimport core \"example.com/v1\"\n\nfunc Run() { core.Helper() }\n",
		"func Helper() { DefaultEvaluator().EvalParsedBase() }\n")
	assert.Len(t, unlabelled(t, root), 1)

	dot := fixtureFile(t, "package engine\n\nimport . \"example.com/v1\"\n\nfunc Run() { Helper() }\n",
		"func Helper() { DefaultEvaluator().EvalParsedBase() }\n")
	_, err := Analyze(dot, "Run")
	require.Error(t, err, "a dot import cannot be followed and must not pass silently")
}

func TestOnlyAMarkerThatBeginsACommentDeclaresAnExemption(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"a trailing comment on the previous statement": "func Helper() {\n\tx := 1 // charge:exempt not for the next line\n\tDefaultEvaluator().EvalParsedBase()\n\t_ = x\n}\n",
		"prose that mentions the marker":               "func Helper() {\n\t// the charge:exempt marker is described elsewhere\n\tDefaultEvaluator().EvalParsedBase()\n}\n",
		"a marker with no reason":                      "func Helper() {\n\t// charge:exempt\n\tDefaultEvaluator().EvalParsedBase()\n}\n",
	} {
		root := fixture(t, "func Run() { v1.Helper() }\n", body)
		assert.Len(t, unlabelled(t, root), 1, name)
	}

	trailing := fixture(t, "func Run() { v1.Helper() }\n",
		"func Helper() {\n\tDefaultEvaluator().EvalParsedBase() // charge:exempt paced by a park\n}\n")
	assert.Empty(t, unlabelled(t, trailing), "a marker trailing the call itself is the site's own")
}

func TestAGenericMethodWithTwoTypeParametersDoesNotPanic(t *testing.T) {
	t.Parallel()

	root := fixture(t, "func Run() { v1.Helper() }\n",
		"type pair[A, B any] struct{}\n\nfunc (pair[A, B]) Method() { DefaultEvaluator().EvalParsedBase() }\n\nfunc Helper() { pair[int, int]{}.Method() }\n")
	assert.Len(t, unlabelled(t, root), 1)
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
