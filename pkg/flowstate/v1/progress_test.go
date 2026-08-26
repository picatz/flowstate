package flowstatev1_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestEveryPhaseReportedIsOneOfTheDeclaredOnes is invariant 7 enforced across the
// tree rather than at one call.
//
// A phase's name is written into an activity heartbeat, and heartbeat details go
// into workflow history — durable, broadly readable, and the one place a secret
// must never reach. [v1.Phase] closes that door by construction: unexported field,
// no constructor, so `ReportProgress(ctx, "requesting "+url)` does not compile.
//
// This is the other half, and it is the half a type cannot do. Nothing stops a
// future edit from adding a constructor, or from building a composite literal
// inside the v1 package where the field *is* reachable — and either would be a
// small, plausible-looking diff that quietly opens a channel from a task's resolved
// inputs into history. So the tree is walked and every argument to ReportProgress
// is required to be a plain reference to one of the declared phases: an identifier,
// nothing computed, nothing constructed.
//
// An AST walk rather than a grep, for the reason the env-var documentation pin is
// one: a grep answers "does this text appear", and the question here is "is this
// expression a bare identifier", which is a property of the syntax and not of the
// characters.
func TestEveryPhaseReportedIsOneOfTheDeclaredOnes(t *testing.T) {
	t.Parallel()

	declared := map[string]bool{
		"PhaseRequesting":      true,
		"PhaseReadingResponse": true,
		"PhaseCallingPlugin":   true,
	}

	root := repoRoot(t)

	// Which declared phases were seen at a call site, for the reverse check below.
	reported := map[string]bool{}

	var checked int
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			// Neither is Go this repository maintains, and walking either is a
			// large amount of parsing for an answer that cannot be about us.
			if entry.Name() == ".git" || entry.Name() == "vendor" {
				return filepath.SkipDir
			}

			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, ".pb.go") {
			return nil
		}

		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}

		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || !isReportProgress(call.Fun) || len(call.Args) != 2 {
				return true
			}

			checked++

			// `v1.PhaseRequesting` from outside the package, `PhaseRequesting` from
			// inside it. Anything else — a variable, a field, a composite literal,
			// a function call — is the shape this test exists to refuse.
			name, ok := phaseIdent(call.Args[1])
			require.True(t, ok,
				"%s reports a phase built from an expression rather than naming a "+
					"declared one; heartbeat details are written into workflow history, "+
					"so anything derived from a task's inputs can reach it", path)
			require.True(t, declared[name],
				"%s reports %q, which is not one of the declared phases in progress.go",
				path, name)
			reported[name] = true

			return true
		})

		return nil
	}))

	// The walk finding nothing would pass every assertion above, which is the
	// failure mode of every test that checks a corpus: this pins that the corpus
	// was actually found.
	require.GreaterOrEqual(t, checked, 2,
		"no ReportProgress call was found in the tree, so this asserted nothing")

	// And the other direction, which is the one that went wrong: every declared
	// phase must be reported from somewhere. A phase nobody sets is a heartbeat
	// carrying the empty string — cancellation still gets delivered, so nothing
	// looks broken, and the diagnosis the phase was added for silently is not
	// there. `PhaseCallingPlugin` shipped in exactly that state and was caught in
	// review rather than by a test, which is what this is.
	for name := range declared {
		require.True(t, reported[name],
			"phase %s is declared in progress.go and never reported anywhere; a phase "+
				"nothing sets is a heartbeat carrying nothing, which looks identical to "+
				"one that works", name)
	}
}

// isReportProgress reports whether a call expression names ReportProgress, in
// either the qualified or the unqualified spelling.
func isReportProgress(fun ast.Expr) bool {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name == "ReportProgress"
	case *ast.SelectorExpr:
		return f.Sel.Name == "ReportProgress"
	default:
		return false
	}
}

// phaseIdent returns the name a phase argument refers to, and whether it is a bare
// reference at all.
func phaseIdent(arg ast.Expr) (string, bool) {
	switch a := arg.(type) {
	case *ast.Ident:
		return a.Name, true
	case *ast.SelectorExpr:
		// Only a package-qualified name, never a field of something: `v1.Phase`
		// qualifies, `task.phase` does not.
		if pkg, ok := a.X.(*ast.Ident); ok && pkg.Obj == nil {
			return a.Sel.Name, true
		}
	}

	return "", false
}

// repoRoot walks up from this package to the directory holding go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	require.NoError(t, err)

	for range 10 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, parent, dir, "walked to the filesystem root without finding go.mod")
		dir = parent
	}

	t.Fatal("go.mod not found within ten directories of the test")

	return ""
}

// TestAPhaseSaysOnlyItsOwnName pins what a driver can put into history.
//
// The containment shapes CLAUDE.md asks for, applied to the value that travels
// *toward* history rather than the ones that must never reach it. A `%+v` or `%#v`
// on a phase — through a struct, through a slice, however a future log line
// happens to be written — must still be a short constant name and never anything
// else, because the whole argument for letting this into history is that there is
// nothing else in it.
func TestAPhaseSaysOnlyItsOwnName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "requesting", v1.PhaseRequesting.String())
	require.Equal(t, "reading the response", v1.PhaseReadingResponse.String())
	require.Equal(t, "calling the plugin", v1.PhaseCallingPlugin.String())

	// The zero value is what a heartbeat carries before a task has reported
	// anything, and it has to be harmless rather than surprising: a step that is
	// running and has not said what it is doing.
	require.Empty(t, v1.Phase{}.String())
}

// TestReportingAPhaseWithNobodyListeningIsFine is the direction every task depends
// on and no driver test would notice.
//
// A task reports its phase unconditionally — it has no business knowing which
// driver is running it — so the local driver, every unit test, and any future
// caller that installs nothing must all be able to run one. A panic or an error
// here would make progress reporting something a task had to guard, which is how
// it would stop being written.
func TestReportingAPhaseWithNobodyListeningIsFine(t *testing.T) {
	t.Parallel()

	// Asserted rather than merely executed. Written as two bare calls, this
	// test made its claim only by not crashing — which is a real claim and an
	// invisible one: it reads identically to a test somebody forgot to finish,
	// and `tools/vacuity` cannot tell them apart either. Saying "does not
	// panic" out loud costs one line and makes the second-hardest thing about
	// the test — what it is for — the first thing on the page.
	require.NotPanics(t, func() {
		v1.ReportProgress(t.Context(), v1.PhaseRequesting)
	}, "a task reporting a phase with no reporter installed is every unit test in the tree")

	require.NotPanics(t, func() {
		v1.ReportProgress(v1.ContextWithProgress(t.Context(), nil), v1.PhaseRequesting)
	}, "a nil reporter is what a caller installs to turn reporting off")
}

// TestAnInstalledReporterHearsEveryPhase is the positive direction.
func TestAnInstalledReporterHearsEveryPhase(t *testing.T) {
	t.Parallel()

	var heard []string
	ctx := v1.ContextWithProgress(t.Context(), func(phase v1.Phase) {
		heard = append(heard, phase.String())
	})

	v1.ReportProgress(ctx, v1.PhaseRequesting)
	v1.ReportProgress(ctx, v1.PhaseReadingResponse)

	require.Equal(t, []string{"requesting", "reading the response"}, heard)
}
