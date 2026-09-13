package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// Kind is which of the two ways a test spends real time waiting.
type Kind int

const (
	// KindSleep is a `time.Sleep`: a duration the author picked, paid in full
	// on every run.
	KindSleep Kind = iota

	// KindPoll is a call from testify's Eventually family: a duration the
	// author picked as a ceiling, asked about a thousand times a second until
	// it is reached. It usually costs less than a sleep and fails the same
	// way — the ceiling is a guess about how slow a runner may be, and the
	// answer when it is wrong is a red test rather than a slow one.
	//
	// [synctest.Wait] is the replacement, and it is not a faster poll: it
	// returns when every goroutine in the bubble is durably blocked, which is
	// a fact the runtime knows rather than a duration anyone has to choose.
	// The assertion then runs once, and says what was wrong rather than
	// "condition never satisfied".
	KindPoll
)

// String names the kind for a report.
func (k Kind) String() string {
	if k == KindPoll {
		return "poll"
	}

	return "sleep"
}

// A Wait is one call in a test file that spends real time.
type Wait struct {
	// File is the absolute path of the file the call is in.
	File string

	// Line is the line of the call.
	Line int

	// Kind is whether the call sleeps or polls.
	Kind Kind
}

// pollNames are the testify assertions that wait by asking repeatedly. Never
// belongs with Eventually: it spends its whole timeout every time, which makes
// it the most expensive wait in the list and the one a bubble helps most.
var pollNames = []string{"Eventually", "EventuallyWithT", "Never", "NeverWithT"}

// Analyze walks every `_test.go` file under root and returns the waits that are
// not inside a synctest bubble, sorted by position, with the number of test
// files it read.
//
// It parses rather than builds, for the reason tools/vacuity does: a plugin
// module's tests are outside this module's build graph, and a syntax tree
// needs no build. The cost is that only a wait *lexically* inside the function
// literal handed to [synctest.Test] is known to be bubbled; one in a helper the
// bubble calls is counted, and belongs in the table with that said beside it.
func Analyze(root string) ([]Wait, int, error) {
	fset := token.NewFileSet()

	var waits []Wait
	files := 0

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".coverage", "node_modules", "testdata", ".worktrees", ".claude":
				// The same set tools/vacuity skips, for the reasons it gives:
				// testdata is input rather than tests, and the agent
				// directories are copies of this tree holding somebody else's
				// unfinished work.
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		files++

		absolute, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		for _, found := range waitsIn(file) {
			waits = append(waits, Wait{
				File: absolute,
				Line: fset.Position(found.pos).Line,
				Kind: found.kind,
			})
		}

		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	slices.SortFunc(waits, func(a, b Wait) int {
		if c := strings.Compare(a.File, b.File); c != 0 {
			return c
		}
		return a.Line - b.Line
	})

	return waits, files, nil
}

// found is one wait's position and kind, before a file set turns it into a line.
type found struct {
	pos  token.Pos
	kind Kind
}

// waitsIn returns every `time.Sleep` and every testify poll in the file that is
// not lexically inside a function literal passed to synctest.Test or
// synctest.Run.
func waitsIn(file *ast.File) []found {
	timeName := localName(file, "time")
	requireName := localName(file, "github.com/stretchr/testify/require")
	assertName := localName(file, "github.com/stretchr/testify/assert")
	if timeName == "" && requireName == "" && assertName == "" {
		return nil
	}

	bubbles := bubblesIn(file)
	inBubble := func(pos token.Pos) bool {
		for _, bubble := range bubbles {
			if pos >= bubble[0] && pos < bubble[1] {
				return true
			}
		}

		return false
	}

	var out []found
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		kind := KindSleep
		switch {
		case timeName != "" && isSelector(call.Fun, timeName, "Sleep"):
		case requireName != "" && isSelector(call.Fun, requireName, pollNames...),
			assertName != "" && isSelector(call.Fun, assertName, pollNames...):
			kind = KindPoll
		default:
			return true
		}

		if !inBubble(call.Pos()) {
			out = append(out, found{pos: call.Pos(), kind: kind})
		}

		return true
	})

	return out
}

// bubblesIn returns the span of every function literal handed to synctest, so a
// wait inside one is recognised by position rather than by walking with a stack.
func bubblesIn(file *ast.File) [][2]token.Pos {
	synctestName := localName(file, "testing/synctest")
	if synctestName == "" {
		return nil
	}

	var bubbles [][2]token.Pos
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !isSelector(call.Fun, synctestName, "Test", "Run") {
			return true
		}
		for _, arg := range call.Args {
			if lit, ok := arg.(*ast.FuncLit); ok {
				bubbles = append(bubbles, [2]token.Pos{lit.Pos(), lit.End()})
			}
		}
		return true
	})

	return bubbles
}

// localName returns the name a file refers to an import by: the package's own
// name ordinarily, an alias where the file wrote one, "." where the file
// dot-imports it, and "" where the file does not import it.
func localName(file *ast.File, path string) string {
	for _, imp := range file.Imports {
		got, err := strconv.Unquote(imp.Path.Value)
		if err != nil || got != path {
			continue
		}
		if imp.Name == nil {
			return path[strings.LastIndex(path, "/")+1:]
		}
		if imp.Name.Name == "_" {
			return ""
		}
		return imp.Name.Name
	}

	return ""
}

// isSelector reports whether expr names one of the names in the package: as
// `pkg.name`, or as the bare `name` when the package is dot-imported.
func isSelector(expr ast.Expr, pkg string, names ...string) bool {
	if pkg == "." {
		ident, ok := expr.(*ast.Ident)
		return ok && slices.Contains(names, ident.Name)
	}

	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	if !ok || ident.Name != pkg {
		return false
	}

	return slices.Contains(names, sel.Sel.Name)
}
