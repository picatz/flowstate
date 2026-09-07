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

// Sleep is one `time.Sleep` call in a test file that spends real time.
type Sleep struct {
	// File is the absolute path of the file the call is in.
	File string

	// Line is the line of the call.
	Line int
}

// Analyze walks every `_test.go` file under root and returns the sleeps that
// are not inside a synctest bubble, sorted by position, with the number of test
// files it read.
//
// It parses rather than builds, for the reason tools/vacuity does: a plugin
// module's tests are outside this module's build graph, and a syntax tree
// needs no build. The cost is that only a sleep *lexically* inside the function
// literal handed to [synctest.Test] is known to be bubbled; one in a helper the
// bubble calls is counted, and belongs in the table with that said beside it.
func Analyze(root string) ([]Sleep, int, error) {
	fset := token.NewFileSet()

	var sleeps []Sleep
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
		for _, pos := range sleepsIn(file) {
			sleeps = append(sleeps, Sleep{File: absolute, Line: fset.Position(pos).Line})
		}

		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	slices.SortFunc(sleeps, func(a, b Sleep) int {
		if c := strings.Compare(a.File, b.File); c != 0 {
			return c
		}
		return a.Line - b.Line
	})

	return sleeps, files, nil
}

// sleepsIn returns the position of every `time.Sleep` call in the file that is
// not lexically inside a function literal passed to synctest.Test or
// synctest.Run.
func sleepsIn(file *ast.File) []token.Pos {
	timeName := localName(file, "time")
	if timeName == "" {
		return nil
	}
	synctestName := localName(file, "testing/synctest")

	// The spans of every function literal handed to synctest, so a sleep inside
	// one is recognised by position rather than by walking with a stack.
	var bubbles [][2]token.Pos
	if synctestName != "" {
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
	}

	var out []token.Pos
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !isSelector(call.Fun, timeName, "Sleep") {
			return true
		}
		for _, bubble := range bubbles {
			if call.Pos() >= bubble[0] && call.Pos() < bubble[1] {
				return true
			}
		}
		out = append(out, call.Pos())
		return true
	})

	return out
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
