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

// pollNames are the testify assertions that wait by asking repeatedly.
//
// Never belongs with Eventually: it spends its whole timeout every time, which
// makes it the most expensive wait in the list and the one a bubble helps most.
// The formatted `…f` spellings belong because they are the same call with a
// message — omitting them left a supported API the ratchet could not see, which
// is the one defect a ratchet cannot survive (#1989).
//
// This is the whole family as of the pinned testify, read off
// assert/*.go and require/*.go rather than recalled: there is no NeverWithT,
// despite the symmetry with EventuallyWithT suggesting one. A testify bump is
// the moment to re-read it, since a name added there is a hole here on the day
// it lands.
var pollNames = []string{
	"Eventually", "Eventuallyf",
	"EventuallyWithT", "EventuallyWithTf",
	"Never", "Neverf",
}

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
//
// A poll is matched by the method name alone, in any file that imports testify,
// and deliberately not by what it is called on. Three rounds of review on #1989
// found three receiver shapes an identifier-based matcher missed — the object
// API bound to a local (`r := require.New(t); r.Eventually(…)`), the same call
// chained (`require.New(t).Eventually(…)`), and the package's own name once a
// local shadowed it — and each fix invited the next: a struct field holding an
// object, a map entry, a method value, a helper's return used directly.
//
// The shapes are not the problem; enumerating them is. A missed poll is the one
// defect a ratchet cannot survive, because the count stays green while the tree
// gets worse, so the matcher takes the side that cannot miss: anything named
// Eventually, Never or one of their variants, called in a file that imports
// testify, is a poll. That over-counts a same-named method on some other type,
// which is the direction every other approximation here already errs in and
// which the table records like any other entry. It needs no type information,
// so the plugin modules' tests stay in scope, and no receiver case is left to
// forget.
func waitsIn(file *ast.File) []found {
	timeName := localName(file, "time")
	requireName := localName(file, "github.com/stretchr/testify/require")
	assertName := localName(file, "github.com/stretchr/testify/assert")
	if timeName == "" && requireName == "" && assertName == "" {
		return nil
	}

	testifyImported := requireName != "" || assertName != ""
	dotImported := requireName == "." || assertName == "."

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
		case testifyImported && isPollCall(call.Fun, dotImported):
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

// isPollCall reports whether expr calls one of [pollNames] on any receiver at
// all — `require.Eventually`, `r.Eventually`, `require.New(t).Eventually`, or a
// bare `Eventually` where testify is dot-imported.
func isPollCall(expr ast.Expr, dotImported bool) bool {
	switch fun := expr.(type) {
	case *ast.SelectorExpr:
		return slices.Contains(pollNames, fun.Sel.Name)
	case *ast.Ident:
		return dotImported && slices.Contains(pollNames, fun.Name)
	default:
		return false
	}
}

// bubblesIn returns the span of every function literal handed to synctest, so a
// wait inside one is recognised by position rather than by walking with a stack.
//
// Shadowing cuts the other way here than it does for a wait, which is why this
// checks for it and the wait matching does not. A local named `require` makes
// this analysis count a call that is not a poll: over-counting, harmless. A
// local named `synctest` with a `Test` method taking a function literal would
// make it treat that literal as a bubble and *stop* counting the real waits
// inside it — under-counting, which is the direction a ratchet cannot afford
// (#1989). So if the file declares the import's own name anywhere, no call in
// it is taken for a bubble and every wait is counted. Blunt, file-scoped, and
// wrong only in the safe direction.
func bubblesIn(file *ast.File) [][2]token.Pos {
	synctestName := localName(file, "testing/synctest")
	if synctestName == "" || shadowsABubbleCall(file, synctestName) {
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

// shadowsABubbleCall reports whether the file declares a name that could make a
// call [bubblesIn] would read as a bubble mean something else.
//
// Which name that is depends on how synctest was imported, and getting it wrong
// under-counts, so the two cases are separate. Under a named or aliased import
// the call is `synctest.Test(…)`, so the name at risk is the import's own. Under
// a dot-import there is no such name — the call is a bare `Test(…)` or `Run(…)`
// — so those are the names a local can displace, and a file declaring either
// gets no bubbles.
//
// The dot-import half was missed on the first pass at this and reported again
// on #1989: the earlier code returned false for a dot-import on the reasoning
// that "a dot-import has no name to shadow". True of the import, and beside the
// point — what matters is the name at the *call*, and for a dot-import that is
// the bare function.
func shadowsABubbleCall(file *ast.File, synctestName string) bool {
	if synctestName != "." {
		return declaresName(file, synctestName)
	}

	return declaresName(file, "Test") || declaresName(file, "Run")
}

// declaresName reports whether the file declares name as anything other than an
// import: a function, a parameter or result, a receiver, a type, or a variable
// or constant, including one defined by `:=`.
func declaresName(file *ast.File, name string) bool {
	declared := false
	noteField := func(fields *ast.FieldList) {
		if fields == nil {
			return
		}
		for _, field := range fields.List {
			for _, ident := range field.Names {
				declared = declared || ident.Name == name
			}
		}
	}

	ast.Inspect(file, func(n ast.Node) bool {
		switch decl := n.(type) {
		case *ast.FuncDecl:
			declared = declared || decl.Name.Name == name
			noteField(decl.Recv)
		case *ast.FuncType:
			noteField(decl.Params)
			noteField(decl.Results)
		case *ast.ValueSpec:
			for _, ident := range decl.Names {
				declared = declared || ident.Name == name
			}
		case *ast.TypeSpec:
			declared = declared || decl.Name.Name == name
		case *ast.AssignStmt:
			if decl.Tok != token.DEFINE {
				return true
			}
			for _, lhs := range decl.Lhs {
				if ident, ok := lhs.(*ast.Ident); ok {
					declared = declared || ident.Name == name
				}
			}
		}

		return true
	})

	return declared
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
