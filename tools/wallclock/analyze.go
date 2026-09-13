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
//
// The other cost is that a wait reached through a value rather than named at
// the call escapes both counts: `sleep := time.Sleep; sleep(d)`, and the same
// for a poll. Recorded here rather than left to be met in a green ratchet.
// Nothing in the tree does it and it is hard to do by accident, but it is the
// one shape the widened poll match above still cannot see, since there is no
// name at the call site to match.
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
// A poll is matched by the method name alone: not by what it is called on, and
// not by whether this file imports testify. Six review rounds on #1989 found
// six ways a narrower rule missed one — the object API bound to a local, the
// same chained onto New, a multi-value binding, the package name once a local
// shadowed it, the formatted spellings, and a helper in a *sibling file*
// returning `*require.Assertions` so the caller imports nothing at all.
//
// Each fix invited the next, because a poll's identity is a type-level fact and
// this is a parse. So the matcher stops approximating it. Anything named
// Eventually, Never or one of their variants, called anywhere in a test file, is
// a poll. There is no receiver to classify, no import to check, and no
// cross-file knowledge to acquire — which is what makes the whole class of miss
// unreachable rather than merely reduced.
//
// The cost is over-counting a same-named method on some unrelated type, which
// is the direction every approximation here already errs in and which costs a
// table entry with a note. Measured against this tree it costs nothing at all:
// all 49 test files calling such a method import testify, so the gate this
// dropped was never excluding anything.
//
// `time.Sleep` keeps its import check because it is a package function with no
// object form, so none of the above can reach it.
func waitsIn(file *ast.File) []found {
	timeName := localName(file, "time")

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
		case isPollCall(call.Fun):
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

// isPollCall reports whether expr calls one of [pollNames], however it is
// spelled: `require.Eventually`, `r.Eventually`, `require.New(t).Eventually`,
// `helper(t).Eventually`, a bare `Eventually`, or any of those in parentheses.
func isPollCall(expr ast.Expr) bool {
	switch fun := unparen(expr).(type) {
	case *ast.SelectorExpr:
		return slices.Contains(pollNames, fun.Sel.Name)
	case *ast.Ident:
		return slices.Contains(pollNames, fun.Name)
	default:
		return false
	}
}

// unparen strips the parentheses around an expression, however many there are.
//
// [ast.ParenExpr] is the one wrapper Go's grammar lets a caller put around a
// callee or a receiver type without changing what it means, and gofmt keeps it
// rather than tidying it away. Every place here that matches on an expression's
// shape strips it first, so a shape is matched by what it is rather than by how
// it was punctuated (#1989).
func unparen(expr ast.Expr) ast.Expr {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			return expr
		}
		expr = paren.X
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
// import.
//
// The enumeration below has to be complete, and that is the opposite of how the
// poll matcher handles the same problem. There, missing a shape over-counts, so
// the matcher takes the widest reading and no case can hurt. Here a missed shape
// means the shadow goes unnoticed, the call is read as a bubble, and the waits
// inside it stop being counted — the direction a ratchet cannot afford. So this
// names every form in Go that binds a value to an identifier:
//
//   - [ast.FuncDecl] — a function's own name, and its receiver.
//   - [ast.FuncType] — parameters and results, closures included.
//   - [ast.ValueSpec] — `var` and `const`, at any scope.
//   - [ast.TypeSpec] — a type name, and a generic type's own type parameters.
//   - [ast.FuncType].TypeParams — a generic function's type parameters, which
//     are binders like any other and were missed until #1989 asked.
//   - [ast.FuncDecl].Recv's *type* — a method on a generic type rebinds its
//     type parameters there rather than in a name list, so
//     `func (b Box[synctest]) …` declares the name even where the type wrote
//     `Box[T any]`. The binder is inside an IndexExpr, which is why noting the
//     receiver's field names alone does not reach it.
//   - [ast.AssignStmt] with `:=`, which is also how the tree spells a type
//     switch guard (`x := y.(type)`) and a `select` receive clause.
//   - [ast.RangeStmt] with `:=` — its key and value. Missed on the first pass
//     at this and reported again on #1989, because a range clause is its own
//     node rather than an assignment inside one.
//
// A label is deliberately absent: [ast.LabeledStmt] binds a name that cannot be
// called or selected from, so it can never produce the `synctest.Test(…)` this
// exists to distrust.
func declaresName(file *ast.File, name string) bool {
	declared := false
	noteIdents := func(exprs ...ast.Expr) {
		for _, expr := range exprs {
			if ident, ok := expr.(*ast.Ident); ok {
				declared = declared || ident.Name == name
			}
		}
	}
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
	// A method on a generic type rebinds its type parameters in the receiver's
	// *type*, not in a name list: `func (b Box[synctest]) …` declares synctest
	// even though the type wrote `Box[T ...]`. The binders sit inside an
	// IndexExpr (one) or IndexListExpr (several), which a pointer receiver and
	// any number of parentheses may wrap in either order — all legal spellings
	// gofmt leaves alone, so the wrappers are peeled rather than pattern-matched.
	noteReceiverTypeParams := func(recv *ast.FieldList) {
		if recv == nil {
			return
		}
		for _, field := range recv.List {
			typ := field.Type
			for {
				typ = unparen(typ)
				star, ok := typ.(*ast.StarExpr)
				if !ok {
					break
				}
				typ = star.X
			}

			switch indexed := typ.(type) {
			case *ast.IndexExpr:
				noteIdents(indexed.Index)
			case *ast.IndexListExpr:
				noteIdents(indexed.Indices...)
			}
		}
	}

	ast.Inspect(file, func(n ast.Node) bool {
		switch decl := n.(type) {
		case *ast.FuncDecl:
			declared = declared || decl.Name.Name == name
			noteField(decl.Recv)
			noteReceiverTypeParams(decl.Recv)
		case *ast.FuncType:
			noteField(decl.TypeParams)
			noteField(decl.Params)
			noteField(decl.Results)
		case *ast.TypeSpec:
			declared = declared || decl.Name.Name == name
			noteField(decl.TypeParams)
		case *ast.ValueSpec:
			for _, ident := range decl.Names {
				declared = declared || ident.Name == name
			}
		case *ast.AssignStmt:
			if decl.Tok != token.DEFINE {
				return true
			}
			noteIdents(decl.Lhs...)
		case *ast.RangeStmt:
			if decl.Tok != token.DEFINE {
				return true
			}
			noteIdents(decl.Key, decl.Value)
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
