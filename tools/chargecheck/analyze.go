package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"path/filepath"
	"slices"
	"strings"
)

// exemptMarker is the comment that declares a CEL evaluation exempt from the
// workflow-slice budget. It sits on the call's line or on the line above, and
// the text after it is the claim: why this evaluation is paced by something
// other than the budget ("an activity follows immediately"). The claim lives at
// the site and not in this tool so the checker is never a second place the
// answer is kept (invariant 2).
const exemptMarker = "charge:exempt"

// Package paths, relative to the repository root, that make up the
// workflow-side surface: the engine, and the v1 package whose functions it
// calls. Code in other packages is outside this analysis, which is a stated
// limit rather than a claim they are charged.
const (
	enginePkg = "pkg/flowstate/v1/engine"
	corePkg   = "pkg/flowstate/v1"
)

// A Site is one uncharged CEL evaluation call reachable from the root.
type Site struct {
	// File is the repository-relative path of the file with the call.
	File string

	// Line is the line of the call.
	Line int

	// Callee is the evaluator method called, such as EvalParsedBase.
	Callee string

	// Exempt is the reason the site declared, or "" if it declared none.
	Exempt string

	// Path is the call chain from the root to the function holding the call.
	Path []string
}

// fn is one declared function or method.
type fn struct {
	id       string // "pkg.Name" or "pkg.Recv.Name"
	pkg      string // "v1" or "engine"
	name     string
	recv     string
	callees  map[string]bool // ids of functions it may call
	uncharge []siteCall      // uncharged evaluator calls in its body
}

type siteCall struct {
	file, callee string
	line         int
	exempt       string
}

// graph is the name-based approximation of the call graph over both packages.
type graph struct {
	fns      map[string]*fn
	byName   map[string][]*fn // method name -> every method with it, any receiver
	funcs    map[string]*fn   // "pkg.Name" -> package-level function
	uncharge map[string]bool  // Evaluator methods that evaluate and return no cost
}

// Analyze parses the workflow-side packages under root and returns every
// uncharged evaluation reachable from the engine function named rootFn, with
// the call path that reaches it. It is name-based: a method call through a
// receiver of unknown type reaches every method of that name in either package,
// and a function used as a value counts as called. Both over-approximate, which
// is the safe direction for a check whose failure is a missed evaluation.
func Analyze(root, rootFn string) ([]Site, error) {
	g := &graph{
		fns:      map[string]*fn{},
		byName:   map[string][]*fn{},
		funcs:    map[string]*fn{},
		uncharge: map[string]bool{},
	}

	fset := token.NewFileSet()
	type parsed struct {
		pkg, rel string
		file     *ast.File
	}
	var files []parsed
	for _, p := range []struct{ dir, pkg string }{{corePkg, "v1"}, {enginePkg, "engine"}} {
		dir := filepath.Join(root, p.dir)
		matches, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			return nil, err
		}
		slices.Sort(matches)
		for _, m := range matches {
			if strings.HasSuffix(m, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(fset, m, nil, parser.ParseComments)
			if err != nil {
				return nil, fmt.Errorf("parse %s: %w", m, err)
			}
			rel, _ := filepath.Rel(root, m)
			files = append(files, parsed{p.pkg, filepath.ToSlash(rel), f})
		}
	}

	// Pass 1: declarations, and which Evaluator methods evaluate without
	// returning a cost. The set is derived from the signatures, so a new
	// uncharged entry point is a sink without anyone listing it here.
	for _, pf := range files {
		for _, d := range pf.file.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil && fd.Recv == nil {
				continue
			}
			f := &fn{pkg: pf.pkg, name: fd.Name.Name, callees: map[string]bool{}}
			if fd.Recv != nil && len(fd.Recv.List) > 0 {
				f.recv = typeName(fd.Recv.List[0].Type)
			}
			f.id = pf.pkg + "." + f.name
			if f.recv != "" {
				f.id = pf.pkg + "." + f.recv + "." + f.name
				g.byName[f.name] = append(g.byName[f.name], f)
			} else {
				g.funcs[f.id] = f
			}
			g.fns[f.id] = f
			if pf.pkg == "v1" && f.recv == "Evaluator" && fd.Name.IsExported() &&
				strings.HasPrefix(f.name, "Eval") && !returnsUint64(fd) {
				g.uncharge[f.name] = true
			}
		}
	}

	// Pass 2: edges and sink calls.
	for _, pf := range files {
		comments := commentLines(fset, pf.file)
		for _, d := range pf.file.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			id := pf.pkg + "." + fd.Name.Name
			if fd.Recv != nil && len(fd.Recv.List) > 0 {
				id = pf.pkg + "." + typeName(fd.Recv.List[0].Type) + "." + fd.Name.Name
			}
			f := g.fns[id]
			inEvaluator := f.recv == "Evaluator"
			locals := map[string]bool{}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.AssignStmt:
					for i, r := range x.Rhs {
						if id, ok := lhsIdent(x.Lhs, i); ok && isEvaluator(r, locals) {
							locals[id] = true
						}
					}
				case *ast.ValueSpec:
					for i, r := range x.Values {
						if i < len(x.Names) && isEvaluator(r, locals) {
							locals[x.Names[i].Name] = true
						}
					}
				case *ast.Ident:
					if t, ok := g.funcs[pf.pkg+"."+x.Name]; ok {
						f.callees[t.id] = true
					}
				case *ast.SelectorExpr:
					name := x.Sel.Name
					if q, ok := x.X.(*ast.Ident); ok && q.Name == "v1" && pf.pkg == "engine" {
						if t, ok := g.funcs["v1."+name]; ok {
							f.callees[t.id] = true
						}
						return true
					}
					for _, t := range g.byName[name] {
						f.callees[t.id] = true
					}
					if g.uncharge[name] && !inEvaluator && (!g.ambiguous(name) || isEvaluator(x.X, locals)) {
						line := fset.Position(x.Sel.Pos()).Line
						f.uncharge = append(f.uncharge, siteCall{
							file: pf.rel, callee: name, line: line,
							exempt: exemption(comments, line),
						})
					}
				}
				return true
			})
		}
	}

	start := g.funcs["engine."+rootFn]
	if start == nil {
		return nil, fmt.Errorf("no function %s in %s", rootFn, enginePkg)
	}

	// Breadth-first from the root, remembering each function's parent so the
	// report names the chain.
	parent := map[string]string{start.id: ""}
	queue := []string{start.id}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		for _, c := range slices.Sorted(maps.Keys(g.fns[id].callees)) {
			if _, seen := parent[c]; !seen {
				parent[c] = id
				queue = append(queue, c)
			}
		}
	}

	var sites []Site
	for _, id := range slices.Sorted(maps.Keys(parent)) {
		for _, s := range g.fns[id].uncharge {
			var path []string
			for at := id; at != ""; at = parent[at] {
				path = append(path, at)
			}
			slices.Reverse(path)
			sites = append(sites, Site{File: s.file, Line: s.line, Callee: s.callee, Exempt: s.exempt, Path: path})
		}
	}
	slices.SortFunc(sites, func(a, b Site) int {
		if c := strings.Compare(a.File, b.File); c != 0 {
			return c
		}
		return a.Line - b.Line
	})
	return sites, nil
}

// ambiguous reports whether a method of that name exists on a type other than
// the Evaluator, so the name alone does not say a call evaluates CEL (a plugin
// task's Eval, an interpreter node's Eval).
func (g *graph) ambiguous(name string) bool {
	return slices.ContainsFunc(g.byName[name], func(f *fn) bool { return f.recv != "Evaluator" })
}

// isEvaluator reports whether e is, syntactically, an *Evaluator: a call to
// DefaultEvaluator or to a method named evaluator, a local assigned from one, or
// a field named Eval (the injectable evaluator on the activation types).
func isEvaluator(e ast.Expr, locals map[string]bool) bool {
	switch t := e.(type) {
	case *ast.ParenExpr:
		return isEvaluator(t.X, locals)
	case *ast.Ident:
		return locals[t.Name]
	case *ast.SelectorExpr:
		return t.Sel.Name == "Eval"
	case *ast.CallExpr:
		switch f := t.Fun.(type) {
		case *ast.Ident:
			return f.Name == "DefaultEvaluator"
		case *ast.SelectorExpr:
			return f.Sel.Name == "DefaultEvaluator" || f.Sel.Name == "evaluator"
		}
	}
	return false
}

func lhsIdent(lhs []ast.Expr, i int) (string, bool) {
	if i >= len(lhs) {
		return "", false
	}
	id, ok := lhs[i].(*ast.Ident)
	if !ok {
		return "", false
	}
	return id.Name, true
}

func typeName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return typeName(t.X)
	case *ast.IndexExpr:
		return typeName(t.X)
	case *ast.Ident:
		return t.Name
	}
	return ""
}

func returnsUint64(fd *ast.FuncDecl) bool {
	if fd.Type.Results == nil {
		return false
	}
	for _, r := range fd.Type.Results.List {
		if id, ok := r.Type.(*ast.Ident); ok && id.Name == "uint64" {
			return true
		}
	}
	return false
}

// commentLines maps a line to the text of the comment ending on it.
func commentLines(fset *token.FileSet, f *ast.File) map[int]string {
	out := map[int]string{}
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			out[fset.Position(c.End()).Line] += " " + c.Text
		}
	}
	return out
}

// exemption returns the reason declared for a call on line: the marker comment
// on the line itself or in the contiguous comment block directly above it, with
// the block's remaining lines through the call joined on.
func exemption(comments map[int]string, line int) string {
	for l := line; l >= line-8; l-- {
		text, ok := comments[l]
		if !ok {
			if l == line {
				continue
			}
			return ""
		}
		if _, after, found := strings.Cut(text, exemptMarker); found {
			parts := []string{after}
			for k := l + 1; k <= line; k++ {
				parts = append(parts, comments[k])
			}
			reason := strings.ReplaceAll(strings.Join(parts, " "), "//", " ")
			return strings.Join(strings.Fields(strings.TrimPrefix(strings.TrimSpace(reason), ":")), " ")
		}
	}
	return ""
}
