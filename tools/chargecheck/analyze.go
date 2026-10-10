package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
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
// corePath is the import path of the v1 package. Other paths that end in /v1
// (generated API packages) are not it.
const corePath = "github.com/picatz/flowstate/" + corePkg

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
		src      []byte
		qual     string // the name this file imports the v1 package under
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
			src, err := os.ReadFile(m)
			if err != nil {
				return nil, err
			}
			f, err := parser.ParseFile(fset, m, src, parser.ParseComments)
			if err != nil {
				return nil, fmt.Errorf("parse %s: %w", m, err)
			}
			rel, _ := filepath.Rel(root, m)
			pf := parsed{pkg: p.pkg, rel: filepath.ToSlash(rel), file: f, src: src}
			for _, imp := range f.Imports {
				path, _ := strconv.Unquote(imp.Path.Value)
				if path != corePath {
					continue
				}
				switch {
				case imp.Name == nil:
					pf.qual = "v1"
				case imp.Name.Name == "." || imp.Name.Name == "_":
					return nil, fmt.Errorf("%s imports the v1 package as %q; the check resolves package-qualified calls and cannot follow that", pf.rel, imp.Name.Name)
				default:
					pf.qual = imp.Name.Name
				}
			}
			files = append(files, pf)
		}
	}

	// A decl is one body to scan: a function, a method, or the initialisers of
	// a package-level var (attributed to a synthetic node named for the var, so
	// a reference to it is an edge).
	type decl struct {
		f      *fn
		pf     parsed
		recv   *ast.Field
		params *ast.FieldList
		body   []ast.Node
	}
	var decls []decl

	// Pass 1: declarations, which Evaluator methods evaluate without returning
	// a cost, and which struct fields hold an Evaluator. The uncharged set is
	// derived from the signatures, so a new uncharged entry point is a sink
	// without anyone listing it here.
	evalFields := map[string]bool{"Eval": true}
	for _, pf := range files {
		for _, d := range pf.file.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if d.Body == nil {
					continue
				}
				f := &fn{pkg: pf.pkg, name: d.Name.Name, callees: map[string]bool{}}
				dc := decl{f: f, pf: pf, params: d.Type.Params, body: []ast.Node{d.Body}}
				f.id = pf.pkg + "." + f.name
				if d.Recv != nil && len(d.Recv.List) > 0 {
					f.recv = typeName(d.Recv.List[0].Type)
					f.id = pf.pkg + "." + f.recv + "." + f.name
					dc.recv = d.Recv.List[0]
					g.byName[f.name] = append(g.byName[f.name], f)
				} else {
					g.funcs[f.id] = f
				}
				g.fns[f.id] = f
				if pf.pkg == "v1" && f.recv == "Evaluator" && d.Name.IsExported() &&
					strings.HasPrefix(f.name, "Eval") && !returnsUint64(d) {
					g.uncharge[f.name] = true
				}
				decls = append(decls, dc)
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch sp := spec.(type) {
					case *ast.ValueSpec:
						if d.Tok != token.VAR {
							continue
						}
						for _, name := range sp.Names {
							f := &fn{pkg: pf.pkg, name: name.Name, id: pf.pkg + "." + name.Name, callees: map[string]bool{}}
							if _, taken := g.fns[f.id]; taken {
								continue
							}
							g.funcs[f.id] = f
							g.fns[f.id] = f
							var body []ast.Node
							for _, v := range sp.Values {
								body = append(body, v)
							}
							decls = append(decls, decl{f: f, pf: pf, body: body})
						}
					case *ast.TypeSpec:
						if st, ok := sp.Type.(*ast.StructType); ok {
							for _, fld := range st.Fields.List {
								if isEvaluatorType(fld.Type) {
									for _, n := range fld.Names {
										evalFields[n.Name] = true
									}
								}
							}
						}
					}
				}
			}
		}
	}

	// Pass 2: edges and sink calls.
	commentsOf := map[string]*lineComments{}
	for _, dc := range decls {
		f, pf := dc.f, dc.pf
		lc := commentsOf[pf.rel]
		if lc == nil {
			lc = newLineComments(fset, pf.file, pf.src)
			commentsOf[pf.rel] = lc
		}
		// Only the definitions of the uncharged entry points are exempt from
		// being sinks; a wrapper that calls one is a sink itself.
		definition := f.recv == "Evaluator" && g.uncharge[f.name]
		locals := map[string]bool{}
		if dc.recv != nil && isEvaluatorType(dc.recv.Type) {
			for _, n := range dc.recv.Names {
				locals[n.Name] = true
			}
		}
		if dc.params != nil {
			for _, fld := range dc.params.List {
				if isEvaluatorType(fld.Type) {
					for _, n := range fld.Names {
						locals[n.Name] = true
					}
				}
			}
		}
		visit := func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				for i, r := range x.Rhs {
					if id, ok := lhsIdent(x.Lhs, i); ok && isEvaluator(r, locals, evalFields) {
						locals[id] = true
					}
				}
			case *ast.ValueSpec:
				for i, name := range x.Names {
					if (x.Type != nil && isEvaluatorType(x.Type)) ||
						(i < len(x.Values) && isEvaluator(x.Values[i], locals, evalFields)) {
						locals[name.Name] = true
					}
				}
			case *ast.FuncLit:
				if x.Type.Params != nil {
					for _, fld := range x.Type.Params.List {
						if isEvaluatorType(fld.Type) {
							for _, n := range fld.Names {
								locals[n.Name] = true
							}
						}
					}
				}
			case *ast.Ident:
				if t, ok := g.funcs[pf.pkg+"."+x.Name]; ok {
					f.callees[t.id] = true
				}
			case *ast.SelectorExpr:
				name := x.Sel.Name
				if q, ok := x.X.(*ast.Ident); ok && pf.pkg == "engine" && pf.qual != "" && q.Name == pf.qual {
					if t, ok := g.funcs["v1."+name]; ok {
						f.callees[t.id] = true
					}
					return true
				}
				for _, t := range g.byName[name] {
					f.callees[t.id] = true
				}
				if g.uncharge[name] && !definition && (!g.ambiguous(name) || isEvaluator(x.X, locals, evalFields)) {
					line := fset.Position(x.Sel.Pos()).Line
					f.uncharge = append(f.uncharge, siteCall{
						file: pf.rel, callee: name, line: line,
						exempt: lc.exemption(line),
					})
				}
			}
			return true
		}
		for _, n := range dc.body {
			ast.Inspect(n, visit)
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

// isEvaluatorType reports whether a declared type is Evaluator or *Evaluator,
// bare or package-qualified.
func isEvaluatorType(e ast.Expr) bool {
	switch t := e.(type) {
	case *ast.StarExpr:
		return isEvaluatorType(t.X)
	case *ast.Ident:
		return t.Name == "Evaluator"
	case *ast.SelectorExpr:
		return t.Sel.Name == "Evaluator"
	}
	return false
}

// isEvaluator reports whether e is, syntactically, an *Evaluator: a call to
// DefaultEvaluator or to a method named evaluator, a parameter, receiver or
// local declared or assigned as one, or a struct field declared with the type.
func isEvaluator(e ast.Expr, locals, fields map[string]bool) bool {
	switch t := e.(type) {
	case *ast.ParenExpr:
		return isEvaluator(t.X, locals, fields)
	case *ast.StarExpr:
		return isEvaluator(t.X, locals, fields)
	case *ast.UnaryExpr:
		return isEvaluator(t.X, locals, fields)
	case *ast.Ident:
		return locals[t.Name]
	case *ast.SelectorExpr:
		return fields[t.Sel.Name]
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
	case *ast.IndexListExpr:
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

// lineComments indexes a file's comments by the line they end on.
type lineComments struct {
	byLine map[int]lineComment
}

type lineComment struct {
	text      string // comment text without its delimiters, trimmed
	own       bool   // nothing but whitespace precedes it on its line
	startLine int
}

func newLineComments(fset *token.FileSet, f *ast.File, src []byte) *lineComments {
	lc := &lineComments{byLine: map[int]lineComment{}}
	tf := fset.File(f.Pos())
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			start := fset.Position(c.Pos())
			end := fset.Position(c.End())
			prefix := src[tf.Offset(tf.LineStart(start.Line)):tf.Offset(c.Pos())]
			text := strings.TrimPrefix(c.Text, "//")
			if strings.HasPrefix(c.Text, "/*") {
				text = strings.TrimSuffix(strings.TrimPrefix(c.Text, "/*"), "*/")
			}
			lc.byLine[end.Line] = lineComment{
				text:      strings.TrimSpace(text),
				own:       len(strings.TrimSpace(string(prefix))) == 0,
				startLine: start.Line,
			}
		}
	}
	return lc
}

// exemption returns the reason declared for a call on line. The marker must
// begin a comment, on the call's own line or in the block of comment-only lines
// directly above it, and must carry a reason; the block's later lines through
// the call are joined onto the reason. Prose that merely mentions the marker,
// or a trailing comment on the previous statement, declares nothing.
func (lc *lineComments) exemption(line int) string {
	var reason []string
	if c, ok := lc.byLine[line]; ok && !c.own {
		if r, ok := markerReason(c.text); ok {
			return r
		}
	}
	for l := line - 1; l >= line-8; l-- {
		c, ok := lc.byLine[l]
		if !ok || !c.own {
			return ""
		}
		reason = append([]string{c.text}, reason...)
		if r, ok := markerReason(c.text); ok {
			reason[0] = r
			if c, ok := lc.byLine[line]; ok && !c.own {
				reason = append(reason, c.text)
			}
			return strings.Join(strings.Fields(strings.Join(reason, " ")), " ")
		}
	}
	return ""
}

func markerReason(text string) (string, bool) {
	rest, ok := strings.CutPrefix(text, exemptMarker)
	rest = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(rest), ":"))
	return rest, ok && rest != ""
}
