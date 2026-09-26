package main

import (
	"bufio"
	"cmp"
	"fmt"
	"go/ast"
	"go/build"
	"go/doc"
	"go/doc/comment"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Finding is one doc link that does not resolve.
type Finding struct {
	File   string // slash-separated, relative to the root
	Line   int
	Link   string // the bracketed text, brackets included
	Reason string
}

// String renders a finding the way a compiler names a position, so an editor
// can jump to the comment line that needs fixing.
func (f Finding) String() string {
	return fmt.Sprintf("%s:%d: %s %s", f.File, f.Line, f.Link, f.Reason)
}

// bracketed matches a bracketed Go name in text a doc comment left plain:
// an identifier, optionally pointer-marked and dot-qualified. The byte before
// it, when there is one, is captured so an index or type argument such as
// Set[T] can be told apart from a link.
var bracketed = regexp.MustCompile(`(^|[^\pL\pN_\]])\[(\*?[\pL_][\pL\pN_]*(?:\.[\pL_][\pL\pN_]*)*)\]`)

// pkg is one parsed package: what its doc comments can link to, and the
// comments to check.
type pkg struct {
	importPath string
	fset       *token.FileSet
	files      []*ast.File
	doc        *doc.Package
}

// Check parses every package under root and returns the doc links that do
// not resolve, sorted by position, with the number of links it examined.
// Directories Go itself ignores (testdata, and names starting with "." or
// "_") are skipped, as are node_modules and test files, which pkg.go.dev
// does not render.
func Check(root string) ([]Finding, int, error) {
	pkgs, err := load(root)
	if err != nil {
		return nil, 0, err
	}

	// exported maps an import path inside the root to the names a link into
	// that package may use: its exported top-level names, and Type.Member for
	// each method, struct field, and interface method, as go/doc resolves them.
	exported := map[string]map[string]bool{}
	for _, p := range pkgs {
		names := map[string]bool{}
		for _, c := range p.doc.Consts {
			for _, n := range c.Names {
				names[n] = true
			}
		}
		for _, v := range p.doc.Vars {
			for _, n := range v.Names {
				names[n] = true
			}
		}
		for _, f := range p.doc.Funcs {
			names[f.Name] = true
		}
		for _, t := range p.doc.Types {
			names[t.Name] = true
			for _, c := range t.Consts {
				for _, n := range c.Names {
					names[n] = true
				}
			}
			for _, v := range t.Vars {
				for _, n := range v.Names {
					names[n] = true
				}
			}
			for _, f := range t.Funcs {
				names[f.Name] = true
			}
			for _, m := range t.Methods {
				names[t.Name+"."+m.Name] = true
			}
			for _, member := range members(t.Decl) {
				names[t.Name+"."+member] = true
			}
		}
		exported[p.importPath] = names
	}

	c := checker{root: root, exported: exported, packageNames: map[string]bool{}}
	for _, p := range pkgs {
		c.packageNames[p.doc.Name] = true
		c.packageNames[path.Base(p.importPath)] = true
	}
	for _, p := range pkgs {
		c.checkPackage(p)
	}
	slices.SortFunc(c.findings, func(a, b Finding) int {
		return cmp.Or(strings.Compare(a.File, b.File), cmp.Compare(a.Line, b.Line), strings.Compare(a.Link, b.Link))
	})
	// A link repeated within one comment is reported at the first line that
	// holds it, so the repeats are the same finding.
	return slices.Compact(c.findings), c.links, nil
}

type checker struct {
	root         string
	exported     map[string]map[string]bool
	packageNames map[string]bool // declared names and import-path bases
	findings     []Finding
	links        int
}

// checkPackage checks the package comment and the doc comment of every
// exported top-level declaration: what go/doc, and so pkg.go.dev, renders.
func (c *checker) checkPackage(p *pkg) {
	pr := p.doc.Parser()
	for _, f := range p.files {
		c.checkComment(p, pr, f.Doc)
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Name.IsExported() && (d.Recv == nil || exportedRecv(d.Recv)) {
					c.checkComment(p, pr, d.Doc)
				}
			case *ast.GenDecl:
				if slices.ContainsFunc(d.Specs, exportedSpec) {
					c.checkComment(p, pr, d.Doc)
				}
				for _, s := range d.Specs {
					if !exportedSpec(s) {
						continue
					}
					switch s := s.(type) {
					case *ast.TypeSpec:
						c.checkComment(p, pr, s.Doc)
					case *ast.ValueSpec:
						c.checkComment(p, pr, s.Doc)
					}
				}
			}
		}
	}
}

func (c *checker) checkComment(p *pkg, pr *comment.Parser, group *ast.CommentGroup) {
	if group == nil {
		return
	}
	d := pr.Parse(group.Text())
	for _, block := range prose(d) {
		for _, t := range block.text {
			switch t := t.(type) {
			case comment.Plain:
				s := string(t)
				for _, m := range bracketed.FindAllStringSubmatchIndex(s, -1) {
					name := s[m[4]:m[5]]
					if !c.looksLikeALink(name) {
						continue
					}
					c.links++
					if block.heading {
						c.report(p, group, "["+name+"]", "is in a heading, which the doc comment syntax renders without links", true)
						continue
					}
					if r, _ := utf8.DecodeRuneInString(s[m[1]:]); unicode.IsLetter(r) || unicode.IsDigit(r) {
						c.report(p, group, "["+name+"]", "is glued to the word after it, which the doc comment syntax never reads as a link", false)
						continue
					}
					c.report(p, group, "["+name+"]", "is plain text: nothing by that name is importable from "+p.importPath, false)
				}
			case *comment.DocLink:
				c.links++
				c.checkDocLink(p, group, t)
			}
		}
	}
}

// looksLikeALink reports whether a bracketed name left in plain text was
// meant as a doc link: an exported name, qualified or not, or the bare name of
// a package in this repository. A bracketed lowercase word that is neither
// ([i], [n], [key]) is prose.
func (c *checker) looksLikeALink(name string) bool {
	name = strings.TrimPrefix(name, "*")
	last := name[strings.LastIndexByte(name, '.')+1:]
	if ast.IsExported(last) {
		return true
	}
	if strings.Contains(name, ".") {
		return false
	}
	return c.packageNames[name]
}

func (c *checker) checkDocLink(p *pkg, group *ast.CommentGroup, l *comment.DocLink) {
	text := "[" + docLinkText(l) + "]"
	if l.ImportPath == "" {
		return // resolved inside the package by the parser itself
	}
	names, inside := c.exported[l.ImportPath]
	if !inside {
		if !importable(l.ImportPath) {
			c.report(p, group, text, "names import path "+l.ImportPath+", which is neither in the standard library nor in this repository", false)
		}
		return
	}
	if l.Name == "" {
		return
	}
	name := l.Name
	if l.Recv != "" {
		name = l.Recv + "." + l.Name
	}
	if !names[name] && !names[l.Name] {
		c.report(p, group, text, "names "+name+", which package "+l.ImportPath+" does not export", false)
	}
}

// report records a finding at the first comment line holding link, or, for a
// link in a heading, the first heading line holding it.
func (c *checker) report(p *pkg, group *ast.CommentGroup, link, reason string, heading bool) {
	pos := p.fset.Position(group.Pos())
	line := pos.Line
	for _, cm := range group.List {
		isHeading := strings.HasPrefix(strings.TrimSpace(strings.TrimPrefix(cm.Text, "//")), "# ")
		if strings.Contains(cm.Text, link) && isHeading == heading {
			line = p.fset.Position(cm.Pos()).Line
			break
		}
	}
	rel, err := filepath.Rel(c.root, pos.Filename)
	if err != nil {
		rel = pos.Filename
	}
	c.findings = append(c.findings, Finding{File: filepath.ToSlash(rel), Line: line, Link: link, Reason: reason})
}

// importable reports whether an import path outside this repository can be
// linked to: a module path (its first element holds a dot) or a
// standard-library package that exists in this toolchain.
func importable(importPath string) bool {
	first, _, _ := strings.Cut(importPath, "/")
	if strings.Contains(first, ".") {
		return true
	}
	p, err := build.Default.Import(importPath, "", build.FindOnly)
	return err == nil && p.Goroot
}

func docLinkText(l *comment.DocLink) string {
	var b strings.Builder
	for _, t := range l.Text {
		if p, ok := t.(comment.Plain); ok {
			b.WriteString(string(p))
		}
	}
	return b.String()
}

// block is the inline text of one block that renders as prose.
type block struct {
	text    []comment.Text
	heading bool
}

// prose returns every block that renders as prose: paragraphs, headings, and
// list items. Code blocks are verbatim and hold no links.
func prose(d *comment.Doc) []block {
	var out []block
	for _, b := range d.Content {
		switch b := b.(type) {
		case *comment.Paragraph:
			out = append(out, block{text: b.Text})
		case *comment.Heading:
			out = append(out, block{text: b.Text, heading: true})
		case *comment.List:
			for _, item := range b.Items {
				for _, ib := range item.Content {
					if para, ok := ib.(*comment.Paragraph); ok {
						out = append(out, block{text: para.Text})
					}
				}
			}
		}
	}
	return out
}

// members returns the exported field names of a struct type declaration and
// the exported method names of an interface type declaration.
func members(decl *ast.GenDecl) []string {
	var out []string
	for _, s := range decl.Specs {
		spec, ok := s.(*ast.TypeSpec)
		if !ok {
			continue
		}
		var fields *ast.FieldList
		switch t := spec.Type.(type) {
		case *ast.StructType:
			fields = t.Fields
		case *ast.InterfaceType:
			fields = t.Methods
		default:
			continue
		}
		for _, f := range fields.List {
			for _, n := range f.Names {
				if n.IsExported() {
					out = append(out, n.Name)
				}
			}
		}
	}
	return out
}

func exportedRecv(recv *ast.FieldList) bool {
	if len(recv.List) == 0 {
		return false
	}
	t := recv.List[0].Type
	for {
		switch x := t.(type) {
		case *ast.StarExpr:
			t = x.X
		case *ast.IndexExpr:
			t = x.X
		case *ast.IndexListExpr:
			t = x.X
		case *ast.Ident:
			return x.IsExported()
		default:
			return false
		}
	}
}

func exportedSpec(s ast.Spec) bool {
	switch s := s.(type) {
	case *ast.TypeSpec:
		return s.Name.IsExported()
	case *ast.ValueSpec:
		return slices.ContainsFunc(s.Names, (*ast.Ident).IsExported)
	}
	return false
}

// load parses every package under root. A directory holding files of more
// than one package name (a generator behind a build tag beside a library)
// yields one package per name. The import path of each comes from the
// nearest go.mod above it.
func load(root string) ([]*pkg, error) {
	var pkgs []*pkg
	err := filepath.WalkDir(root, func(dir string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		name := d.Name()
		if dir != root && (name == "testdata" || name == "node_modules" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
			return filepath.SkipDir
		}
		found, err := loadDir(dir)
		if err != nil {
			return err
		}
		pkgs = append(pkgs, found...)
		return nil
	})
	return pkgs, err
}

func loadDir(dir string) ([]*pkg, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	byName := map[string][]*ast.File{}
	var names []string
	for _, e := range entries {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, n), nil, parser.ParseComments)
		if err != nil {
			return nil, err
		}
		if _, ok := byName[f.Name.Name]; !ok {
			names = append(names, f.Name.Name)
		}
		byName[f.Name.Name] = append(byName[f.Name.Name], f)
	}
	if len(names) == 0 {
		return nil, nil
	}
	importPath, err := importPathOf(dir)
	if err != nil {
		return nil, err
	}
	var pkgs []*pkg
	for _, name := range names {
		files := byName[name]
		dp, err := doc.NewFromFiles(fset, files, importPath, doc.PreserveAST)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", dir, err)
		}
		pkgs = append(pkgs, &pkg{importPath: importPath, fset: fset, files: files, doc: dp})
	}
	return pkgs, nil
}

// importPathOf derives a directory's import path from the module line of the
// nearest go.mod at or above it.
func importPathOf(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for d := abs; ; d = filepath.Dir(d) {
		module, err := modulePath(filepath.Join(d, "go.mod"))
		if err == nil {
			rel, err := filepath.Rel(d, abs)
			if err != nil {
				return "", err
			}
			if rel == "." {
				return module, nil
			}
			return module + "/" + filepath.ToSlash(rel), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		if filepath.Dir(d) == d {
			return "", fmt.Errorf("%s: no go.mod at or above it", dir)
		}
	}
}

func modulePath(goMod string) (string, error) {
	f, err := os.Open(goMod)
	if err != nil {
		return "", err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(s.Text()), "module "); ok {
			return strings.Trim(strings.TrimSpace(rest), `"`), nil
		}
	}
	if err := s.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("%s: no module line", goMod)
}
