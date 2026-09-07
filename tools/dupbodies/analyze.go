package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
)

// MinStatements is the smallest body the report looks at, counted as every
// statement node in the body, nested ones included. A two-line accessor
// written twice is a coincidence; a body this size written twice is a copy.
const MinStatements = 8

// Group is one body that appears in more than one function.
type Group struct {
	// Members names each function holding the body, as
	// `path/file.go:Name` or `path/file.go:Type.Name` relative to the root,
	// sorted.
	Members []string

	// Statements is the body's size, for ordering the report.
	Statements int
}

// Key is the group's identity in the ratchet table: its members, joined.
func (g Group) Key() string { return strings.Join(g.Members, " = ") }

// Analyze walks every Go file under root that is not generated and returns
// the bodies that appear in more than one function, with the number of
// functions it read.
//
// It parses rather than builds, so a plugin module's sources count, and
// compares bodies after printing them without comments or positions: two
// bodies match when they are the same code, whatever they were called and
// however they were laid out. A body whose identifiers were renamed does not
// match, which is the trade the report makes for never crying wolf.
func Analyze(root string) ([]Group, int, error) {
	fset := token.NewFileSet()

	type member struct {
		name       string
		statements int
	}
	bodies := map[string][]member{}
	functions := 0

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".coverage", "node_modules", "testdata", ".worktrees", ".claude":
				// The same set tools/vacuity skips, for the reasons it gives.
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), ".go") {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		if ast.IsGenerated(file) {
			// A generator writes the same body as often as it likes, and the
			// copy to fix is in the generator.
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			functions++

			statements := countStatements(fn.Body)
			if statements < MinStatements {
				continue
			}

			key, err := bodyKey(fn.Body)
			if err != nil {
				return err
			}
			bodies[key] = append(bodies[key], member{name: rel + ":" + funcName(fn), statements: statements})
		}

		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	var groups []Group
	for _, members := range bodies {
		if len(members) < 2 {
			continue
		}
		group := Group{Statements: members[0].statements}
		for _, m := range members {
			group.Members = append(group.Members, m.name)
		}
		slices.Sort(group.Members)
		groups = append(groups, group)
	}
	slices.SortFunc(groups, func(a, b Group) int {
		if c := b.Statements - a.Statements; c != 0 {
			return c
		}
		return strings.Compare(a.Key(), b.Key())
	})

	return groups, functions, nil
}

// countStatements counts every statement in the body, nested ones included.
func countStatements(body *ast.BlockStmt) int {
	n := 0
	ast.Inspect(body, func(node ast.Node) bool {
		if _, ok := node.(ast.Stmt); ok {
			n++
		}
		return true
	})
	return n
}

// bodyKey prints the body without comments and hashes the text, so layout
// and commentary do not separate two copies of the same code.
//
// It prints against an empty file set on purpose: go/printer keeps the
// source's line breaks and blank lines when it can see them, so a comment or
// a blank line inside one copy would keep it apart from the other. With no
// positions to consult it lays every body out the same way.
func bodyKey(body *ast.BlockStmt) (string, error) {
	var buf bytes.Buffer
	if err := printer.Fprint(&buf, token.NewFileSet(), body); err != nil {
		return "", err
	}
	sum := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(sum[:]), nil
}

// funcName is `Name` for a function and `Type.Name` for a method.
func funcName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	recv := fn.Recv.List[0].Type
	for {
		switch t := recv.(type) {
		case *ast.StarExpr:
			recv = t.X
			continue
		case *ast.IndexExpr:
			recv = t.X
			continue
		case *ast.IndexListExpr:
			recv = t.X
			continue
		case *ast.Ident:
			return t.Name + "." + fn.Name.Name
		}
		return fn.Name.Name
	}
}
