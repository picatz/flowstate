package main

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

// conditionalClaim reports whether every claim a test makes is inside a loop
// that nothing says will run, and over what.
//
// The rule in one sentence: if a test asserts nothing outside a loop, asserts
// nothing about any length, and the thing it ranges over is not something a
// reader of this function can count, then an empty range makes the test green
// and silent.
//
// Each of those three clauses is there to keep an ordinary table test out of
// the report, and the third is doing most of the work. `for _, c := range
// []case{…}` and `for _, c := range cases` where `cases :=` a literal above are
// both countable by the person reading the test, and flagging them would bury
// the finding that matters under the whole tree: adding that clause removed
// about half the sites, and every one it removed was the same idiom. Run
// `make vacuity SITES=1` for what is left.
func conditionalClaim(fn *analyzed, asserters map[string]bool) (loop ast.Node, subject string, found bool) {
	var ranges []*ast.RangeStmt
	ast.Inspect(fn.decl.Body, func(node ast.Node) bool {
		if stmt, ok := node.(*ast.RangeStmt); ok {
			ranges = append(ranges, stmt)
		}

		return true
	})
	if len(ranges) == 0 {
		return nil, "", false
	}

	// A claim anywhere but inside a loop settles it: the test asserts
	// something whatever the corpus holds.
	if assertsOutsideLoops(fn, asserters) {
		return nil, "", false
	}

	// And so does a guard that *skips* on an empty corpus, which the clause
	// above cannot see: a skip is not a failure, so nothing in it looks like an
	// assertion — but a test that skips itself rather than passing silently is
	// reporting the emptiness, which is the whole ask.
	if skipsWhenEmpty(fn.decl.Body) {
		return nil, "", false
	}

	for _, stmt := range ranges {
		name, countable := rangeSubject(stmt, fn.decl)
		if countable {
			continue
		}
		if !holdsAssertion(stmt, fn.handles, asserters) {
			continue
		}

		return stmt, name, true
	}

	return nil, "", false
}

// assertsOutsideLoops reports whether the test makes any claim that is not
// inside a range statement.
func assertsOutsideLoops(fn *analyzed, asserters map[string]bool) bool {
	outside := false

	inspectOutsideLoops(fn.decl.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if assertion(call, fn.handles) || handsOverTheHandle(call.Args, fn.handles) {
			outside = true
		}
		if name, ok := call.Fun.(*ast.Ident); ok && asserters[name.Name] {
			outside = true
		}

		return true
	})

	return outside
}

// inspectOutsideLoops walks a body, never descending into a range statement.
//
// The one place both questions this check asks are answered from — "does it
// claim anything unconditionally" and "does it claim anything about a length"
// — because they are the same question about two vocabularies, and answering
// them from two walks is how one of them came to be answered wrongly.
func inspectOutsideLoops(body ast.Node, visit func(ast.Node) bool) {
	ast.Inspect(body, func(node ast.Node) bool {
		if _, isRange := node.(*ast.RangeStmt); isRange {
			// What is in there is the conditional half, which is what these
			// questions are asked *about* rather than answered from.
			return false
		}

		return visit(node)
	})
}

// skipsWhenEmpty reports whether the body skips the test on a length,
// somewhere a loop's emptiness would not skip past.
//
// Only the skip, and that narrowness was found by mutation rather than by
// design. The first version also looked for `require.NotEmpty`, `require.Len`
// and their neighbours — and breaking that whole list changed no answer,
// because a length *assertion* is an assertion outside the loop and
// [assertsOutsideLoops] has already returned by then. A clause that cannot
// change an answer is a clause nobody can test, which is this tool's own
// subject, so it is gone.
//
// A skip is the one guard that does not read as an assertion and still means
// the emptiness was noticed.
func skipsWhenEmpty(body ast.Node) bool {
	skips := false

	inspectOutsideLoops(body, func(node ast.Node) bool {
		stmt, ok := node.(*ast.IfStmt)
		if !ok {
			return true
		}
		// The branch has to be the *empty* one. Merely mentioning a length is
		// not enough: `if len(cases) > max { t.Skip(…) }` skips an oversized
		// corpus and is not taken by an empty one, so the loop still runs zero
		// times and the test still passes having claimed nothing — while the
		// report goes quiet about it, which is worse than never having looked
		// (Codex, #1125).
		if emptiness(stmt.Cond) && skipsWithin(stmt.Body) {
			skips = true
		}

		return true
	})

	return skips
}

// skipsWithin reports whether a block skips the test.
func skipsWithin(block ast.Node) bool {
	found := false

	ast.Inspect(block, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if selector, ok := call.Fun.(*ast.SelectorExpr); ok && strings.HasPrefix(selector.Sel.Name, "Skip") {
			found = true
		}

		return true
	})

	return found
}

// emptiness reports whether a condition is true exactly when something is
// empty.
//
// The three spellings people use — `len(x) == 0`, `len(x) < 1`, `len(x) <= 0` —
// in either operand order. Anything else is refused rather than guessed at: a
// condition this cannot read is one whose taken branch might be the non-empty
// case, and settling a finding on that basis is how a check goes quiet about
// the thing it was written for.
func emptiness(expr ast.Expr) bool {
	compare, ok := expr.(*ast.BinaryExpr)
	if !ok {
		return false
	}

	length, bound, operator := compare.X, compare.Y, compare.Op
	if !callsLen(length) {
		// `0 == len(x)`, which is the same claim written the other way round.
		length, bound = compare.Y, compare.X
		switch operator {
		case token.LSS:
			operator = token.GTR
		case token.LEQ:
			operator = token.GEQ
		case token.GTR:
			operator = token.LSS
		case token.GEQ:
			operator = token.LEQ
		}
	}
	if !callsLen(length) {
		return false
	}

	switch operator {
	case token.EQL:
		return isInt(bound, 0)
	case token.LSS:
		return isInt(bound, 1)
	case token.LEQ:
		return isInt(bound, 0)
	}

	return false
}

// callsLen reports whether an expression is a call to len.
func callsLen(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	name, ok := call.Fun.(*ast.Ident)

	return ok && name.Name == "len"
}

// isInt reports whether an expression is the given untyped integer literal.
func isInt(expr ast.Expr, want int) bool {
	literal, ok := expr.(*ast.BasicLit)
	if !ok || literal.Kind != token.INT {
		return false
	}
	value, err := strconv.Atoi(literal.Value)

	return err == nil && value == want
}

// holdsAssertion reports whether a node contains a claim.
func holdsAssertion(node ast.Node, handles, asserters map[string]bool) bool {
	found := false
	ast.Inspect(node, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if assertion(call, handles) || handsOverTheHandle(call.Args, handles) {
			found = true
		}
		if name, ok := call.Fun.(*ast.Ident); ok && asserters[name.Name] {
			found = true
		}

		return true
	})

	return found
}

// rangeSubject renders what a loop walks, and reports whether a reader of this
// function can count it.
func rangeSubject(stmt *ast.RangeStmt, within *ast.FuncDecl) (name string, countable bool) {
	switch subject := stmt.X.(type) {
	case *ast.CompositeLit:
		// Written right there, entries and all.
		return "a literal", len(subject.Elts) > 0

	case *ast.BasicLit:
		// `range "abcd"` and `range 5`.
		return "a literal", true

	case *ast.Ident:
		// A name assigned a literal in this same function is still countable:
		// the entries are a few lines up, in front of whoever is reading.
		if entries, assigned := literalAssignedTo(within, subject.Name); assigned {
			return subject.Name, entries
		}

		return subject.Name, false

	case *ast.CallExpr, *ast.SelectorExpr:
		// The shape that matters. What it yields is decided somewhere else,
		// and somewhere else is exactly where a case gets moved out of a
		// corpus without anybody looking at this test.
		return render(stmt.X), false
	}

	return render(stmt.X), false
}

// literalAssignedTo reports whether a name is assigned a composite literal
// anywhere in this function, and whether that literal has entries.
//
// Assignments are not ordered against the loop, deliberately. A name assigned
// a literal *anywhere* in the function is one whose entries a reader has in
// front of them, and a flow-sensitive answer would need types this does not
// have — for a distinction that changes nothing about whether the reader can
// count.
func literalAssignedTo(within *ast.FuncDecl, name string) (entries, assigned bool) {
	ast.Inspect(within.Body, func(node ast.Node) bool {
		assignment, ok := node.(*ast.AssignStmt)
		if !ok {
			return true
		}

		for i, left := range assignment.Lhs {
			target, ok := left.(*ast.Ident)
			if !ok || target.Name != name || i >= len(assignment.Rhs) {
				continue
			}
			if literal, ok := assignment.Rhs[i].(*ast.CompositeLit); ok {
				assigned = true
				entries = len(literal.Elts) > 0
			}
		}

		return true
	})

	return entries, assigned
}

// render is an expression as source, for the report.
func render(expr ast.Expr) string {
	switch expr := expr.(type) {
	case *ast.Ident:
		return expr.Name
	case *ast.SelectorExpr:
		return render(expr.X) + "." + expr.Sel.Name
	case *ast.CallExpr:
		return render(expr.Fun) + "()"
	case *ast.IndexExpr:
		return render(expr.X) + "[…]"
	}

	return "what it ranges over"
}
