package flowstatev1

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/types"
)

// Filtering a listing, in the language the rest of the system already speaks.
//
// A filter is CEL, because everything else an author writes here is CEL: the
// expressions in a Flowfile, the rules in an egress or auth policy, the condition
// on a step. A query language of its own would be a second grammar to learn, a
// second parser to bound, and a second place for `status` to mean something
// slightly different.
//
// # What it runs against, and where
//
// Today every filter is evaluated by the server, once per execution it reads,
// beside the tenant check that is already there. That is not an implementation
// detail deferred until later — it follows from the tenant being recorded as a
// memo, which Temporal cannot query, so a listing is already a bounded scan with
// a predicate applied to each result (see `server/list.go`).
//
// The shape this leaves room for is the one worth having. When a deployment
// registers search attributes, the parts of a filter Temporal *can* answer become
// a visibility query pushed down into the store, and what is left stays here as a
// residual predicate. The split changes what a listing costs and never what it
// means — the same filter returns the same runs either way, which is the property
// that lets an operator turn pushdown on without re-reading every saved query.
// That is why the vocabulary below is defined here rather than in whichever layer
// happens to evaluate it.
//
// # Absence is not a zero value
//
// `close_time` is null while a run is going, and null rather than the epoch on
// purpose: a run that has not finished has no close time, and a filter that
// compared one against a date would otherwise quietly report every running run as
// having finished in 1970.
//
// Comparing null errors, which is correct and is also why `finished` exists. CEL's
// `&&` absorbs errors from the side it does not need — `false && <error>` is
// `false` — so the guarded form does what it looks like it does:
//
//	finished && close_time > timestamp("2026-01-01T00:00:00Z")
//
// The unguarded form is a mistake, and it is reported as one rather than silently
// answering. See [RunFilter.Match].

// maxFilterCost is the CEL cost budget for evaluating a filter against one run.
//
// Deliberately far below [DefaultCostLimit], and the reason is multiplication
// rather than caution. A filter is evaluated once per execution the listing reads,
// and how many that is has its own bound (`maxListScan`) — so the work a caller
// can ask for with one request is the product of the two. At the default budget
// that product is a billion cost units for a single `flow list`; at this one it is
// ten million, which is the same order as a single ordinary evaluation elsewhere
// in the system.
//
// It is also generous for what a filter legitimately is. A predicate over six
// scalar fields costs single-digit units; anything approaching this bound is not a
// filter, it is a program someone is running once per run in a listing.
const maxFilterCost uint64 = 10_000

// Names a filter binds. Kept together because they are the vocabulary — a caller
// writing a filter and a reader of the documentation are looking at this list.
const (
	filterWorkflowID = "workflow_id"
	filterRunID      = "run_id"
	filterStatus     = "status"
	filterStartTime  = "start_time"
	filterCloseTime  = "close_time"
	filterFinished   = "finished"
)

// A RunFilter is a compiled predicate over the runs in a listing.
//
// Compiled once per request and evaluated once per execution read, which is the
// only arrangement that makes sense: compiling parses and type-checks, and doing
// that per run would cost more than the listing.
type RunFilter struct {
	source  string
	program cel.Program
}

// NewRunFilter compiles a filter expression.
//
// An empty expression is not an error and not a filter: it returns nil, which
// [RunFilter.Match] treats as matching everything. That keeps the caller free of a
// branch, and it means "no filter" and "a filter that matches everything" are the
// same code path rather than two.
//
// Compiled here rather than only in the server, so that `flow list --filter` can
// refuse a malformed expression before it makes a request — the authoring rule
// this repository holds to. A caller that mistyped `stauts` learns so immediately,
// with the compiler's own position, instead of learning it from a round trip.
func NewRunFilter(expression string) (*RunFilter, error) {
	if strings.TrimSpace(expression) == "" {
		return nil, nil
	}

	env, err := runFilterEnv()
	if err != nil {
		return nil, fmt.Errorf("building the filter environment: %w", err)
	}

	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("filter: %w", issues.Err())
	}

	// A filter is a question with a yes-or-no answer. Anything else is a mistake
	// that CEL is perfectly happy to compile — `status` on its own type-checks as
	// a string — and refusing it here names what is wrong rather than letting the
	// listing return nothing for a reason the caller cannot see.
	if ast.OutputType() != cel.BoolType {
		return nil, fmt.Errorf(
			"filter must be a condition, and this one is %s: it has to answer yes or no "+
				"about each run, so write a comparison such as `%s == \"FAILED\"`",
			ast.OutputType(), filterStatus)
	}

	if err := checkStatusLiterals(ast); err != nil {
		return nil, err
	}

	program, err := env.Program(ast,
		cel.CostLimit(maxFilterCost),
		cel.InterruptCheckFrequency(DefaultInterruptCheckFrequency),
	)
	if err != nil {
		return nil, fmt.Errorf("filter: %w", err)
	}

	return &RunFilter{source: expression, program: program}, nil
}

// String returns the expression as written, for a diagnostic that has to quote it.
func (f *RunFilter) String() string {
	if f == nil {
		return ""
	}

	return f.source
}

// Match reports whether a run satisfies the filter.
//
// A nil filter matches everything, which is what "no filter" means.
//
// An evaluation error fails rather than skipping the run, and that is the
// deliberate choice. Almost every error a filter can raise is a property of the
// expression rather than of the run — an unguarded `close_time` comparison errors
// on exactly the runs that are still going — so skipping would answer "no runs
// matched" to a question that was never asked correctly. Reporting it gives the
// caller a reason to doubt their filter, which is what the diagnostics rule is
// for; silently returning an empty page does not.
func (f *RunFilter) Match(ctx context.Context, run *RunSummary) (bool, error) {
	if f == nil {
		return true, nil
	}

	out, _, err := f.program.ContextEval(ctx, f.activation(run))
	if err != nil {
		return false, fmt.Errorf("evaluating filter %q: %w", f.source, err)
	}

	matched, ok := out.Value().(bool)
	if !ok {
		// Unreachable while the output type is checked at compile time, and kept
		// because "unreachable" is a claim about today's code: a later change that
		// relaxed the check above would otherwise turn every run into a match.
		return false, fmt.Errorf("filter %q did not answer yes or no", f.source)
	}

	return matched, nil
}

// activation binds a run's fields to the names a filter uses.
//
// `close_time` is bound to a typed nil rather than omitted, so that referring to
// it on a running run is null — an error when compared, which is the honest
// answer — rather than an unbound-identifier failure, which would blame the
// expression for something true about the run.
func (f *RunFilter) activation(run *RunSummary) map[string]any {
	var closeTime any
	if t := run.GetCloseTime(); t != nil {
		closeTime = t.AsTime()
	} else {
		closeTime = types.NullValue
	}

	var startTime any = types.NullValue
	if t := run.GetStartTime(); t != nil {
		startTime = t.AsTime()
	}

	return map[string]any{
		filterWorkflowID: run.GetWorkflowId(),
		filterRunID:      run.GetRunId(),
		filterStatus:     StatusName(run.GetStatus()),
		filterStartTime:  startTime,
		filterCloseTime:  closeTime,
		filterFinished:   run.GetCloseTime() != nil,
	}
}

// runFilterEnv is the environment a filter compiles against.
//
// Built per compilation rather than cached, unlike the profile environments: a
// listing compiles one filter per request, where a workflow evaluates many
// expressions per run, so the cost that made caching necessary there is not
// present here — and a package-level cache would be a mutable global for a saving
// nobody can measure.
func runFilterEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable(filterWorkflowID, cel.StringType),
		cel.Variable(filterRunID, cel.StringType),

		// A string rather than the enum, and the strings are the enum's own value
		// names with the `STATUS_` prefix removed — see [StatusName], which derives
		// them from the descriptor so that a status added to the schema is
		// filterable without anybody remembering to come here.
		//
		// The alternative was exposing the enum itself, which would make a filter
		// read `status == flowstate.v1.RunResponse.Status.STATUS_FAILED`. That is
		// the schema's name for the value and nobody would type it twice.
		cel.Variable(filterStatus, cel.StringType),

		cel.Variable(filterStartTime, cel.TimestampType),
		cel.Variable(filterCloseTime, cel.TimestampType),
		cel.Variable(filterFinished, cel.BoolType),
	)
}

// checkStatusLiterals refuses a comparison against a status that does not exist.
//
// CEL cannot catch this: `status == "FAILD"` is a perfectly well-typed comparison
// of two strings, and it compiles, runs, and matches nothing. The listing then
// comes back empty, which is indistinguishable from a listing that legitimately
// has nothing in it — so the caller is told their filter is wrong, with the names
// they could have meant.
//
// Deliberately a check on *literals* and nothing cleverer. A filter comparing
// `status` to something computed is not something this can decide, and refusing
// what it cannot understand would be a false diagnostic, which the house rule
// holds to be worse than a missing one.
func checkStatusLiterals(ast *cel.Ast) error {
	valid := StatusNames()

	var bad []string
	walkStatusComparisons(ast.NativeRep().Expr(), func(literal string) {
		if !valid[literal] {
			bad = append(bad, literal)
		}
	})

	if len(bad) == 0 {
		return nil
	}

	return fmt.Errorf(
		"filter compares %s to %s, which is not a run status; the statuses are %s",
		filterStatus, quoteAll(bad), strings.Join(sortedNames(valid), ", "))
}

// quoteAll renders the offending literals for a diagnostic.
func quoteAll(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, fmt.Sprintf("%q", name))
	}

	return strings.Join(quoted, " and ")
}

// walkStatusComparisons calls found for every string literal compared against
// `status` with `==` or `!=`.
//
// Only those two operators, and only a literal on the other side. `status` inside
// an `in` list, or compared to something computed, is left alone — a check that
// guessed at those would produce a false diagnostic, and the house rule is that a
// false one is worse than a missing one.
func walkStatusComparisons(e ast.Expr, found func(string)) {
	if e == nil {
		return
	}

	if e.Kind() == ast.CallKind {
		call := e.AsCall()
		if op := call.FunctionName(); op == operators.Equals || op == operators.NotEquals {
			args := call.Args()
			if len(args) == 2 {
				if literal, ok := statusComparison(args[0], args[1]); ok {
					found(literal)
				}
			}
		}

		if call.IsMemberFunction() {
			walkStatusComparisons(call.Target(), found)
		}
		for _, arg := range call.Args() {
			walkStatusComparisons(arg, found)
		}

		return
	}

	// Everything else that can hold a comparison beneath it. Lists and maps are
	// walked because `status == x` can sit inside one; a select or a comprehension
	// because a filter is allowed to be more than a flat conjunction.
	switch e.Kind() {
	case ast.ListKind:
		for _, element := range e.AsList().Elements() {
			walkStatusComparisons(element, found)
		}
	case ast.MapKind, ast.StructKind:
		for _, field := range e.AsMap().Entries() {
			entry := field.AsMapEntry()
			walkStatusComparisons(entry.Key(), found)
			walkStatusComparisons(entry.Value(), found)
		}
	case ast.SelectKind:
		walkStatusComparisons(e.AsSelect().Operand(), found)
	case ast.ComprehensionKind:
		comp := e.AsComprehension()
		walkStatusComparisons(comp.IterRange(), found)
		walkStatusComparisons(comp.LoopCondition(), found)
		walkStatusComparisons(comp.LoopStep(), found)
		walkStatusComparisons(comp.Result(), found)
	}
}

// statusComparison reports the string literal a `status` comparison names, from
// either side of the operator.
func statusComparison(left, right ast.Expr) (string, bool) {
	if isStatusIdent(left) {
		return stringLiteral(right)
	}
	if isStatusIdent(right) {
		return stringLiteral(left)
	}

	return "", false
}

// isStatusIdent reports whether an expression is the bare `status` name.
func isStatusIdent(e ast.Expr) bool {
	return e != nil && e.Kind() == ast.IdentKind && e.AsIdent() == filterStatus
}

// stringLiteral returns the value of a string constant, if that is what it is.
func stringLiteral(e ast.Expr) (string, bool) {
	if e == nil || e.Kind() != ast.LiteralKind {
		return "", false
	}
	value, ok := e.AsLiteral().Value().(string)

	return value, ok
}
