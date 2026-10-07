package flowstatev1

import (
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
)

// PolicyExpression is one CEL expression an author or operator wrote that can
// read the caller's identity, with where it was written.
type PolicyExpression struct {
	// Where names the stanza, in the Flowfile's own spelling (`signals.deploy.allow`,
	// `debug.allow`, `triggers.manual.allow`).
	Where string

	// Source is the expression without its `${` `}` fence.
	Source string
}

// WorkflowIdentityExpressions returns every expression in a workflow that
// decides on the caller: each `signals:` allow predicate, `debug:` and
// `triggers: manual:`. They are the same predicates [CompileSignalPolicyPredicate]
// compiles and the server evaluates at the action, listed here so that a tool
// reading them looks at what runs rather than at a second enumeration.
func WorkflowIdentityExpressions(wf *Workflow) []PolicyExpression {
	var out []PolicyExpression

	for _, name := range slices.Sorted(maps.Keys(wf.GetSignals())) {
		if allow := wf.GetSignals()[name].GetAllow(); allow != "" {
			out = append(out, PolicyExpression{Where: "signals." + name + ".allow", Source: allow})
		}
	}
	if allow := wf.GetDebug().GetAllow(); allow != "" {
		out = append(out, PolicyExpression{Where: "debug.allow", Source: allow})
	}
	if allow := wf.GetTriggers().GetManual().GetAllow(); allow != "" {
		out = append(out, PolicyExpression{Where: "triggers.manual.allow", Source: allow})
	}

	return out
}

// identityClaimsParser parses without a variable set, so one environment reads
// an expression written for any policy surface: the walk below is over syntax,
// and never evaluates.
var identityClaimsParser = sync.OnceValues(func() (*cel.Env, error) { return cel.NewEnv() })

// IdentityClaimReads returns, sorted and without repeats, the names of the
// caller's claims an expression reads on any policy surface: the keys of
// `identity.claims` (egress, exec, task, secret and assumption rules),
// `sender.identity.claims` and `run.identity.claims` (`signals:`, `debug:`,
// `manual:`), read as `claims.name`, `claims["name"]`, `has(claims.name)` or
// `"name" in claims`.
//
// A read that names no key (`claims[k]` for a computed k, `claims` passed
// whole, an optional select) contributes nothing: the name is not known without
// running the expression, so a tool asking whether a claim is carried can say
// nothing about it. This is the same walk and the same key rule as
// [CompileSignalPolicyPredicate]'s read of `inputs`, so the two cannot disagree
// about what "reads a key" means.
//
// The expression is parsed, not compiled: it need not type-check in any one
// environment. An expression that does not parse is an error.
func IdentityClaimReads(src string) ([]string, error) {
	env, err := identityClaimsParser()
	if err != nil {
		return nil, fmt.Errorf("building the claim-read parser: %w", err)
	}

	parsed, issues := env.Parse(src)
	if issues.Err() != nil {
		return nil, issues.Err()
	}

	root := celast.NavigateAST(parsed.NativeRep())

	// An explicit stack, as in [analyzeSignalPredicate]: the walk's depth is
	// heap, and its work is linear in an expression the parser already bounded.
	names := map[string]struct{}{}
	stack := []celast.NavigableExpr{root}
	for len(stack) > 0 {
		e := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		stack = append(stack, e.Children()...)

		if e.Kind() != celast.SelectKind || e.AsSelect().FieldName() != "claims" || e.AsSelect().IsTestOnly() {
			continue
		}
		if !isCallerIdentity(e.Children()[0]) {
			continue
		}
		if name, ok := signalPolicyMapKey(e); ok {
			names[name] = struct{}{}
		}
	}

	return slices.Sorted(maps.Keys(names)), nil
}

// isCallerIdentity reports whether e is the caller's identity on some surface:
// the `identity` variable, or `identity` selected from `sender` or `run`. A
// comprehension variable spelled the same is the author's local, not the scope.
func isCallerIdentity(e celast.NavigableExpr) bool {
	global := func(e celast.NavigableExpr, names ...string) bool {
		return e.Kind() == celast.IdentKind && slices.Contains(names, e.AsIdent()) && !signalPolicyIsLocal(e)
	}

	if global(e, "identity") {
		return true
	}

	return e.Kind() == celast.SelectKind && e.AsSelect().FieldName() == "identity" &&
		!e.AsSelect().IsTestOnly() && global(e.Children()[0], "sender", "run")
}
