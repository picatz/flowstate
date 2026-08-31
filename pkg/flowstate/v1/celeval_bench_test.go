package flowstatev1

import (
	"context"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
)

// Benchmarks in this repository are a map, not a gate: nothing in ci.yml or
// `make check` runs them, no number is enforced anywhere, and a regression is
// found by someone who suspects one running `benchstat` across two revisions.
// That is the same standing this repository gives `make coverage` and the
// modernizer report, for the same reason — a threshold on a number rewards
// making the number move rather than making the system faster, and a
// benchmark's honest job is to make a suspicion checkable.
//
// So the set is deliberately four, on the paths where a regression would reach
// a user, rather than a sweep. This file holds the CEL one; the other three are
// in flowfile (parse-and-validate, and marshal) and in cmd/flow/internal/mcp
// (the descriptor to JSON-Schema projection).

// BenchmarkEval measures one compiled expression evaluated against an
// activation, which is the operation a workflow performs most: every `if:`,
// every `${…}` interpolation and every computed input on every step of every
// run goes through [Evaluator.Eval].
//
// The environment and the AST are built outside the timed region on purpose.
// Compiling is cached per profile in production ([Evaluator.Env] holds the
// environments, and a spec's expressions are compiled once), so timing it here
// would measure a cost a run does not pay per step and hide the one it does.
//
// It also stands under #885's cost estimator. Cost tracking is installed on
// every environment this evaluator builds (celenv.go's [cel.CostTracking] with
// [evaluationCostEstimator]), so it is charged on every evaluation in the
// system — including the ones whose cost is nowhere near the limit. The claim
// that the estimator is cheap enough to be always-on is exactly the kind of
// claim that decays silently, and this is where it becomes measurable: run it
// with `WithLimits` at zero cost and against the default and compare.
func BenchmarkEval(b *testing.B) {
	cases := []struct {
		name string
		expr string
	}{
		// A step condition: the shape `if:` compiles to, and the cheapest
		// thing the evaluator is asked to do.
		{"condition", `steps.fetch.status == 200 && vars.enabled`},

		// An interpolation over nested step output — the common read path, and
		// the one that walks an activation rather than a bare variable.
		{"nested_output", `steps.fetch.body.items[0].id`},

		// String building, which is where the byte-charging estimator in
		// celcost.go actually has something to charge for: the result's size is
		// what it prices, so this is the case whose cost is not a constant.
		{"string_build", `"run-" + vars.name + "-" + string(steps.fetch.status)`},
	}

	activation := map[string]any{
		"vars": map[string]any{
			"enabled": true,
			"name":    "nightly-reconciliation",
		},
		"steps": map[string]any{
			"fetch": map[string]any{
				"status": 200,
				"body": map[string]any{
					"items": []any{
						map[string]any{"id": "a1", "amount": 12.5},
						map[string]any{"id": "a2", "amount": 44.0},
					},
				},
			},
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			evaluator := NewEvaluator()

			env, err := evaluator.ProfileEnv(CurrentProfile)
			if err != nil {
				b.Fatalf("building the profile environment: %v", err)
			}

			// Parsed, not compiled, because that is what the engine
			// evaluates: `steps` and `vars` are resolved dynamically by
			// [StepsOutputActivation] rather than declared on the environment,
			// so a *checked* AST is not something this system produces for a
			// step expression — see [Evaluator.EvalParsedBase], the call
			// eval.go actually makes.
			ast, issues := env.Parse(tc.expr)
			if issues != nil && issues.Err() != nil {
				b.Fatalf("parsing %q: %v", tc.expr, issues.Err())
			}

			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := evaluator.Eval(ctx, env, ast, activation); err != nil {
					b.Fatalf("evaluating %q: %v", tc.expr, err)
				}
			}
		})
	}
}

// BenchmarkEvalProgramConstruction separates the half of [Evaluator.Eval] that
// is not evaluation.
//
// Eval calls env.Program on every call, so each evaluation pays for building a
// program as well as running it — and a benchmark of Eval alone cannot say
// which half a regression landed in. This times the construction on its own so
// the two are comparable. The split did come to justify caching programs —
// [Evaluator.EvalParsed] now reuses a compiled program per specification site,
// and BenchmarkEvalParsedCached below is the cached half of this measurement —
// while Eval itself stays uncached for the freshly built ASTs a REPL hands it,
// so this construction cost is still what that path pays per call.
func BenchmarkEvalProgramConstruction(b *testing.B) {
	evaluator := NewEvaluator()

	env, err := evaluator.ProfileEnv(CurrentProfile)
	if err != nil {
		b.Fatalf("building the profile environment: %v", err)
	}

	ast, issues := env.Parse(`steps.fetch.status == 200 && vars.enabled`)
	if issues != nil && issues.Err() != nil {
		b.Fatalf("parsing: %v", issues.Err())
	}

	options := evaluator.Limits().programOptions()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := env.Program(ast, options...); err != nil {
			b.Fatalf("building the program: %v", err)
		}
	}
}

// BenchmarkEvalParsedCached measures the path the engine actually takes per
// iteration — [Evaluator.EvalParsedBase] down to a program-cache hit — which
// is BenchmarkEval's condition case minus the per-call program construction.
// The gap between this number and BenchmarkEval's is what the cache buys on
// every `if:`, `items:`, `until:` and computed input past a site's first
// evaluation; if the two ever converge, the cache stopped hitting.
func BenchmarkEvalParsedCached(b *testing.B) {
	evaluator := NewEvaluator()

	env, err := evaluator.ProfileEnv(CurrentProfile)
	if err != nil {
		b.Fatalf("building the profile environment: %v", err)
	}

	ast, issues := env.Parse(`steps.fetch.status == 200 && vars.enabled`)
	if issues != nil && issues.Err() != nil {
		b.Fatalf("parsing: %v", issues.Err())
	}
	parsed, err := cel.AstToParsedExpr(ast)
	if err != nil {
		b.Fatalf("converting to a parsed expression: %v", err)
	}

	activation := map[string]any{
		"vars":  map[string]any{"enabled": true},
		"steps": map[string]any{"fetch": map[string]any{"status": 200}},
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := evaluator.EvalParsed(ctx, env, parsed, activation); err != nil {
			b.Fatalf("evaluating: %v", err)
		}
	}
}

// BenchmarkEvalString measures the whole parse-and-evaluate path an
// interpolation takes when it is evaluated from source rather than from a
// stored AST — [Evaluator.EvalString], which the interpolation scanner reaches
// for. It is the pessimistic end of the same measurement BenchmarkEval makes.
func BenchmarkEvalString(b *testing.B) {
	evaluator := NewEvaluator()
	ctx := context.Background()

	activation := map[string]any{
		"vars": map[string]any{"name": strings.Repeat("a", 32)},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := evaluator.EvalString(ctx, `"run-" + vars.name`, nil, activation); err != nil {
			b.Fatalf("evaluating: %v", err)
		}
	}
}
