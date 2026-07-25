package flowstatev1

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestEvaluatorEnvCaching(t *testing.T) {
	e := NewEvaluator()

	base1, err := e.Env()
	if err != nil {
		t.Fatalf("Env() error: %v", err)
	}
	base2, err := e.Env()
	if err != nil {
		t.Fatalf("Env() error: %v", err)
	}
	if base1 != base2 {
		t.Error("base environment was rebuilt; environments must be cached")
	}

	// Case and ordering must not produce distinct environments, otherwise a
	// workflow could evade the cache and pay construction cost per step.
	a, err := e.Env("math", "strings")
	if err != nil {
		t.Fatalf("Env(math, strings) error: %v", err)
	}
	b, err := e.Env("STRINGS", "Math", "math")
	if err != nil {
		t.Fatalf("Env(STRINGS, Math, math) error: %v", err)
	}
	if a != b {
		t.Error("equivalent library sets produced different environments")
	}
	if a == base1 {
		t.Error("library set produced the base environment")
	}
}

func TestEvaluatorEnvUnknownLibrary(t *testing.T) {
	e := NewEvaluator()

	_, err := e.Env("definitely-not-a-library")
	if err == nil {
		t.Fatal("expected an error for an unknown extension library")
	}
	// The message must name what is available, or a typo is a dead end for the
	// workflow author.
	if !strings.Contains(err.Error(), "math") {
		t.Errorf("error does not list available libraries: %v", err)
	}
}

// TestEvaluatorCostLimit is a regression test for unbounded CEL evaluation. A
// security review verified that an expression of this shape allocated gigabytes
// of heap and ran for seconds, ignoring its context deadline entirely.
func TestEvaluatorCostLimit(t *testing.T) {
	e := NewEvaluator()

	tests := []struct {
		name string
		expr string
		libs []string
	}{
		{
			name: "large range allocation",
			expr: "size(lists.range(50000000))",
			libs: []string{"lists"},
		},
		{
			name: "nested comprehension blowup",
			expr: "size([1,2,3,4,5,6,7,8,9,10].map(a, [1,2,3,4,5,6,7,8,9,10].map(b, " +
				"[1,2,3,4,5,6,7,8,9,10].map(c, [1,2,3,4,5,6,7,8,9,10].map(d, " +
				"[1,2,3,4,5,6,7,8,9,10].map(e, [1,2,3,4,5,6,7,8,9,10].map(f, a+b+c+d+e+f)))))))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			start := time.Now()
			_, err := e.EvalString(ctx, tt.expr, tt.libs, map[string]any{})
			elapsed := time.Since(start)

			if err == nil {
				t.Fatal("expected the expression to be rejected by the cost limit")
			}
			// The point of the limit is that it trips quickly. If this takes
			// seconds, the budget is not actually bounding the work.
			if elapsed > 5*time.Second {
				t.Errorf("cost limit took %v to trip; expected it to fail fast", elapsed)
			}
			t.Logf("rejected in %v: %v", elapsed, err)
		})
	}
}

// TestEvaluatorContextCancellation verifies that a caller's deadline actually
// stops evaluation. Previously expressions were evaluated with Eval rather than
// ContextEval, so a canceled context was ignored until evaluation finished on
// its own.
func TestEvaluatorContextCancellation(t *testing.T) {
	// A large cost budget ensures cancellation, not the cost limit, is what
	// ends this evaluation.
	e := NewEvaluator(WithLimits(Limits{
		Cost:                    0,
		InterruptCheckFrequency: DefaultInterruptCheckFrequency,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Already canceled before evaluation begins.

	// This expression must be expensive but not caught by any library's own
	// input guard, so that cancellation is demonstrably what stops it.
	const expensive = "size([1,2,3,4,5,6,7,8,9,10].map(a, [1,2,3,4,5,6,7,8,9,10].map(b, " +
		"[1,2,3,4,5,6,7,8,9,10].map(c, [1,2,3,4,5,6,7,8,9,10].map(d, " +
		"[1,2,3,4,5,6,7,8,9,10].map(e, [1,2,3,4,5,6,7,8,9,10].map(f, a+b+c+d+e+f)))))))"

	_, err := e.EvalString(ctx, expensive, nil, map[string]any{})
	if err == nil {
		t.Fatal("expected evaluation to be interrupted by the canceled context")
	}
	if !strings.Contains(err.Error(), "cancel") && !strings.Contains(err.Error(), "context") {
		t.Errorf("error does not indicate cancellation, so the limit may have tripped instead: %v", err)
	}
	t.Logf("interrupted: %v", err)
}

func TestEvaluatorEvalString(t *testing.T) {
	e := NewEvaluator()
	ctx := context.Background()

	tests := []struct {
		name string
		expr string
		libs []string
		vars map[string]any
		want any
	}{
		{
			name: "arithmetic",
			expr: "1 + 2",
			want: int64(3),
		},
		{
			name: "variable reference",
			expr: "greeting + ' world'",
			vars: map[string]any{"greeting": "hello"},
			want: "hello world",
		},
		{
			name: "strings library",
			expr: "'a-b-c'.split('-')[1]",
			libs: []string{"strings"},
			want: "b",
		},
		{
			name: "json library parses an object",
			expr: `json_parse('{"name":"flowstate"}')['name']`,
			libs: []string{"json"},
			want: "flowstate",
		},
		{
			name: "json library is unavailable unless enabled",
			expr: `json_parse('{}')`,
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := tt.vars
			if vars == nil {
				vars = map[string]any{}
			}
			out, err := e.EvalString(ctx, tt.expr, tt.libs, vars)
			if tt.want == nil {
				if err == nil {
					t.Fatalf("expected an error, got result %v", out)
				}
				return
			}
			if err != nil {
				t.Fatalf("EvalString() error: %v", err)
			}
			if got := out.Value(); got != tt.want {
				t.Errorf("got %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestExtensionLibrariesAreSortedAndBuildable(t *testing.T) {
	e := NewEvaluator()
	names := ExtensionLibraries()
	if len(names) == 0 {
		t.Fatal("no extension libraries reported")
	}
	for i, name := range names {
		if i > 0 && names[i-1] >= name {
			t.Errorf("library names are not sorted: %q before %q", names[i-1], name)
		}
		// Every advertised library must actually produce an environment;
		// otherwise documentation and completion promise something broken.
		if _, err := e.Env(name); err != nil {
			t.Errorf("advertised library %q failed to build: %v", name, err)
		}
	}
}
