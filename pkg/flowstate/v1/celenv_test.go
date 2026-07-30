package flowstatev1

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
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

// A profile names a frozen set, and both halves of that need checking.
//
// The membership below was first written from memory and was wrong in both
// directions — it invented two libraries this build does not have and omitted two
// it does. A list of names beside the thing it describes is a second source of
// truth, which is the failure this repo keeps finding in documentation; a list of
// names that decides what a stored expression *means* is the same failure with a
// run attached.

// TestEveryProfileNamesRealLibraries holds forever, for every profile.
//
// A profile naming a library this build does not have is a spec whose vocabulary
// cannot be assembled, which surfaces as a run failing to evaluate an expression
// that was checked when it was written.
func TestEveryProfileNamesRealLibraries(t *testing.T) {
	t.Parallel()

	available := map[string]bool{}
	for _, name := range ExtensionLibraries() {
		available[name] = true
	}

	for profile, libs := range profiles {
		for _, name := range libs {
			if !available[name] {
				t.Errorf("profile %q names CEL library %q, which this build does not have\n"+
					"  available: %s", profile, name, strings.Join(ExtensionLibraries(), ", "))
			}
		}
	}
}

// TestCurrentProfileCoversEveryLibrary is deliberately a forcing function rather
// than an invariant.
//
// It is true today only because profiles were introduced when every library
// existed, and it is *meant* to fail the day a library is added — at which point
// the person adding it has a decision to make, and this test is where they are
// told to make it:
//
//   - while nothing is released, add the library to the current profile
//   - once a release exists, add a *new* profile, because a spec already recorded
//     as CurrentProfile was checked against a vocabulary that did not include it
//
// Silently growing the current profile after a release is the failure this whole
// mechanism exists to prevent, so the choice belongs to a human rather than to a
// derived list.
func TestCurrentProfileCoversEveryLibrary(t *testing.T) {
	t.Parallel()

	libs, err := ProfileLibraries(CurrentProfile)
	if err != nil {
		t.Fatalf("the current profile does not resolve: %v", err)
	}

	inProfile := map[string]bool{}
	for _, name := range libs {
		inProfile[name] = true
	}

	for _, name := range ExtensionLibraries() {
		if !inProfile[name] {
			t.Errorf("CEL library %q exists and profile %q does not include it\n"+
				"  if nothing is released yet, add it to that profile\n"+
				"  if a release exists, add a new profile instead — a spec recorded as %q was\n"+
				"  checked against a vocabulary without %q, and must keep meaning that",
				name, CurrentProfile, CurrentProfile, name)
		}
	}
}

// TestAnUnknownProfileIsRefused covers the fail-closed direction.
//
// A worker that cannot resolve the vocabulary a spec was compiled against does
// not know what its expressions mean. Falling back to whatever this build has is
// how a run quietly starts computing something else.
func TestAnUnknownProfileIsRefused(t *testing.T) {
	t.Parallel()

	if _, err := ProfileLibraries("2099.9"); err == nil {
		t.Fatal("an unknown profile resolved; a worker would evaluate a spec it cannot read")
	}

	// The empty profile is the one exception, and it is a compatibility arm rather
	// than a guess: nothing has been released, so a spec without a profile can only
	// be one this build compiled before the field existed.
	if _, err := ProfileLibraries(""); err != nil {
		t.Fatalf("a spec with no recorded profile was refused: %v", err)
	}
}

// The engine has to honour the profile a spec recorded, and that is a different
// claim from `ProfileLibraries` resolving a name.
//
// The first attempt at profiles recorded `Workflow.profile` and then hardcoded
// `CurrentProfile` at both evaluation sites, so the field was written and never
// read. Every test written for it passed, because every one of them called
// `ProfileLibraries` directly — the function that was correct — rather than
// running a workflow. These go through execution instead.

// TestARunEvaluatesAgainstTheProfileItsSpecRecords is the positive direction.
func TestARunEvaluatesAgainstTheProfileItsSpecRecords(t *testing.T) {
	t.Parallel()

	// `upperAscii` comes from the `strings` library, which the current profile
	// includes and the bare environment does not. It has to be an *expression* for
	// this to mean anything: a literal input is carried rather than evaluated, so a
	// step holding one never asks which vocabulary it was compiled against.
	workflow := &Workflow{
		Name:    "profiled",
		Profile: CurrentProfile,
		Steps: []*Node{
			{Id: "greet", Kind: &Node_Task{Task: &Task{
				Name:   "log",
				Inputs: map[string]*Value{"message": NewExpr(`"hi".upperAscii()`)},
			}}},
		},
	}

	if _, err := Run(t.Context(), workflow); err != nil {
		t.Fatalf("a workflow recording the current profile failed to run: %v", err)
	}
}

// TestARunWithAnUnknownProfileIsRefused is the direction that proves the recorded
// value is read at all.
//
// A spec compiled by a build this worker is older than names a profile it cannot
// assemble. Executing it anyway means evaluating expressions against a vocabulary
// nobody checked them with — so the run has to stop, and it has to stop because of
// the *recorded* name rather than anything about this build.
//
// This is the test whose absence let a recorded-and-ignored field ship: with the
// profile hardcoded, a spec saying "2099.9" ran perfectly.
func TestARunWithAnUnknownProfileIsRefused(t *testing.T) {
	t.Parallel()

	workflow := &Workflow{
		Name:    "from-the-future",
		Profile: "2099.9",
		Steps: []*Node{
			{Id: "compute", Kind: &Node_Task{Task: &Task{
				Name: "log",
				Inputs: map[string]*Value{
					"message": NewExpr(`"the sum is " + string(1 + 1)`),
				},
			}}},
		},
	}

	_, err := Run(t.Context(), workflow)
	if err == nil {
		t.Fatal("a spec naming a profile this build cannot assemble ran anyway;\n" +
			"  its expressions were evaluated against a vocabulary nobody checked them with")
	}
	if !strings.Contains(err.Error(), "2099.9") {
		t.Errorf("the refusal does not name the profile that caused it: %v", err)
	}
}

// TestAScopeCarriesTheProfileThroughNesting covers the derived scopes.
//
// A loop body and a parallel branch build their own scopes from the enclosing
// one. A copy that dropped the profile would leave the body evaluating against
// the empty string — which resolves, quietly, to the first profile — so the
// failure would appear only once a second profile existed, which is exactly when
// nobody would be looking for it.
func TestAScopeCarriesTheProfileThroughNesting(t *testing.T) {
	t.Parallel()

	root := NewScope("2026.1", nil)

	if got := root.WithLocal("item", NewLiteral(&expr.Value{Kind: &expr.Value_Int64Value{Int64Value: 1}})).GetProfile(); got != "2026.1" {
		t.Errorf("WithLocal dropped the profile: got %q", got)
	}
	if got := root.WithAmbientVars(map[string]*Value{"region": NewLiteral("eu")}).GetProfile(); got != "2026.1" {
		t.Errorf("WithAmbientVars dropped the profile: got %q", got)
	}
	if got := root.WithOutputs(nil).GetProfile(); got != "2026.1" {
		t.Errorf("WithOutputs dropped the profile: got %q", got)
	}
}

// TestAnUnrecordedProfilePinsTheOriginalVocabulary is the legacy-run guarantee.
//
// A spec written before the profile field existed carries the empty string, and
// resolving that to whatever is *current* would hand it a new vocabulary on every
// release — which is the pinning failure this mechanism exists to prevent, aimed
// at the runs least able to survive it. It resolves to the original profile
// instead, forever.
//
// Asserted against [OriginalProfile] rather than against the literal set, so it
// keeps meaning this after CurrentProfile advances. Reported in review, where the
// first version of this resolved an empty profile to CurrentProfile.
func TestAnUnrecordedProfilePinsTheOriginalVocabulary(t *testing.T) {
	t.Parallel()

	unrecorded, err := ProfileLibraries("")
	if err != nil {
		t.Fatalf("a spec with no recorded profile was refused: %v", err)
	}

	original, err := ProfileLibraries(OriginalProfile)
	if err != nil {
		t.Fatalf("the original profile does not resolve: %v", err)
	}

	if !slices.Equal(unrecorded, original) {
		t.Errorf("an unrecorded profile resolved to %v, want the original %v", unrecorded, original)
	}
}

// TestAParallelBranchKeepsTheProfile covers the scope the local driver builds for
// each branch.
//
// It was constructed by hand — `&Scope{Outputs: ..., Vars: ...}` — which named the
// two fields somebody was thinking about and silently omitted the profile. Every
// expression inside a parallel branch therefore resolved the empty profile: an
// unknown recorded profile would run instead of being refused, and once a second
// profile existed an older workflow would quietly use the current vocabulary,
// making local execution disagree with the durable engine.
//
// Reported in review. My own derived-scope test did not reach it, because that
// one exercises WithAmbientVars and WithOutputs and this site used neither.
func TestAParallelBranchKeepsTheProfile(t *testing.T) {
	t.Parallel()

	// An unknown profile is the sharpest probe: if the branch keeps it, the run is
	// refused; if the branch drops it, the empty string resolves and the branch
	// runs against a vocabulary nobody checked it with.
	workflow := &Workflow{
		Name:    "branching",
		Profile: "2099.9",
		Steps: []*Node{{
			Id: "fan",
			Kind: &Node_Parallel{Parallel: &Parallel{
				Branches: []*Parallel_Branch{{
					Steps: []*Node{{
						Id: "compute",
						Kind: &Node_Task{Task: &Task{
							Name: "log",
							Inputs: map[string]*Value{
								"message": NewExpr(`"the sum is " + string(1 + 1)`),
							},
						}},
					}},
				}},
			}},
		}},
	}

	if _, err := Run(t.Context(), workflow); err == nil {
		t.Fatal("a parallel branch ran against a profile this build cannot assemble;\n" +
			"  the branch scope dropped the profile, so the empty string resolved instead")
	}
}
