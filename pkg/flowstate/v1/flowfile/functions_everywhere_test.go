package flowfile_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// A declared function in every expression position, from the file's side: that
// the positions the compiler reads as a value (`if`, `value`, `vars`, a trigger's
// `when:` and key, an output) and the ones the server reads as
// source text (`allow:` on a signal, on `debug:` and on a manual start) all expand
// the same way, that the stored form is plain CEL, and that the file writes back
// as authored.

const everywhereFile = `edition: v2026.4
name: everywhere
functions:
  isOps:
    params:
      team: string
    returns: bool
    body: ${team == "ops"}
  isOpened:
    params:
      action: string
    returns: bool
    body: ${action == "opened"}
  tier:
    params:
      count: int
    returns: string
    body: ${string(count * 2)}
inputs:
  size:
    type: int
    default: 1
triggers:
  - webhook: hook
    verify:
      stripe: ${secret('env:HOOK_SECRET')}
    when: ${isOpened(event.body.action)}
    idempotency_key: ${tier(event.body.size)}
  - manual:
      allow: ${isOps(sender.identity.claims.team)}
signals:
  go:
    allow: ${isOps(sender.identity.claims.team) && sender.identity.principal != run.identity.principal}
debug:
  allow: ${isOps(sender.identity.claims.team)}
vars:
  label: ${tier(3)}
steps:
  - id: a
    if: ${isOps("ops")}
    value: ${tier(inputs.size)}
  - id: w
    wait_for_signal:
      name: go
      timeout: 1h
outputs:
  size:
    value: ${tier(inputs.size)}
    type: string
`

// expressionsOf calls visit with every expression value a workflow holds.
func expressionsOf(m protoreflect.Message, visit func(*v1.Value)) {
	if value, ok := m.Interface().(*v1.Value); ok && value.GetExpr() != nil {
		visit(value)
	}

	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap() && fd.MapValue().Message() != nil:
			v.Map().Range(func(_ protoreflect.MapKey, e protoreflect.Value) bool {
				expressionsOf(e.Message(), visit)

				return true
			})
		case fd.IsList() && fd.Message() != nil:
			for i := range v.List().Len() {
				expressionsOf(v.List().Get(i).Message(), visit)
			}
		case fd.Message() != nil && !fd.IsList() && !fd.IsMap():
			expressionsOf(v.Message(), visit)
		}

		return true
	})
}

func TestAFunctionIsCallableInEveryExpressionPosition(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(everywhereFile))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf))
	require.NoError(t, v1.Validate(wf))

	set, errs := v1.NewFunctionSet(v1.CurrentProfile, wf.GetDeclaredFunctions())
	require.Empty(t, errs)

	// Nothing a run evaluates calls a declared function: every position holds the
	// plain CEL the call expanded to.
	seen := 0
	expressionsOf(wf.ProtoReflect(), func(value *v1.Value) {
		if set.Retains(value.GetExpr()) {
			seen++
		}
		assert.False(t, set.Calls(value.GetExpr()), "a call to a declared function survived into a stored expression")
	})
	assert.Equal(t, 6, seen, "the expressions that called a function were not expanded where they were written")

	for name, got := range map[string][2]string{
		"manual": {wf.GetTriggers().GetManual().GetAllow(), wf.GetTriggers().GetManual().GetAllowSource()},
		"signal": {wf.GetSignals()["go"].GetAllow(), wf.GetSignals()["go"].GetAllowSource()},
		"debug":  {wf.GetDebug().GetAllow(), wf.GetDebug().GetAllowSource()},
	} {
		allow, source := got[0], got[1]
		assert.NotEmpty(t, source, name)
		assert.NotContains(t, allow, "isOps", "%s: a call to the function survived into the predicate the server evaluates", name)
		assert.Contains(t, allow, "cel.bind(team, sender.identity.claims.team, team == \"ops\")", name)
		assert.NotEqual(t, source, allow, name)
	}
	assert.Equal(t, "isOps(sender.identity.claims.team)", wf.GetTriggers().GetManual().GetAllowSource())
	assert.Equal(t, "isOps(sender.identity.claims.team) && sender.identity.principal != run.identity.principal",
		wf.GetSignals()["go"].GetAllowSource())
}

func TestAFunctionInAnAllowWritesBackAsAuthoredAndIsAFixedPoint(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(everywhereFile))
	require.NoError(t, err)

	written, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	assert.Equal(t, everywhereFile, string(written), "the calls are written as the author wrote them, not as their expansion")

	formatted, err := flowfile.Format([]byte(everywhereFile), wf)
	require.NoError(t, err)
	assert.Equal(t, everywhereFile, string(formatted))

	again, _, err := flowfile.Parse(written)
	require.NoError(t, err)
	assert.True(t, proto.Equal(wf, again), "the written file compiles to a different workflow")
}

// What the server decides is the expansion: the same caller is admitted and
// refused by a predicate that calls a function as by the plain predicate.
func TestAnExpandedAllowDecidesAsThePlainPredicateDoes(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(everywhereFile))
	require.NoError(t, err)

	caller := func(team string) *v1.WorkloadIdentity {
		return &v1.WorkloadIdentity{Principal: &v1.Principal{
			Issuer: "https://issuer.example.com", Subject: "ada@example.com",
			Claims: v1.StringClaimValues(map[string]string{"team": team}),
		}}
	}
	starter := &v1.WorkloadIdentity{Principal: &v1.Principal{Issuer: "https://issuer.example.com", Subject: "grace@example.com"}}

	ctx := context.Background()
	for _, test := range []struct {
		team  string
		admit bool
	}{{"ops", true}, {"finance", false}, {"", false}} {
		name := fmt.Sprintf("team %q", test.team)
		who := caller(test.team)

		err := v1.SignalPolicyCheck(ctx, wf.GetSignals()["go"], caller(test.team), starter, true, nil)
		assert.Equal(t, test.admit, err == nil, "signal: %s: %v", name, err)

		err = v1.SignalPolicyCheck(ctx, wf.GetDebug(), caller(test.team), starter, true, nil)
		assert.Equal(t, test.admit, err == nil, "debug: %s: %v", name, err)

		err = v1.CheckManualStart(ctx, wf, who, v1.QualifiedSubject(who.GetPrincipal().GetIssuer(), who.GetPrincipal().GetSubject()), "", nil)
		assert.Equal(t, test.admit, err == nil, "manual: %s: %v", name, err)
	}
}

// The predicate the server evaluates is the expansion alone: a specification that
// carries a source with nothing expanded behind it is read by `allow`.
func TestAnAllowSourceIsNeverEvaluated(t *testing.T) {
	t.Parallel()

	wf, _, err := flowfile.Parse([]byte(everywhereFile))
	require.NoError(t, err)

	wf.GetSignals()["go"].AllowSource = proto.String("neverDeclared(sender.identity.claims.team)")
	wf.GetDebug().AllowSource = proto.String("neverDeclared(sender.identity.claims.team)")
	wf.GetTriggers().GetManual().AllowSource = proto.String("neverDeclared(sender.identity.claims.team)")
	require.Empty(t, flowfile.Validate(wf))
	require.NoError(t, v1.Validate(wf))
}

func TestAFunctionInAnAllowIsHeldToTheSameRulesAsAnywhere(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		from, to string
		want     string
	}{
		{
			name: "an undeclared function is refused",
			from: "allow: ${isOps(sender.identity.claims.team)}\nsignals:",
			to:   "allow: ${isNotDeclared(sender.identity.claims.team)}\nsignals:",
			want: "isNotDeclared",
		},
		{
			name: "an argument of the wrong type is refused where the call is written",
			from: "allow: ${isOps(sender.identity.claims.team)}\nsignals:",
			to:   "allow: ${isOps(1)}\nsignals:",
			want: "isOps",
		},
		{
			name: "a call with the wrong number of arguments is refused",
			from: "allow: ${isOps(sender.identity.claims.team)}\nsignals:",
			to:   "allow: ${isOps(sender.identity.claims.team, 1)}\nsignals:",
			want: "isOps",
		},
		{
			name: "a body that reads the caller is still refused",
			from: `body: ${team == "ops"}`,
			to:   `body: ${sender.identity.claims.team == "ops"}`,
			want: "a function sees only its parameters",
		},
		{
			name: "the narrowing rule is asked of the expansion, not of the arguments' absence",
			from: "allow: ${isOps(sender.identity.claims.team)}\nsignals:",
			to:   "allow: ${isOps(string(inputs.size))}\nsignals:",
			want: "reads `inputs` but not `sender.identity.claims`",
		},
		{
			name: "a predicate that is not a bool is still refused",
			from: "allow: ${isOps(sender.identity.claims.team)}\nsignals:",
			to:   "allow: ${tier(1)}\nsignals:",
			want: "bool",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			source := replaceOnce(t, everywhereFile, test.from, test.to)
			wf, _, err := flowfile.Parse([]byte(source))
			if err == nil {
				err = flowfile.Validate(wf).Err()
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}

// An expansion that is longer than the field holds is refused where it is written
// rather than at admission, and says why.
func TestAnExpandedAllowPastTheFieldsLengthIsRefused(t *testing.T) {
	t.Parallel()

	long := strings.Repeat(`team == "ops" || `, 130) + `team == "ops"`
	source := replaceOnce(t, everywhereFile, `body: ${team == "ops"}`, "body: ${"+long+"}")

	wf, _, err := flowfile.Parse([]byte(source))
	if err == nil {
		err = flowfile.Validate(wf).Err()
	}
	if err == nil {
		err = v1.Validate(wf)
	}
	require.Error(t, err)
	assert.Contains(t, err.Error(), "2048")
}

// An `allow:` spends the budget an expression does, so a few hundred predicates
// cannot copy a large composed function where a few hundred steps could not.
func TestAFunctionInAnAllowSharesTheFilesExpansionBudget(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	b.WriteString("edition: v2026.4\nname: t\nfunctions:\n  f0:\n    params:\n      k: int\n    returns: int\n    body: ${k + k}\n")
	for i := 1; i <= 11; i++ {
		fmt.Fprintf(&b, "  f%d:\n    params:\n      k: int\n    returns: int\n    body: ${f%d(k) + f%d(k + 1)}\n", i, i-1, i-1)
	}
	b.WriteString("signals:\n")
	for i := range 300 {
		fmt.Fprintf(&b, "  s%d:\n    allow: ${f11(size(sender.identity.principal)) > %d}\n", i, i)
	}
	b.WriteString("steps:\n  - id: a\n    value: ${1}\n")

	_, _, err := flowfile.Parse([]byte(b.String()))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expand past 100000 CEL nodes in this file altogether")
}

// The shared driver cases are held to the predicate the compiler stores, so this
// is the test that keeps them honest about what that is.
func TestAFunctionInAnAllowIsCompiledToThePlainPredicate(t *testing.T) {
	t.Parallel()

	const file = `edition: v2026.4
name: gate
functions:
  isReleaseTeam:
    params:
      team: string
    returns: bool
    body: ${team == "release-managers"}
steps:
  - id: ask
    wait_for_signal:
      name: deploy-approved
      timeout: 1h
signals:
  deploy-approved:
    allow: ${isReleaseTeam(sender.identity.claims.team)}
`

	wf, _, err := flowfile.Parse([]byte(file))
	require.NoError(t, err)
	require.Empty(t, flowfile.Validate(wf))

	policy := wf.GetSignals()["deploy-approved"]
	assert.Equal(t, conformance.FunctionAllowExpansion, policy.GetAllow(),
		"the shared driver cases are held to a predicate the compiler no longer produces")
	assert.Equal(t, conformance.FunctionAllowSource, policy.GetAllowSource())

	plain := replaceOnce(t, file, "isReleaseTeam(sender.identity.claims.team)", `sender.identity.claims.team == "release-managers"`)
	unexpanded, _, err := flowfile.Parse([]byte(plain))
	require.NoError(t, err)
	assert.Nil(t, unexpanded.GetSignals()["deploy-approved"].AllowSource, "a predicate with no call keeps no source")
	assert.Equal(t, `sender.identity.claims.team == "release-managers"`, unexpanded.GetSignals()["deploy-approved"].GetAllow(),
		"a predicate that calls nothing is stored exactly as written")
}
