package flowstatev1_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestTaskPolicyZeroCase pins #187's zero case: no policy configured
// restricts nothing, exactly current behavior. [v1.TaskPolicyIn] falling back
// to nil, and nil's [v1.TaskPolicy.Check] permitting everything, is what
// keeps an existing deployment's next run from being denied the day this
// ships.
func TestTaskPolicyZeroCase(t *testing.T) {
	require.Nil(t, v1.DefaultTaskPolicy(), "no policy installed at test start")

	err := v1.CheckTaskPolicy(context.Background(), "codex.exec", &v1.WorkloadIdentity{
		Subject:   "anyone@example.com",
		Namespace: "any-namespace",
	}, false)
	require.NoError(t, err, "no policy configured must permit every dispatch")
}

// TestTaskPolicyDenyRuleDenies is the core fail-closed matrix case: a
// configured deny rule that matches refuses the dispatch, and the error names
// the task, the policy source (the rule text), and the remedy — CLAUDE.md's
// "Diagnostics are a feature" applied to a deployment refusal.
func TestTaskPolicyDenyRuleDenies(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`task == "codex.exec" && identity.namespace != "platform"`},
	}
	policy, err := cfg.Policy()
	require.NoError(t, err)

	err = policy.Check(context.Background(), "codex.exec", &v1.WorkloadIdentity{
		Namespace: "not-platform",
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, v1.ErrTaskPolicyDenied))

	var denied *v1.TaskPolicyDeniedError
	require.True(t, errors.As(err, &denied))
	require.Equal(t, "codex.exec", denied.Task)
	require.Equal(t, v1.TaskPolicyReasonDenyRule, denied.Reason)
	require.Contains(t, denied.Detail, `identity.namespace != "platform"`,
		"the denial must name the rule that fired, so an operator can find it")
	// Which task was refused is read from the structure, not from this
	// sentence: [v1.CheckTaskPolicy] is what renders the name, by classifying
	// the denial as a [v1.TaskError] over it, and this bare Check is a level
	// below that. Asserting it on the string here passed either way — the rule
	// text above quotes "codex.exec" — which is precisely why the naming was
	// free to be duplicated for as long as it was (#184, see
	// [TestDenialNamesTheTaskExactlyOnce]).
	require.Contains(t, err.Error(), "task-shape policy",
		"the message must read as a deployment refusal, not a task failure")
	require.Contains(t, err.Error(), "contact the operator",
		"the message must say what to do about the denial, not only that it happened")
}

// TestTaskPolicyDenyRuleAllowsOutsideMatch is the negative-direction pair of
// [TestTaskPolicyDenyRuleDenies], per CLAUDE.md's "test that A cannot reach
// B, not that A can reach A": a deny rule scoped to non-platform callers must
// let a platform caller *through*, proving the rule discriminates rather
// than merely existing.
func TestTaskPolicyDenyRuleAllowsOutsideMatch(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`task == "codex.exec" && identity.namespace != "platform"`},
	}
	policy, err := cfg.Policy()
	require.NoError(t, err)

	err = policy.Check(context.Background(), "codex.exec", &v1.WorkloadIdentity{
		Namespace: "platform",
	})
	require.NoError(t, err, "a platform caller must not be caught by a rule scoped to non-platform ones")

	// And a task the rule never names dispatches regardless of namespace.
	err = policy.Check(context.Background(), "log", &v1.WorkloadIdentity{
		Namespace: "not-platform",
	})
	require.NoError(t, err, "a deny rule scoped to codex.exec must not reach an unrelated task")
}

// TestTaskPolicyAllowlistDeniesNoMatch checks the allowlist half: configuring
// any allow rule turns the policy into an allowlist, and a dispatch matching
// none of them is denied with [v1.TaskPolicyReasonNoAllowRule].
func TestTaskPolicyAllowlistDeniesNoMatch(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Allow: []string{`task == "log"`},
	}
	policy, err := cfg.Policy()
	require.NoError(t, err)

	require.NoError(t, policy.Check(context.Background(), "log", &v1.WorkloadIdentity{}))

	err = policy.Check(context.Background(), "codex.exec", &v1.WorkloadIdentity{})
	require.Error(t, err)
	var denied *v1.TaskPolicyDeniedError
	require.True(t, errors.As(err, &denied))
	require.Equal(t, v1.TaskPolicyReasonNoAllowRule, denied.Reason)
}

// TestTaskPolicyRuleErrorDenies is the "errored rule denies" cell of the
// fail-closed matrix: a rule that type-checks (so it compiles and loads
// successfully) but fails at evaluation time — here, integer division by
// zero, which CEL only catches at runtime — must refuse the dispatch rather
// than let it through because the policy "couldn't decide."
func TestTaskPolicyRuleErrorDenies(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`1 / 0 == 0`},
	}
	policy, err := cfg.Policy()
	require.NoError(t, err, "a rule that only fails at runtime must still compile and load")

	err = policy.Check(context.Background(), "log", &v1.WorkloadIdentity{})
	require.Error(t, err, "a rule that cannot be evaluated must deny, not permit")

	var denied *v1.TaskPolicyDeniedError
	require.True(t, errors.As(err, &denied))
	require.Equal(t, v1.TaskPolicyReasonRuleError, denied.Reason)
}

// TestTaskPolicyLoadRefusesInvalidRule is the "policy load error = startup
// refusal" cell: a rule that does not compile must refuse at
// [v1.TaskPolicyConfig.Policy] — configuration load — never lazily on the
// first dispatch that happens to reach it.
func TestTaskPolicyLoadRefusesInvalidRule(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`task == `}, // syntactically invalid
	}
	_, err := cfg.Policy()
	require.Error(t, err)
	require.True(t, errors.Is(err, v1.ErrInvalidTaskPolicy))
}

// TestTaskPolicyLoadRefusesNonBoolRule checks the same load-time refusal for
// a rule that compiles but does not type-check to bool — a rule that
// evaluates to a string, say, which CEL happily compiles but which a
// deny/allow decision cannot use.
func TestTaskPolicyLoadRefusesNonBoolRule(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`task`}, // evaluates to a string, not a bool
	}
	_, err := cfg.Policy()
	require.Error(t, err)
	require.True(t, errors.Is(err, v1.ErrInvalidTaskPolicy))
}

// TestTaskPolicyLoadRefusesUnknownField checks that a rule naming a field the
// activation does not declare — a misspelling, or `inputs.*` before slice 2
// lands — is refused at load. This is the "a rule referencing a nonexistent
// input fails at load, not silently at runtime" property #187 names,
// narrowed to slice 1's actual surface (identity and task).
func TestTaskPolicyLoadRefusesUnknownField(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`identity.nmaespace == "platform"`}, // misspelled
	}
	_, err := cfg.Policy()
	require.Error(t, err)
	require.True(t, errors.Is(err, v1.ErrInvalidTaskPolicy))
}

// TestTaskPolicyLoadRefusesEmptyConfig checks that a config with no rules at
// all is refused rather than silently accepted as a no-op — an operator who
// pointed --task-policy at a file meant it to restrict something, and a file
// that restricts nothing is almost certainly a mistake, not an intentional
// wide-open policy.
func TestTaskPolicyLoadRefusesEmptyConfig(t *testing.T) {
	_, err := v1.TaskPolicyConfig{}.Policy()
	require.Error(t, err)
	require.True(t, errors.Is(err, v1.ErrInvalidTaskPolicy))
}

// TestTaskPolicyParseConfigRejectsUnknownKeys checks that the YAML decoder
// used by `flow worker --task-policy` is strict, mirroring
// [netpolicy.ParseConfig] and auth's ParsePolicy: a misspelled key in an
// operator's file must fail loudly at load rather than silently drop the
// restriction the operator meant to write.
func TestTaskPolicyParseConfigRejectsUnknownKeys(t *testing.T) {
	_, err := v1.ParseTaskPolicyConfig([]byte("denny:\n  - \"true\"\n"))
	require.Error(t, err)
	require.True(t, errors.Is(err, v1.ErrInvalidTaskPolicy))
}

// TestTaskPolicyContextOverridesDefault checks that a context-scoped policy
// (set by [v1.NewContextWithTaskPolicy]) takes precedence over the
// process-wide default, and that a context explicitly carrying nil forces
// "no policy" for that one call even while a process default is installed —
// [v1.TaskPolicyFromContext]'s own documented behavior.
func TestTaskPolicyContextOverridesDefault(t *testing.T) {
	restrictive, err := v1.TaskPolicyConfig{Deny: []string{"true"}}.Policy()
	require.NoError(t, err)

	v1.SetDefaultTaskPolicy(restrictive)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	// The process default denies everything.
	require.Error(t, v1.CheckTaskPolicy(context.Background(), "log", &v1.WorkloadIdentity{}, false))

	// A context carrying nil overrides it back to unrestricted for this call.
	ctx := v1.NewContextWithTaskPolicy(context.Background(), nil)
	require.NoError(t, v1.CheckTaskPolicy(ctx, "log", &v1.WorkloadIdentity{}, false))

	// A context carrying its own permissive policy also overrides the
	// restrictive default.
	permissive, err := v1.TaskPolicyConfig{Allow: []string{"true"}}.Policy()
	require.NoError(t, err)
	ctx = v1.NewContextWithTaskPolicy(context.Background(), permissive)
	require.NoError(t, v1.CheckTaskPolicy(ctx, "log", &v1.WorkloadIdentity{}, false))
}

// TestTaskPolicyNilIdentityReadsEmpty checks that a nil identity — what a
// local run and a legacy scope both carry — renders as every field empty
// rather than panicking or being treated specially, matching how
// [Scope.identity] itself documents a nil identity.
func TestTaskPolicyNilIdentityReadsEmpty(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`identity.subject == "" && identity.namespace == ""`},
	}
	policy, err := cfg.Policy()
	require.NoError(t, err)

	err = policy.Check(context.Background(), "log", nil)
	require.Error(t, err, "a nil identity must read as empty fields, which the rule above denies")
}

// TestTaskPolicyClassifiedNonRetryable checks that a denial is classified
// [v1.ErrorKindPolicyDenied], the same classification egress and secret
// denials carry — not the [v1.ErrorKindInternal] default an unclassified
// error would fall through to, which [v1.ErrorKind.Retryable] treats as
// worth retrying. Retrying a denied dispatch would repeat a decision that
// cannot change between attempts.
func TestTaskPolicyClassifiedNonRetryable(t *testing.T) {
	cfg := v1.TaskPolicyConfig{Deny: []string{"true"}}
	policy, err := cfg.Policy()
	require.NoError(t, err)
	v1.SetDefaultTaskPolicy(policy)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	err = v1.CheckTaskPolicy(context.Background(), "log", &v1.WorkloadIdentity{}, false)
	require.Error(t, err)
	require.Equal(t, v1.ErrorKindPolicyDenied, v1.ClassifyError(err))
	require.False(t, v1.ClassifyError(err).Retryable())
}

// TestLocalOnlyChangesTheMessageNotTheDecision is the test #652 item 3 asks
// for by name: the same identity against the same policy, once with
// local:true and once with local:false, must produce the identical
// allow/deny decision — same [TaskPolicyReason], same rule [Detail] — with
// only [TaskPolicyDeniedError.Error]'s string differing. Proves
// [CheckTaskPolicy]'s local parameter is exactly what its doc comment claims
// and nothing more: informational, never load-bearing.
func TestLocalOnlyChangesTheMessageNotTheDecision(t *testing.T) {
	cfg := v1.TaskPolicyConfig{
		Deny: []string{`task == "codex.exec" && identity.namespace != "platform"`},
	}
	policy, err := cfg.Policy()
	require.NoError(t, err)
	v1.SetDefaultTaskPolicy(policy)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	identity := &v1.WorkloadIdentity{Namespace: "not-platform"}

	rehearsalErr := v1.CheckTaskPolicy(context.Background(), "codex.exec", identity, true)
	productionErr := v1.CheckTaskPolicy(context.Background(), "codex.exec", identity, false)

	require.Error(t, rehearsalErr)
	require.Error(t, productionErr)

	// Same classification on both — a rehearsal denial is exactly as
	// non-retryable as a production one.
	require.Equal(t, v1.ClassifyError(productionErr), v1.ClassifyError(rehearsalErr))

	var rehearsalDenied, productionDenied *v1.TaskPolicyDeniedError
	require.True(t, errors.As(rehearsalErr, &rehearsalDenied))
	require.True(t, errors.As(productionErr, &productionDenied))

	// Same decision: identical task, reason, and the rule responsible.
	require.Equal(t, productionDenied.Task, rehearsalDenied.Task)
	require.Equal(t, productionDenied.Reason, rehearsalDenied.Reason)
	require.Equal(t, productionDenied.Detail, rehearsalDenied.Detail)

	// Only Local, and therefore only the message, differs.
	require.True(t, rehearsalDenied.Local)
	require.False(t, productionDenied.Local)
	require.NotEqual(t, rehearsalErr.Error(), productionErr.Error(),
		"a rehearsal denial must read differently from a production one")
	require.Contains(t, rehearsalErr.Error(), "local rehearsal",
		"a rehearsal denial must say it came from a rehearsal")
	require.NotContains(t, productionErr.Error(), "local rehearsal",
		"a production denial must not claim to be a rehearsal")
}

// TestDenialSaysWhichIdentityItEvaluated is #652 item 3's other half, and the
// one [v1.TaskPolicyDeniedError.Local] could not supply on its own.
//
// #877 documented that a rehearsal's `run.identity` is empty whatever a
// `flow test` case's `starter:` says, which turns a rule reading
// `identity.namespace` into a blanket refusal in that venue. Before this, the
// resulting denial was textually identical to one where the rule looked at a
// real identity and rejected it — so the author's next move ("change the
// starter", "check my namespace") could not be derived from what they were
// told. This asserts the two now read differently, in the direction that
// matters: the empty case must say it had nothing to match.
func TestDenialSaysWhichIdentityItEvaluated(t *testing.T) {
	cfg := v1.TaskPolicyConfig{Allow: []string{`identity.namespace == "platform"`}}
	policy, err := cfg.Policy()
	require.NoError(t, err)
	v1.SetDefaultTaskPolicy(policy)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	// A rehearsal that named nobody — `flow run local` with no `--as-*`, or
	// any `flow test` case, whose `starter:` never reaches this policy.
	anonymous := v1.CheckTaskPolicy(context.Background(), "codex.exec", nil, true)
	require.Error(t, anonymous)

	// A rehearsal that named somebody the allowlist still refuses. Same
	// policy, same task, same venue: only the identity differs.
	named := v1.CheckTaskPolicy(context.Background(), "codex.exec",
		&v1.WorkloadIdentity{Namespace: "not-platform", Subject: "spiffe://acme/deployer"}, true)
	require.Error(t, named)

	anonymousDenied, ok := errors.AsType[*v1.TaskPolicyDeniedError](anonymous)
	require.True(t, ok)
	namedDenied, ok := errors.AsType[*v1.TaskPolicyDeniedError](named)
	require.True(t, ok)

	// Same decision — provenance changes what the denial says, never what it
	// decides, exactly as Local does.
	require.Equal(t, namedDenied.Reason, anonymousDenied.Reason)
	require.Equal(t, namedDenied.Detail, anonymousDenied.Detail)

	// The negative direction, and the whole point: an author cannot be told
	// the same thing in both cases.
	require.NotEqual(t, anonymous.Error(), named.Error(),
		"a denial for want of any identity must not read like one where a rule matched an identity")

	require.Contains(t, anonymous.Error(), "no identity",
		"a denial evaluated against no identity must say so")
	require.Contains(t, anonymous.Error(), "--as-subject",
		"and must name how a rehearsal comes to have one")
	require.Contains(t, anonymous.Error(), "flow test",
		"and must say `flow test` has no way to supply one, since that is the venue #877 documented")

	require.Contains(t, namedDenied.Error(), `namespace="not-platform"`,
		"a denial that did evaluate an identity must name it")
	require.NotContains(t, namedDenied.Error(), "no identity",
		"a denial that evaluated a real identity must not claim it had none")
	require.NotContains(t, namedDenied.Error(), "--as-subject",
		"and must not send its reader hunting for an identity flag they already used")
}

// TestDenialRemedyIsTrueInEveryLocalVenue guards a false diagnostic that
// shipped with the rehearsal clause: it told the reader to check "the
// --task-policy passed to this local invocation", and `flow test` — one of
// the four venues [v1.TaskPolicyDeniedError.Local] speaks for — takes no such
// flag (#652 item 2), inheriting instead whatever the hosting process
// installed. CLAUDE.md rates a false diagnostic worse than a missing one, so
// the clause must name the process, not a flag only some of these commands
// accept.
func TestDenialRemedyIsTrueInEveryLocalVenue(t *testing.T) {
	denied := &v1.TaskPolicyDeniedError{
		Task:     "codex.exec",
		Reason:   v1.TaskPolicyReasonDenyRule,
		Detail:   `task == "codex.exec"`,
		Local:    true,
		Identity: "no identity — every field a rule can read is empty",
	}

	require.NotContains(t, denied.Error(), "--task-policy",
		"the rehearsal remedy must not name a flag `flow test` does not accept")
	require.Contains(t, denied.Error(), "the task-shape policy this process installed",
		"it must point at the process's installed policy instead")
}

// TestDenialNeverRendersAClaimValue is the containment half. This message
// travels wherever the denial does, which durably means through Temporal's
// failure conversion and into workflow history — durable and broadly
// readable, which is the property CLAUDE.md's "secrets never enter workflow
// history" section is about. A claim is caller-supplied data of a shape this
// package does not constrain, so provenance renders claim *keys* and never
// values.
func TestDenialNeverRendersAClaimValue(t *testing.T) {
	const claimValue = "value-that-must-not-be-rendered"

	cfg := v1.TaskPolicyConfig{Deny: []string{`task == "codex.exec"`}}
	policy, err := cfg.Policy()
	require.NoError(t, err)
	v1.SetDefaultTaskPolicy(policy)
	t.Cleanup(func() { v1.SetDefaultTaskPolicy(nil) })

	err = v1.CheckTaskPolicy(context.Background(), "codex.exec", &v1.WorkloadIdentity{
		Namespace: "team-a",
		Claims:    map[string]string{"session_token": claimValue},
	}, false)
	require.Error(t, err)

	denied, ok := errors.AsType[*v1.TaskPolicyDeniedError](err)
	require.True(t, ok)

	// The key is useful — it answers "did the claim my rule reads travel at
	// all" — and is already known to whoever wrote the rule.
	require.Contains(t, denied.Error(), "session_token",
		"the claim keys carried are provenance an author needs")

	// Every containment shape CLAUDE.md enumerates: the value directly, in a
	// struct holding it, and in a slice of those.
	holder := struct{ Err error }{Err: denied}
	for _, rendered := range []string{
		denied.Error(),
		err.Error(),
		fmt.Sprintf("%v", denied),
		fmt.Sprintf("%+v", denied),
		fmt.Sprintf("%#v", denied),
		// Spelled as the %s verb rather than a call to Error(), because %s
		// is what an operator's own log line writes and is therefore the
		// thing under test; calling Error() here would assert something the
		// two lines above already assert.
		fmt.Sprintf("%s", denied),
		fmt.Sprintf("%v", holder),
		fmt.Sprintf("%+v", holder),
		fmt.Sprintf("%#v", holder),
		fmt.Sprintf("%v", []any{holder}),
		fmt.Sprintf("%+v", []any{holder}),
	} {
		require.NotContains(t, rendered, claimValue,
			"a claim value must never reach a denial message, which travels into workflow history")
	}
}

// TestDenialNamesTheTaskExactlyOnce is #184's garbling rule applied to the one
// error in this package that only ever travels wrapped by another.
//
// Every denial reaches a surface through [v1.CheckTaskPolicy], which classifies
// it as a [v1.TaskError] naming the same task — so a denial that also named the
// task, prefixed by a sentinel that also named the policy, rendered one failure
// three times over one hop:
//
//	step "fetch": task "http": denied by task-shape policy: task "http"
//	refused by deployment task-shape policy …
//
// The assertion is a count rather than a `Contains`, because `Contains` is
// satisfied by any number of copies including the wrong one — which is how this
// stood while three tests in this file asserted the message names the task.
func TestDenialNamesTheTaskExactlyOnce(t *testing.T) {
	cfg := v1.TaskPolicyConfig{Deny: []string{`identity.subject != "platform"`}}
	policy, err := cfg.Policy()
	require.NoError(t, err)

	ctx := v1.NewContextWithTaskPolicy(context.Background(), policy)
	err = v1.CheckTaskPolicy(ctx, "codex.exec", &v1.WorkloadIdentity{Subject: "someone"}, true)
	require.Error(t, err)

	rendered := err.Error()

	// The wrapper is what names the task, and it must still do so: dropping the
	// name from both halves would satisfy a count of one by saying nothing.
	require.Contains(t, rendered, `task "codex.exec"`,
		"the classified failure must still name the task it refused")

	// The rule this case denies with deliberately does not mention the task, so
	// the only namings counted here are renderings rather than an operator's own
	// rule text quoted back to them.
	require.Equal(t, 1, strings.Count(rendered, "codex.exec"),
		"one failure, one hop, one naming of the task:\n%s", rendered)

	// The sentinel leaves the message and stays matchable, which is where a
	// caller branching on it reads it from.
	require.True(t, errors.Is(err, v1.ErrTaskPolicyDenied),
		"the sentinel must stay matchable after leaving the message")
}

// TestDenialNamesTheTaskOnEverySurfaceExactlyOnce is #899's correction to
// [TestDenialNamesTheTaskExactlyOnce]: the exactly-once count has to hold on the
// *direct* [v1.TaskPolicy.Check] surface too, not only the wrapped one.
//
// #184's first cut moved the name out of [v1.TaskPolicyDeniedError.Error] and
// onto [v1.CheckTaskPolicy]'s [v1.TaskError] wrapper. That names it once for
// every dispatch the drivers make — and zero times for a direct caller of
// [v1.TaskPolicy.Check], which docs/DSL.md recommends for exercising denials and
// which receives the bare denial with no wrapper. So the name belongs on the
// denial itself, and the wrappers defer; this checks all three renderings — the
// bare denial, the classified [v1.TaskError], and [v1.StepErrorText] (the
// recorded/durable text) — name the task once and only once.
//
// The rule denies on identity alone, so the only "codex.exec" any surface
// contains is a rendering of the name rather than the operator's rule text
// quoted back — the same care [TestDenialNamesTheTaskExactlyOnce] takes.
func TestDenialNamesTheTaskOnEverySurfaceExactlyOnce(t *testing.T) {
	cfg := v1.TaskPolicyConfig{Deny: []string{`identity.subject != "platform"`}}
	policy, err := cfg.Policy()
	require.NoError(t, err)

	// The direct surface: a bare denial from Check, no wrapper. This is the one
	// #184's first cut left naming no task at all.
	direct := policy.Check(context.Background(), "codex.exec", &v1.WorkloadIdentity{Subject: "someone"})
	require.Error(t, direct)
	require.Contains(t, direct.Error(), `task "codex.exec"`,
		"a direct TaskPolicy.Check caller must be told which task was refused")
	require.Equal(t, 1, strings.Count(direct.Error(), "codex.exec"),
		"the bare denial names its task exactly once:\n%s", direct.Error())

	// The wrapped surface, and the recorded/durable surface, over the same
	// dispatch — each must still name the task once, now by deferring to the
	// denial rather than adding a second naming of their own.
	ctx := v1.NewContextWithTaskPolicy(context.Background(), policy)
	wrapped := v1.CheckTaskPolicy(ctx, "codex.exec", &v1.WorkloadIdentity{Subject: "someone"}, false)
	require.Error(t, wrapped)

	for name, rendered := range map[string]string{
		"TaskError.Error": wrapped.Error(),
		"StepErrorText":   v1.StepErrorText(wrapped),
	} {
		require.Containsf(t, rendered, `task "codex.exec"`,
			"%s must name the refused task", name)
		require.Equalf(t, 1, strings.Count(rendered, "codex.exec"),
			"%s names the task exactly once:\n%s", name, rendered)
	}

	// The sentinel and the structured task name survive on every surface, so a
	// caller reading structure rather than text is unaffected by where the name
	// is rendered.
	require.True(t, errors.Is(direct, v1.ErrTaskPolicyDenied))
	var denied *v1.TaskPolicyDeniedError
	require.True(t, errors.As(direct, &denied))
	require.Equal(t, "codex.exec", denied.Task)
}
