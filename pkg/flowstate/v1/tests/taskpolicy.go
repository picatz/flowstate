package tests

import (
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// Shared cases for #187 slice 1's task-shape policy, run against both
// execution drivers — [flowstatev1_test.TestRunWorkflowTaskPolicy] locally,
// and `engine`'s identically-named durable test. Both must reach a denial
// through the same seam ([v1.Task.EvalInScope] locally, each activity entry
// point durably), which is exactly the property invariant 3 exists to hold
// two implementations to.
//
// [Case] does not carry a policy, so this file defines its own small case
// shape rather than force one in: what is under test here is not what a
// workflow produces (every other case set in this package) but whether a
// deployment's policy lets a dispatch happen *at all* — a different axis, one
// [TaskPolicyCase] states directly instead of overloading [Case] with a field
// every other case in this package would leave zero.

// TaskPolicyCase pairs a workflow with the task-shape policy it must run
// under and the outcome that policy requires.
type TaskPolicyCase struct {
	// Name identifies the case in test output.
	Name string

	// Workflow is the file under test. Every workflow here dispatches its
	// task unconditionally (no `if:` at all, or `if: true`), which is the
	// point: the policy — not the file's own condition — is what decides.
	Workflow *v1.Workflow

	// Identity is who the run acts as while the policy is evaluated, or nil
	// for a run whose starter named nobody.
	//
	// Both drivers must reach the policy with *this* identity, by whatever
	// route each has: the durable driver carries it on
	// [v1.RunState.Identity], and the local driver carries it as the
	// rehearsal identity `flow run local --as-*` establishes
	// ([v1.NewContextWithRehearsalIdentity]). That the two routes differ is
	// the point — what invariant 3 constrains is the answer, and a case here
	// is the only place the two answers are compared.
	Identity *v1.WorkloadIdentity

	// Policy is the deployment-side configuration governing this run. It is
	// compiled once by the caller and installed as the process default for
	// the duration of the case — see each driver's own runner for how it is
	// scoped and restored, since installing it is process-global state and
	// each driver's test file owns its own save/restore around that.
	Policy v1.TaskPolicyConfig

	// DeniedTask names the task a caller must find refused via
	// errors.As(err, *v1.TaskPolicyDeniedError) when the run fails. Empty
	// means the run must succeed — the policy permits every dispatch the
	// workflow makes.
	DeniedTask string

	// DeniedReason is the [v1.TaskPolicyReason] the denial must carry, when
	// DeniedTask is set.
	DeniedReason v1.TaskPolicyReason

	// ExpectedOutputs is what the run must produce when DeniedTask is empty.
	ExpectedOutputs *v1.Workflow_StepOutputs
}

// TaskPolicyCases returns the shared cases both drivers must agree on.
//
// # The identity cases, and why they were missing
//
// This set used to say, in this comment, that every rule here is over `task`
// alone because "the local driver's [Scope.identity] is always empty… a case
// that asserted identity-based denial here would either always pass locally
// regardless of what the rule said or require the local driver to carry an
// identity it structurally does not have."
//
// That was an accurate description of the defect and a bad reason to leave it
// untested. The local driver did not structurally lack an identity — `flow run
// local --as-namespace` had built one for years and handed it to the secret
// rules, the credential broker and every plugin task; the scope simply did not
// receive it. So the untested gap and the divergence were the same fact, which
// is what #295 found and what a case set with two verified callers exists to
// prevent: the set was shaped around what the drivers *did* agree on, and
// agreement was therefore trivially true.
//
// The identity cases below are the join CLAUDE.md asks for rather than either
// half — a rule over identity *and* task, since a rule over identity alone
// cannot tell a policy that discriminates from one that denies everything —
// and they are written in the negative direction: each admitted tenant is
// paired with a case proving the same policy refuses another one.
func TaskPolicyCases() []TaskPolicyCase {
	// One tenant's identity and another's, distinct in every field a rule can
	// read, so a case that passes cannot be passing because two identities
	// looked alike where it mattered.
	teamA := &v1.WorkloadIdentity{
		Subject:   "spiffe://acme/team-a/deployer",
		Issuer:    "https://issuer.example.com",
		Namespace: "team-a",
		Claims:    map[string]string{"team": "a"},
	}
	teamB := &v1.WorkloadIdentity{
		Subject:   "spiffe://acme/team-b/deployer",
		Issuer:    "https://issuer.example.com",
		Namespace: "team-b",
		Claims:    map[string]string{"team": "b"},
	}

	return []TaskPolicyCase{
		{
			// #295's own reproduction, in the direction that issue reports:
			// an allow rule naming the identity the run acts as must permit
			// the dispatch. This is the case that failed locally and passed
			// durably — a rehearsal refusing what production runs — and it
			// fails again the moment either driver stops carrying identity
			// to the policy.
			Name: "an allow rule keyed on the run's identity permits its own tenant",
			Workflow: &v1.Workflow{
				Name:    "task-policy-identity-allow",
				Profile: v1.CurrentProfile,
				Steps:   []*v1.Node{says("report", "this may run")},
			},
			Identity:        teamA,
			Policy:          v1.TaskPolicyConfig{Allow: []string{`task == "log" && identity.namespace == "team-a"`}},
			ExpectedOutputs: held("report"),
		},
		{
			// And the negative direction, which is what makes the case above
			// a test of a boundary rather than of a rule that says yes: the
			// same policy, a different tenant, refused — with
			// [v1.TaskPolicyReasonNoAllowRule], since an allowlist that
			// matches nobody is an allowlist and not a deny match.
			Name: "the same allow rule refuses another tenant",
			Workflow: &v1.Workflow{
				Name:    "task-policy-identity-allow-other-tenant",
				Profile: v1.CurrentProfile,
				Steps:   []*v1.Node{says("report", "this must not run")},
			},
			Identity:     teamB,
			Policy:       v1.TaskPolicyConfig{Allow: []string{`task == "log" && identity.namespace == "team-a"`}},
			DeniedTask:   "log",
			DeniedReason: v1.TaskPolicyReasonNoAllowRule,
		},
		{
			// A deny rule is the other half of the surface and has the
			// opposite failure mode: where a missing identity makes an allow
			// rule refuse too much, it makes a deny rule refuse too little.
			// Both directions have to be pinned, because a driver that
			// carried identity to one and not the other would still be wrong
			// and would still pass the four cases this set had before.
			Name: "a deny rule keyed on identity refuses the tenant it names",
			Workflow: &v1.Workflow{
				Name:    "task-policy-identity-deny",
				Profile: v1.CurrentProfile,
				Steps:   []*v1.Node{says("report", "this must not run")},
			},
			Identity:     teamB,
			Policy:       v1.TaskPolicyConfig{Deny: []string{`identity.namespace == "team-b"`}},
			DeniedTask:   "log",
			DeniedReason: v1.TaskPolicyReasonDenyRule,
		},
		{
			// Its pair: the same deny rule leaves the other tenant alone.
			Name: "the same deny rule does not reach another tenant",
			Workflow: &v1.Workflow{
				Name:    "task-policy-identity-deny-other-tenant",
				Profile: v1.CurrentProfile,
				Steps:   []*v1.Node{says("report", "this may run")},
			},
			Identity:        teamA,
			Policy:          v1.TaskPolicyConfig{Deny: []string{`identity.namespace == "team-b"`}},
			ExpectedOutputs: held("report"),
		},
		{
			// A claim rather than the namespace, because the claims map is
			// the one field of the activation that is not a plain string and
			// the one a driver can lose separately: an identity whose claims
			// failed to travel still has a subject and a namespace, so every
			// case above would keep passing.
			Name: "an allow rule keyed on a carried claim permits the identity carrying it",
			Workflow: &v1.Workflow{
				Name:    "task-policy-identity-claims",
				Profile: v1.CurrentProfile,
				Steps:   []*v1.Node{says("report", "this may run")},
			},
			Identity:        teamA,
			Policy:          v1.TaskPolicyConfig{Allow: []string{`identity.claims["team"] == "a"`}},
			ExpectedOutputs: held("report"),
		},
		{
			// The run that names nobody, kept explicitly: a starter-less run
			// under an identity-scoped allowlist is refused on both drivers,
			// which is the fail-closed reading [v1.TaskPolicy.Check]'s own
			// doc states for the nil identity. Carrying a rehearsal identity
			// is what a local run does when its starter names one — not a
			// blanket exemption for local runs, and this case is what would
			// fail if it ever became one.
			Name: "an identity-scoped allowlist refuses a run that names nobody",
			Workflow: &v1.Workflow{
				Name:    "task-policy-identity-absent",
				Profile: v1.CurrentProfile,
				Steps:   []*v1.Node{says("report", "this must not run")},
			},
			Policy:       v1.TaskPolicyConfig{Allow: []string{`identity.namespace == "team-a"`}},
			DeniedTask:   "log",
			DeniedReason: v1.TaskPolicyReasonNoAllowRule,
		},
		{
			// The core fail-closed matrix case, and the reachability property
			// #187's design record asks slice 1 to prove: a step with no
			// condition at all — nothing for an author to have weakened —
			// still does not dispatch once the deployment denies its task.
			Name: "a deny rule refuses a task the workflow dispatches unconditionally",
			Workflow: &v1.Workflow{
				Name:    "task-policy-deny",
				Profile: v1.CurrentProfile,
				Steps:   []*v1.Node{says("report", "this must not run")},
			},
			Policy:       v1.TaskPolicyConfig{Deny: []string{`task == "log"`}},
			DeniedTask:   "log",
			DeniedReason: v1.TaskPolicyReasonDenyRule,
		},
		{
			// The negative-direction pair, per CLAUDE.md ("test that A cannot
			// reach B, not that A can reach A"): a deny rule scoped to a task
			// name this workflow never uses must not reach the task it does
			// use. Proves the rule discriminates by task name rather than
			// denying regardless of what it says.
			Name: "a deny rule naming a different task does not reach this one",
			Workflow: &v1.Workflow{
				Name:    "task-policy-deny-unrelated",
				Profile: v1.CurrentProfile,
				Steps:   []*v1.Node{says("report", "this may run")},
			},
			Policy:          v1.TaskPolicyConfig{Deny: []string{`task == "codex.exec"`}},
			ExpectedOutputs: held("report"),
		},
		{
			// The allowlist half: configuring any allow rule turns the policy
			// into one, and a task not named by any rule is refused with
			// [v1.TaskPolicyReasonNoAllowRule] — distinct from a deny match,
			// which is what [DeniedReason] pins here.
			Name: "an allowlist denies a task no allow rule names",
			Workflow: &v1.Workflow{
				Name:    "task-policy-allowlist-denies",
				Profile: v1.CurrentProfile,
				Steps:   []*v1.Node{says("report", "this must not run")},
			},
			Policy:       v1.TaskPolicyConfig{Allow: []string{`task == "codex.exec"`}},
			DeniedTask:   "log",
			DeniedReason: v1.TaskPolicyReasonNoAllowRule,
		},
		{
			// And its own negative-direction pair: the same allowlist permits
			// exactly the task it names.
			Name: "an allowlist permits the task it names",
			Workflow: &v1.Workflow{
				Name:    "task-policy-allowlist-permits",
				Profile: v1.CurrentProfile,
				Steps:   []*v1.Node{says("report", "this may run")},
			},
			Policy:          v1.TaskPolicyConfig{Allow: []string{`task == "log"`}},
			ExpectedOutputs: held("report"),
		},
		{
			// A step whose own `if:` would let it through is still refused —
			// the file's condition and the deployment's policy are two
			// independent gates, and this pins that the second one is not
			// short-circuited by the first already having said yes. This is
			// the shape `examples/task-shape-policy` exercises end to end
			// under the durable harness with a real identity; this case pins
			// the same property with the minimal workflow this package's
			// other cases already use.
			Name: "a deny rule refuses a task even when the step's own if: is true",
			Workflow: &v1.Workflow{
				Name:    "task-policy-deny-if-true",
				Profile: v1.CurrentProfile,
				Steps: []*v1.Node{
					{
						Id:        "report",
						Condition: v1.NewLiteral(true),
						Kind: &v1.Node_Task{Task: &v1.Task{
							Name:   "log",
							Inputs: map[string]*v1.Value{"message": v1.NewLiteral("this must not run")},
						}},
					},
				},
			},
			Policy:       v1.TaskPolicyConfig{Deny: []string{`task == "log"`}},
			DeniedTask:   "log",
			DeniedReason: v1.TaskPolicyReasonDenyRule,
		},
	}
}
