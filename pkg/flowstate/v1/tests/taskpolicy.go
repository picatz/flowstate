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
// Every rule here is over `task` alone, deliberately: `identity` is part of
// the activation slice 1 ships (see taskpolicy.go's package doc), but the
// local driver's [Scope.identity] is always empty — a local run has no
// authenticated caller at all, which is the honest answer
// [v1.RunIdentityWorkflow]'s own doc states, not a task-policy shortcut. A
// case that asserted identity-based denial here would therefore either
// always pass locally regardless of what the rule said (proving nothing) or
// require the local driver to carry an identity it structurally does not
// have. The identity half of the activation is exercised where a real
// attested identity actually exists: durably, in
// `engine/task_policy_identity_test.go`, alongside
// [flowstatev1_test.TestTaskPolicyNilIdentityReadsEmpty]'s unit coverage of
// the nil case every local run hits.
func TaskPolicyCases() []TaskPolicyCase {
	return []TaskPolicyCase{
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
