package authz

import (
	"fmt"
	"strings"
)

// Layer says who owns a decision point's configuration.
type Layer string

const (
	// LayerTransport is who may reach the system at all.
	LayerTransport Layer = "transport"

	// LayerDeployment is the operator's: the trust policy and the tenancy it
	// carries. It is the outer bound on everything a workload author writes.
	LayerDeployment Layer = "deployment"

	// LayerAuthor is the Flowfile author's own predicates, evaluated against the
	// run's scope. A deployment does not override them.
	LayerAuthor Layer = "author"

	// LayerWorker is a policy the worker process holds about what a task may do.
	LayerWorker Layer = "worker"
)

// ZeroCase says what a decision point does when nothing is configured for it.
type ZeroCase string

const (
	// ZeroClosed denies: nothing is permitted until something says so.
	ZeroClosed ZeroCase = "closed"

	// ZeroDefault applies a bounded built-in posture rather than denying
	// outright, such as permitting only public addresses.
	ZeroDefault ZeroCase = "default"

	// ZeroOpen permits. Each such point must say why, because an open zero case
	// is the one place a missing line of configuration grants authority.
	ZeroOpen ZeroCase = "open"
)

// DecisionPoint is one place Flowstate decides whether something may happen.
//
// The registry is the single list of them. docs/AUTHORIZATION.md is rendered
// from it, and the test beside it holds every proof to a real test, so a
// decision point cannot be described in prose that nothing checks.
type DecisionPoint struct {
	// ID is the stable slug other documents link to.
	ID string

	// Name says what is decided, as a question.
	Name string

	Layer Layer

	// Enforced is where the decision is made, as a path from the repository
	// root and the function or type that holds it.
	Enforced string

	Zero ZeroCase

	// ZeroNote says what the zero case does, in one sentence. For an open or
	// default zero case it also says why that is the right behavior.
	ZeroNote string

	// Proof names the test functions that prove the zero case. Each must exist
	// in a _test.go file under the repository. Empty only for a point the host
	// does not enforce.
	Proof []string
}

// DecisionPoints returns every decision point in the order the document shows
// them: who may reach the system, then deployment authority, then the author's
// predicates, then what a worker will do.
func DecisionPoints() []DecisionPoint {
	return []DecisionPoint{
		{
			ID: "admission", Name: "Who is this caller, and may it reach the system at all?", Layer: LayerTransport,
			Enforced: "pkg/flowstate/v1/auth/admission.go: admitBearer",
			Zero:     ZeroClosed, ZeroNote: "A server with no verifier refuses every token, and `flow server` refuses to start without an auth policy unless told `--insecure-no-auth`.",
			Proof: []string{"TestAuthenticatorWithoutVerifier"},
		},
		{
			ID: "insecure-no-auth", Name: "May an unauthenticated caller use a server that was explicitly left open?", Layer: LayerTransport,
			Enforced: "pkg/flowstate/v1/auth/connect.go: InsecureAnonymousVerifier",
			Zero:     ZeroOpen, ZeroNote: "Only by opting in by name, and refused together with an auth policy. The anonymous caller holds the ordinary actions and never an explicit one; open because a loopback development server is the point of the flag.",
			Proof: []string{"TestInsecureAnonymousVerifier"},
		},
		{
			ID: "mcp-stdio", Name: "May a process that speaks MCP over its own stdio act?", Layer: LayerTransport,
			Enforced: "cmd/flow/internal/mcp/mcp.go: withMCPActions",
			Zero:     ZeroOpen, ZeroNote: "The process's own trust is the caller's, so no principal holds every ordinary action and no explicit one; open because whoever started the process is the operator.",
			Proof: []string{"TestMCPToolsAreGatedByTheCallersEffectiveActions", "TestMCPStdioCallerWithNoPrincipalIsUnrestricted"},
		},
		{
			ID: "action", Name: "Does a verified caller hold the action this operation needs?", Layer: LayerDeployment,
			Enforced: "pkg/flowstate/v1/authz/authz.go: DecidePrincipal",
			Zero:     ZeroClosed, ZeroNote: "A verified caller holds exactly what its trusted issuer entry lists, an entry cannot omit the list, and an embedder's Decider can only narrow it. This is not authentication: a context with no principal at all, which is a deployment that configured none or a process-trusted transport, holds every ordinary action and no explicit one.",
			Proof: []string{"TestDecidePrincipal", "TestNoActionCheckOutsideAuthz", "TestRestrictOnlyNarrowsThePolicy"},
		},
		{
			ID: "explicit-actions", Name: "May this caller read sensitive values or use the codec server?", Layer: LayerDeployment,
			Enforced: "pkg/flowstate/v1/server/sensitive.go: revealAuthorized",
			Zero:     ZeroClosed, ZeroNote: "reveal_sensitive, payload.decode and payload.encode are never implied, not even for the anonymous caller or a process with no principal.",
			Proof: []string{"TestEveryRevealRequestIsAuditedUnderItsOwnAction", "TestAnAgentCannotAskForWhatItsOperatorDidNot"},
		},
		{
			ID: "tenancy", Name: "Does this run belong to the caller's tenant?", Layer: LayerDeployment,
			Enforced: "pkg/flowstate/v1/server/lifecycle.go: authorizeRunDecision",
			Zero:     ZeroClosed, ZeroNote: "A run with no tenant memo is refused, and another tenant's run answers NotFound rather than PermissionDenied.",
			Proof: []string{"TestAMemoLessExecutionOfTheEnginesOwnWorkflowTypeIsRefused", "TestAnotherTenantCannotAddressARun"},
		},
		{
			ID: "codec-server", Name: "May this caller decode or encode a tenant's payloads?", Layer: LayerDeployment,
			Enforced: "pkg/flowstate/v1/codecserver/codecserver.go: authorize",
			Zero:     ZeroClosed, ZeroNote: "Both endpoints need an explicit action and the caller's own namespace; `--insecure-no-auth` serves any caller, on loopback only.",
			Proof: []string{"TestTwoTenantsCannotReadEachOther", "TestInsecureModeServesAnyCaller"},
		},
		{
			ID: "manual-trigger", Name: "May this caller start this workflow by hand?", Layer: LayerAuthor,
			Enforced: "pkg/flowstate/v1/trigger.go: CheckManualStart",
			Zero:     ZeroOpen, ZeroNote: "A workflow with no `triggers.manual:` block is startable by any caller the action check admits, in its tenant, including the anonymous caller of an insecure server; open so a plain workflow runs without ceremony.",
			Proof: []string{"TestCheckManualStartPreservesOpenDeniedAndReasonBehavior", "TestCheckManualStartRefusesAnAnonymousCallerWhateverThePredicateSays"},
		},
		{
			ID: "signal", Name: "May this sender answer this signal?", Layer: LayerAuthor,
			Enforced: "pkg/flowstate/v1/server/lifecycle.go: authorizeSignal",
			Zero:     ZeroOpen, ZeroNote: "A signal name with no declared policy admits any sender the action check admits, in the tenant, which keeps existing workflows working; a declared policy with no `allow:` admits nobody, and an error or a non-true result refuses.",
			Proof: []string{"TestAuthorizeSignalZeroCaseNoMemoKey", "TestAuthorizeSignalZeroCasePerName"},
		},
		{
			ID: "debug", Name: "May this caller pause and step this run?", Layer: LayerAuthor,
			Enforced: "pkg/flowstate/v1/debuglease.go: DebugPolicyCheck",
			Zero:     ZeroClosed, ZeroNote: "A workflow with no `debug:` block cannot be debugged, unlike signals, because there is no earlier behavior to keep.",
			Proof: []string{"TestAWorkflowWithNoDebugStanzaIsNotDebuggable", "TestARunWithoutADebugPolicyCannotBeAttached"},
		},
		{
			ID: "debug-lease", Name: "Is this caller the one holding the debug lease?", Layer: LayerAuthor,
			Enforced: "pkg/flowstate/v1/debuglease.go: DebugLeaseHeld",
			Zero:     ZeroClosed, ZeroNote: "A nil lease holds nothing, a lease with no expiry never holds, and `DebugLeaseHolder` lets only the identity that took the lease resume it.",
			Proof: []string{"TestOnlyTheHolderMayBeTheHolder", "TestALeaseThatHasLapsedHoldsNothing"},
		},
		{
			ID: "webhook-delivery", Name: "Is this webhook delivery authentic and wanted?", Layer: LayerAuthor,
			Enforced: "pkg/flowstate/v1/webhookverify.go: VerifyWebhookDelivery",
			Zero:     ZeroClosed, ZeroNote: "A trigger with no `verify:` is refused and a scheme with no resolved key is refused. An absent `when:` admits after verification; a `when:` that errors refuses.",
			Proof: []string{"TestADeliveryIsRefusedWhenTheDeploymentHoldsNoKey", "TestAWhenThatCannotBeAnsweredFailsClosed"},
		},
		{
			ID: "webhook-signal-bridge", Name: "May a webhook answer this signal gate?", Layer: LayerAuthor,
			Enforced: "pkg/flowstate/v1/webhooksignal.go: CheckWebhookSignalPolicy",
			Zero:     ZeroClosed, ZeroNote: "A bridge to a signal with no policy is refused at validation, deliberately unlike a plain signal, because a public route plus a leaked signing key would otherwise answer every unpoliced gate.",
			Proof: []string{"TestABridgeNeedsAPolicyThatCouldAdmitItsTrigger"},
		},
		{
			ID: "task-shape", Name: "May this task run with these inputs?", Layer: LayerWorker,
			Enforced: "pkg/flowstate/v1/taskpolicy.go: check",
			Zero:     ZeroOpen, ZeroNote: "A process with no task-shape policy restricts nothing, which keeps existing deployments working; once configured an unmatched allowlist denies and a rule error denies.",
			Proof: []string{"TestTaskPolicyZeroCase", "TestTaskPolicyLoadRefusesEmptyConfig"},
		},
		{
			ID: "egress", Name: "May this task reach this host?", Layer: LayerWorker,
			Enforced: "pkg/flowstate/v1/netpolicy/netpolicy.go: decideDial",
			Zero:     ZeroDefault, ZeroNote: "With no policy a task reaches public addresses only: loopback, private, link-local and metadata ranges are denied and every redirect is re-checked. The MCP run-local tool is stricter: with no egress policy file it denies all egress. Elsewhere a deployment closes the default by writing an egress policy.",
			Proof: []string{"Test_Policy_Client_addressPolicy", "Test_Policy_Client_redirectPolicy", "TestTheRunLocalToolRefusesEgressByDefault"},
		},
		{
			ID: "exec", Name: "May this task run this program?", Layer: LayerWorker,
			Enforced: "pkg/flowstate/v1/execpolicy/policy.go: Check",
			Zero:     ZeroClosed, ZeroNote: "A nil policy permits nothing, and even a policy permits only a named program with no PATH search. It is not a sandbox.",
			Proof: []string{"TestANilPolicyPermitsNothing"},
		},
		{
			ID: "secret", Name: "May this workload read this secret?", Layer: LayerWorker,
			Enforced: "pkg/flowstate/v1/auth/secretpolicy.go: Authorize",
			Zero:     ZeroClosed, ZeroNote: "A deployment with no rules permits no secret, a deny-only policy permits nothing, and a zero identity is refused.",
			Proof: []string{"TestSecretPolicyDefaultsToNothing"},
		},
		{
			ID: "credential-assumption", Name: "May this workload assume this credential target?", Layer: LayerWorker,
			Enforced: "pkg/flowstate/v1/auth/assume.go: evaluate",
			Zero:     ZeroClosed, ZeroNote: "No allow rule means no target is permitted, and a deny-only policy refuses too.",
			Proof: []string{"TestBrokerAssumePolicy"},
		},
		{
			ID: "plugin", Name: "May this identity use the plugin's own capability?", Layer: LayerWorker,
			Enforced: "proto/flowstate/plugin/v1/plugin.proto: ExecuteRequest.identity",
			Zero:     ZeroOpen, ZeroNote: "The host enforces nothing here: a plugin is trusted worker code and receives the identity so it can apply its own check in addition to the engine's.",
		},
	}
}

// Document renders the decision points as docs/AUTHORIZATION.md.
func Document() string {
	var b strings.Builder

	b.WriteString("<!-- Generated from pkg/flowstate/v1/authz/registry.go. Do not edit by hand; run\n" +
		"     go test ./pkg/flowstate/v1/authz -run TestAuthorizationDocIsCurrent -update -->\n\n")
	b.WriteString("# Authorization decision points\n\n")
	b.WriteString("Every place Flowstate decides whether something may happen, and what each does when nothing is configured.\n\n")
	b.WriteString("Two layers decide who may do what. The **deployment** owns the trust policy: which issuers are trusted and the `actions:` each grants. It is the outer bound. The **author** owns the `allow:` predicates in a Flowfile, which decide who may answer one gate or start one workflow, and a deployment does not override them. Worker policies bound what a task may do. Authors and operators read the same table; the zero case column is the one to check before relying on a default.\n\n")
	b.WriteString("An **open** zero case permits when nothing is configured and must say why. A **default** applies a bounded built-in posture. **Closed** denies.\n\n")
	b.WriteString("| Decision point | Layer | Zero case | Enforced in |\n| --- | --- | --- | --- |\n")
	for _, p := range DecisionPoints() {
		fmt.Fprintf(&b, "| [%s](#%s) | %s | **%s** | `%s` |\n", p.Name, p.ID, p.Layer, p.Zero, p.Enforced)
	}
	b.WriteString("\n")
	for _, p := range DecisionPoints() {
		fmt.Fprintf(&b, "## %s\n\n<a id=%q></a>%s\n\n- Layer: %s\n- Zero case: **%s**. %s\n- Enforced in: `%s`\n", p.ID, p.ID, p.Name, p.Layer, p.Zero, p.ZeroNote, p.Enforced)
		if len(p.Proof) > 0 {
			b.WriteString("- Proven by: `" + strings.Join(p.Proof, "`, `") + "`\n")
		} else {
			b.WriteString("- Proven by: nothing; the host does not enforce this point.\n")
		}
		b.WriteString("\n")
	}
	b.WriteString("## Not yet listed\n\nSchedule tenancy and actions, the required audit recorder that refuses to release what it cannot record, the webhook `jwt` bearer scheme, the credential target catalog and the delegated (`act`) token rules, which admit an inbound actor chain only under a delegation stanza and refuse delegated callers at minting, brokering and `jose.verify`, each decide something and are not in the registry yet.\n")

	return b.String()
}
