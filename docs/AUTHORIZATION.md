<!-- Generated from pkg/flowstate/v1/authz/registry.go. Do not edit by hand; run
     go test ./pkg/flowstate/v1/authz -run TestAuthorizationDocIsCurrent -update -->

# Authorization decision points

Every place Flowstate decides whether something may happen, and what each does when nothing is configured.

Two layers decide who may do what. The **deployment** owns the trust policy: which issuers are trusted and the `actions:` each grants. It is the outer bound. The **author** owns the `allow:` predicates in a Flowfile, which decide who may answer one gate or start one workflow, and a deployment does not override them. Worker policies bound what a task may do. Authors and operators read the same table; the zero case column is the one to check before relying on a default.

An **open** zero case permits when nothing is configured and must say why. A **default** applies a bounded built-in posture. **Closed** denies.

| Decision point | Layer | Zero case | Enforced in |
| --- | --- | --- | --- |
| [Who is this caller, and may it reach the system at all?](#admission) | transport | **closed** | `pkg/flowstate/v1/auth/admission.go: admitBearer` |
| [May an unauthenticated caller use a server that was explicitly left open?](#insecure-no-auth) | transport | **open** | `pkg/flowstate/v1/auth/connect.go: InsecureAnonymousVerifier` |
| [May a process that speaks MCP over its own stdio act?](#mcp-stdio) | transport | **open** | `cmd/flow/internal/mcp/mcp.go: withMCPActions` |
| [Does a verified caller hold the action this operation needs?](#action) | deployment | **closed** | `pkg/flowstate/v1/authz/authz.go: DecidePrincipal` |
| [May this caller read sensitive values or use the codec server?](#explicit-actions) | deployment | **closed** | `pkg/flowstate/v1/server/sensitive.go: revealAuthorized` |
| [Does this run belong to the caller's tenant?](#tenancy) | deployment | **closed** | `pkg/flowstate/v1/server/lifecycle.go: authorizeRunDecision` |
| [May this caller decode or encode a tenant's payloads?](#codec-server) | deployment | **closed** | `pkg/flowstate/v1/codecserver/codecserver.go: authorize` |
| [May this caller start this workflow by hand?](#manual-trigger) | author | **open** | `pkg/flowstate/v1/trigger.go: CheckManualStart` |
| [May this sender answer this signal?](#signal) | author | **open** | `pkg/flowstate/v1/server/lifecycle.go: authorizeSignal` |
| [May this caller pause and step this run?](#debug) | author | **closed** | `pkg/flowstate/v1/debuglease.go: DebugPolicyCheck` |
| [Is this caller the one holding the debug lease?](#debug-lease) | author | **closed** | `pkg/flowstate/v1/debuglease.go: DebugLeaseHeld` |
| [Is this webhook delivery authentic and wanted?](#webhook-delivery) | author | **closed** | `pkg/flowstate/v1/webhookverify.go: VerifyWebhookDelivery` |
| [May a webhook answer this signal gate?](#webhook-signal-bridge) | author | **closed** | `pkg/flowstate/v1/webhooksignal.go: CheckWebhookSignalPolicy` |
| [May this task run with these inputs?](#task-shape) | worker | **open** | `pkg/flowstate/v1/taskpolicy.go: check` |
| [May this task reach this host?](#egress) | worker | **default** | `pkg/flowstate/v1/netpolicy/netpolicy.go: decideDial` |
| [May this task run this program?](#exec) | worker | **closed** | `pkg/flowstate/v1/execpolicy/policy.go: Check` |
| [May this workload read this secret?](#secret) | worker | **closed** | `pkg/flowstate/v1/auth/secretpolicy.go: Authorize` |
| [May this workload assume this credential target?](#credential-assumption) | worker | **closed** | `pkg/flowstate/v1/auth/assume.go: evaluate` |
| [May this identity use the plugin's own capability?](#plugin) | worker | **open** | `proto/flowstate/plugin/v1/plugin.proto: ExecuteRequest.identity` |

## admission

<a id="admission"></a>Who is this caller, and may it reach the system at all?

- Layer: transport
- Zero case: **closed**. A server with no verifier refuses every token, and `flow server` refuses to start without an auth policy unless told `--insecure-no-auth`.
- Enforced in: `pkg/flowstate/v1/auth/admission.go: admitBearer`
- Proven by: `TestAuthenticatorWithoutVerifier`

## insecure-no-auth

<a id="insecure-no-auth"></a>May an unauthenticated caller use a server that was explicitly left open?

- Layer: transport
- Zero case: **open**. Only by opting in by name, and refused together with an auth policy. The anonymous caller holds the ordinary actions and never an explicit one; open because a loopback development server is the point of the flag.
- Enforced in: `pkg/flowstate/v1/auth/connect.go: InsecureAnonymousVerifier`
- Proven by: `TestInsecureAnonymousVerifier`

## mcp-stdio

<a id="mcp-stdio"></a>May a process that speaks MCP over its own stdio act?

- Layer: transport
- Zero case: **open**. The process's own trust is the caller's, so no principal holds every ordinary action and no explicit one; open because whoever started the process is the operator.
- Enforced in: `cmd/flow/internal/mcp/mcp.go: withMCPActions`
- Proven by: `TestMCPToolsAreGatedByTheCallersEffectiveActions`, `TestMCPStdioCallerWithNoPrincipalIsUnrestricted`

## action

<a id="action"></a>Does a verified caller hold the action this operation needs?

- Layer: deployment
- Zero case: **closed**. A verified caller holds exactly what its trusted issuer entry lists, an entry cannot omit the list, and an embedder's Decider can only narrow it. This is not authentication: a context with no principal at all, which is a deployment that configured none or a process-trusted transport, holds every ordinary action and no explicit one.
- Enforced in: `pkg/flowstate/v1/authz/authz.go: DecidePrincipal`
- Proven by: `TestDecidePrincipal`, `TestNoActionCheckOutsideAuthz`, `TestRestrictOnlyNarrowsThePolicy`

## explicit-actions

<a id="explicit-actions"></a>May this caller read sensitive values or use the codec server?

- Layer: deployment
- Zero case: **closed**. reveal_sensitive, payload.decode and payload.encode are never implied, not even for the anonymous caller or a process with no principal.
- Enforced in: `pkg/flowstate/v1/server/sensitive.go: revealAuthorized`
- Proven by: `TestEveryRevealRequestIsAuditedUnderItsOwnAction`, `TestAnAgentCannotAskForWhatItsOperatorDidNot`

## tenancy

<a id="tenancy"></a>Does this run belong to the caller's tenant?

- Layer: deployment
- Zero case: **closed**. A run with no tenant memo is refused, and another tenant's run answers NotFound rather than PermissionDenied.
- Enforced in: `pkg/flowstate/v1/server/lifecycle.go: authorizeRunDecision`
- Proven by: `TestAMemoLessExecutionOfTheEnginesOwnWorkflowTypeIsRefused`, `TestAnotherTenantCannotAddressARun`

## codec-server

<a id="codec-server"></a>May this caller decode or encode a tenant's payloads?

- Layer: deployment
- Zero case: **closed**. Both endpoints need an explicit action and the caller's own namespace; `--insecure-no-auth` serves any caller, on loopback only.
- Enforced in: `pkg/flowstate/v1/codecserver/codecserver.go: authorize`
- Proven by: `TestTwoTenantsCannotReadEachOther`, `TestInsecureModeServesAnyCaller`

## manual-trigger

<a id="manual-trigger"></a>May this caller start this workflow by hand?

- Layer: author
- Zero case: **open**. A workflow with no `triggers.manual:` block is startable by any caller the action check admits, in its tenant, including the anonymous caller of an insecure server; open so a plain workflow runs without ceremony.
- Enforced in: `pkg/flowstate/v1/trigger.go: CheckManualStart`
- Proven by: `TestCheckManualStartPreservesOpenDeniedAndReasonBehavior`, `TestCheckManualStartRefusesAnAnonymousCallerWhateverThePredicateSays`

## signal

<a id="signal"></a>May this sender answer this signal?

- Layer: author
- Zero case: **open**. A signal name with no declared policy admits any sender the action check admits, in the tenant, which keeps existing workflows working; a declared policy with no `allow:` admits nobody, and an error or a non-true result refuses.
- Enforced in: `pkg/flowstate/v1/server/lifecycle.go: authorizeSignal`
- Proven by: `TestAuthorizeSignalZeroCaseNoMemoKey`, `TestAuthorizeSignalZeroCasePerName`

## debug

<a id="debug"></a>May this caller pause and step this run?

- Layer: author
- Zero case: **closed**. A workflow with no `debug:` block cannot be debugged, unlike signals, because there is no earlier behavior to keep.
- Enforced in: `pkg/flowstate/v1/debuglease.go: DebugPolicyCheck`
- Proven by: `TestAWorkflowWithNoDebugStanzaIsNotDebuggable`, `TestARunWithoutADebugPolicyCannotBeAttached`

## debug-lease

<a id="debug-lease"></a>Is this caller the one holding the debug lease?

- Layer: author
- Zero case: **closed**. A nil lease holds nothing, a lease with no expiry never holds, and `DebugLeaseHolder` lets only the identity that took the lease resume it.
- Enforced in: `pkg/flowstate/v1/debuglease.go: DebugLeaseHeld`
- Proven by: `TestOnlyTheHolderMayBeTheHolder`, `TestALeaseThatHasLapsedHoldsNothing`

## webhook-delivery

<a id="webhook-delivery"></a>Is this webhook delivery authentic and wanted?

- Layer: author
- Zero case: **closed**. A trigger with no `verify:` is refused and a scheme with no resolved key is refused. An absent `when:` admits after verification; a `when:` that errors refuses.
- Enforced in: `pkg/flowstate/v1/webhookverify.go: VerifyWebhookDelivery`
- Proven by: `TestADeliveryIsRefusedWhenTheDeploymentHoldsNoKey`, `TestAWhenThatCannotBeAnsweredFailsClosed`

## webhook-signal-bridge

<a id="webhook-signal-bridge"></a>May a webhook answer this signal gate?

- Layer: author
- Zero case: **closed**. A bridge to a signal with no policy is refused at validation, deliberately unlike a plain signal, because a public route plus a leaked signing key would otherwise answer every unpoliced gate.
- Enforced in: `pkg/flowstate/v1/webhooksignal.go: CheckWebhookSignalPolicy`
- Proven by: `TestABridgeNeedsAPolicyThatCouldAdmitItsTrigger`

## task-shape

<a id="task-shape"></a>May this task run with these inputs?

- Layer: worker
- Zero case: **open**. A process with no task-shape policy restricts nothing, which keeps existing deployments working; once configured an unmatched allowlist denies and a rule error denies.
- Enforced in: `pkg/flowstate/v1/taskpolicy.go: check`
- Proven by: `TestTaskPolicyZeroCase`, `TestTaskPolicyLoadRefusesEmptyConfig`

## egress

<a id="egress"></a>May this task reach this host?

- Layer: worker
- Zero case: **default**. With no policy a task reaches public addresses only: loopback, private, link-local and metadata ranges are denied and every redirect is re-checked. The MCP run-local tool is stricter: with no egress policy file it denies all egress. Elsewhere a deployment closes the default by writing an egress policy.
- Enforced in: `pkg/flowstate/v1/netpolicy/netpolicy.go: decideDial`
- Proven by: `Test_Policy_Client_addressPolicy`, `Test_Policy_Client_redirectPolicy`, `TestTheRunLocalToolRefusesEgressByDefault`

## exec

<a id="exec"></a>May this task run this program?

- Layer: worker
- Zero case: **closed**. A nil policy permits nothing, and even a policy permits only a named program with no PATH search. It is not a sandbox.
- Enforced in: `pkg/flowstate/v1/execpolicy/policy.go: Check`
- Proven by: `TestANilPolicyPermitsNothing`

## secret

<a id="secret"></a>May this workload read this secret?

- Layer: worker
- Zero case: **closed**. A deployment with no rules permits no secret, a deny-only policy permits nothing, and a zero identity is refused.
- Enforced in: `pkg/flowstate/v1/auth/secretpolicy.go: Authorize`
- Proven by: `TestSecretPolicyDefaultsToNothing`

## credential-assumption

<a id="credential-assumption"></a>May this workload assume this credential target?

- Layer: worker
- Zero case: **closed**. No allow rule means no target is permitted, and a deny-only policy refuses too.
- Enforced in: `pkg/flowstate/v1/auth/assume.go: evaluate`
- Proven by: `TestBrokerAssumePolicy`

## plugin

<a id="plugin"></a>May this identity use the plugin's own capability?

- Layer: worker
- Zero case: **open**. The host enforces nothing here: a plugin is trusted worker code and receives the identity so it can apply its own check in addition to the engine's.
- Enforced in: `proto/flowstate/plugin/v1/plugin.proto: ExecuteRequest.identity`
- Proven by: nothing; the host does not enforce this point.

## Not yet listed

Schedule tenancy and actions, the required audit recorder that refuses to release what it cannot record, the webhook `jwt` bearer scheme, the credential target catalog and the refusal of delegated (`act`) tokens each decide something and are not in the registry yet.
