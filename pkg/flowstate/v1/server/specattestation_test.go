package server_test

import (
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/server"
)

// The server's half of #734: a caller is told whether the specification it sent is
// the one that ran.
//
// A deployment that registers its own copy of a workflow substitutes it for a
// caller's copy of the same name, which is the point — that is what makes
// `manual:` policy rather than caller input. The consequence nobody had accounted
// for is on the caller's screen: a client redacts a run's declared outputs against
// the specification it holds, so a client holding a copy that did not run redacts
// against declarations that did not apply. [v1.RunResponse.specification_as_submitted]
// is the fact that lets it know, and these are the two answers it can carry.

// declaringWorkflow is a workflow whose single output is or is not marked
// sensitive, so a trusted copy and a submitted copy can differ in exactly the way
// that matters to a client's redaction and in no other way.
func declaringWorkflow(sensitive bool) *v1.Workflow {
	return &v1.Workflow{
		Name:    "attested",
		Profile: v1.CurrentProfile,
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "token", Value: v1.NewExpr("steps.mint.value"), Sensitive: sensitive},
		},
		Steps: []*v1.Node{{
			Id:   "mint",
			Kind: &v1.Node_Value{Value: v1.NewLiteral("shh-do-not-print-me")},
		}},
	}
}

// TestRunAttestsThatTheSubmittedSpecificationRan is the ordinary case, and the one
// that keeps the precise view working: nothing was substituted, so the caller's own
// copy describes the run and the server says so.
//
// The presence check is not decoration. A client cannot distinguish an unset field
// from a false one through the generated getter, so an attestation that is true but
// left unset would read exactly like a server that never answered — and the client
// would fail closed on a run it had no reason to.
func TestRunAttestsThatTheSubmittedSpecificationRan(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := server.New(temporal)

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: declaringWorkflow(false),
	}))
	require.NoError(t, err)

	require.NotNil(t, started.Msg.SpecificationAsSubmitted,
		"the server said nothing, which a client must read as a substitution it cannot rule out")
	assert.True(t, started.Msg.GetSpecificationAsSubmitted())
	assert.True(t, started.Msg.RanSubmittedSpecification())
}

// TestRunRefusesToAttestASubstitutedSpecification is the direction the bug lived
// in.
//
// The deployment's copy marks the output sensitive and the submitted copy does not,
// which is exactly the disagreement that reaches a terminal as a secret printed in
// the clear. Nothing here can tell the client *which* names the trusted copy marks
// — that is deployment configuration the caller did not send — so what it is owed
// and gets is the one bit that lets it stop trusting its own copy.
func TestRunRefusesToAttestASubstitutedSpecification(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := server.New(temporal,
		server.WithTrustedWorkflows("", declaringWorkflow(true)))

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: declaringWorkflow(false),
	}))
	require.NoError(t, err)

	require.NotNil(t, started.Msg.SpecificationAsSubmitted,
		"a server that substitutes must say so rather than leaving the caller to infer it")
	assert.False(t, started.Msg.GetSpecificationAsSubmitted(),
		"the deployment ran its own copy and told the caller theirs was what ran")
	assert.False(t, started.Msg.RanSubmittedSpecification())

	// And the answer is about the two specifications rather than about the
	// registration: a deployment that registered the identical workflow has
	// substituted nothing a caller could observe, so the caller's copy still
	// describes the run and the precise view survives.
	identical := server.New(temporal,
		server.WithTrustedWorkflows("", declaringWorkflow(false)))

	same, err := identical.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: declaringWorkflow(false),
	}))
	require.NoError(t, err)
	assert.True(t, same.Msg.RanSubmittedSpecification(),
		"a trusted copy equal to the submitted one was reported as a substitution")
}

// TestRunRefusesToAttestASpecificationItPinnedPluginsOnto is the second way the
// executed specification stops being the submitted one, found by Codex on #826.
//
// Nothing is substituted here: the deployment registered no trusted copy, and the
// caller's own workflow is what runs. But the server writes its own selection of
// plugin versions onto that specification before the engine sees it — the
// `resolved_plugins` the caller never sent and cannot predict — so what executes is
// not what arrived. An attestation answered at the trusted lookup said "unchanged"
// about a message that had not finished being assembled; answered against the
// specification actually handed to the engine, it says what is true.
//
// The client's consequence is the one this field exists for: a caller told true
// redacts against its own copy, and a pin is the deployment reaching into the
// specification, which is exactly the class of edit a caller cannot vouch for.
func TestRunRefusesToAttestASpecificationItPinnedPluginsOnto(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)

	catalog := &v1.PluginCatalog{
		ClaimsSchemaVersion: v1.CurrentClaimsSchemaVersion,
		Plugins: []*v1.PluginDescription{{
			Name: "storefront", Version: "v2.1.0", ProtocolVersion: 2,
			TaskSchemaDigest:   "sha256:schema",
			DistributionDigest: "sha256:binary",
			ClaimsDigest:       "sha256:claims",
		}},
	}
	flowstate := server.New(temporal, server.WithPluginCatalog(catalog))

	requiring := declaringWorkflow(false)
	requiring.PluginRequirements = []*v1.PluginRequirement{{
		Name: "storefront", MinimumVersion: "v2.0.0",
	}}

	started, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: requiring,
	}))
	require.NoError(t, err)

	require.NotNil(t, started.Msg.SpecificationAsSubmitted)
	assert.False(t, started.Msg.RanSubmittedSpecification(),
		"a specification the deployment pinned plugin versions onto was attested as the submitted one")

	// And the other direction, on the same server, so false is a property of the
	// pin rather than of a deployment that has a catalog at all: a workflow
	// requiring no plugins is pinned to nothing, which changes nothing, and keeps
	// its caller the precise view.
	plain, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
		Workflow: declaringWorkflow(false),
	}))
	require.NoError(t, err)
	assert.True(t, plain.Msg.RanSubmittedSpecification(),
		"a workflow with no plugin requirements lost its precise view to an empty pin")
}
