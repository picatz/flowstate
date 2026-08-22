package server_test

import (
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

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
	flowstate := mustNew(t, temporal)

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
	flowstate := mustNew(t, temporal,
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
	identical := mustNew(t, temporal,
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
	flowstate := mustNew(t, temporal, server.WithPluginCatalog(catalog))

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

// #844's half: the two RPCs that are not `Run` and bring durable work into
// existence from a caller's specification anyway.
//
// `SignalWithStart` and `CreateSchedule` substitute the same trusted copy `Run`
// does and pin the same plugin versions onto it, and until now carried no
// attestation at all. Nothing renders declared outputs from either verb today —
// so this is a trap armed for the client that does, rather than a live leak, and
// the tests below are what make the field's answer true before anybody reads it.

// attestableWorkflow is one specification all three creating verbs accept, so
// their answers can be compared rather than merely each asserted.
//
// It waits for a signal (which `SignalWithStart` requires — a name nothing waits
// for is refused before anything is created) and carries a schedule trigger
// (which `CreateSchedule` requires), and its single declared output is or is not
// marked sensitive — the one difference between a submitted and a trusted copy
// that reaches a caller's terminal as a secret printed in the clear.
func attestableWorkflow(sensitive bool) *v1.Workflow {
	return &v1.Workflow{
		Name:    "attestable",
		Profile: v1.CurrentProfile,
		Triggers: &v1.Triggers{
			Schedule: &v1.ScheduleTrigger{
				// Hourly, and every schedule below is created paused: the cadence
				// only has to be legal, because nothing here waits for a firing.
				Cron:    []string{"0 * * * *"},
				Overlap: v1.ScheduleTrigger_OVERLAP_SKIP,
			},
		},
		DeclaredOutputs: []*v1.OutputDeclaration{
			{Name: "token", Value: v1.NewExpr("steps.lifecycle.value"), Sensitive: sensitive},
		},
		Steps: []*v1.Node{{
			Id: "lifecycle",
			Kind: &v1.Node_Loop{Loop: &v1.Loop{
				State:         "total",
				Initial:       v1.NewLiteral(int64(0)),
				Update:        v1.NewExpr("total + steps.mutation.payload.amount"),
				Until:         v1.NewExpr("steps.mutation.payload.close"),
				MaxIterations: 20,
				Body: []*v1.Node{{
					Id: "mutation",
					Kind: &v1.Node_Wait{Wait: &v1.Wait{
						Kind:    &v1.Wait_Signal{Signal: &v1.Signal{Name: "update"}},
						Timeout: durationpb.New(2 * time.Minute),
					}},
				}},
			}},
		}},
	}
}

// signalWithStart is the request every SignalWithStart test below sends, under a
// key each test names for itself so parallel tests cannot collide.
func signalWithStart(key string, workflow *v1.Workflow) *connect.Request[v1.SignalWithStartRequest] {
	return connect.NewRequest(&v1.SignalWithStartRequest{
		EntityKey: key,
		Workflow:  workflow,
		Name:      "update",
		Payload:   updatePayload(1, false),
	})
}

// TestSignalWithStartAttestsTheSpecificationItCreatedTheEntityFrom is the
// ordinary case: nothing was substituted, so the caller's own copy describes the
// entity and the server says so.
//
// The presence check is not decoration, for the reason it is not decoration on
// `Run`: a client cannot tell an unset field from a false one through the
// generated getter, so an attestation that is true and left unset reads exactly
// like a server that never answered.
func TestSignalWithStartAttestsTheSpecificationItCreatedTheEntityFrom(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := mustNew(t, temporal)

	created, err := flowstate.SignalWithStart(t.Context(), signalWithStart("attest-plain", attestableWorkflow(false)))
	require.NoError(t, err)
	require.True(t, created.Msg.GetCreated())

	require.NotNil(t, created.Msg.SpecificationAsSubmitted,
		"the server said nothing, which a client must read as a substitution it cannot rule out")
	assert.True(t, created.Msg.GetSpecificationAsSubmitted())
	assert.True(t, created.Msg.RanSubmittedSpecification())
}

// TestSignalWithStartRefusesToAttestASubstitutedSpecification is the direction
// the field exists for, on this verb.
//
// The deployment's copy marks the output sensitive and the submitted copy does
// not — the disagreement that reaches a terminal as a secret in the clear. And
// the second half is the same distinction `Run`'s test draws: the answer is about
// the two specifications rather than about whether a registration exists, so a
// trusted copy identical to the submitted one keeps its caller the precise view.
func TestSignalWithStartRefusesToAttestASubstitutedSpecification(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := mustNew(t, temporal,
		server.WithTrustedWorkflows("", attestableWorkflow(true)))

	substituted, err := flowstate.SignalWithStart(t.Context(), signalWithStart("attest-substituted", attestableWorkflow(false)))
	require.NoError(t, err)
	require.True(t, substituted.Msg.GetCreated())

	require.NotNil(t, substituted.Msg.SpecificationAsSubmitted,
		"a server that substitutes must say so rather than leaving the caller to infer it")
	assert.False(t, substituted.Msg.GetSpecificationAsSubmitted(),
		"the deployment created the entity from its own copy and told the caller theirs was what ran")
	assert.False(t, substituted.Msg.RanSubmittedSpecification())

	identical := mustNew(t, temporal,
		server.WithTrustedWorkflows("", attestableWorkflow(false)))

	same, err := identical.SignalWithStart(t.Context(), signalWithStart("attest-identical", attestableWorkflow(false)))
	require.NoError(t, err)
	assert.True(t, same.Msg.RanSubmittedSpecification(),
		"a trusted copy equal to the submitted one was reported as a substitution")
}

// TestSignalWithStartRefusesToAttestADeliveryToAnEntityItDidNotCreate is this
// verb's own half of the question, and the one `Run` has no equivalent of.
//
// A second call under the same key delivers a mutation to an entity that was
// already running — created by an earlier call, possibly by another caller, from
// a file this server never compared against anything. Whatever that run is
// executing, this handler did not hand it to any engine, so it may not attest it:
// false, stated rather than left unset, because the server is answering rather
// than staying silent.
//
// Nothing about the specifications changes between the two calls here — the same
// server, the same bytes, an answer of true on the first call — which is what
// makes the second answer a property of the delivery rather than of the file.
func TestSignalWithStartRefusesToAttestADeliveryToAnEntityItDidNotCreate(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	startWorker(t, temporal)
	flowstate := mustNew(t, temporal)

	first, err := flowstate.SignalWithStart(t.Context(), signalWithStart("attest-existing", attestableWorkflow(false)))
	require.NoError(t, err)
	require.True(t, first.Msg.GetCreated())
	require.True(t, first.Msg.RanSubmittedSpecification())

	second, err := flowstate.SignalWithStart(t.Context(), signalWithStart("attest-existing", attestableWorkflow(false)))
	require.NoError(t, err)
	require.False(t, second.Msg.GetCreated(), "the second call should have found the entity already running")

	require.NotNil(t, second.Msg.SpecificationAsSubmitted,
		"a server with an opinion must state it rather than fall silent on the delivery path")
	assert.False(t, second.Msg.GetSpecificationAsSubmitted(),
		"a delivery to a run this call did not create was attested as executing the caller's copy")
	assert.False(t, second.Msg.RanSubmittedSpecification())
}

// TestCreateScheduleAttestsTheSpecificationItFroze is the ordinary case on the
// third verb, and the one whose answer has the longest reach: what a schedule
// freezes is what fires at three in the morning, when nobody is present to be
// told anything about it.
func TestCreateScheduleAttestsTheSpecificationItFroze(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := mustNew(t, temporal)

	created, err := flowstate.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
		Workflow: attestableWorkflow(false),
		Name:     "attested-plain",
		Paused:   true,
	}))
	require.NoError(t, err)

	require.NotNil(t, created.Msg.SpecificationAsSubmitted,
		"the server said nothing, which a client must read as a substitution it cannot rule out")
	assert.True(t, created.Msg.GetSpecificationAsSubmitted())
	assert.True(t, created.Msg.RanSubmittedSpecification())
}

// TestCreateScheduleRefusesToAttestASubstitutedSpecification is the direction the
// field exists for, on the verb where the substitution is worth the most: a
// caller must not be able to take a trusted workflow, schedule their own copy of
// it under the trusted name, and be told the copy they hold is what will fire.
func TestCreateScheduleRefusesToAttestASubstitutedSpecification(t *testing.T) {
	t.Parallel()

	temporal, _ := newTemporalNamespace(t)
	flowstate := mustNew(t, temporal,
		server.WithTrustedWorkflows("", attestableWorkflow(true)))

	created, err := flowstate.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
		Workflow: attestableWorkflow(false),
		Name:     "attested-substituted",
		Paused:   true,
	}))
	require.NoError(t, err)

	require.NotNil(t, created.Msg.SpecificationAsSubmitted,
		"a server that substitutes must say so rather than leaving the caller to infer it")
	assert.False(t, created.Msg.GetSpecificationAsSubmitted(),
		"the deployment scheduled its own copy and told the caller theirs was what would fire")
	assert.False(t, created.Msg.RanSubmittedSpecification())
}

// TestTheCreatingVerbsAgreeOnTheAttestation is the point of the whole change,
// and the reason it is one test rather than three assertions in three places.
//
// One server, one submitted specification, three RPCs that each bring durable
// work into existence from it. A caller must get the same answer from all three,
// because it is the same question — is the copy I hold the one that runs — and a
// client that has to remember which verb answers it differently is a client that
// will eventually read the wrong one.
//
// Run twice over: against a deployment that substitutes nothing, where all three
// must attest, and against one that substitutes a copy differing only in what it
// marks sensitive, where none of them may. Comparing the answers to each other
// rather than only to a constant is what would catch a fourth verb added later
// with the field left unset — the failure this issue was filed about, arriving
// again.
func TestTheCreatingVerbsAgreeOnTheAttestation(t *testing.T) {
	t.Parallel()

	for _, deployment := range []struct {
		name     string
		trusted  []server.Option
		attested bool
	}{
		{
			name:     "nothing substituted",
			attested: true,
		},
		{
			name:     "a trusted copy substituted",
			trusted:  []server.Option{server.WithTrustedWorkflows("", attestableWorkflow(true))},
			attested: false,
		},
	} {
		t.Run(deployment.name, func(t *testing.T) {
			t.Parallel()

			temporal, _ := newTemporalNamespace(t)
			flowstate := mustNew(t, temporal, deployment.trusted...)

			run, err := flowstate.Run(t.Context(), connect.NewRequest(&v1.RunRequest{
				Workflow: attestableWorkflow(false),
			}))
			require.NoError(t, err)

			entity, err := flowstate.SignalWithStart(t.Context(), signalWithStart("agreement", attestableWorkflow(false)))
			require.NoError(t, err)
			require.True(t, entity.Msg.GetCreated())

			schedule, err := flowstate.CreateSchedule(t.Context(), connect.NewRequest(&v1.CreateScheduleRequest{
				Workflow: attestableWorkflow(false),
				Name:     "agreement",
				Paused:   true,
			}))
			require.NoError(t, err)

			for verb, answered := range map[string]bool{
				"Run":             run.Msg.RanSubmittedSpecification(),
				"SignalWithStart": entity.Msg.RanSubmittedSpecification(),
				"CreateSchedule":  schedule.Msg.RanSubmittedSpecification(),
			} {
				assert.Equal(t, deployment.attested, answered,
					"%s disagreed with the other creating verbs about the same specification on the same server", verb)
			}

			// And each of the three said something, rather than agreeing by all
			// falling silent — which for the substituting deployment would satisfy
			// every assertion above while being exactly the gap #844 reports.
			require.NotNil(t, run.Msg.SpecificationAsSubmitted)
			require.NotNil(t, entity.Msg.SpecificationAsSubmitted)
			require.NotNil(t, schedule.Msg.SpecificationAsSubmitted)
		})
	}
}
