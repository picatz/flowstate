package flowfile_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// The mapping form of a `plugins:` entry, from the file: a credential bound once
// for a plugin's steps. The fixture plugin is one task, `bound.use`, whose
// `token` input claims the plugin's `api_token` credential.

const boundHeader = `edition: v2026.4
name: bound
`

// registerBoundPlugin installs the fixture task for the test. Not parallel: it
// writes the process-wide registry the compiler reads.
func registerBoundPlugin(t *testing.T) {
	t.Helper()

	require.NoError(t, v1.DefaultRegistry().Register(conformance.BoundCredentialTaskDef()))
	require.NoError(t, v1.DefaultRegistry().Register(conformance.FederatedCredentialTaskDef()))
	t.Cleanup(func() {
		v1.DefaultRegistry().Unregister(conformance.BoundCredentialTaskName)
		v1.DefaultRegistry().Unregister(conformance.FederatedCredentialTaskName)
	})
}

func tokenName(t *testing.T, node *v1.Node) string {
	t.Helper()

	return node.GetTask().GetInputs()["token"].GetSecretRef().GetName()
}

func TestPluginCredentialsAreBoundOnceAndExpandedIntoSteps(t *testing.T) {
	registerBoundPlugin(t)

	wf, _, err := flowfile.Parse([]byte(boundHeader + `plugins:
  bound:
    version: v0.1.0
    credentials:
      api_token: ${secret('env:BOUND_TOKEN')}
  git: v0.1.0
steps:
  - id: bound
    bound.use:
      note: a
  - id: overridden
    bound.use:
      note: b
      token: ${secret('env:OVERRIDE_TOKEN')}
`))
	require.NoError(t, err)

	require.Len(t, wf.GetPluginRequirements(), 2)
	bound := wf.GetPluginRequirements()[0]
	require.Equal(t, "v0.1.0", bound.GetMinimumVersion())
	require.Equal(t, "BOUND_TOKEN", bound.GetCredentials()["api_token"].GetSecretRef().GetName())
	require.Empty(t, wf.GetPluginRequirements()[1].GetCredentials(), "the scalar form binds nothing")

	require.Equal(t, "BOUND_TOKEN", tokenName(t, wf.GetSteps()[0]), "the omitting step did not receive the binding")
	require.Equal(t, "OVERRIDE_TOKEN", tokenName(t, wf.GetSteps()[1]), "the binding replaced a step's own reference")
}

func TestPluginCredentialsAreRefusedWhereTheyAreWritten(t *testing.T) {
	registerBoundPlugin(t)

	tooMany := new(strings.Builder)
	for i := range v1.MaxPluginCredentials + 1 {
		fmt.Fprintf(tooMany, "      credential_%d: ${secret('env:X')}\n", i)
	}

	steps := "steps:\n  - id: a\n    bound.use:\n      note: a\n      token: ${secret('env:T')}\n"
	tests := []struct{ name, plugins, want string }{
		{
			name:    "a literal binding",
			plugins: "plugins:\n  bound:\n    version: v0.1.0\n    credentials:\n      api_token: plain-text\n",
			want:    `plugin "bound" credential "api_token" must be bound to a whole reference`,
		},
		{
			name:    "an expression binding",
			plugins: "plugins:\n  bound:\n    version: v0.1.0\n    credentials:\n      api_token: ${inputs.token}\n",
			want:    `plugin "bound" credential "api_token" must be bound to a whole reference`,
		},
		{
			name:    "a credential reference binding",
			plugins: "plugins:\n  bound:\n    version: v0.1.0\n    credentials:\n      api_token: ${credential('partner')}\n",
			want:    `plugin "bound" credential "api_token": is not declared federated, so bind it to a whole secret reference`,
		},
		{
			name:    "a credential the plugin does not declare",
			plugins: "plugins:\n  bound:\n    version: v0.1.0\n    credentials:\n      nope: ${secret('env:X')}\n",
			want:    `plugin "bound" has no credential "nope" to bind; it declares api_token`,
		},
		{
			name:    "a credential named like no credential",
			plugins: "plugins:\n  bound:\n    version: v0.1.0\n    credentials:\n      Not-A-Name: ${secret('env:X')}\n",
			want:    `"Not-A-Name" is not a credential name`,
		},
		{
			name:    "more bindings than a plugin may declare",
			plugins: "plugins:\n  bound:\n    version: v0.1.0\n    credentials:\n" + tooMany.String(),
			want:    fmt.Sprintf("plugin \"bound\" binds %d credentials; at most %d are allowed", v1.MaxPluginCredentials+1, v1.MaxPluginCredentials),
		},
		{
			name:    "a mapping with no version",
			plugins: "plugins:\n  bound:\n    credentials:\n      api_token: ${secret('env:X')}\n",
			want:    `plugin "bound" needs a version`,
		},
		{
			name:    "an unknown key beside the version",
			plugins: "plugins:\n  bound:\n    version: v0.1.0\n    credential:\n      api_token: ${secret('env:X')}\n",
			want:    `credential`,
		},
		{
			name:    "a bad version in the mapping form",
			plugins: "plugins:\n  bound:\n    version: \"0.1.0\"\n",
			want:    `plugin "bound" requires a semantic version written as vMAJOR.MINOR.PATCH`,
		},
		{
			name:    "credentials that are not a mapping",
			plugins: "plugins:\n  bound:\n    version: v0.1.0\n    credentials: [api_token]\n",
			want:    "must be a mapping",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := diagnose(t, boundHeader+tc.plugins+steps)
			require.Contains(t, got, tc.want)
			require.NotContains(t, got, "plain-text", "the diagnostic echoed what was written")
		})
	}
}

func TestAnUnboundCredentialInputIsReportedWithBothSpellings(t *testing.T) {
	registerBoundPlugin(t)

	got := diagnose(t, boundHeader+`plugins:
  bound: v0.1.0
steps:
  - id: send
    bound.use:
      note: a
`)

	require.Contains(t, got, `task "bound.use" input "token" receives the plugin's credential "api_token"`)
	require.Contains(t, got, "credentials: {api_token: ${secret('env:NAME')}}")
	require.Equal(t, 1, strings.Count(got, "\n")+1, "the missing input was reported twice: %s", got)

	// And bound, the same file validates.
	require.Empty(t, diagnose(t, boundHeader+`plugins:
  bound:
    version: v0.1.0
    credentials:
      api_token: ${secret('env:BOUND_TOKEN')}
steps:
  - id: send
    bound.use:
      note: a
`))
}

func TestPluginCredentialsSurviveARoundTrip(t *testing.T) {
	registerBoundPlugin(t)

	// Already in Marshal's own layout, so a correct Marshal reproduces it
	// exactly: the binding once, the step that relies on it without the input,
	// and the step that overrides it with its own.
	src := boundHeader + `plugins:
  bound:
    version: v0.1.0
    credentials:
      api_token: ${secret('env:BOUND_TOKEN')}
  git: v0.1.0
steps:
  - id: bound
    bound.use:
      note: a
  - id: overridden
    bound.use:
      token: ${secret('env:OVERRIDE_TOKEN')}
      note: b
`

	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)
	expanded := proto.Clone(wf)

	out, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	require.Equal(t, src, string(out), "Marshal did not write the binding once")
	require.True(t, proto.Equal(expanded, wf), "Marshal changed the workflow it was handed")

	again, err := flowfile.Unmarshal(out)
	require.NoError(t, err)
	require.True(t, proto.Equal(wf, again), "the marshalled file did not read back as the same workflow")
}

func TestMarshalWritesWhatItCannotElideAndRefusesWhatItCannotRead(t *testing.T) {
	registerBoundPlugin(t)

	src := boundHeader + `plugins:
  bound:
    version: v0.1.0
    credentials:
      api_token: ${secret('env:BOUND_TOKEN')}
steps:
  - id: bound
    bound.use:
      note: a
`
	wf, _, err := flowfile.Parse([]byte(src))
	require.NoError(t, err)

	// Without the plugin's tasks to say which input claims the credential,
	// nothing is elided: the reference is written on the step, which reads back
	// as the same specification.
	v1.DefaultRegistry().Unregister(conformance.BoundCredentialTaskName)
	out, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	require.Contains(t, string(out), "token: ${secret('env:BOUND_TOKEN')}")
	require.NoError(t, v1.DefaultRegistry().Register(conformance.BoundCredentialTaskDef()))

	again, err := flowfile.Unmarshal(out)
	require.NoError(t, err)
	require.True(t, proto.Equal(wf, again))

	// A binding the parser would refuse is refused here, not written.
	wf.GetPluginRequirements()[0].Credentials["api_token"] = v1.NewLiteral("plain-text")
	_, err = flowfile.Marshal(wf)
	require.ErrorContains(t, err, `plugin "bound" credential "api_token"`)
	require.NotContains(t, err.Error(), "plain-text")
}

func TestPluginCredentialsAreKeptWhenTheRegistryDoesNotKnowThePlugin(t *testing.T) {
	// No fixture registered: the compiler cannot say which input claims the
	// credential, so it expands nothing and refuses nothing. The server's
	// admission expands it against the deployment's registry.
	wf, _, err := flowfile.Parse([]byte(boundHeader + `plugins:
  elsewhere:
    version: v1.0.0
    credentials:
      api_token: ${secret('env:X')}
steps:
  - id: a
    log:
      message: hi
`))
	require.NoError(t, err)
	require.Len(t, wf.GetPluginRequirements()[0].GetCredentials(), 1)
}

// TestValidateExpandsBindingsOnAHandBuiltWorkflow: Validate is handed workflows
// that never went through Parse, so a binding it meets unexpanded is judged as
// the step it will become, and one that cannot be expanded is the finding.
func TestValidateExpandsBindingsOnAHandBuiltWorkflow(t *testing.T) {
	registerBoundPlugin(t)

	build := func(credentials map[string]*v1.Value) *v1.Workflow {
		return &v1.Workflow{
			Name:               "hand-built",
			Profile:            v1.CurrentProfile,
			PluginRequirements: []*v1.PluginRequirement{conformance.BoundCredentialRequirement(credentials)},
			Steps: []*v1.Node{{Id: "send", Kind: &v1.Node_Task{Task: &v1.Task{
				Name: conformance.BoundCredentialTaskName, Inputs: map[string]*v1.Value{"note": v1.NewLiteral("a")},
			}}}},
		}
	}
	ref := &v1.Value{Kind: &v1.Value_SecretRef{SecretRef: &v1.SecretRef{Scheme: "env", Name: "T"}}}

	wf := build(map[string]*v1.Value{conformance.BoundCredentialName: ref})
	require.Empty(t, flowfile.Validate(wf), "a valid binding was reported as an unwritten input")
	require.NotContains(t, wf.GetSteps()[0].GetTask().GetInputs(), "token", "Validate expanded the caller's workflow")

	ds := flowfile.Validate(build(map[string]*v1.Value{conformance.BoundCredentialName: v1.NewLiteral("plain-text")}))
	require.Len(t, ds, 1)
	require.Contains(t, ds[0].Message, "must be bound to a whole reference")
	require.NotContains(t, ds[0].Message, "plain-text")

	ds = flowfile.Validate(build(nil))
	require.NotEmpty(t, ds)
	require.Contains(t, ds[0].Message, "receives the plugin's credential")
}

// A federated credential is bound by a credential reference and a stored one by a
// secret reference, in the file as in a hand-built specification; the diagnostics
// name the declaration and never echo the reference written.

func TestAFederatedCredentialIsBoundByACredentialReference(t *testing.T) {
	registerBoundPlugin(t)

	wf, _, err := flowfile.Parse([]byte(boundHeader + `plugins:
  federated:
    version: v0.1.0
    credentials:
      partner_token: ${credential('partner')}
steps:
  - id: minted
    federated.use:
      note: a
  - id: own
    federated.use:
      note: b
      token: ${credential('other')}
`))
	require.NoError(t, err)

	require.Equal(t, "partner", wf.GetPluginRequirements()[0].GetCredentials()["partner_token"].GetCredentialRef().GetTarget())
	require.Equal(t, "partner", wf.GetSteps()[0].GetTask().GetInputs()["token"].GetCredentialRef().GetTarget())
	require.Equal(t, "other", wf.GetSteps()[1].GetTask().GetInputs()["token"].GetCredentialRef().GetTarget(), "the binding replaced a step's own reference")

	// And it reads back as written: the binding once, the override where it was.
	out, err := flowfile.Marshal(wf)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(out), "${credential('partner')}"), string(out))
}

func TestTheWrongKindOfReferenceIsRefusedForACredential(t *testing.T) {
	registerBoundPlugin(t)

	tests := []struct{ name, source, want string }{
		{
			name: "a stored secret bound to a federated credential",
			source: boundHeader + "plugins:\n  federated:\n    version: v0.1.0\n    credentials:\n      partner_token: ${secret('env:LEAKY_NAME')}\n" +
				"steps:\n  - id: a\n    federated.use:\n      note: a\n",
			want: `plugin "federated" credential "partner_token": is declared federated, so bind it to a whole credential reference`,
		},
		{
			name: "a step writing a stored secret into a federated credential",
			source: boundHeader + "plugins:\n  federated: v0.1.0\n" +
				"steps:\n  - id: a\n    federated.use:\n      note: a\n      token: ${secret('env:LEAKY_NAME')}\n",
			want: `task "federated.use" input "token" receives the plugin's federated credential "partner_token"`,
		},
		{
			name: "a step writing a credential reference into a stored credential",
			source: boundHeader + "plugins:\n  bound: v0.1.0\n" +
				"steps:\n  - id: a\n    bound.use:\n      note: a\n      token: ${credential('LEAKY_TARGET')}\n",
			want: `task "bound.use" input "token" receives the plugin's credential "api_token", which is not federated`,
		},
		{
			name: "a federated credential input written nowhere",
			source: boundHeader + "plugins:\n  federated: v0.1.0\n" +
				"steps:\n  - id: a\n    federated.use:\n      note: a\n",
			want: `write it as a whole credential reference such as ${credential('target')}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := diagnose(t, tc.source)
			require.Contains(t, got, tc.want)
			require.NotContains(t, got, "LEAKY", "the diagnostic echoed the reference written")
		})
	}
}
