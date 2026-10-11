package flowfile_test

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	decisionv1 "github.com/picatz/flowstate/pkg/flowstate/decision/v1"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
	"github.com/picatz/flowstate/pkg/flowstate/v1/internal/conformance"
)

// The knobs a task definition turns, each of which a registry other than the
// default must be the one to answer: nested secret inputs, a literal check, an
// input that must be an expression, an input the task evaluates itself, and the
// enum names an output holds. Every case is a pair of registries that define the
// same task name with the knob set in one and not the other, so the same source
// is clean under exactly one of them.

const (
	knobNest  = "acme.nest"
	knobLit   = "acme.lit"
	knobExpr  = "acme.expr"
	knobDefer = "acme.defer"
	knobEnum  = "acme.enum"
)

// knobDefs defines the five tasks with the knobs on, or all off.
func knobDefs(on bool) []v1.TaskDef {
	http := (&v1.Task_HTTP_Inputs{}).ProtoReflect().Descriptor()
	defs := []v1.TaskDef{
		{Name: knobNest, Inputs: http},
		{Name: knobLit, Inputs: http},
		{Name: knobExpr, Inputs: http},
		{Name: knobDefer, Inputs: http},
		{Name: knobEnum, Inputs: (&v1.Task_Log_Inputs{}).ProtoReflect().Descriptor()},
	}
	if on {
		defs[0].NestedSecretInputs = []string{"headers"}
		defs[1].CheckLiteral = func(input string, value *v1.Value) error {
			if input == "url" && strings.Contains(value.GetLiteral().GetStringValue(), "forbidden") {
				return errors.New("acme.lit refuses this url")
			}

			return nil
		}
		defs[2].ExpressionInputs = []string{"body"}
		defs[3].DeferredInputs = []string{"body"}
		defs[4].Outputs = (&decisionv1.Answer{}).ProtoReflect().Descriptor()
	}

	return defs
}

func knobSource(task, inputs string) string {
	return "edition: " + flowfile.CurrentEdition + "\nname: t\nsteps:\n  - id: s\n    " + task + ":\n" + inputs
}

var knobCases = []struct {
	name, task, src string
	// onClean is true when the source is clean under the registry with the knob
	// on, and so reported under the one without; false is the reverse.
	onClean bool
}{
	{"nested secret input", knobNest,
		knobSource(knobNest, "      url: https://example.com\n      headers:\n        Authorization: ${secret('env:TOKEN')}\n"), true},
	{"literal check", knobLit, knobSource(knobLit, "      url: https://forbidden.example.com\n"), false},
	{"input that must be an expression", knobExpr, knobSource(knobExpr, "      url: https://example.com\n      body: plain text\n"), false},
	{"input the task evaluates itself", knobDefer, knobSource(knobDefer, "      url: https://example.com\n      body: ${steps.nope.result}\n"), true},
	{"enum names of an output", knobEnum, "edition: " + flowfile.CurrentEdition + `
name: t
steps:
  - id: s
    ` + knobEnum + `:
      message: hi
  - id: gate
    value: ${steps.s.calibration == CALIBRATION_SELF_REPORTED}
`, true},
}

// problems is everything that is wrong with src under env, from the compiler or
// the validator.
func problemsIn(env flowfile.Env, src string) string {
	wf, err := env.Unmarshal([]byte(src))
	if err != nil {
		return err.Error()
	}

	return env.Validate(wf).Error()
}

func checkKnobs(t *testing.T, on, off flowfile.Env) {
	t.Helper()

	for _, tc := range knobCases {
		withKnob, without := problemsIn(on, tc.src), problemsIn(off, tc.src)
		if tc.onClean {
			assert.Empty(t, withKnob, "%s: the registry that sets it", tc.name)
			assert.NotEmpty(t, without, "%s: the registry that does not", tc.name)
		} else {
			assert.NotEmpty(t, withKnob, "%s: the registry that sets it", tc.name)
			assert.Empty(t, without, "%s: the registry that does not", tc.name)
		}
	}
}

func TestEnvPolicyKnobsComeFromTheEnvsRegistry(t *testing.T) {
	t.Parallel()

	checkKnobs(t, flowfile.WithRegistry(envRegistry(t, knobDefs(true)...)), flowfile.WithRegistry(envRegistry(t, knobDefs(false)...)))

	// The lowered enum name is the number the output holds, under the registry
	// that describes it and not otherwise.
	on := flowfile.WithRegistry(envRegistry(t, knobDefs(true)...))
	named, err := on.Unmarshal([]byte(knobCases[4].src))
	require.NoError(t, err)
	numbered, err := on.Unmarshal([]byte(strings.Replace(knobCases[4].src, "CALIBRATION_SELF_REPORTED", "2", 1)))
	require.NoError(t, err)
	assert.True(t, proto.Equal(named.GetSteps()[1].GetValue().GetExpr().GetExpr(), numbered.GetSteps()[1].GetValue().GetExpr().GetExpr()))
}

// A default registry that disagrees changes nothing for an Env of its own, and is
// what the zero Env answers from. Not parallel: it writes the process default.
func TestEnvPolicyKnobsIgnoreADefaultThatDisagrees(t *testing.T) {
	for _, def := range knobDefs(false) {
		def.Fn = noopTask
		require.NoError(t, v1.DefaultRegistry().Register(def))
		t.Cleanup(func() { v1.DefaultRegistry().Unregister(def.Name) })
	}

	// The default holds the knobs-off definitions, so the zero Env behaves as the
	// knobs-off registry does, and the knobs-on Env still behaves as itself.
	checkKnobs(t, flowfile.WithRegistry(envRegistry(t, knobDefs(true)...)), flowfile.Env{})
}

// Plugin credential binding is a security path: the binding is expanded onto a
// step, and elided from it on the way out, only for a plugin the Env's registry
// holds. The default registry here does not hold it, which is the negative.

const credentialBindingSource = `edition: ` + flowfile.CurrentEdition + `
name: bound
plugins:
  bound:
    version: v0.1.0
    credentials:
      api_token: ${secret('env:BOUND_TOKEN')}
steps:
  - id: send
    bound.use:
      note: a
`

func credentialEnv(t *testing.T) flowfile.Env {
	t.Helper()

	return flowfile.WithRegistry(envRegistry(t, conformance.BoundCredentialTaskDef()))
}

func TestEnvExpandsPluginCredentialBindingsFromItsOwnRegistry(t *testing.T) {
	t.Parallel()

	env := credentialEnv(t)
	wf, _, err := env.Parse([]byte(credentialBindingSource))
	require.NoError(t, err)
	assert.Equal(t, "BOUND_TOKEN", tokenName(t, wf.GetSteps()[0]), "the binding was expanded onto the step")

	// The default registry has never heard of the plugin, so nothing is expanded
	// and nothing is refused.
	plain, _, err := flowfile.Env{}.Parse([]byte(credentialBindingSource))
	require.NoError(t, err)
	assert.NotContains(t, plain.GetSteps()[0].GetTask().GetInputs(), "token")
}

func TestEnvValidateExpandsBindingsOnAnUnexpandedWorkflow(t *testing.T) {
	t.Parallel()

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
	env := credentialEnv(t)

	wf := build(map[string]*v1.Value{conformance.BoundCredentialName: ref})
	assert.Empty(t, env.Validate(wf), "the Env's registry says which input the binding fills")
	assert.NotContains(t, wf.GetSteps()[0].GetTask().GetInputs(), "token", "the caller's workflow was not expanded")

	// A binding the registry can judge and refuses is the finding.
	ds := env.Validate(build(map[string]*v1.Value{conformance.BoundCredentialName: v1.NewLiteral("plain-text")}))
	require.Len(t, ds, 1)
	assert.Contains(t, ds[0].Message, "must be bound to a whole reference")

	// The default cannot judge it: the task is unknown there.
	assert.Contains(t, flowfile.Validate(wf).Error(), "no plugin task")
}

func TestEnvMarshalElidesBoundCredentialsOnlyWhereTheRegistryKnowsThePlugin(t *testing.T) {
	t.Parallel()

	env := credentialEnv(t)
	wf, _, err := env.Parse([]byte(credentialBindingSource))
	require.NoError(t, err)

	out, err := env.Marshal(wf)
	require.NoError(t, err)
	assert.Contains(t, string(out), "plugins:")
	assert.Contains(t, string(out), "api_token: ${secret('env:BOUND_TOKEN')}", "the binding is kept")
	assert.Equal(t, 1, strings.Count(string(out), "BOUND_TOKEN"), "the reference is written once")
	again, _, err := env.Parse(out)
	require.NoError(t, err)
	assert.True(t, proto.Equal(wf, again), "and reads back as the same workflow")

	// The default registry cannot say which input claims the credential, so it
	// elides nothing and the step carries the reference.
	unelided, err := flowfile.Env{}.Marshal(wf)
	require.NoError(t, err)
	assert.Equal(t, 2, strings.Count(string(unelided), "BOUND_TOKEN"), "the step keeps its reference beside the binding")
}

// A step that says nothing it does is told what a step may be, and that list is
// the Env's: a registry holding one task lists that task and none of the
// built-ins the default holds. Covers each site that renders the list.
func TestEnvKindlessStepListsOnlyItsOwnTasks(t *testing.T) {
	t.Parallel()

	only := v1.NewRegistry()
	require.NoError(t, only.Register(v1.TaskDef{Name: "acme.only", Fn: noopTask}))
	env := flowfile.WithRegistry(only)

	assertOwn := func(t *testing.T, where, message string) {
		t.Helper()
		assert.Contains(t, message, "acme.only", where)
		for _, builtin := range []string{"http", "exec", "log"} {
			assert.NotRegexp(t, regexp.MustCompile(`\b`+builtin+`\b`), message, "%s lists the default's %s", where, builtin)
		}
	}
	defaultList := flowfile.StepKindList()
	require.Regexp(t, `\bhttp\b`, defaultList, "the default does list the built-ins, so the assertion above can fail")

	_, err := env.Unmarshal([]byte("edition: " + flowfile.CurrentEdition + "\nname: t\nsteps:\n  - id: s\n"))
	require.Error(t, err)
	assertOwn(t, "compiling a kind-less step", err.Error())

	_, err = env.Unmarshal([]byte("edition: " + flowfile.CurrentEdition + "\nname: t\nsteps: oops\n"))
	require.Error(t, err)
	assertOwn(t, "compiling a non-list steps", err.Error())

	kindless := &v1.Workflow{Name: "t", Profile: v1.CurrentProfile, Steps: []*v1.Node{{Id: "s"}}}
	assertOwn(t, "validating a kind-less step", env.Validate(kindless).Error())

	nested := &v1.Workflow{Name: "t", Profile: v1.CurrentProfile, Steps: []*v1.Node{{Id: "loop", Kind: &v1.Node_ForEach{ForEach: &v1.ForEach{
		Items: v1.NewLiteral([]any{"a"}), Body: []*v1.Node{{Id: "inner"}},
	}}}}}
	assertOwn(t, "validating a nested kind-less step", env.Validate(nested).Error())

	_, err = env.Marshal(kindless)
	require.Error(t, err)
	assertOwn(t, "marshalling a kind-less step", err.Error())

	assertOwn(t, "StepKindList", env.StepKindList())
}
