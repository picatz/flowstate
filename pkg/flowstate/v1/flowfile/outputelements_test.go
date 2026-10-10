package flowfile_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	decisionv1 "github.com/picatz/flowstate/pkg/flowstate/decision/v1"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The probe stands in for a plugin task whose output holds a repeated message
// (`questions`, each a decision Question), the shape of `anthropic.decide`'s
// `answers`. Its descriptor is the one a catalog would carry.
const elementProbeTask = "test_output_elements_probe"

func registerElementProbe(t *testing.T) {
	t.Helper()

	require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
		Name:    elementProbeTask,
		Inputs:  (&v1.Task_Log_Inputs{}).ProtoReflect().Descriptor(),
		Outputs: (&decisionv1.QuestionSet{}).ProtoReflect().Descriptor(),
		Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
			return &v1.Node_Outputs{}, nil
		},
	}))
	t.Cleanup(func() { v1.DefaultRegistry().Unregister(elementProbeTask) })
}

func elementSource(expression string) string {
	return `edition: v2026.4
name: elements
steps:
  - id: probe
    ` + elementProbeTask + `:
      message: hi
  - id: gate
    vars:
      rows:
        - nme: 1
    value: '${` + expression + `}'
`
}

func TestOutputElementFieldsAreChecked(t *testing.T) {
	registerElementProbe(t)

	for name, tc := range map[string]struct {
		expression string
		want       []string
	}{
		"a typo after filter and index": {
			expression: `steps.probe.questions.filter(q, true)[0].nme == "x"`,
			want:       []string{"`nme` is not a field of Question", "did you mean `name`?"},
		},
		"a typo after plain indexing": {
			expression: `steps.probe.questions[0].instructon`,
			want:       []string{"did you mean `instructions`?"},
		},
		"a typo in a nested message": {
			expression: `steps.probe.questions[0].choice.optons`,
			want:       []string{"is not a field of Choice", "did you mean `options`?"},
		},
		"a typo on a macro variable over the list": {
			expression: `steps.probe.questions.exists(q, q.nmae == "x")`,
			want:       []string{"`nmae` is not a field of Question", "did you mean `name`?"},
		},
		"a typo in the predicate of a filter": {
			expression: `steps.probe.questions.filter(q, q.nmae == "x").size() > 0`,
			want:       []string{"did you mean `name`?"},
		},
		"a typo through optional selection": {
			expression: `steps.probe.questions[0].?nmae.orValue("")`,
			want:       []string{"`nmae` is not a field of Question", "did you mean `name`?"},
		},
		"a typo after an optional message field": {
			expression: `steps.probe.questions[0].?choice.?optons.orValue([])`,
			want:       []string{"is not a field of Choice", "did you mean `options`?"},
		},
		"a typo inside has": {
			expression: `steps.probe.questions.exists(q, has(q.chioce))`,
			want:       []string{"did you mean `choice`?"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ds, err := flowfile.ValidateSource([]byte(elementSource(tc.expression)))
			require.NoError(t, err)
			require.NotEmpty(t, ds, "a misspelled field must be refused")
			for _, want := range tc.want {
				assert.Contains(t, ds.Error(), want)
			}
		})
	}
}

// TestOutputElementFieldsStayValidWhereNotProvable pins the other direction: a
// real field is accepted, and a name that merely looks like an element is not
// judged.
func TestOutputElementFieldsStayValidWhereNotProvable(t *testing.T) {
	registerElementProbe(t)

	for name, expression := range map[string]string{
		"real fields":                        `steps.probe.questions.filter(q, q.name == "a")[0].instructions == ""`,
		"a nested real field":                `size(steps.probe.questions[0].choice.options)`,
		"has on a real field":                `steps.probe.questions.exists(q, has(q.choice))`,
		"every macro variable use":           `steps.probe.questions.all(q, q.name != "")`,
		"a var of the same field name":       `rows[0].nme == 1`,
		"a record built with map":            `steps.probe.questions.map(q, {"nme": 1})[0].nme == 1`,
		"a variable over a map result":       `steps.probe.questions.map(q, {"nme": 1}).exists(r, r.nme == 1)`,
		"an inner variable that shadows":     `steps.probe.questions.exists(q, rows.exists(q, q.nme == 1))`,
		"a two-variable macro":               `steps.probe.questions.exists(i, q, q.nme == 1)`,
		"optional selection of real fields":  `steps.probe.questions[0].?name.orValue("") == "" && steps.probe.questions[0].?choice.?options.hasValue()`,
		"optional selection on a var record": `rows[0].?nme.orValue(0) == 1`,
		"an undeclared bare name":            `unknown.nme`,
	} {
		t.Run(name, func(t *testing.T) {
			ds, err := flowfile.ValidateSource([]byte(elementSource(expression)))
			require.NoError(t, err)
			for _, d := range ds {
				assert.NotContains(t, d.Message, "is not a field of", "%s must not be judged: %s", expression, d.Message)
			}
		})
	}
}
