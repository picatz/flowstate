package flowfile_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	chatv1 "github.com/picatz/flowstate/pkg/flowstate/chat/v1"
	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// registerChatProbe installs two tasks whose input messages are the engine's own
// chat messages, which carry the literal claim on Markup.template: one takes a
// Markup directly, one a Notice, which holds Text in a list.
func registerChatProbe(t *testing.T) (markup, notice string) {
	t.Helper()

	markup, notice = "test_literal_markup_probe", "test_literal_notice_probe"
	register := func(name string, inputs proto.Message) {
		require.NoError(t, v1.DefaultRegistry().Register(v1.TaskDef{
			Name:   name,
			Inputs: inputs.ProtoReflect().Descriptor(),
			Fn: func(context.Context, map[string]*v1.Value, *v1.Scope) (*v1.Node_Outputs, error) {
				return nil, nil
			},
		}))
		t.Cleanup(func() { v1.DefaultRegistry().Unregister(name) })
	}
	register(markup, &chatv1.Markup{})
	register(notice, &chatv1.Notice{})

	return markup, notice
}

// TestLiteralFieldIsHeldToItsClaim pins both directions of the claim on
// Markup.template through the real compiler: an author's literal template is
// accepted whatever expressions sit in its args, and an expression, a nested
// expression or a reference at the template is refused with the field path,
// positioned on the input.
func TestLiteralFieldIsHeldToItsClaim(t *testing.T) {
	markup, notice := registerChatProbe(t)

	body := func(task, inputs string) string {
		return "edition: v2026.4\nname: t\nsteps:\n  - id: a\n    " + task + ":\n" + inputs
	}

	for _, tc := range []struct {
		name string
		src  string
		// want is the substring the diagnostic must carry; empty means clean.
		want string
		line int
	}{
		{
			name: "a literal template with an expression argument is accepted",
			src: body(markup, `      template: "*{who}*"
      args:
        who: ${run.workflow_id}
`),
		},
		{
			name: "a literal template written entirely out is accepted",
			src:  body(markup, "      template: hello\n"),
		},
		{
			name: "an expression template is refused",
			src:  body(markup, "      template: ${run.workflow_id}\n"),
			want: "template must be written as a literal, but is an expression",
			line: 6,
		},
		{
			name: "a template spliced from an expression is refused",
			src:  body(markup, "      template: \"*${run.workflow_id}*\"\n"),
			want: "template must be written as a literal, but is an expression",
			line: 6,
		},
		{
			name: "a secret reference as the template is refused",
			src:  body(markup, "      template: ${secret('env:T')}\n"),
			want: "template must be written as a literal, but is a secret reference",
			line: 6,
		},
		{
			name: "a literal template inside a list element is accepted",
			src: body(notice, `      title: {plain: hi}
      body:
        - markup: {template: "*{a}*", args: {a: "${run.workflow_id}"}}
`),
		},
		{
			name: "an expression template inside a list element is refused",
			src: body(notice, `      title: {plain: hi}
      body:
        - plain: fine
        - markup: {template: "${run.workflow_id}", args: {}}
`),
			want: "body[1].markup.template must be written as a literal",
			line: 8,
		},
		{
			name: "a whole expression where a nested template might hide is refused",
			src: body(notice, `      title: ${steps.x.value}
`),
			want: "title.markup.template inside it must be written as a literal",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ds, err := flowfile.ValidateSource([]byte(tc.src))
			require.NoError(t, err)

			if tc.want == "" {
				for _, d := range ds {
					require.NotContains(t, d.Message, "literal", ds.Error())
				}

				return
			}

			var found *flowfile.Diagnostic
			for i := range ds {
				if ds[i].Message != "" && strings.Contains(ds[i].Message, tc.want) {
					found = &ds[i]
				}
			}
			require.NotNil(t, found, "no diagnostic carries %q; got:\n%s", tc.want, ds.Error())
			require.Equal(t, "a", found.Step)
			if tc.line != 0 {
				require.Equal(t, tc.line, found.Line, ds.Error())
			}
		})
	}
}
