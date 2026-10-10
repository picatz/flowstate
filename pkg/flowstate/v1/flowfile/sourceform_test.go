package flowfile_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// A source form is written back in place of the rule that runs, so Marshal refuses
// one that does not expand to it: a stale or hand-built specification never writes
// a file that compiles to a different policy.
func TestMarshalRefusesASourceFormThatDoesNotExpandToTheRuleThatRuns(t *testing.T) {
	t.Parallel()

	parse := func(t *testing.T, source string) *v1.Workflow {
		t.Helper()

		wf, _, err := flowfile.Parse([]byte(source))
		require.NoError(t, err)
		_, err = flowfile.Marshal(wf)
		require.NoError(t, err, "the compiled workflow must marshal before it is made stale")

		return wf
	}

	for name, test := range map[string]struct {
		source string
		stale  func(*v1.Workflow)
		want   string
	}{
		"signal allow": {everywhereFile, func(wf *v1.Workflow) {
			wf.GetSignals()["go"].AllowSource = proto.String("isOps(sender.identity.claims.team) || true")
		}, `signal "go" allow`},
		"debug allow": {everywhereFile, func(wf *v1.Workflow) {
			wf.GetDebug().AllowSource = proto.String("isOps(sender.identity.claims.team) || true")
		}, "debug allow"},
		"manual allow": {everywhereFile, func(wf *v1.Workflow) {
			wf.GetTriggers().GetManual().AllowSource = proto.String("isOps(sender.identity.claims.team) || true")
		}, "triggers manual allow"},
		"an undeclared function": {everywhereFile, func(wf *v1.Workflow) {
			wf.GetDebug().AllowSource = proto.String("neverDeclared(sender.identity.claims.team)")
		}, "debug allow"},
		"input must": {mustFile, func(wf *v1.Workflow) {
			wf.DeclaredInputs[0].MustSource = proto.String("size(this) > 0")
		}, "must"},
		"a scalar type with no rule": {mustFile, func(wf *v1.Workflow) {
			wf.DeclaredInputs[0].TypeSource = proto.String("Code")
			wf.DeclaredInputs[0].Must = nil
		}, "does not declare"},
		"a scalar use whose rule was swapped": {scalarFile, func(wf *v1.Workflow) {
			for _, d := range wf.DeclaredInputs {
				if d.GetName() == "alias" {
					d.Must = proto.String("false")
				}
			}
		}, "not the rule of scalar type"},
		"a scalar use whose own rule was swapped": {scalarFile, func(wf *v1.Workflow) {
			for _, d := range wf.DeclaredInputs {
				if d.GetName() == "id" {
					d.MustSource = proto.String("this != \"zzz-99\"")
				}
			}
		}, "not the rule of scalar type"},
		"a scalar output whose rule was swapped": {scalarFile, func(wf *v1.Workflow) {
			wf.DeclaredOutputs[0].Must = proto.String("false")
		}, "not the rule of scalar type"},
		"a scalar record field whose rule was swapped": {scalarFile, func(wf *v1.Workflow) {
			for _, ty := range wf.DeclaredTypes {
				for _, f := range ty.GetFields() {
					f.Must = proto.String("false")
				}
			}
		}, "not the rule of scalar type"},
		"a type must": {mustFile, func(wf *v1.Workflow) {
			wf.DeclaredTypes[0].MustSource = proto.String("size(this) > 0")
		}, "must"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			wf := parse(t, test.source)
			test.stale(wf)

			out, err := flowfile.Marshal(wf)
			require.Error(t, err)
			assert.Nil(t, out)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}

// A module that uses another module's scalar carries a record field typed by the
// chained name; that declaration is the module's to write back, so Marshal's source
// check must not ask this workflow to declare it.
func TestMarshalAcceptsAScalarCarriedThroughTwoModules(t *testing.T) {
	t.Parallel()

	dir := tree(t, map[string]string{
		"m.yaml": workflowUsing("use:\n  b:\n    path: ./b.yaml\n", ""),
		"b.yaml": "edition: " + flowfile.CurrentEdition + "\nname: b\nuse:\n  a:\n    path: ./a.yaml\ntypes:\n  Rec:\n    fields:\n      c:\n        type: a.Code\n        required: true\n",
		"a.yaml": "edition: " + flowfile.CurrentEdition + "\nname: a\ntypes:\n  Code:\n    type: string\n    must: size(this) > 2\n",
	})
	wf, err := compileAt(t, dir, "m.yaml")
	require.NoError(t, err)

	_, err = flowfile.Marshal(wf)
	require.NoError(t, err)
}
