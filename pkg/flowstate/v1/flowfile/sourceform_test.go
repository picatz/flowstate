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
		}, "carries no rule"},
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
