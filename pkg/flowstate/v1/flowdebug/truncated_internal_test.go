package flowdebug

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// TestATruncatedProgramRefusesAStepItNeverDeclares: a session given only a
// workflow — a DAP launch, an embedded debug — whose sites pass
// [v1.MaxDebugStaticSites] judges a target by the steps the program declares,
// as the durable driver does, so a misspelling is refused by both drivers
// rather than armed by one and refused by the other.
func TestATruncatedProgramRefusesAStepItNeverDeclares(t *testing.T) {
	t.Parallel()

	log := func(id string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}
	}
	callee := &v1.Workflow{Name: "wide"}
	for i := range 512 {
		callee.Steps = append(callee.Steps, log(fmt.Sprintf("s%d", i)))
	}
	spec := &v1.Workflow{Name: "fanout", Profile: v1.CurrentProfile}
	for i := range v1.MaxDebugStaticSites/len(callee.Steps) + 1 {
		spec.Steps = append(spec.Steps, &v1.Node{Id: fmt.Sprintf("call%d", i), Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}})
	}
	spec.Steps = append(spec.Steps, log("last"))
	_, truncated := v1.DebugStaticSites(spec)
	require.True(t, truncated, "the program did not pass the cap, so this proves nothing")

	session, err := New(Options{Workflow: spec, Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	_, unknown := session.unknownStepNotice("last")
	assert.False(t, unknown, "a declared step past the cut was refused")
	_, unknown = session.unknownStepNotice("s7")
	assert.False(t, unknown, "a callee's declared step was refused")
	notice, unknown := session.unknownStepNotice("lsat")
	assert.True(t, unknown, "a step the program never declares was accepted")
	assert.Contains(t, notice, `"lsat"`)
	_, unknown = session.unknownStepNotice("call3/s7")
	assert.False(t, unknown, "a callee's step under the call that declares it was refused")
	notice, unknown = session.unknownStepNotice("bogus/last")
	assert.True(t, unknown, "a declared step under a container the program does not have was accepted")
	assert.Contains(t, notice, "bogus/last", "the refusal named the bare step, which is declared")

	// A step the run has gone past is still judged by the program: having
	// run `last` does not make `bogus/last` a place it can stop.
	session.mu.Lock()
	session.seen["last"] = StepDone
	session.mu.Unlock()
	_, unknown = session.unknownStepNotice("bogus/last")
	assert.True(t, unknown, "a qualified target was admitted because its step had run")
}

// TestATruncatedScriptIsCheckedAsItsSessionJudges: a script checked against a
// program too large to enumerate is judged by the steps the program declares
// within the containers an address names, as the session running it judges
// them, so a line the check passes is never one the session then refuses.
func TestATruncatedScriptIsCheckedAsItsSessionJudges(t *testing.T) {
	t.Parallel()

	log := func(id string) *v1.Node {
		return &v1.Node{Id: id, Kind: &v1.Node_Task{Task: &v1.Task{Name: "log"}}}
	}
	callee := &v1.Workflow{Name: "wide"}
	for i := range 512 {
		callee.Steps = append(callee.Steps, log(fmt.Sprintf("s%d", i)))
	}
	spec := &v1.Workflow{Name: "fanout", Profile: v1.CurrentProfile}
	for i := range v1.MaxDebugStaticSites/len(callee.Steps) + 1 {
		spec.Steps = append(spec.Steps, &v1.Node{Id: fmt.Sprintf("call%d", i), Kind: &v1.Node_Call{Call: &v1.Call{Workflow: callee}}})
	}
	spec.Steps = append(spec.Steps, log("last"))
	steps := slices.Collect(v1.DebugDeclaredSteps(spec))

	session, err := New(Options{Workflow: spec, Controlled: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })

	for _, line := range []string{"break last", "break call3/s7", "until s7", "break bogus/last", "until bogus/last", "break lsat"} {
		problems, _ := CheckScriptFor([]string{line}, steps, spec)
		_, refused := session.unknownStepNotice(strings.Fields(line)[1])
		assert.Equal(t, refused, len(problems) > 0, "%q: the check and the session disagree (problems %v)", line, problems)
	}
	problems, _ := CheckScriptFor([]string{"break bogus/last"}, steps, spec)
	require.Len(t, problems, 1)
}
