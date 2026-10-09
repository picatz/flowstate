package flowdebug_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

func verbNames(verbs []flowdebug.Verb) []string {
	names := make([]string, 0, len(verbs))
	for _, verb := range verbs {
		names = append(names, verb.Name)
	}

	return names
}

// TestALocalRunThatCannotStepBackHasNoKeyForIt: in this process the capabilities
// are the whole truth, so a run that does not report reverse hides the verbs
// that return to an earlier stop, where a durable run keeps them and refuses
// them by name.
func TestALocalRunThatCannotStepBackHasNoKeyForIt(t *testing.T) {
	t.Parallel()

	forward := &v1.DebugCapabilities{StepIn: true}
	reversible := &v1.DebugCapabilities{StepIn: true, Reverse: true}

	assert.NotContains(t, verbNames(flowdebug.LocalVerbsFor(forward)), "back")
	assert.NotContains(t, verbNames(flowdebug.LocalVerbsFor(forward)), "reverse-continue")
	assert.NotContains(t, verbNames(flowdebug.LocalVerbsFor(forward)), "goto")
	assert.Contains(t, verbNames(flowdebug.LocalVerbsFor(forward)), "step", "a forward verb went with them")

	assert.Contains(t, verbNames(flowdebug.LocalVerbsFor(reversible)), "back")
	assert.Contains(t, verbNames(flowdebug.LocalVerbsFor(reversible)), "reverse-continue")

	// A durable run keeps them: its engine answers with the history sentence.
	assert.Contains(t, verbNames(flowdebug.VerbsFor(forward)), "back")
}
