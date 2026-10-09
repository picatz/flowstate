package flowdebug

import (
	"slices"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
)

// recordedRefuses are the driver verbs a recorded run cannot do, because each
// needs a run that executes: [Historical] answers every one of them with a
// refusal naming why. A front that binds keys to verbs leaves these without one
// rather than offer a key that can only be refused.
var recordedRefuses = []string{"until", "pause", "break", "log", "catch", "delete", "clear"}

// VerbsFor is [DriverVerbs] narrowed to the verbs a target with these
// capabilities can do, for a surface that binds keys to them (#2248).
//
// A target that reports [v1.DebugCapabilities.History] is a recorded run
// walked point by point, and nothing runs there, so the verbs that run until a
// step, hold the run, or arm a breakpoint are left out. They are still
// understood: typed at a console they reach the target, which refuses them by
// name with its own reason. Any other capabilities leave the table whole, so a
// live durable run keeps `back` and `goto` and refuses them with the sentence
// that points at the history walk.
func VerbsFor(capabilities *v1.DebugCapabilities) []Verb {
	verbs := DriverVerbs()
	if !capabilities.GetHistory() {
		return verbs
	}

	verbs = slices.DeleteFunc(verbs, func(v Verb) bool { return slices.Contains(recordedRefuses, v.Name) })
	for i := range verbs {
		// There is no run to let go on: leaving a record changes nothing.
		if verbs[i].Name == "detach" {
			verbs[i].Help = "leave the record: nothing is held, and the run is untouched"
		}
	}

	return verbs
}
