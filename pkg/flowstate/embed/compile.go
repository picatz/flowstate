package embed

import (
	"errors"

	v1 "github.com/picatz/flowstate/pkg/flowstate/v1"
	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// Workflow is a compiled Flowfile, ready to run with [RunLocal] or submit
// through [RunDurable]'s worker. It is [v1.Workflow] under another name — see
// that package's doc for why an embedder reaches it through this alias
// instead of importing v1 directly.
type Workflow = v1.Workflow

// Diagnostics is one or more problems found compiling a Flowfile — a
// misspelled task name, a step referencing another that does not exist, a
// malformed expression — each naming the line and column it was found on. It
// is [flowfile.Diagnostics] under this package's own name.
type Diagnostics = flowfile.Diagnostics

// Compile parses a Flowfile from bytes into the workflow [RunLocal] and
// [RunDurable] run.
//
// This is [flowfile.Parse], the same compile boundary `flow validate` and
// every other consumer of a Flowfile use — a Go caller gets driver parity for
// free rather than a second, ad hoc reading of the DSL.
//
// Compiled from bytes with no file identity, so a `call:` step cannot be
// resolved — there is no directory to resolve it relative to — and is refused
// with a diagnostic saying so, the same restriction [flowfile.Parse] itself
// documents. An embedding program that wants `call:` support reads the file
// itself and uses [flowfile.ParseFile] directly.
//
// Compile does not check whether a step's task is one this build actually
// knows — [flowfile.Parse] decides a step's task purely from its shape,
// leaving "is this task registered at all" to [flowfile.Validate], which
// Compile deliberately does not call (see that package's doc on `Parse` vs
// `Validate`). A Flowfile naming a task nobody registered compiles cleanly
// here and fails only once [RunLocal] or a durable run actually reaches that
// step — "unknown task %q", from the engine itself. An embedder that wants
// the earlier, richer diagnostic — the one `flow validate` gives, naming the
// task's line and column — calls [flowfile.Validate] on the result, or
// [flowfile.ValidateSource] directly on data. Either way, a workflow naming a
// custom task needs that task [Tasks.Install]ed first for the check to see
// it: validation asks what this *build* knows a task is, which is a question
// [v1.DefaultRegistry] answers, not anything [RunOptions.Tasks] configures
// for one run — see [Tasks]'s doc for why the two questions are answered by
// two different registries on purpose.
//
// err is non-nil for any compile failure. When the failure is one or more
// diagnosable problems in the file itself — as opposed to, say, a document
// that is not YAML at all — diagnostics carries them individually and err
// wraps diagnostics, so a caller that only wants the summary can check err
// and one that wants each problem can range over diagnostics; both see the
// same failure.
func Compile(data []byte) (workflow *Workflow, diagnostics Diagnostics, err error) {
	workflow, _, err = flowfile.Parse(data)
	if err != nil {
		var diags flowfile.Diagnostics
		if errors.As(err, &diags) {
			return nil, diags, err
		}
		return nil, nil, err
	}
	return workflow, nil, nil
}
