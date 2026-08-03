package plugin

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowfile"
)

// The plugin surface had everything except the one thing the house rule asks for.
//
// Discovery, handshake, descriptor reconstruction, dispatch, supervision, and a
// registration seam into the registry the engine reads — all tested here, and
// `examples/` held no file naming a plugin task. CLAUDE.md's rule is that a
// capability lands when a Flowfile can express it, `flow validate` accepts it, and
// an example in `examples/` exercises it in CI; VISION.md says the same of every
// plugin, "each landing with a worked example verified in CI". The plugin surface
// itself was the subsystem failing its own rule.
//
// # Where the example lives, and why not beside the others
//
// `examples/plugins/greet/workflow.yaml` — one directory deeper than every other
// example, which is deliberate and is the interesting decision in this file.
//
// `examples/*/workflow.yaml` is not merely a glob, it is a claim: CI's `flow fix
// --check`, `flow validate examples/*/workflow.yaml`, and seven tests across four
// packages enumerate it and check each file **with the built-in task registry**.
// A file naming `example.greet` is correctly *refused* by all of those, because
// with no plugin loaded the honest answer is the diagnostic the validator already
// gives: no such task is registered *here*, and whether one is registered on the
// worker this will run on is a deployment's decision rather than a property of the
// file. That is CLAUDE.md's own rule for diagnostics, and the corpus checks are
// right to be strict.
//
// The two ways to put the example inside that glob were both worse. Teaching six
// checks to carve out an exception puts a hole in each of them, permanently, for
// one file. Making `flow validate` quieter about an unregistered plugin task would
// trade a real diagnostic — the one that tells an author their worker has no such
// plugin — for the convenience of a glob.
//
// So the corpus keeps its meaning, and this example is enumerated by name, from the
// one package that can build a plugin and launch it. What it loses by not being in
// the glob is `flow fix --check` and the compile/round-trip checks the corpus gets
// for free; those are asserted below instead, explicitly, rather than assumed.

// pluginExamplePath is the shipped example that names a plugin's task.
func pluginExamplePath() string {
	return filepath.Join("..", "..", "..", "..", "examples", "plugins", "greet", "workflow.yaml")
}

// readPluginExample reads it, failing rather than skipping when it is not there: a
// deleted or renamed example is the failure this whole file exists to catch.
func readPluginExample(tb testing.TB) []byte {
	tb.Helper()

	data, err := os.ReadFile(pluginExamplePath())
	require.NoError(tb, err,
		"the plugin example is missing; it is the only file in examples/ that names a plugin task, "+
			"and without it the plugin surface has no worked example in CI")

	return data
}

// pluginExampleRan records that the example was actually executed against a real
// plugin process, for the check in [TestMain].
var pluginExampleRan atomic.Bool

// TestThePluginExampleIsAShippedFile is the half that runs with no toolchain, no
// plugin, and no process.
//
// Everything that *executes* the example goes through [exampleHost], which skips
// when there is no Go compiler to build a plugin with. A skip is the right answer
// there and the wrong place to leave the whole claim: a renamed directory, a
// deleted README, or an example quietly edited into something that no longer names
// a plugin task would then be green wherever that skip fires. So the structural
// facts are asserted unconditionally here, and the running is asserted separately.
func TestThePluginExampleIsAShippedFile(t *testing.T) {
	t.Parallel()

	source := readPluginExample(t)

	// It compiles with no plugin in sight — parsing is not registry-dependent, and
	// only the *validator* has anything to say about an unknown task. An example
	// that stopped compiling would otherwise be found by nothing until somebody
	// built a plugin.
	workflow, _, err := flowfile.Parse(source)
	require.NoError(t, err, "the plugin example does not compile")

	// Exactly one step naming a plugin task, and it is the one the plugin beside
	// this package provides. Counted rather than searched for: an example that
	// gained a second plugin task would need a second plugin built, and an example
	// that lost its only one would still parse, still validate, still run, and
	// prove nothing at all.
	var plugins []string
	for _, step := range workflow.GetSteps() {
		if name := step.GetTask().GetName(); strings.Contains(name, ".") {
			plugins = append(plugins, name)
		}
	}
	require.Equal(t, []string{"example.greet"}, plugins,
		"the plugin example no longer names exactly the one plugin task this package can build")

	// `flow fix --check examples/*/workflow.yaml` runs in CI and this file is not in
	// that glob, so the property that command protects is asserted here instead:
	// the example is already written in the current edition, byte for byte.
	fixed, err := flowfile.Fix(source)
	require.NoError(t, err)
	assert.Empty(t, fixed.Refusals)
	assert.Equal(t, string(source), string(fixed.Source),
		"the plugin example is not in the current edition; `flow fix` would rewrite it, and the "+
			"CI check that would have said so does not reach outside examples/*/workflow.yaml")

	// The setup this example needs is not expressible in the file — a built binary
	// and a worker told where to look — so the README beside it is load-bearing
	// rather than decoration.
	readme := filepath.Join(filepath.Dir(pluginExamplePath()), "README.md")
	_, err = os.Stat(readme)
	require.NoError(t, err,
		"the plugin example has no README, and it is the one example nobody can run from the file alone")

	// And it is linked from the index, since nothing else does that for it: the
	// corpus test that checks every example is listed enumerates the same glob this
	// file sits outside of.
	index, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "examples", "README.md"))
	require.NoError(t, err)
	assert.Contains(t, string(index), "plugins/greet",
		"examples/README.md does not list the plugin example; an example nobody links is an "+
			"example nobody runs")
}

// requirePluginExampleRan reports whether the example was executed, and is called
// from [TestMain] after the suite.
//
// The no-silent-capability rule, applied to the one thing that could hide this: the
// example runs inside a test that skips without a Go toolchain, and a skip in a
// package of 40 tests is a line nobody reads. So a run of this package that could
// have executed the example and did not is a failure of the package, not a skip
// inside it.
//
// Both conditions are necessary. Without a toolchain there is genuinely nothing to
// build, which is the case [exampleHost] skips for and the case this must not turn
// into a false failure. And a filtered run — `go test -run TestSomethingElse` — was
// never going to run it, so demanding it there would make every focused run red.
// Anything else, including CI, must have run it.
func requirePluginExampleRan() error {
	if pluginExampleRan.Load() {
		return nil
	}

	if filter := flag.Lookup("test.run"); filter != nil && filter.Value.String() != "" {
		return nil
	}

	if _, err := exec.LookPath("go"); err != nil {
		return nil
	}

	return fmt.Errorf(
		"the plugin example at %s was never executed, although this run could have built the "+
			"plugin and did: the one worked example of the plugin surface passed CI without "+
			"being exercised", pluginExamplePath())
}
