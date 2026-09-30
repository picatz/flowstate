// Package plugintest is the test kit for a Flowstate plugin author.
//
// A plugin is a separate executable a worker launches, so the only faithful
// test of one runs the executable. This package makes that the easy thing:
// [Build] compiles the plugin's main package under the name discovery looks for,
// [Launch] starts it through the same [plugin.Host] a worker uses, and the
// returned [Session] calls its tasks down the path a workflow step takes —
// secret-input resolution, the output contract check against the declared
// descriptor, secret scrubbing, and error classification all included. A test
// written against a Session therefore cannot pass on something a worker would
// refuse, which is the failure an in-process call of the task function hides.
//
// # A plugin's first test
//
//	func TestGreet(t *testing.T) {
//		dir := plugintest.Build(t, ".", "hello")
//		s := plugintest.Launch(t, dir)
//
//		out := s.Run(t, "hello.greet", map[string]any{"name": "Ada"})
//		if got := out.String(t, "message"); got != "Hello, Ada!" {
//			t.Fatalf("message = %q", got)
//		}
//
//		_, err := s.Call(t.Context(), "hello.greet", nil)
//		if kind := plugintest.ErrorKind(err); kind != flowstatev1.ErrorKindInvalidInput {
//			t.Fatalf("an empty name was %v, want invalid input", kind)
//		}
//
//		s.Conform(t)
//	}
//
// # What [Session.Conform] checks
//
// The contract a plugin makes with the engine beyond "it runs": that what it
// advertises is complete enough to validate, document, complete in an editor,
// and pin. See [Session.Conform] for the list. Every check is about the
// plugin's own declarations and none of them calls a task, so a plugin whose
// tasks reach a network or mutate state is as safe to conform as a pure one.
//
// # Why a separate package
//
// The host half of the protocol lives in [github.com/picatz/flowstate/pkg/flowstate/v1/plugin]
// and the plugin half in [github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk];
// a test is a third role, a consumer of both that is not a worker and must not
// be shipped in one. Importing it from a non-test file is a mistake the
// standard library's own testing helpers warn about the same way.
//
// # Keep the plugin's generated types out of the test binary
//
// A test in the same package as the plugin's main function imports the
// plugin's generated messages, which registers their file descriptors in the
// test binary's process-wide registry. The host reconstructs a task's
// descriptors from the bytes the plugin sent, and a process that already holds
// the schema cannot tell a working reconstruction from a hit on its own
// registry. Put tests that use [Launch] in their own package (`reachable` is
// the repository's own name for it) that imports neither the plugin's main nor
// its generated code.
package plugintest
