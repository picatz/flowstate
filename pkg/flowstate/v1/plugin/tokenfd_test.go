package plugin

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/internal/protocol"
)

// TestTheTokenIsNotInTheProcessEnvironment is the whole reason the token moved
// onto a descriptor, checked where the old delivery was actually visible.
//
// /proc/<pid>/environ is the environment block the kernel copied at execve(2).
// Nothing the process does afterwards changes it: setenv and unsetenv edit the
// process's own copy, so a secret delivered there is readable for as long as the
// plugin runs — to root, to anything that can ptrace it, and to anything that
// sweeps environments into a diagnostic bundle or a core dump. The SDK used to
// unset the variable and say in a comment that this bounded the exposure. It did
// not.
//
// The plugin under test is serving: the host completed a Describe against it,
// and the fake refuses every request that does not carry the exact token, so
// reaching this point already proves descriptor delivery works. What is left to
// check is that the value is nowhere in the launched process's environment.
func TestTheTokenIsNotInTheProcessEnvironment(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("/proc/<pid>/environ is where the exposure was visible, and only Linux has it; " +
			"the delivery itself is exercised on every platform by the tests that talk to a plugin")
	}

	// The launched fake writes the token it read here. The real string is what
	// makes this assertion mean something: a check for the variable name alone
	// would also pass on a host that had merely renamed it.
	sink := filepath.Join(t.TempDir(), "token")

	cfg := testConfig(t, pluginDir(t, "ok"))
	cfg.Env = []string{"FLOWSTATE_TEST_TOKEN_SINK=" + sink}

	host := openHost(t, cfg)

	plugins := host.Plugins()
	if len(plugins) != 1 {
		t.Fatalf("host launched %d plugins, want 1", len(plugins))
	}

	pid := plugins[0].PID()
	if pid == 0 {
		t.Fatal("the plugin has no process")
	}

	token, err := os.ReadFile(sink)
	if err != nil {
		t.Fatalf("the plugin did not report the token it read: %v", err)
	}
	if len(token) == 0 {
		t.Fatal("the plugin reported an empty token, so the check below would prove nothing")
	}

	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/environ")
	if err != nil {
		// A hardened kernel can refuse this read even for the parent. Saying so
		// is honest; passing quietly would claim a check that never ran.
		if errors.Is(err, fs.ErrPermission) {
			t.Skipf("this kernel will not let the parent read /proc/%d/environ: %v", pid, err)
		}
		t.Fatalf("reading /proc/%d/environ: %v", pid, err)
	}

	if bytes.Contains(raw, token) {
		t.Errorf("the per-launch token is readable in /proc/%d/environ", pid)
	}

	// The named surface too, so a future delivery that put a token back under
	// any spelling of the old variable fails here rather than only if it happened
	// to reuse this launch's value.
	var (
		entries  = strings.Split(strings.TrimSuffix(string(raw), "\x00"), "\x00")
		sawFD    bool
		wantFD   = protocol.TokenFDEnv + "=" + strconv.Itoa(tokenFD)
		retired  = protocol.TokenEnv + "="
		sawSink  bool
		wantSink = "FLOWSTATE_TEST_TOKEN_SINK=" + sink
	)

	for _, entry := range entries {
		switch {
		case strings.HasPrefix(entry, retired):
			t.Errorf("%s is in /proc/%d/environ; the token variable is retired, not renamed", entry, pid)
		case entry == wantFD:
			sawFD = true
		case entry == wantSink:
			sawSink = true
		}
	}

	// Without these the checks above would pass on an environment block this
	// test failed to read at all.
	if !sawFD {
		t.Errorf("/proc/%d/environ does not carry %q, so it is not the environment this host built", pid, wantFD)
	}
	if !sawSink {
		t.Errorf("/proc/%d/environ does not carry the operator entry this test set", pid)
	}
}
