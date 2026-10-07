//go:build unix

package execpolicy_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/execpolicy"
	"github.com/picatz/flowstate/pkg/flowstate/v1/principal"
)

// tool finds a program the tests run for real, skipping where the machine
// lacks it: the package's whole claim is about starting real processes, so a
// fake would test nothing.
func tool(t *testing.T, name string) string {
	t.Helper()
	path, err := exec.LookPath(name)
	if err != nil {
		t.Skipf("%s is not installed: %v", name, err)
	}
	path, err = filepath.EvalSymlinks(path)
	require.NoError(t, err)
	return path
}

// base is a working configuration: a temp root, sh and env, generous bounds.
func base(t *testing.T) (execpolicy.Config, string) {
	t.Helper()
	root := t.TempDir()
	resolved, err := filepath.EvalSymlinks(root)
	require.NoError(t, err)
	return execpolicy.Config{
		Executables:    map[string]string{"sh": tool(t, "sh"), "env": tool(t, "env")},
		Roots:          []string{root},
		Timeout:        30 * time.Second,
		MaxOutputBytes: 64 << 10,
	}, resolved
}

func mustPolicy(t *testing.T, cfg execpolicy.Config) *execpolicy.Policy {
	t.Helper()
	p, err := execpolicy.New(cfg)
	require.NoError(t, err)
	return p
}

func denied(t *testing.T, err error, reason execpolicy.Reason) *execpolicy.DeniedError {
	t.Helper()
	require.ErrorIs(t, err, execpolicy.ErrDenied)
	var d *execpolicy.DeniedError
	require.ErrorAs(t, err, &d)
	assert.Equal(t, reason, d.Reason, d.Error())
	return d
}

func TestNewRefusesWhatCannotBeBounded(t *testing.T) {
	t.Parallel()

	for name, edit := range map[string]func(*execpolicy.Config){
		"no timeout":       func(c *execpolicy.Config) { c.Timeout = 0 },
		"negative timeout": func(c *execpolicy.Config) { c.Timeout = -time.Second },
		"timeout ceiling":  func(c *execpolicy.Config) { c.Timeout = execpolicy.MaxTimeout + time.Second },
		"no output bound":  func(c *execpolicy.Config) { c.MaxOutputBytes = 0 },
		"output ceiling":   func(c *execpolicy.Config) { c.MaxOutputBytes = execpolicy.MaxOutputBytes + 1 },
		"negative output":  func(c *execpolicy.Config) { c.MaxOutputBytes = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg, _ := base(t)
			edit(&cfg)
			_, err := execpolicy.New(cfg)
			require.ErrorIs(t, err, execpolicy.ErrInvalidPolicy)
		})
	}

	t.Run("the ceilings themselves are accepted", func(t *testing.T) {
		t.Parallel()
		cfg, _ := base(t)
		cfg.Timeout = execpolicy.MaxTimeout
		cfg.MaxOutputBytes = execpolicy.MaxOutputBytes
		mustPolicy(t, cfg)
	})
}

func TestNewVerifiesEveryExecutable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	write := func(name string, mode os.FileMode, body string) string {
		path := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(path, []byte(body), mode))
		require.NoError(t, os.Chmod(path, mode)) // umask
		return path
	}
	good := write("good", 0o755, "#!/bin/sh\nexit 0\n")

	body := "#!/bin/sh\nexit 3\n"
	sum := sha256.Sum256([]byte(body))
	pinned := write("pinned", 0o755, body)
	pin := hex.EncodeToString(sum[:])

	link := filepath.Join(dir, "link")
	require.NoError(t, os.Symlink(good, link))

	for _, test := range []struct {
		name    string
		exes    map[string]string
		pins    map[string]string
		wantErr string
	}{
		{"absolute file", map[string]string{"x": good}, nil, ""},
		{"a link is resolved", map[string]string{"x": link}, nil, ""},
		{"relative path", map[string]string{"x": "bin/x"}, nil, "not an absolute path"},
		{"missing file", map[string]string{"x": filepath.Join(dir, "absent")}, nil, "no such file"},
		{"directory", map[string]string{"x": dir}, nil, "not a regular file"},
		{"not executable", map[string]string{"x": write("plain", 0o644, "x")}, nil, "not executable"},
		{"world writable", map[string]string{"x": write("open", 0o777, "x")}, nil, "writable by everyone"},
		{"pin matches", map[string]string{"x": pinned}, map[string]string{"x": pin}, ""},
		{"pin mismatch", map[string]string{"x": pinned}, map[string]string{"x": strings.Repeat("0", 64)}, "not the pinned"},
		{"pin for an unlisted name", map[string]string{"x": good}, map[string]string{"y": pin}, "not in executables"},
		{"name with a slash", map[string]string{"a/b": good}, nil, "not a bare program name"},
		{"name starting with a dash", map[string]string{"-x": good}, nil, "not a bare program name"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg, _ := base(t)
			cfg.Executables = test.exes
			cfg.ExecutableSHA256 = test.pins
			p, err := execpolicy.New(cfg)
			if test.wantErr == "" {
				require.NoError(t, err)
				require.NotNil(t, p)
				return
			}
			require.ErrorIs(t, err, execpolicy.ErrInvalidPolicy)
			assert.Contains(t, err.Error(), test.wantErr)
		})
	}

	t.Run("a link resolves to the file that runs", func(t *testing.T) {
		t.Parallel()
		cfg, root := base(t)
		cfg.Executables = map[string]string{"x": link}
		cmd, err := mustPolicy(t, cfg).Check(context.Background(), execpolicy.Request{Argv: []string{"x"}, Dir: root})
		require.NoError(t, err)
		resolved, err := filepath.EvalSymlinks(good)
		require.NoError(t, err)
		assert.Equal(t, resolved, cmd.Executable())
	})
}

func TestNewRefusesBadRootsAndEnvironment(t *testing.T) {
	t.Parallel()
	file := filepath.Join(t.TempDir(), "f")
	require.NoError(t, os.WriteFile(file, nil, 0o600))

	for name, edit := range map[string]func(*execpolicy.Config){
		"relative root":            func(c *execpolicy.Config) { c.Roots = []string{"work"} },
		"missing root":             func(c *execpolicy.Config) { c.Roots = []string{"/nonexistent-flowstate-root"} },
		"root is a file":           func(c *execpolicy.Config) { c.Roots = []string{file} },
		"LD_PRELOAD passthrough":   func(c *execpolicy.Config) { c.EnvPassthrough = []string{"PATH", "LD_PRELOAD"} },
		"LD_LIBRARY_PATH":          func(c *execpolicy.Config) { c.EnvPassthrough = []string{"LD_LIBRARY_PATH"} },
		"DYLD passthrough":         func(c *execpolicy.Config) { c.EnvPassthrough = []string{"DYLD_INSERT_LIBRARIES"} },
		"LD_ in env_authored":      func(c *execpolicy.Config) { c.EnvAuthored = []string{"LD_PRELOAD"} },
		"DYLD in env_authored":     func(c *execpolicy.Config) { c.EnvAuthored = []string{"DYLD_LIBRARY_PATH"} },
		"bad passthrough name":     func(c *execpolicy.Config) { c.EnvPassthrough = []string{"A=B"} },
		"bad authored name":        func(c *execpolicy.Config) { c.EnvAuthored = []string{"1A"} },
		"bad operator env name":    func(c *execpolicy.Config) { c.Env = map[string]string{"a b": "x"} },
		"NUL in operator env":      func(c *execpolicy.Config) { c.Env = map[string]string{"A": "x\x00y"} },
		"rule that does not parse": func(c *execpolicy.Config) { c.Deny = []string{"argv.nonsense("} },
		"rule over an unknown var": func(c *execpolicy.Config) { c.Allow = []string{`cwd == "/"`} },
		"rule that is not a bool":  func(c *execpolicy.Config) { c.Allow = []string{`name`} },
		"empty rule":               func(c *execpolicy.Config) { c.Deny = []string{""} },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg, _ := base(t)
			edit(&cfg)
			_, err := execpolicy.New(cfg)
			require.ErrorIs(t, err, execpolicy.ErrInvalidPolicy)
		})
	}
}

func TestANilPolicyPermitsNothing(t *testing.T) {
	t.Parallel()
	var p *execpolicy.Policy
	_, err := p.Check(context.Background(), execpolicy.Request{Argv: []string{"sh"}, Dir: "/"})
	denied(t, err, execpolicy.ReasonNoPolicy)
}

func TestArgv0IsANameInTheTableAndNothingElse(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	p := mustPolicy(t, cfg)
	ctx := context.Background()

	_, err := p.Check(ctx, execpolicy.Request{Argv: []string{"sh", "-c", "true"}, Dir: root})
	require.NoError(t, err)

	for name, argv := range map[string][]string{
		"unlisted name":     {"curl", "https://example.com"},
		"absolute path":     {cfg.Executables["sh"]},
		"relative path":     {"./sh"},
		"dotdot path":       {"../sh"},
		"windows separator": {`bin\sh`},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := p.Check(ctx, execpolicy.Request{Argv: argv, Dir: root})
			d := denied(t, err, execpolicy.ReasonExecutable)
			assert.Contains(t, d.Detail, "it lists env, sh", "the denial names what the table does hold")
		})
	}

	t.Run("PATH is never searched", func(t *testing.T) {
		// "ls" is on PATH and not in the table.
		_, err := p.Check(ctx, execpolicy.Request{Argv: []string{"ls"}, Dir: root})
		denied(t, err, execpolicy.ReasonExecutable)
	})
}

func TestArgvIsBounded(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	p := mustPolicy(t, cfg)
	ctx := context.Background()
	sh := func(extra ...string) execpolicy.Request {
		return execpolicy.Request{Argv: append([]string{"sh"}, extra...), Dir: root}
	}

	_, err := p.Check(ctx, execpolicy.Request{Dir: root})
	denied(t, err, execpolicy.ReasonArgv)

	_, err = p.Check(ctx, sh("a\x00b"))
	denied(t, err, execpolicy.ReasonArgv)

	_, err = p.Check(ctx, sh(strings.Repeat("a", execpolicy.MaxArgBytes+1)))
	denied(t, err, execpolicy.ReasonArgv)

	_, err = p.Check(ctx, sh(strings.Repeat("a", execpolicy.MaxArgBytes)))
	require.NoError(t, err, "a word at the limit is admitted")

	_, err = p.Check(ctx, sh(make([]string, execpolicy.MaxArgs)...))
	denied(t, err, execpolicy.ReasonArgv)

	many := make([]string, 0, 40)
	for range 40 {
		many = append(many, strings.Repeat("a", execpolicy.MaxArgBytes))
	}
	_, err = p.Check(ctx, sh(many...))
	denied(t, err, execpolicy.ReasonArgv)
}

func TestDirMustResolveUnderARoot(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	outside := t.TempDir()
	sub := filepath.Join(root, "sub")
	require.NoError(t, os.Mkdir(sub, 0o700))
	// A sibling whose name merely begins with the root's.
	sibling := root + "-evil"
	require.NoError(t, os.Mkdir(sibling, 0o700))
	t.Cleanup(func() { _ = os.Remove(sibling) })
	// A link inside the root pointing out of it, and one pointing within.
	escape := filepath.Join(root, "escape")
	require.NoError(t, os.Symlink(outside, escape))
	inside := filepath.Join(root, "inside")
	require.NoError(t, os.Symlink(sub, inside))
	file := filepath.Join(root, "file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))

	p := mustPolicy(t, cfg)
	check := func(dir string) (*execpolicy.Command, error) {
		return p.Check(context.Background(), execpolicy.Request{Argv: []string{"sh"}, Dir: dir})
	}

	for name, dir := range map[string]string{
		"the root":                 root,
		"a subdirectory":           sub,
		"a link that stays inside": inside,
		"an unclean path inside":   root + "/sub/../sub/.",
		"a trailing slash":         sub + "/",
	} {
		t.Run("admits "+name, func(t *testing.T) {
			cmd, err := check(dir)
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(cmd.Dir(), root), cmd.Dir())
		})
	}
	cmd, err := check(inside)
	require.NoError(t, err)
	assert.Equal(t, sub, cmd.Dir(), "the directory the command runs in is the resolved one")

	for name, dir := range map[string]string{
		"empty":                      "",
		"relative":                   "sub",
		"outside every root":         outside,
		"a sibling sharing a prefix": sibling,
		"a link out of the root":     escape,
		"dotdot out of the root":     filepath.Join(root, "..", filepath.Base(outside)),
		"missing":                    filepath.Join(root, "absent"),
		"a file":                     file,
		"NUL":                        root + "\x00",
	} {
		t.Run("refuses "+name, func(t *testing.T) {
			_, err := check(dir)
			denied(t, err, execpolicy.ReasonDir)
		})
	}

	t.Run("a refusal does not name where a link leads", func(t *testing.T) {
		_, err := check(escape)
		d := denied(t, err, execpolicy.ReasonDir)
		assert.Contains(t, d.Detail, escape, "the author's own path is fine to echo")
		assert.NotContains(t, d.Detail, outside,
			"a denial lands in durable history, so it must not say where a symbolic link leads")
	})

	t.Run("no roots admits no directory", func(t *testing.T) {
		cfg, root := base(t)
		cfg.Roots = nil
		_, err := mustPolicy(t, cfg).Check(context.Background(), execpolicy.Request{Argv: []string{"sh"}, Dir: root})
		d := denied(t, err, execpolicy.ReasonDir)
		assert.Contains(t, d.Detail, "no roots")
	})
}

func TestEnvironmentIsBuiltFromNothing(t *testing.T) {
	t.Parallel()
	cfg, root := base(t)
	cfg.Env = map[string]string{"GOFLAGS": "-mod=readonly", "PINNED": "operator"}
	cfg.EnvPassthrough = []string{"HOME", "ABSENT", "PINNED"}
	cfg.EnvAuthored = []string{"CI_TARGET", "HOME"}
	cfg.LookupEnv = func(k string) (string, bool) {
		v, ok := map[string]string{"HOME": "/home/worker", "PINNED": "from-worker", "SECRET_TOKEN": "hunter2"}[k]
		return v, ok
	}
	p := mustPolicy(t, cfg)
	check := func(env map[string]string) (*execpolicy.Command, error) {
		return p.Check(context.Background(), execpolicy.Request{Argv: []string{"env"}, Dir: root, Env: env})
	}

	cmd, err := check(map[string]string{"CI_TARGET": "release"})
	require.NoError(t, err)
	assert.Equal(t, []string{"CI_TARGET", "GOFLAGS", "HOME", "PINNED"}, cmd.EnvKeys(),
		"operator literals, present passthrough variables and authored keys; the worker's other variables never")

	_, err = check(map[string]string{"UNLISTED": "x"})
	d := denied(t, err, execpolicy.ReasonEnv)
	assert.Contains(t, d.Detail, "UNLISTED")

	_, err = check(map[string]string{"GOFLAGS": "-mod=mod"})
	denied(t, err, execpolicy.ReasonEnv) // not authored

	_, err = check(map[string]string{"HOME": "/tmp"})
	d = denied(t, err, execpolicy.ReasonEnv) // authored, but passthrough already set it
	assert.Contains(t, d.Detail, "cannot override")

	_, err = check(map[string]string{"1BAD": "x"})
	denied(t, err, execpolicy.ReasonEnv)

	_, err = check(map[string]string{"CI_TARGET": "a\x00b"})
	denied(t, err, execpolicy.ReasonEnv)

	tooMany := map[string]string{}
	for i := range execpolicy.MaxStepEnv + 1 {
		tooMany["K"+strings.Repeat("x", i)] = "v"
	}
	_, err = check(tooMany)
	denied(t, err, execpolicy.ReasonEnv)

	t.Run("a step may set an authored key the worker lacks", func(t *testing.T) {
		cfg2 := cfg
		cfg2.LookupEnv = func(string) (string, bool) { return "", false }
		cmd, err := mustPolicy(t, cfg2).Check(context.Background(),
			execpolicy.Request{Argv: []string{"env"}, Dir: root, Env: map[string]string{"HOME": "/tmp/home"}})
		require.NoError(t, err)
		assert.Equal(t, []string{"GOFLAGS", "HOME", "PINNED"}, cmd.EnvKeys())
	})
}

func TestRulesSeeResolvedValuesAndFailClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	run := func(t *testing.T, edit func(*execpolicy.Config), req execpolicy.Request) error {
		t.Helper()
		cfg, root := base(t)
		cfg.Env = map[string]string{"MODE": "ci"}
		cfg.EnvAuthored = []string{"TARGET"}
		cfg.Executables["go"] = cfg.Executables["sh"]
		edit(&cfg)
		if req.Dir == "" {
			req.Dir = root
		}
		_, err := mustPolicy(t, cfg).Check(ctx, req)
		return err
	}
	argv := func(a ...string) execpolicy.Request { return execpolicy.Request{Argv: a} }

	t.Run("deny rule on argv wins", func(t *testing.T) {
		err := run(t, func(c *execpolicy.Config) { c.Deny = []string{`"-c" in argv`} }, argv("sh", "-c", "true"))
		d := denied(t, err, execpolicy.ReasonDenyRule)
		assert.Equal(t, `"-c" in argv`, d.Detail)
		require.NoError(t, run(t, func(c *execpolicy.Config) { c.Deny = []string{`"-c" in argv`} }, argv("sh", "script.sh")))
	})

	t.Run("deny beats allow", func(t *testing.T) {
		err := run(t, func(c *execpolicy.Config) {
			c.Allow = []string{`name == "sh"`}
			c.Deny = []string{`name == "sh"`}
		}, argv("sh"))
		denied(t, err, execpolicy.ReasonDenyRule)
	})

	t.Run("an allowlist nothing matched denies", func(t *testing.T) {
		edit := func(c *execpolicy.Config) { c.Allow = []string{`name == "go" && argv[1] == "test"`} }
		denied(t, run(t, edit, argv("go", "build")), execpolicy.ReasonNoAllowRule)
		require.NoError(t, run(t, edit, argv("go", "test")))
		denied(t, run(t, edit, argv("sh")), execpolicy.ReasonNoAllowRule)
	})

	t.Run("rules read the resolved executable, dir and env keys", func(t *testing.T) {
		edit := func(c *execpolicy.Config) {
			c.Allow = []string{`executable == "` + c.Executables["sh"] + `" && dir.startsWith("` + c.Roots[0] + `") && env_keys == ["MODE", "TARGET"]`}
		}
		require.NoError(t, run(t, edit, execpolicy.Request{Argv: []string{"sh"}, Env: map[string]string{"TARGET": "x"}}))
		// Without the authored key the final environment differs, and the rule
		// reads the final one.
		denied(t, run(t, edit, execpolicy.Request{Argv: []string{"sh"}}), execpolicy.ReasonNoAllowRule)
	})

	t.Run("a rule never sees environment values", func(t *testing.T) {
		// env_keys is a list of names; a rule reaching for a value has nothing
		// to reach, and says so at load.
		cfg, _ := base(t)
		cfg.Deny = []string{`env["TARGET"] == "x"`}
		_, err := execpolicy.New(cfg)
		require.ErrorIs(t, err, execpolicy.ErrInvalidPolicy)
	})

	t.Run("a rule that errors denies", func(t *testing.T) {
		// argv[5] is out of range at run time; the checker cannot see it.
		err := run(t, func(c *execpolicy.Config) { c.Allow = []string{`argv[5] == "x"`} }, argv("sh"))
		denied(t, err, execpolicy.ReasonRuleError)

		err = run(t, func(c *execpolicy.Config) { c.Deny = []string{`argv[5] == "x"`} }, argv("sh"))
		denied(t, err, execpolicy.ReasonRuleError)
	})

	t.Run("identity scopes the rule, and no identity matches no tenant", func(t *testing.T) {
		edit := func(c *execpolicy.Config) { c.Allow = []string{`identity.namespace == "team-a"`} }
		withID := func(ns string) execpolicy.Request {
			r := argv("sh")
			r.Identity = principal.Caller{Namespace: ns, Claims: map[string]string{"team": ns}}
			return r
		}
		require.NoError(t, run(t, edit, withID("team-a")))
		denied(t, run(t, edit, withID("team-b")), execpolicy.ReasonNoAllowRule)
		denied(t, run(t, edit, argv("sh")), execpolicy.ReasonNoAllowRule)

		claims := func(c *execpolicy.Config) { c.Allow = []string{`identity.claims["team"] == "team-a"`} }
		require.NoError(t, run(t, claims, withID("team-a")))
		// Absent identity has a non-nil empty claims map; indexing a missing key
		// errors, and an errored rule denies.
		denied(t, run(t, claims, argv("sh")), execpolicy.ReasonRuleError)
	})

	t.Run("identity kind and actions scope the rule", func(t *testing.T) {
		as := func(c principal.Caller) execpolicy.Request {
			r := argv("sh")
			r.Identity = c
			return r
		}
		human := principal.Caller{Kind: "human", Actions: []string{"run.start"}}
		workload := principal.Caller{Kind: "workload"}

		allow := func(c *execpolicy.Config) { c.Allow = []string{`identity.kind == "workload"`} }
		require.NoError(t, run(t, allow, as(workload)))
		denied(t, run(t, allow, as(human)), execpolicy.ReasonNoAllowRule)
		denied(t, run(t, allow, argv("sh")), execpolicy.ReasonNoAllowRule)

		deny := func(c *execpolicy.Config) { c.Deny = []string{`identity.kind == "human"`} }
		denied(t, run(t, deny, as(human)), execpolicy.ReasonDenyRule)
		require.NoError(t, run(t, deny, as(workload)))

		byAction := func(c *execpolicy.Config) { c.Allow = []string{`"run.start" in identity.actions`} }
		require.NoError(t, run(t, byAction, as(human)))
		denied(t, run(t, byAction, as(workload)), execpolicy.ReasonNoAllowRule)
	})

	t.Run("a cancelled context is not a policy decision", func(t *testing.T) {
		cfg, root := base(t)
		cfg.Allow = []string{`argv[5] == "x"`}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err := mustPolicy(t, cfg).Check(cancelled, execpolicy.Request{Argv: []string{"sh"}, Dir: root})
		require.ErrorIs(t, err, context.Canceled)
		assert.NotErrorIs(t, err, execpolicy.ErrDenied)
	})

	t.Run("structural checks run before rules", func(t *testing.T) {
		// A rule that would allow everything cannot admit an unlisted program.
		err := run(t, func(c *execpolicy.Config) { c.Allow = []string{`true`} }, argv("curl"))
		denied(t, err, execpolicy.ReasonExecutable)
	})
}

// TestExactArgvShapesRefuseWhatRootsDoNotConfine is the rule set the exec-checks
// example ships, run against a stand-in program: roots confine `dir` only, so the
// rules, not the roots, are what keep argv from naming code or files.
func TestExactArgvShapesRefuseWhatRootsDoNotConfine(t *testing.T) {
	t.Parallel()

	cfg, root := base(t)
	cfg.Allow = []string{
		`name == "env" && argv == ["env", "--version"]`,
		`name == "env" && argv == ["env", "rev-parse", "--git-dir"]`,
	}
	cfg.Deny = []string{
		`argv.exists(a, a == "-c" || a.startsWith("--output") || a == "--no-index" || a.startsWith("--ext-diff"))`,
		`argv.exists(a, a.startsWith("/") || a.contains(".."))`,
	}
	p := mustPolicy(t, cfg)

	check := func(argv ...string) error {
		_, err := p.Check(t.Context(), execpolicy.Request{Argv: argv, Dir: root})
		return err
	}

	require.NoError(t, check("env", "--version"))
	require.NoError(t, check("env", "rev-parse", "--git-dir"))

	for _, argv := range [][]string{
		{"env", "rev-parse", "--output=/tmp/x"},
		{"env", "rev-parse", "--ext-diff"},
		{"env", "rev-parse", "/etc/passwd"},
		{"env", "rev-parse", "../other"},
		{"env", "-c", "core.pager=sh"},
	} {
		denied(t, check(argv...), execpolicy.ReasonDenyRule)
	}

	// Not named by a deny rule, but outside every allowed shape.
	for _, argv := range [][]string{
		{"env", "rev-parse", "--unlisted-flag"},
		{"env", "--version", "extra"},
		// Subcommands that read the repository's own config, and so can run a
		// program it names (core.fsmonitor, diff.external, textconv), are not
		// listed.
		{"env", "status"},
		{"env", "diff"},
		{"env", "log", "--oneline"},
	} {
		denied(t, check(argv...), execpolicy.ReasonNoAllowRule)
	}
}
