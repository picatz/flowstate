package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveCodexBinaryFailsClosedWhenUnset(t *testing.T) {
	t.Setenv(codexBinaryEnv, "")
	if _, err := resolveCodexBinary(); err == nil {
		t.Fatal("resolveCodexBinary with no env set: got no error, want one")
	}
}

func TestResolveCodexBinaryNeverSearchesPATH(t *testing.T) {
	// A relative path is refused outright - this proves resolveCodexBinary
	// does not hand a bare name to exec.LookPath (which would search
	// $PATH); it must require an absolute path instead.
	t.Setenv(codexBinaryEnv, "codex")
	if _, err := resolveCodexBinary(); err == nil {
		t.Fatal("resolveCodexBinary with a relative path: got no error, want one")
	}
}

func TestResolveCodexBinaryRefusesADirectory(t *testing.T) {
	t.Setenv(codexBinaryEnv, t.TempDir())
	if _, err := resolveCodexBinary(); err == nil {
		t.Fatal("resolveCodexBinary pointed at a directory: got no error, want one")
	}
}

func TestResolveCodexBinaryRefusesANonExecutableFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-executable")
	if err := os.WriteFile(path, []byte("hi"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	t.Setenv(codexBinaryEnv, path)
	if _, err := resolveCodexBinary(); err == nil {
		t.Fatal("resolveCodexBinary pointed at a non-executable file: got no error, want one")
	}
}

func TestResolveCodexBinaryAcceptsAConfiguredExecutable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	t.Setenv(codexBinaryEnv, path)

	got, err := resolveCodexBinary()
	if err != nil {
		t.Fatalf("resolveCodexBinary: unexpected error: %v", err)
	}
	if got != path {
		t.Fatalf("resolveCodexBinary = %q, want %q", got, path)
	}
}

func TestResolveWorkingContextEmptyIsAlwaysAllowed(t *testing.T) {
	t.Setenv(workdirRootEnv, "")
	got, err := resolveWorkingContext("")
	if err != nil || got != "" {
		t.Fatalf("resolveWorkingContext(\"\") = (%q, %v), want (\"\", nil)", got, err)
	}
}

func TestResolveWorkingContextFailsClosedWithNoRootConfigured(t *testing.T) {
	t.Setenv(workdirRootEnv, "")
	if _, err := resolveWorkingContext("some/dir"); err == nil {
		t.Fatal("resolveWorkingContext with no root configured: got no error, want one")
	}
}

// TestResolveWorkingContextRefusesEscapingTheRoot is the direction
// CLAUDE.md's "test that A cannot reach B" asks for: proving a workflow
// cannot walk a relative or absolute path out of the configured jail, not
// merely that a path inside it works.
func TestResolveWorkingContextRefusesEscapingTheRoot(t *testing.T) {
	root := t.TempDir()
	t.Setenv(workdirRootEnv, root)

	outside := t.TempDir()

	cases := []string{
		"../outside",
		"..",
		"a/../../outside",
		outside, // an absolute path elsewhere on the filesystem entirely
	}
	for _, raw := range cases {
		if _, err := resolveWorkingContext(raw); err == nil {
			t.Errorf("resolveWorkingContext(%q) with root %q: got no error, want one - "+
				"this must not resolve outside the configured root", raw, root)
		}
	}
}

func TestResolveWorkingContextAcceptsADirectoryInsideTheRoot(t *testing.T) {
	root := t.TempDir()
	t.Setenv(workdirRootEnv, root)

	sub := filepath.Join(root, "checkout")
	if err := os.Mkdir(sub, 0o700); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	got, err := resolveWorkingContext("checkout")
	if err != nil {
		t.Fatalf("resolveWorkingContext: unexpected error: %v", err)
	}
	if got != sub {
		t.Fatalf("resolveWorkingContext = %q, want %q", got, sub)
	}
}

func TestResolveWorkingContextRefusesAFileNotADirectory(t *testing.T) {
	root := t.TempDir()
	t.Setenv(workdirRootEnv, root)

	file := filepath.Join(root, "notadir")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if _, err := resolveWorkingContext("notadir"); err == nil {
		t.Fatal("resolveWorkingContext pointed at a file: got no error, want one")
	}
}
