package main

import (
	"bufio"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// TestDocumentedServerInvocationsStart walks every `flow server` command line
// in this repository's Markdown and runs the one start-up decision a reader
// cannot see until they try it: whether [resolveRPCResource] accepts the flags
// as written against the policy file the command names.
//
// It exists because per-surface audience binding (#1007) made a flag required
// of any deployment whose trust policy names a kind: oidc issuer, and every
// walkthrough that started a server with such a policy and no --rpc-resource
// stopped working in the same commit — three READMEs and two recipes, all of
// them still reading as authoritative instructions. Reported by Codex on
// picatz/flowstate#1007.
//
// The check is derived rather than declared: nothing lists which documents
// contain a server command, so a walkthrough added tomorrow is covered the day
// it is written, and one whose policy file drifts out of agreement with the
// audience it documents fails here rather than in somebody's terminal. What it
// cannot check is a command naming a policy path outside the repository
// (/etc/flowstate/trust.yaml in a production recipe) — there is no file to
// load, so those are counted and skipped, and the count is asserted to keep
// the walk itself honest about how much it is actually reading.
func TestDocumentedServerInvocationsStart(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..")

	var checked, skipped int
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "node_modules", "reference":
				return fs.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".md" {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		for _, command := range serverCommandsIn(string(data)) {
			flags, policyPath := parseServerCommand(command)
			if policyPath == "" {
				// --insecure-no-auth, or a server started with no trust
				// policy at all: resolveRPCResource has nothing to load and
				// nothing to require.
				continue
			}
			if _, statErr := os.Stat(filepath.Join(root, policyPath)); statErr != nil {
				skipped++
				continue
			}

			raw, err := os.ReadFile(filepath.Join(root, policyPath))
			require.NoError(t, err)
			policy, err := auth.ParsePolicy(raw)
			require.NoErrorf(t, err, "%s documents a server started with %s, which does not parse", path, policyPath)

			_, err = resolveRPCResource(flags, authFlags{policyPath: policyPath}, &policy)
			require.NoErrorf(t, err, "%s documents a `flow server` that cannot start:\n\t%s\n", path, command)
			checked++
		}
		return nil
	})
	require.NoError(t, err)

	require.NotZero(t, checked, "no documented server invocation was checked; the walk or the fence syntax changed")
	require.NotZero(t, skipped, "no invocation named a policy outside the repository; if that is now true of every "+
		"recipe, drop this assertion rather than the walk")
}

// serverCommandsIn returns every `flow server ...` command line in a Markdown
// document, with shell line continuations joined so a wrapped invocation is
// read as the one command it is.
func serverCommandsIn(document string) []string {
	var commands []string

	scanner := bufio.NewScanner(strings.NewReader(document))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var current string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if current != "" {
			continued := strings.HasSuffix(line, `\`)
			current += " " + strings.TrimSuffix(line, `\`)
			if !continued {
				commands = append(commands, strings.Join(strings.Fields(current), " "))
				current = ""
			}
			continue
		}

		line = strings.TrimPrefix(line, "$ ")
		if !strings.HasPrefix(line, "flow server") {
			continue
		}
		if strings.HasSuffix(line, `\`) {
			current = strings.TrimSuffix(line, `\`)
			continue
		}
		commands = append(commands, strings.Join(strings.Fields(line), " "))
	}

	return commands
}

// parseServerCommand reads the flags resolveRPCResource looks at off one
// command line, returning the --auth-policy path separately because it is what
// decides whether there is a policy to load at all.
func parseServerCommand(command string) (rpcResourceFlags, string) {
	var (
		flags      rpcResourceFlags
		policyPath string
	)

	fields := strings.Fields(command)
	for i, field := range fields {
		next := func() string {
			if i+1 < len(fields) {
				return fields[i+1]
			}
			return ""
		}
		switch field {
		case "--auth-policy":
			policyPath = next()
		case "--rpc-resource":
			flags.resource = next()
		case "--allow-issuer-wide-audiences":
			flags.allowIssuerWideAudiences = true
		}
	}

	return flags, policyPath
}
