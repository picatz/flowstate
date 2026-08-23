package main

import (
	"bufio"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/picatz/flowstate/pkg/flowstate/v1/auth"
)

// certificateOnlyMarker is the phrase a documented command's own comment uses
// to say that the deployment it belongs to authenticates by client certificate
// alone — every issuers[] entry kind: mtls.
//
// It is prose rather than a directive, because it has to earn its place in the
// document for a human reader first: the comment above such a command explains
// why it names no audience, and this test happens to read the same sentence.
// The alternative — a marker that means nothing to a reader — is a second
// spelling of the deployment's shape, maintained only for the benefit of a
// test, and it would rot the first time someone rewrote the prose around it.
const certificateOnlyMarker = "certificate-only"

// documentedCommand is one documented way of starting `flow server` — a
// command line as written, or a service unit plus the environment file it
// reads — reduced to what [resolveRPCResource] looks at.
type documentedCommand struct {
	// text is the invocation as a reader sees it, for the failure message.
	text string

	// flags and policyPath are what the invocation resolves to, whether it
	// spelled them as flags or as environment variables.
	flags      rpcResourceFlags
	policyPath string

	// insecure reports --insecure-no-auth: the one way a server starts
	// without a trust policy.
	insecure bool

	// subcommand reports `flow server <verb>` — `dev`, today — which is a
	// different command with its own authentication and none of these rules.
	subcommand bool

	// certificateOnly reports that [certificateOnlyMarker] appeared in the
	// comment block introducing this invocation.
	certificateOnly bool

	// envConfigured distinguishes a systemd-style recipe from a command line,
	// so the walk can assert it kept reading both kinds.
	envConfigured bool
}

// TestDocumentedServerInvocationsStart walks every `flow server` command line
// in this repository's Markdown and runs the one start-up decision a reader
// cannot see until they try it: whether [resolveRPCResource] accepts the flags
// as written against the policy the command names.
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
// audience it documents fails here rather than in somebody's terminal.
//
// # Two strengths of check, and why the weaker one is not a skip
//
// A production recipe names a policy path outside this repository
// (/etc/flowstate/trust.yaml), and there is no file to load, so the exact
// audience cannot be compared against any issuer's audiences. What can still be
// read is the *flag surface*: whether the command says anything at all about
// the Connect RPC audience.
//
// Skipping those outright is what the first version of this test did, and it
// left THREAT_MODEL.md's two key-rotation commands broken while reporting
// green, and would not have noticed the Tier 2 flag in docs/DEPLOYMENT.md being
// deleted again — the very line this test was written alongside. Reported by
// Codex on picatz/flowstate#1053.
//
// # Which flag surface is right depends on the deployment, so the document says
//
// The requirement is not "always pass --rpc-resource". [resolveRPCResource]
// *refuses* both audience flags for a policy that admits no bearer token —
// every entry kind: mtls, where a client certificate carries no audience claim
// — so demanding one of every documented command would make a valid
// certificate-only recipe impossible to write down. Also reported by Codex on
// picatz/flowstate#1053.
//
// A document says which it is, in the comment above the command, and this
// reads it: marked certificate-only, the command must carry *neither* flag;
// unmarked, it must carry one. Both directions are asserted, so a recipe that
// gains a flag it would be refused for fails here too, and a marker cannot be
// used to quiet the check — where the policy file is loadable, the claim is
// checked against the policy itself.
//
// # A recipe that spells the flags as environment variables
//
// Not every documented server is a command line. The systemd recipe in
// docs/DEPLOYMENT.md starts one with `ExecStart=/usr/local/bin/flow server`
// carrying no flags at all, because `EnvironmentFile=` supplies
// FLOWSTATE_AUTH_POLICY and FLOWSTATE_RPC_RESOURCE — and being a command line
// is what this walk originally looked for, so that invocation was invisible to
// it. Deleting the resource variable left every counter unchanged and the suite
// green over a recipe that would then refuse to start. Also reported by Codex
// on picatz/flowstate#1053. [envConfiguredServersIn] reads those through the
// association the unit itself declares, and they are checked by exactly the
// rules above, since an environment variable is a flag's default rather than a
// different requirement.
//
// # A server with no policy has said something too
//
// `authVerifier` (cmd/flow/main.go) refuses a server that names neither
// --auth-policy nor --insecure-no-auth: anonymous access is a flag an operator
// asks for, never a fallback for a policy that is missing. An earlier version
// of this walk read "no --auth-policy" as "no requirement", which let eight
// `flow server &` lines across the plugin and operations walkthroughs stay
// broken — they refuse to start with "no authentication configured" — while
// this test reported green over them. Also reported by Codex on
// picatz/flowstate#1053. A command with no policy must therefore say
// --insecure-no-auth, and the walkthroughs now do.
//
// What remains genuinely unchecked is one thing and it is named:
// `flow server dev`, a different command with its own authentication, skipped
// on the subcommand rather than on the absence of a flag.
//
// The five counts are each asserted non-zero: a walk that stopped reading any
// one kind would otherwise pass by finding nothing.
func TestDocumentedServerInvocationsStart(t *testing.T) {
	t.Parallel()

	root := filepath.Join("..", "..")

	var resolved, flagsOnly, certificateOnly, envConfigured, anonymous int
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

		invocations := append(serverCommandsIn(string(data)), envConfiguredServersIn(string(data))...)
		for _, documented := range invocations {
			flags, policyPath := documented.flags, documented.policyPath

			if documented.subcommand {
				// `flow server dev`: a different command, and the only
				// invocation this walk deliberately says nothing about.
				continue
			}

			if policyPath == "" {
				// authVerifier (cmd/flow/main.go) refuses a server that names
				// neither, before anything is served: anonymous access is not
				// a fallback for a missing policy, it is a flag. So "no
				// policy" is only half an answer, and the other half decides
				// whether this is a rehearsal or a broken walkthrough.
				require.Truef(t, documented.insecure,
					"%s documents a `flow server` with neither --auth-policy nor --insecure-no-auth, which "+
						"refuses to start: \"no authentication configured\". A local walkthrough says "+
						"--insecure-no-auth and a deployment names a policy; there is no third answer:"+
						"\n\t%s\n", path, documented.text)
				anonymous++
				continue
			}

			names := flags.resource != "" || flags.allowIssuerWideAudiences

			if _, statErr := os.Stat(filepath.Join(root, policyPath)); statErr != nil {
				// The weaker check: the policy lives on the deployment's own
				// disk, so all this can read is whether the command's flag
				// surface matches the deployment the document describes.
				if documented.certificateOnly {
					require.Falsef(t, names,
						"%s documents a certificate-only `flow server` that names an audience flag, which "+
							"resolveRPCResource refuses: a kind: mtls entry admits a caller by client "+
							"certificate and carries no audience claim, so there is nothing to bind:"+
							"\n\t%s\n", path, documented.text)
					certificateOnly++
					if documented.envConfigured {
						envConfigured++
					}
					continue
				}

				require.Truef(t, names,
					"%s documents a `flow server` whose policy this test cannot load (%s), and which names "+
						"neither --rpc-resource nor --allow-issuer-wide-audiences — an operator whose policy "+
						"has a kind: oidc issuer, which is every authenticated deployment, cannot start it. "+
						"If the recipe is for a certificate-only deployment, say so in the comment above it "+
						"(%q):\n\t%s\n", path, policyPath, certificateOnlyMarker, documented.text)
				flagsOnly++
				if documented.envConfigured {
					envConfigured++
				}
				continue
			}

			raw, err := os.ReadFile(filepath.Join(root, policyPath))
			require.NoError(t, err)
			policy, err := auth.ParsePolicy(raw)
			require.NoErrorf(t, err, "%s documents a server started with %s, which does not parse", path, policyPath)

			// A marker is a claim about the policy, so where the policy is
			// here it is checked rather than believed: a command calling
			// itself certificate-only above a policy that mints bearer tokens
			// is describing some other deployment.
			if documented.certificateOnly {
				require.Falsef(t, auth.AdmitsBearerTokens(&policy),
					"%s calls this command certificate-only, but %s trusts an issuer that mints bearer "+
						"tokens:\n\t%s\n", path, policyPath, documented.text)
				certificateOnly++
			}

			_, err = resolveRPCResource(flags, authFlags{policyPath: policyPath, insecure: documented.insecure}, &policy)
			require.NoErrorf(t, err, "%s documents a `flow server` that cannot start:\n\t%s\n", path, documented.text)
			resolved++
			if documented.envConfigured {
				envConfigured++
			}
		}
		return nil
	})
	require.NoError(t, err)

	require.NotZero(t, resolved, "no documented server invocation was resolved against a policy in this "+
		"repository; the walk or the fence syntax changed")
	require.NotZero(t, flagsOnly, "no documented server invocation named a policy outside this repository; if that "+
		"is now true of every production recipe, delete this assertion deliberately rather than letting the walk "+
		"quietly stop reading them")
	require.NotZerof(t, certificateOnly, "no documented server invocation was marked %q; the marker or the "+
		"certificate-only recipe it reads went away, and with it the half of this check that keeps an audience "+
		"flag off a deployment that has no audience", certificateOnlyMarker)
	require.NotZero(t, envConfigured, "no documented server invocation was configured through an environment "+
		"file; a systemd-style recipe spells the same requirement as variables rather than flags, and one that "+
		"stopped being recognized here would be unstartable with every counter unchanged")
	require.NotZero(t, anonymous, "no documented server invocation asked for anonymous access; every local "+
		"walkthrough having grown a trust policy would be news, and a walk that stopped seeing them would look "+
		"exactly the same from here")
}

// serverCommandsIn returns every `flow server ...` command line in a Markdown
// document, with shell line continuations joined so a wrapped invocation is
// read as the one command it is, and with the contiguous comment block above
// each command carried along — that is where a document says what kind of
// deployment the command belongs to.
func serverCommandsIn(document string) []documentedCommand {
	var commands []documentedCommand

	scanner := bufio.NewScanner(strings.NewReader(document))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var (
		current   string
		comment   string
		preceding string
	)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if current != "" {
			continued := strings.HasSuffix(line, `\`)
			current += " " + strings.TrimSuffix(line, `\`)
			if !continued {
				commands = append(commands, shellCommand(current, preceding))
				current = ""
			}
			continue
		}

		// A comment block accumulates and is handed to the next command;
		// anything else between the two clears it, so a marker cannot drift
		// onto a command several paragraphs away.
		if strings.HasPrefix(line, "#") {
			comment += " " + line
			continue
		}

		line = strings.TrimPrefix(line, "$ ")
		if !strings.HasPrefix(line, "flow server") {
			comment = ""
			continue
		}

		preceding, comment = comment, ""
		if strings.HasSuffix(line, `\`) {
			current = strings.TrimSuffix(line, `\`)
			continue
		}
		commands = append(commands, shellCommand(line, preceding))
	}

	return commands
}

// shellCommand reduces one written-out command line, plus the comment block
// above it, to what the check needs.
func shellCommand(command, preceding string) documentedCommand {
	text := strings.Join(strings.Fields(command), " ")
	flags, policyPath, insecure, subcommand := parseServerCommand(text)

	return documentedCommand{
		text:            text,
		flags:           flags,
		policyPath:      policyPath,
		insecure:        insecure,
		subcommand:      subcommand,
		certificateOnly: strings.Contains(strings.ToLower(preceding), certificateOnlyMarker),
	}
}

// envConfiguredServersIn returns every `flow server` a document starts through
// a service unit rather than a written-out command line — the systemd recipe in
// docs/DEPLOYMENT.md, whose `ExecStart=/usr/local/bin/flow server` carries no
// flags at all because `EnvironmentFile=` supplies them.
//
// That invocation is invisible to [serverCommandsIn], which reads command lines,
// and it was invisible to this test: deleting FLOWSTATE_RPC_RESOURCE from the
// environment file left every counter unchanged and the suite green, on a recipe
// that would then refuse to start. Reported by Codex on picatz/flowstate#1053.
//
// The association is the one the unit itself declares, rather than proximity in
// the page: an env fence is keyed by the file path named just above it
// (`/etc/flowstate/server.env`:), a unit fence names the files it reads with
// `EnvironmentFile=`, and the variables of exactly those files are what the
// ExecStart line runs with. A recipe that renamed one and not the other would
// stop matching here, which is the same mistake it would be on the host.
//
// Flags written on the ExecStart line itself win over the environment, matching
// cobra: the environment value is the flag's default.
func envConfiguredServersIn(document string) []documentedCommand {
	type unit struct {
		execStart string
		envFiles  []string
		comment   string
	}

	var (
		commands  []documentedCommand
		units     []unit
		envFiles  = map[string]map[string]string{}
		fence     string
		fenceBody []string
		label     string
	)

	scanner := bufio.NewScanner(strings.NewReader(document))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, "```") {
			if fence == "" {
				fence, fenceBody = strings.TrimPrefix(line, "```"), nil
				continue
			}

			switch fence {
			case "env":
				if label != "" {
					envFiles[label] = parseEnvFile(fenceBody)
				}
			case "ini":
				if current, ok := parseServiceUnit(fenceBody); ok {
					units = append(units, unit(current))
				}
			}
			fence = ""
			continue
		}

		if fence != "" {
			fenceBody = append(fenceBody, line)
			continue
		}

		// The path a fence belongs to is written immediately above it, as
		// `/etc/flowstate/server.env`: — the same line a reader uses to know
		// which file they are being shown.
		if path, ok := backtickedPath(line); ok {
			label = path
		}
	}

	for _, current := range units {
		fields := strings.Fields(current.execStart)
		if len(fields) < 2 || filepath.Base(fields[0]) != "flow" || fields[1] != "server" {
			continue
		}

		environment := map[string]string{}
		for _, file := range current.envFiles {
			for name, value := range envFiles[file] {
				environment[name] = value
			}
		}

		documented := shellCommand(strings.Join(fields, " "), current.comment)
		documented.envConfigured = true
		documented.text = current.execStart + " (with " + strings.Join(current.envFiles, ", ") + ")"
		if documented.policyPath == "" {
			documented.policyPath = environment["FLOWSTATE_AUTH_POLICY"]
		}
		if documented.flags.resource == "" {
			documented.flags.resource = environment["FLOWSTATE_RPC_RESOURCE"]
		}

		commands = append(commands, documented)
	}

	return commands
}

// parseEnvFile reads NAME=value lines out of an ```env fence.
func parseEnvFile(body []string) map[string]string {
	variables := map[string]string{}
	for _, line := range body {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		variables[strings.TrimSpace(name)] = strings.TrimSpace(value)
	}
	return variables
}

// parseServiceUnit reads the one ExecStart and every EnvironmentFile out of a
// systemd unit fence, along with its comments, reporting false for a fence that
// starts nothing.
func parseServiceUnit(body []string) (struct {
	execStart string
	envFiles  []string
	comment   string
}, bool) {
	var parsed struct {
		execStart string
		envFiles  []string
		comment   string
	}

	for _, line := range body {
		switch {
		case strings.HasPrefix(line, "#"):
			parsed.comment += " " + line
		case strings.HasPrefix(line, "ExecStart="):
			parsed.execStart = strings.TrimPrefix(line, "ExecStart=")
		case strings.HasPrefix(line, "EnvironmentFile="):
			parsed.envFiles = append(parsed.envFiles, strings.TrimPrefix(line, "EnvironmentFile="))
		}
	}

	return parsed, parsed.execStart != ""
}

// isShellOperator reports whether a field ends the command rather than
// continuing it.
func isShellOperator(field string) bool {
	switch field {
	case "&", "&&", "|", "||", ";":
		return true
	}
	return false
}

// isVerb reports whether a field is a cobra subcommand name rather than a flag,
// a path, a shell operator or an argument.
func isVerb(field string) bool {
	if field == "" || field[0] < 'a' || field[0] > 'z' {
		return false
	}
	for _, r := range field {
		if (r < 'a' || r > 'z') && r != '-' {
			return false
		}
	}
	return true
}

// backtickedPath reads a line that is nothing but one backticked path, with or
// without a trailing colon — how these documents label the file a fence holds.
func backtickedPath(line string) (string, bool) {
	line = strings.TrimSuffix(line, ":")
	if !strings.HasPrefix(line, "`") || !strings.HasSuffix(line, "`") || len(line) < 3 {
		return "", false
	}
	path := strings.Trim(line, "`")
	if strings.ContainsAny(path, "` ") {
		return "", false
	}
	return path, true
}

// parseServerCommand reads the flags resolveRPCResource looks at off one
// command line, returning the --auth-policy path separately because it is what
// decides whether there is a policy to load at all.
func parseServerCommand(command string) (rpcResourceFlags, string, bool, bool) {
	var (
		flags      rpcResourceFlags
		policyPath string
		insecure   bool
	)

	fields := strings.Fields(command)

	// A walkthrough backgrounds its server (`flow server --insecure-no-auth &`)
	// and sometimes chains onto it, so the command ends at the first shell
	// operator. Reading "&" as an argument is not a cosmetic slip: it looked
	// like a subcommand below, which skipped every backgrounded server in the
	// repository — the exact recipes this round is about.
	if end := slices.IndexFunc(fields, isShellOperator); end >= 0 {
		fields = fields[:end]
	}

	// `flow server dev` is a different command — its own flags, its own
	// authentication — and nothing here applies to it. A verb, specifically:
	// anything else in that position is an argument to `flow server` itself.
	if len(fields) > 2 && isVerb(fields[2]) {
		return flags, "", false, true
	}

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
		case "--insecure-no-auth":
			insecure = true
		}
	}

	return flags, policyPath, insecure, false
}
