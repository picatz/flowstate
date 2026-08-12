package docsgen

import (
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// renderCLIReference documents the command line, from the cobra tree.
//
// [Sources.NewRoot] exists to be asked — the README's command pin tests already
// walk the same tree — and this is the deeper form of the same question: not
// only which commands exist but what each one takes. Hidden commands are left out on the
// grounds that a hidden command is a build step rather than a capability.
func (g *Generator) renderCLIReference() string {
	var commands []cliCommand
	var mirrors map[string]string

	g.withCleanEnvironment(func() {
		commands = g.describeCommands(g.src.NewRoot())
		mirrors = g.environmentMirrors()
	})

	var b strings.Builder

	b.WriteString(generatedNotice + "\n\n")
	b.WriteString("# CLI reference\n\n")
	b.WriteString("Every command and flag `flow` has, derived from the cobra tree the binary builds\n")
	b.WriteString("at startup. [docs/CLI.md](../CLI.md) is the philosophy this surface holds itself\n")
	b.WriteString("to; this is the enumeration.\n\n")
	b.WriteString("A flag whose default comes from an environment variable says so. Those defaults\n")
	b.WriteString("are read when the command tree is built, which is why the generator clears the\n")
	b.WriteString("environment first: otherwise this document would record whoever ran it.\n\n")

	for _, cmd := range commands {
		fmt.Fprintf(&b, "## `%s`\n\n", cmd.path)
		if cmd.short != "" {
			fmt.Fprintf(&b, "%s\n\n", cmd.short)
		}
		fmt.Fprintf(&b, "```\n%s\n```\n\n", cmd.use)
		if cmd.long != "" && cmd.long != cmd.short {
			fmt.Fprintf(&b, "%s\n\n", cmd.long)
		}
		if cmd.example != "" {
			b.WriteString("Examples:\n\n")
			fmt.Fprintf(&b, "```sh\n%s\n```\n\n", cmd.example)
		}

		if len(cmd.flags) == 0 {
			continue
		}

		b.WriteString("| Flag | Type | Default | Environment | Description |\n|---|---|---|---|---|\n")
		for _, flag := range cmd.flags {
			fmt.Fprintf(&b, "| `%s` | `%s` | %s | %s | %s |\n",
				cell(flag.spelling), cell(flag.kind), orDash(codeOrEmpty(flag.defValue)),
				orDash(codeOrEmpty(mirrors[cmd.path+" "+flag.name])), cell(flag.usage))
		}
		b.WriteString("\n")
	}

	return b.String()
}

// cliCommand is one command, as the reference needs it.
type cliCommand struct {
	path    string
	use     string
	short   string
	long    string
	example string
	flags   []cliFlag
}

// cliFlag is one flag of one command.
type cliFlag struct {
	name     string
	spelling string
	kind     string
	defValue string
	usage    string
}

// describeCommands walks the tree, parents before children and siblings by name.
//
// cobra's own `help` and `completion` are left out for the reason the README's
// pin test leaves them out: they are the framework's commands rather than this
// tool's, and documenting them here would be describing cobra.
func (g *Generator) describeCommands(root *cobra.Command) []cliCommand {
	out := []cliCommand{g.describeCommand(root)}

	children := slices.Clone(root.Commands())
	sort.Slice(children, func(i, j int) bool { return children[i].Name() < children[j].Name() })

	for _, child := range children {
		switch {
		case child.Hidden, child.Name() == "help", child.Name() == "completion":
			continue
		}
		out = append(out, g.describeCommands(child)...)
	}

	return out
}

// describeCommand reads one command's own documentation.
func (g *Generator) describeCommand(cmd *cobra.Command) cliCommand {
	described := cliCommand{
		path:    cmd.CommandPath(),
		use:     g.src.UseLine(cmd),
		short:   strings.TrimSpace(cmd.Short),
		long:    strings.TrimSpace(cmd.Long),
		example: strings.TrimSpace(cmd.Example),
	}

	seen := map[string]bool{}
	add := func(f *pflag.Flag) {
		if f.Hidden || seen[f.Name] {
			return
		}
		seen[f.Name] = true

		described.flags = append(described.flags, cliFlag{
			name:     f.Name,
			spelling: g.src.FlagName(f),
			kind:     f.Value.Type(),
			defValue: f.DefValue,
			usage:    strings.TrimSpace(f.Usage),
		})
	}

	// Local flags only. An inherited flag is documented on the command that
	// declares it, and repeating `--verbose` on all twenty-two commands is a page
	// nobody reads twice.
	cmd.LocalFlags().VisitAll(add)

	sort.Slice(described.flags, func(i, j int) bool { return described.flags[i].name < described.flags[j].name })

	return described
}

// environmentMirrors reports which flag defaults come from which variable, keyed
// by "<command path> <flag>".
//
// Derived rather than declared, because pflag has nowhere to record it: a flag
// whose default is `os.Getenv("FLOWSTATE_ADDRESS")` looks exactly like one whose
// default is a constant. So each documented variable is set to a value nothing
// else could produce, the whole command tree is rebuilt, and any flag defaulting
// to that value is the flag that variable feeds.
//
// It answers for the mirrors and nothing else. A variable read as a *condition*
// rather than as a default — `FLOWSTATE_VERBOSE_LOGGING == "true"`, which yields
// a bool — cannot be seen this way, and the env-var reference says where those
// are read in prose. This finds every one that is genuinely a default, including
// one added tomorrow to a flag nobody remembers to document.
func (g *Generator) environmentMirrors() map[string]string {
	mirrors := map[string]string{}

	for i, variable := range g.documentedEnvironmentVariables() {
		if variable.family {
			continue
		}

		sentinel := fmt.Sprintf("flowstate-docsgen-sentinel-%d", i)
		if err := os.Setenv(variable.name, sentinel); err != nil {
			continue
		}

		for _, cmd := range g.describeCommands(g.src.NewRoot()) {
			for _, flag := range cmd.flags {
				if strings.Contains(flag.defValue, sentinel) {
					mirrors[cmd.path+" "+flag.name] = variable.name
				}
			}
		}

		_ = os.Unsetenv(variable.name)
	}

	return mirrors
}

// EnvironmentMirrors is [Generator.environmentMirrors] with the environment
// cleared around it, which is the only way the answer means anything.
//
// Exported for the test that pins the derivation. Setting a sentinel and
// looking for it is clever enough to deserve one: a change to how a default is
// composed (wrapping it in a `cmp.Or`, say, which is already how two of them
// are written) could silently empty the whole Environment column, and an empty
// column reads as "no flag takes a variable" rather than as a broken
// derivation. That test has to run where the real command tree is built, which
// is cmd/flow, so this is the seam it asks through.
func (g *Generator) EnvironmentMirrors() map[string]string {
	var mirrors map[string]string

	g.withCleanEnvironment(func() {
		mirrors = g.environmentMirrors()
	})

	return mirrors
}

// withCleanEnvironment runs f with every variable this reference documents
// removed, and puts the environment back afterwards.
//
// Both halves matter. Without the clearing, the generated CLI reference records
// the machine it ran on and CI's pin fails for the wrong reason. Without the
// restoring, the generator would be a command that quietly unconfigures the
// process it runs in.
func (g *Generator) withCleanEnvironment(f func()) {
	saved := map[string]string{}
	for _, variable := range g.documentedEnvironmentVariables() {
		if value, found := os.LookupEnv(variable.name); found {
			saved[variable.name] = value
			_ = os.Unsetenv(variable.name)
		}
	}

	defer func() {
		for name, value := range saved {
			_ = os.Setenv(name, value)
		}
	}()

	f()
}
