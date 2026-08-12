// Command pidguard is a Claude Code PreToolUse hook on Bash (#482): it
// refuses commands that would kill processes by pattern, naming CLAUDE.md's
// PID discipline. On a machine shared by several agents, `pkill -f 'go
// test'` matches every sibling's suite, and one night it matched the
// compound command containing it and killed its own shell; three agents
// re-learned this independently before it became a rule.
//
// Wired in .claude/settings.json as:
//
//	go -C "${CLAUDE_PROJECT_DIR}" run ./tools/hooks/pidguard
//
// Matching is deliberately conservative, because a false positive on prose
// would teach people to ignore the guard: quoted regions are stripped first
// (so a commit message or a grep pattern mentioning pkill passes), and
// pkill/killall match only in command position, where the shell would
// execute them. A pattern kill smuggled through `sh -c '...'` slips by;
// this is an advisory guard for a habit, not a sandbox.
package main

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/picatz/flowstate/tools/hooks/internal/hook"
)

func main() {
	in, err := hook.Read(os.Stdin)
	if err != nil {
		return // lenient: unrecognized input allows
	}
	if word := patternKill(in.Command()); word != "" {
		hook.Deny(fmt.Sprintf(
			"`%s` kills by pattern, and CLAUDE.md's discipline is to kill by PID, never by pattern: on this shared machine a pattern matches every sibling agent's processes, and one pattern kill once matched the compound command that contained it and ended its own shell. Record the PIDs of what you start ($!, a pidfile, or `ps -Ao pid,args` filtered by your own worktree path) and kill exactly those.",
			word))
	}
}

// wrappers are tokens that pass command position through to their argument:
// `sudo pkill` is still a pattern kill. Shell keywords that precede a
// command are here for the same reason.
var wrappers = map[string]bool{
	"sudo": true, "doas": true, "exec": true, "command": true, "builtin": true,
	"nohup": true, "setsid": true, "env": true, "xargs": true, "timeout": true,
	"nice": true, "ionice": true, "time": true, "stdbuf": true,
	"if": true, "then": true, "else": true, "elif": true, "while": true,
	"until": true, "do": true, "!": true, "{": true,
}

// assignment matches an environment assignment prefix (FOO=bar cmd), which
// also passes command position through.
var assignment = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*=`)

// digits matches a bare number, so `timeout 5 pkill x` still sees pkill in
// command position.
var digits = regexp.MustCompile(`^[0-9]+$`)

// patternKill returns the offending command word ("pkill" or "killall")
// when cmd would kill by pattern, or "" when it would not.
func patternKill(cmd string) string {
	for _, tok := range tokens(cmd) {
		base := path.Base(tok)
		if base == "pkill" || base == "killall" {
			return base
		}
	}
	return ""
}

// tokens returns the words of cmd that sit in command position: the first
// word, and each word after a separator, looking through wrapper commands,
// assignments, flags and bare numbers. Quoted regions are removed first so
// prose never reaches the matcher.
func tokens(cmd string) []string {
	fields := strings.Fields(spaceSeparators(stripQuoted(cmd)))
	var out []string
	cmdPos := true
	for _, tok := range fields {
		if isSeparator(tok) {
			cmdPos = true
			continue
		}
		if !cmdPos {
			continue
		}
		switch {
		case wrappers[tok]:
			// Command position passes through to the next word.
		case assignment.MatchString(tok), digits.MatchString(tok), strings.HasPrefix(tok, "-"):
			// Environment for, or arguments to, a wrapper; keep looking.
		default:
			out = append(out, tok)
			cmdPos = false
		}
	}
	return out
}

// stripQuoted removes single- and double-quoted regions and
// backslash-escaped characters, so `git commit -m "document pkill"` or
// `grep 'pkill' CLAUDE.md` never look like kills.
func stripQuoted(s string) string {
	var b strings.Builder
	var inSingle, inDouble, escaped bool
	for _, r := range s {
		switch {
		case escaped:
			escaped = false
		case inSingle:
			if r == '\'' {
				inSingle = false
			}
		case inDouble:
			switch r {
			case '\\':
				escaped = true
			case '"':
				inDouble = false
			}
		case r == '\\':
			escaped = true
		case r == '\'':
			inSingle = true
		case r == '"':
			inDouble = true
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// separatorChars are the characters after which the shell reads a new
// command: control operators, subshells, command substitution.
const separatorChars = ";&|()`"

// spaceSeparators isolates separator characters as their own fields. A
// newline also starts a new command, but strings.Fields would swallow it as
// whitespace, so it is rewritten to `;` rather than spaced.
func spaceSeparators(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '\n':
			b.WriteString(" ; ")
		case strings.ContainsRune(separatorChars, r):
			b.WriteRune(' ')
			b.WriteRune(r)
			b.WriteRune(' ')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func isSeparator(tok string) bool {
	return tok != "" && strings.Trim(tok, separatorChars) == ""
}
