// Command commitcheck holds a pull request or commit message to the
// comms-commit and comms-pr conventions (#1728).
//
//	go run ./tools/commitcheck                       # the PR from $GITHUB_EVENT_PATH
//	git log -1 --format=%B | go run ./tools/commitcheck   # a commit message
//	go run ./tools/commitcheck -title 'x: y' -body-file body.md
//
// Under GitHub Actions each finding is a `::warning` annotation naming the
// rule and the skill that explains it; elsewhere it is a line on stderr. The
// exit status is zero unless -strict is given: the check runs warning-only
// until 2026-09-21 so the ratchet is visible before it bites, and the plan
// job flips the flag then.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/picatz/flowstate/internal/commitcheck"
)

func main() {
	title := flag.String("title", "", "the subject to check; with -body-file, the two halves of a message")
	bodyFile := flag.String("body-file", "", "a file holding the body; \"-\" reads stdin")
	strict := flag.Bool("strict", false, "exit 1 on any finding rather than reporting only")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: go run ./tools/commitcheck [-strict] [-title subject -body-file file]\n\n")
		fmt.Fprintf(os.Stderr, "Reads the pull request from $GITHUB_EVENT_PATH, or a whole message from stdin.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	subject, body, err := message(*title, *bodyFile, os.Getenv("GITHUB_EVENT_PATH"), os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "commitcheck: %v\n", err)
		os.Exit(2)
	}

	findings := commitcheck.Check(subject, body)
	report(os.Stderr, findings, os.Getenv("GITHUB_ACTIONS") == "true")

	if *strict && len(findings) > 0 {
		os.Exit(1)
	}
}

// message picks the subject and body from wherever this invocation carries
// them: the flags, the pull request in the event payload, or a whole message
// on stdin with the subject as its first line.
func message(title, bodyFile, eventPath string, stdin io.Reader) (subject, body string, err error) {
	switch {
	case title != "":
		if bodyFile != "" {
			data, err := readFile(bodyFile, stdin)
			if err != nil {
				return "", "", err
			}
			body = string(data)
		}
		return title, body, nil

	case eventPath != "":
		data, err := readBounded(eventPath, nil)
		if err != nil {
			return "", "", fmt.Errorf("reading the event payload: %w", err)
		}
		var event struct {
			PullRequest *struct {
				Title string `json:"title"`
				Body  string `json:"body"`
			} `json:"pull_request"`
		}
		if err := json.Unmarshal(data, &event); err != nil {
			return "", "", fmt.Errorf("decoding the event payload: %w", err)
		}
		if event.PullRequest == nil {
			return "", "", fmt.Errorf("the event payload carries no pull request; this check reads pull_request events")
		}
		return event.PullRequest.Title, event.PullRequest.Body, nil

	default:
		data, err := readBounded("-", stdin)
		if err != nil {
			return "", "", fmt.Errorf("reading the message from stdin: %w", err)
		}
		subject, body, _ = strings.Cut(strings.TrimSpace(string(data)), "\n")
		return subject, body, nil
	}
}

// maxInput bounds what this reads from a file, the event payload, or stdin: a
// message is kilobytes, an event payload is under a megabyte, and a file that
// is larger is the wrong file, refused rather than loaded (Codex, #1848).
const maxInput = 4 << 20

func readFile(name string, stdin io.Reader) ([]byte, error) {
	return readBounded(name, stdin)
}

// readBounded reads name, or stdin for "-", refusing anything over maxInput.
func readBounded(name string, stdin io.Reader) ([]byte, error) {
	var r io.Reader = stdin
	if name != "-" {
		f, err := os.Open(name)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	}

	data, err := io.ReadAll(io.LimitReader(r, maxInput+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxInput {
		return nil, fmt.Errorf("%s is over %d bytes, which no message or event payload is; check the path", name, maxInput)
	}
	return data, nil
}

// report writes the findings, as workflow annotations under Actions.
//
// A finding repeats part of the author's line, so what reaches the runner
// log is escaped the way workflow commands require: a newline or a `::` in
// a title or body must not become a second command, or a spoofed one.
func report(out io.Writer, findings []commitcheck.Finding, actions bool) {
	for _, f := range findings {
		if actions {
			fmt.Fprintf(out, "::warning title=%s::%s\n",
				escapeProperty("commitcheck/"+string(f.Rule)),
				escapeData(f.Message+" (see "+f.Skill+")"))
			continue
		}
		fmt.Fprintf(out, "commitcheck: %s\n", f)
	}
	if len(findings) == 0 {
		fmt.Fprintln(out, "commitcheck: the message follows the conventions")
	}
}

// escapeData escapes a workflow command's data section, per the runner's
// rules: percent first, then the line breaks that would end the command.
func escapeData(s string) string {
	return strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A").Replace(s)
}

// escapeProperty escapes a workflow command's property value, which also
// cannot hold the separators the command's grammar uses.
func escapeProperty(s string) string {
	return strings.NewReplacer("%", "%25", "\r", "%0D", "\n", "%0A", ":", "%3A", ",", "%2C").Replace(s)
}
