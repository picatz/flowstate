// Command fakecodex stands in for the real `codex` CLI in this plugin's own
// tests, so exec_test.go and errors_test.go can exercise readRun,
// classifyRunError, and the plugin's own bounds against a real subprocess -
// real os/exec, a real pipe, a real exit code - without a network
// dependency on OpenAI or a machine that happens to have codex installed.
//
// It reads the prompt from stdin (and discards it, the same as the real CLI
// would consume it), then does exactly what its own environment tells it
// to:
//
//	FAKECODEX_EVENTS_FILE  a file of newline-delimited JSON event lines,
//	                       copied to stdout verbatim
//	FAKECODEX_STDERR       text written to stderr before exiting
//	FAKECODEX_EXIT_CODE    the process's own exit code (default 0)
//	FAKECODEX_SLEEP_MS     how long to sleep before doing any of the above,
//	                       for a test that needs this process to still be
//	                       running when its caller's deadline expires
package main

import (
	"io"
	"os"
	"strconv"
	"time"
)

func main() {
	io.Copy(io.Discard, os.Stdin)

	if ms := os.Getenv("FAKECODEX_SLEEP_MS"); ms != "" {
		if n, err := strconv.Atoi(ms); err == nil {
			time.Sleep(time.Duration(n) * time.Millisecond)
		}
	}

	if path := os.Getenv("FAKECODEX_EVENTS_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			os.Stdout.Write(data)
		}
	}

	if stderr := os.Getenv("FAKECODEX_STDERR"); stderr != "" {
		os.Stderr.WriteString(stderr)
	}

	code := 0
	if c := os.Getenv("FAKECODEX_EXIT_CODE"); c != "" {
		if n, err := strconv.Atoi(c); err == nil {
			code = n
		}
	}
	os.Exit(code)
}
