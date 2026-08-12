// Command gofmtcheck is a Claude Code PostToolUse hook on Edit and Write
// (#482): when the edited file is a .go file that is not gofmt formatted, it
// returns non-blocking feedback naming the file, so formatting is fixed at
// edit time instead of surfacing at the gate.
//
// Wired in .claude/settings.json as:
//
//	go -C "${CLAUDE_PROJECT_DIR}" run ./tools/hooks/gofmtcheck
//
// It checks with go/format, the library face of gofmt: the same printer the
// gofmt binary uses, from the same toolchain that runs this hook, with no
// dependency on a gofmt binary being on PATH. A file that does not parse is
// left alone; formatting feedback on a file mid-refactor would be noise, and
// the build owns reporting parse errors.
package main

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"strings"

	"github.com/picatz/flowstate/tools/hooks/internal/hook"
)

// maxFile bounds how much of the edited file is read; a .go file larger
// than this is not something formatting feedback helps with.
const maxFile = 16 << 20

func main() {
	in, err := hook.Read(os.Stdin)
	if err != nil {
		return // lenient: unrecognized input produces no feedback
	}
	path := in.FilePath()
	if !strings.HasSuffix(path, ".go") {
		return
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() > maxFile {
		return
	}
	src, err := os.ReadFile(path)
	if err != nil {
		return
	}
	formatted, err := format.Source(src)
	if err != nil {
		return // does not parse; the build reports that, not this hook
	}
	if !bytes.Equal(src, formatted) {
		hook.Advise(fmt.Sprintf("gofmt: %s is not gofmt formatted; run `gofmt -w %s` (CI fails on unformatted files)", path, path))
	}
}
