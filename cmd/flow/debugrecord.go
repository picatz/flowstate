package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/picatz/flowstate/pkg/flowstate/v1/flowdebug"
)

// addRecordFlag declares `--record`, the file a debugged session writes its
// accepted commands to, in the format `flow debug replay` reads.
func addRecordFlag(cmd *cobra.Command) {
	cmd.Flags().String("record", "",
		"with --debug, write the commands the session accepted to this file when it ends (end of run, `quit`, error), "+
			"so `flow debug replay` can reproduce the session (a mistyped command, or a `break` the run "+
			"refused, is not in it)")
}

// recordPath is `--record`'s value, refused where there is no session to
// record: a flag that silently does nothing is the failure the debugger's other
// refusals state.
func recordPath(cmd *cobra.Command) (string, error) {
	path, _ := cmd.Flags().GetString("record")
	if debugging, _ := cmd.Flags().GetBool("debug"); path != "" && !debugging {
		return "", errors.New("--record writes a debugger session's commands, and there is no session without --debug")
	}

	return path, nil
}

// refuseRecordingOver refuses `--record` naming the script being replayed: the
// script is read first and the recording written last, so a replay that is
// shortened or quit would truncate the very file it came from.
func refuseRecordingOver(cmd *cobra.Command, script string) error {
	record, _ := cmd.Flags().GetString("record")
	if record == "" {
		return nil
	}
	scriptInfo, err := os.Stat(script)
	if err != nil {
		return nil
	}
	if recordInfo, err := os.Stat(record); err == nil && os.SameFile(scriptInfo, recordInfo) {
		return fmt.Errorf("--record %s is the script being replayed, and recording over it would truncate it; "+
			"name another file", record)
	}

	return nil
}

// recordSession returns the function a caller defers to write the session's
// script to path, and a no-op where path is empty.
//
// It is written once, after the run, from [flowdebug.Session.Script] — the
// commands the session accepted, which is what makes the file a session
// `flow debug replay` reaches the same stops with, and not a log of what was
// typed. A failure to write is reported on stderr and never replaces the run's
// own verdict: the recording is a by-product, and losing it must not turn a
// passing case into a failing command.
func recordSession(path string, session *flowdebug.Session, stderr io.Writer) func() {
	if path == "" || session == nil {
		return func() {}
	}

	return func() {
		var b strings.Builder
		for _, line := range session.Script() {
			b.WriteString(line)
			b.WriteByte('\n')
		}
		if session.ScriptTruncated() {
			b.WriteString("# the recording stopped at the script bounds; this file replays a prefix of the session\n")
		}
		// Owner-only: an expression typed at `inspect` can name anything in scope.
		// Narrowed before a byte is written when the file already existed, since
		// a write keeps an existing file's mode and the file holds typed
		// expressions.
		err := os.Chmod(path, 0o600)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
		if err == nil {
			err = os.WriteFile(path, []byte(b.String()), 0o600)
		}
		if err != nil {
			fmt.Fprintf(stderr, "could not write the recording to %s: %v\n", path, err)
		}
	}
}
