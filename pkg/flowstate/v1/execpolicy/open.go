package execpolicy

import (
	"fmt"
	"os"
)

// openExecutable re-verifies the table entry at the moment of use and returns
// what to execute and how to release it.
//
// It opens the file once and judges that descriptor: still a regular, executable
// file nobody can write, and still the pinned content when it has a pin. When the
// platform can execute a descriptor the returned path is that descriptor's, and
// the file stays open until release; otherwise it is the resolved path (see
// [Command.Run] for the window that leaves).
func (c *Command) openExecutable() (path string, release func(), err error) {
	denied := func(detail string) (string, func(), error) {
		return "", nil, &DeniedError{Reason: ReasonIntegrity, Detail: detail}
	}

	f, err := os.Open(c.exe.path)
	if err != nil {
		return "", nil, &RunError{Outcome: OutcomeDidNotStart, Err: startFailure(c.argv[0], err)}
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return "", nil, &RunError{Outcome: OutcomeDidNotStart, Err: startFailure(c.argv[0], err)}
	}
	if err := checkExecutableInfo(info); err != nil {
		f.Close()
		return denied(fmt.Sprintf("program %q (%s) %v", c.argv[0], c.exe.path, err))
	}
	if c.exe.sha256 != "" {
		if err := verifyPin(f, c.exe.sha256); err != nil {
			f.Close()
			return denied(fmt.Sprintf("program %q (%s) %v", c.argv[0], c.exe.path, err))
		}
	}

	if isScript(f) {
		f.Close()
		return c.exe.path, func() {}, nil
	}
	if fdPath, dup, ok := fdExecPath(f); ok {
		return fdPath, func() {
			if dup != nil {
				dup.Close()
			}
			f.Close()
		}, nil
	}

	f.Close()
	return c.exe.path, func() {}, nil
}

// isScript reports whether the file starts with a #! interpreter line. A
// script cannot be executed through a close-on-exec descriptor: the kernel
// hands the interpreter the descriptor's path, and the descriptor is gone by
// then.
func isScript(f *os.File) bool {
	var head [2]byte
	n, _ := f.ReadAt(head[:], 0)
	return n == 2 && head[0] == '#' && head[1] == '!'
}
