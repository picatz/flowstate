package execpolicy

import (
	"fmt"
	"os"
)

// openExecutable re-verifies the table entry at the moment of use and returns
// what to execute and how to release it.
//
// It opens the file once, without blocking, and judges that descriptor: still a
// regular, executable file nobody can write, and still the pinned content when
// it has a pin. When the platform can execute a descriptor the returned path is
// that descriptor's, and the file stays open until release; otherwise it is the
// resolved path (see [Command.Run] for the window that leaves).
//
// files are the standard streams the child will be given, in order, which a
// descriptor-based exec needs to place the executable's descriptor clear of the
// descriptors the child-side setup renumbers.
func (c *Command) openExecutable(files []*os.File) (path string, release func(), err error) {
	denied := func(detail string) (string, func(), error) {
		return "", nil, &DeniedError{Reason: ReasonIntegrity, Detail: detail}
	}

	f, err := openRegular(c.exe.path)
	if err != nil {
		return "", nil, &RunError{Outcome: OutcomeDidNotStart, Err: startFailure(c.argv[0], err)}
	}

	// Judged before anything is read: a FIFO, device or directory swapped in
	// for the file is refused here, and a read of one could block.
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

	if fdPath, release, ok := pinToDescriptor(f, info, files); ok {
		return fdPath, release, nil
	}

	f.Close()
	return c.exe.path, func() {}, nil
}
