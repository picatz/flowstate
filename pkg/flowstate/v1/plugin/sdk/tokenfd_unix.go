//go:build unix

package sdk

import (
	"fmt"
	"os"
	"syscall"
)

// openTokenDescriptor wraps the inherited token descriptor so that reading it
// can be bounded in time.
//
// The descriptor is put in non-blocking mode before it is wrapped, and that
// order is the whole of it: [os.NewFile] hands a descriptor to the runtime
// poller only when it is already non-blocking, and only a polled descriptor
// honors [os.File.SetReadDeadline]. A descriptor inherited through exec is
// blocking, so without this the deadline the caller sets is refused and the read
// waits on the launcher for as long as the launcher likes.
//
// The alternative — a goroutine holding a timer that closes the file — cannot
// do this job. Closing an [os.File] the poller does not own does not interrupt a
// read already blocked in the kernel: the close waits for the read, the read
// waits for a writer that is not coming, and the goroutine and the descriptor it
// holds outlive every deadline for as long as the process runs. Nothing here
// starts a goroutine, and the file returned is closed by the caller that reads
// it.
func openTokenDescriptor(fd int) (*os.File, error) {
	if err := syscall.SetNonblock(fd, true); err != nil {
		return nil, err
	}

	file := os.NewFile(uintptr(fd), "flowstate-plugin-token")
	if file == nil {
		return nil, fmt.Errorf("descriptor %d is not one this process can name", fd)
	}

	return file, nil
}
