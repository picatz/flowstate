//go:build !windows

package temporaltest

import "os"

type parentWatch struct {
	pid int
}

func newParentWatch(pid int) (*parentWatch, error) {
	return &parentWatch{pid: pid}, nil
}

func (w *parentWatch) gone() bool {
	return os.Getppid() != w.pid
}

func (*parentWatch) close() {}
