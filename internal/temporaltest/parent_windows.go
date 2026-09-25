package temporaltest

import "golang.org/x/sys/windows"

type parentWatch struct {
	handle windows.Handle
}

func newParentWatch(pid int) (*parentWatch, error) {
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		return nil, err
	}
	return &parentWatch{handle: handle}, nil
}

func (w *parentWatch) gone() bool {
	status, err := windows.WaitForSingleObject(w.handle, 0)
	return err != nil || status == windows.WAIT_OBJECT_0
}

func (w *parentWatch) close() {
	_ = windows.CloseHandle(w.handle)
}
