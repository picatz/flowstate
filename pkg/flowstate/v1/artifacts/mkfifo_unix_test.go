//go:build unix

package artifacts_test

import "syscall"

func mkfifo(path string) error { return syscall.Mkfifo(path, 0o600) }
