//go:build !unix

package artifacts_test

import "errors"

func mkfifo(string) error { return errors.New("fifos are unix-only") }
