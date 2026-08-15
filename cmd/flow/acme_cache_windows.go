package main

import "os"

// Windows does not expose POSIX ownership and mode bits through os.FileInfo.
func checkACMECacheDirSecurity(_ string, _ os.FileInfo) error {
	return nil
}
