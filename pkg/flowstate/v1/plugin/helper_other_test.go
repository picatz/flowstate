//go:build !unix

package plugin

// ignoreSIGTERM does nothing here. SIGTERM and process groups are POSIX
// notions this platform has no mechanism for (see [proc_other.go]), and the
// one fake plugin mode that calls this is only ever launched by
// launch_linux_test.go, which this platform never builds.
func ignoreSIGTERM() {}
