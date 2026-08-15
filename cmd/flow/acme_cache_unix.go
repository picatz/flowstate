//go:build !windows

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// checkACMECacheDirSecurity verifies both the cache directory's ownership and
// the path used to reach it. A private mode is not sufficient when another
// identity owns the directory or can replace a path component before autocert
// opens a key. Sticky directories (notably /tmp) are safe for an already-owned
// child: the kernel prevents other users from renaming that child.
func checkACMECacheDirSecurity(path string, info os.FileInfo) error {
	uid := uint32(os.Geteuid())
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uid {
		return fmt.Errorf("--tls-acme-cache %s is not owned by the service identity (uid %d)", path, uid)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("--tls-acme-cache %s has mode %s; this must be readable and "+
			"writable by its owner only (0700) because it holds an ACME account key and "+
			"issued certificates' private keys", path, info.Mode().Perm())
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolving --tls-acme-cache path %s: %w", path, err)
	}
	for component := abs; ; component = filepath.Dir(component) {
		componentInfo, err := os.Lstat(component)
		if err != nil {
			return fmt.Errorf("checking --tls-acme-cache path component %s: %w", component, err)
		}
		if componentInfo.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("--tls-acme-cache path component %s is a symbolic link; private-key paths must not be replaceable through links", component)
		}
		componentStat, ok := componentInfo.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("checking ownership of --tls-acme-cache path component %s", component)
		}
		otherWritable := componentInfo.Mode().Perm()&0o022 != 0
		sticky := componentInfo.Mode()&os.ModeSticky != 0
		if componentStat.Uid != uid && otherWritable && !sticky {
			return fmt.Errorf("--tls-acme-cache path component %s is writable by another identity and can be swapped", component)
		}
		parent := filepath.Dir(component)
		if parent == component {
			break
		}
	}
	return nil
}
