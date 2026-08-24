//go:build unix

package main

import (
	"fmt"
	"os"
	"syscall"

	"github.com/picatz/flowstate/internal/pathsec"
)

// checkACMECacheDirSecurity verifies both the cache directory's ownership and
// the path used to reach it. A private mode is not sufficient when another
// identity owns the directory or can replace a path component before autocert
// opens a key.
//
// The path walk itself lives in [pathsec], because the plugin search path asks
// the identical question of a directory of programs this process executes, and
// two walks answering one question at two standards is how they come to
// disagree (#972). What stays here is the part that is about an ACME cache:
// the leaf's private mode, and the prose an operator reading a
// `--tls-acme-cache` refusal needs.
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

	if err := pathsec.New(uid).Check(path); err != nil {
		return acmeCachePathError(path, err)
	}

	return nil
}

// acmeCachePathError puts a [pathsec] refusal in the sentence a
// `--tls-acme-cache` operator can act on: which flag, which value, and — for a
// relative value — that the component named is one the walk reached rather
// than one they typed.
func acmeCachePathError(path string, err error) error {
	return fmt.Errorf("--tls-acme-cache %s cannot be opened safely: %w; private-key paths must "+
		"not be replaceable by another identity", path, err)
}
