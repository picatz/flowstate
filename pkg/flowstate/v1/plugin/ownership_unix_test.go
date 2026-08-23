//go:build unix

package plugin

import (
	"io/fs"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestOwnedByTrustedUser(t *testing.T) {
	t.Parallel()

	untrustedUID := uint32(os.Geteuid()) + 1
	if untrustedUID == 0 {
		untrustedUID++
	}

	for _, test := range []struct {
		name string
		uid  uint32
		want bool
	}{
		{name: "worker", uid: uint32(os.Geteuid()), want: true},
		{name: "root", uid: 0, want: true},
		{name: "another user", uid: untrustedUID, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			info := statFileInfo{sys: &syscall.Stat_t{Uid: test.uid}}
			if got := ownedByTrustedUser(info); got != test.want {
				t.Errorf("ownedByTrustedUser(uid %d) = %v, want %v", test.uid, got, test.want)
			}
		})
	}
}

type statFileInfo struct{ sys any }

func (statFileInfo) Name() string       { return "plugin" }
func (statFileInfo) Size() int64        { return 0 }
func (statFileInfo) Mode() fs.FileMode  { return 0o755 }
func (statFileInfo) ModTime() time.Time { return time.Time{} }
func (statFileInfo) IsDir() bool        { return false }
func (i statFileInfo) Sys() any         { return i.sys }
