package lsp

import (
	"testing"

	"github.com/sourcegraph/go-lsp"
	"github.com/stretchr/testify/assert"
)

// TestFilesystemPath covers the URI shapes a real editor sends, not only the
// `file:///plain/path.yaml` one a prefix trim happens to get right.
//
// A `file://` URI is percent-encoded and may carry an authority, and trimming
// the scheme off the front of the string — the shape this used to take —
// leaves a space as `%20`, a `#` as `%23`, and a non-ASCII name as its UTF-8
// escapes, none of which is a path that exists on disk. This is the table for
// the decode this package's `call:` resolution depends on.
func TestFilesystemPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		uri  lsp.DocumentURI
		want string
		ok   bool
	}{
		{
			name: "plain absolute path",
			uri:  "file:///home/user/workflow.yaml",
			want: "/home/user/workflow.yaml",
			ok:   true,
		},
		{
			name: "a space, percent-encoded",
			uri:  "file:///home/user/my%20workflows/workflow.yaml",
			want: "/home/user/my workflows/workflow.yaml",
			ok:   true,
		},
		{
			name: "a hash, percent-encoded",
			uri:  "file:///home/user/issue%23123/workflow.yaml",
			want: "/home/user/issue#123/workflow.yaml",
			ok:   true,
		},
		{
			name: "a non-ASCII name, percent-encoded UTF-8",
			uri:  "file:///home/user/caf%C3%A9/workflow.yaml",
			want: "/home/user/café/workflow.yaml",
			ok:   true,
		},
		{
			name: "an explicit localhost authority",
			uri:  "file://localhost/home/user/workflow.yaml",
			want: "/home/user/workflow.yaml",
			ok:   true,
		},
		{
			name: "a genuine remote authority is refused",
			uri:  "file://otherhost/home/user/workflow.yaml",
			want: "",
			ok:   false,
		},
		{
			name: "windows drive with the empty-authority form",
			uri:  "file:///C:/Users/dev/workflow.yaml",
			want: "C:/Users/dev/workflow.yaml",
			ok:   true,
		},
		{
			name: "windows drive parsed as the authority",
			uri:  "file://C:/Users/dev/workflow.yaml",
			want: "C:/Users/dev/workflow.yaml",
			ok:   true,
		},
		{
			name: "an untitled buffer has no path",
			uri:  "untitled:Untitled-1",
			want: "",
			ok:   false,
		},
		{
			name: "a synthesized scheme has no path",
			uri:  "vscode-notebook-cell:/home/user/workflow.yaml",
			want: "",
			ok:   false,
		},
		{
			name: "an empty URI has no path",
			uri:  "",
			want: "",
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			doc := &document{uri: tt.uri}
			got, ok := doc.filesystemPath()

			assert.Equal(t, tt.ok, ok, "filesystemPath(%q) ok", tt.uri)
			assert.Equal(t, tt.want, got, "filesystemPath(%q) path", tt.uri)
		})
	}
}
