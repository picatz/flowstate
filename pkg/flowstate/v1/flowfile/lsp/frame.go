package lsp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/sourcegraph/jsonrpc2"
)

// MaxFrameBytes bounds one JSON-RPC frame — its header block and its body
// together — as read from whatever is on the other end of the language
// server's stdin.
//
// # Why there is a bound here at all
//
// Everything else this package reads is bounded already: a document is bounded
// by [maxDocumentBytes], a `call:` target read off disk by the same number
// through an [io.LimitReader]. The frame the document arrives *inside* was not,
// and it is the one an outside party writes first.
//
// The unbounded read is in the codec, not here. [jsonrpc2.VSCodeObjectCodec]'s
// ReadObject parses the header block with `stream.ReadString('\r')`, which has
// no bound of any kind: it returns when it finds a carriage return or when the
// stream ends, and until then it accumulates into a buffer it grows by
// doubling. Measured against v0.2.2 before this bound existed, 256 MiB of a
// single header line with no `\r` in it left ~512 MiB live in one process —
// the peer's bytes, plus the copy the doubling had not yet released. That is
// an amplifying bound-free read on the first bytes an editor sends, which is
// the shape CLAUDE.md's "bound anything that consumes untrusted input" names:
// the resource the far side controls is bytes, so bytes are what must be
// counted.
//
// The body half is less dramatic and bounded here for the same reason.
// `Content-Length` is parsed as a uint32 and handed to an [io.LimitReader], so
// the decoder will not preallocate on a declared length the peer never sends —
// but it will happily read a 4 GiB body that the peer does send, into a JSON
// document this package then hands to the store.
//
// # Where the bound lives
//
// On the reader underneath the codec rather than in a codec option, because
// the codec has no option and because CLAUDE.md already records what happens
// when a bound is expressed through a library's own knobs (`plugin/transport.go`:
// `connect.WithReadMaxBytes` bounds a successful response and misses the error
// path). A limit under the library covers every path the library has, including
// the ones it treats specially.
//
// # The number
//
// A `textDocument/didOpen` carries a document as a JSON string. The store
// accepts a document larger than [maxDocumentBytes] on purpose — it reports one
// as a diagnostic rather than refusing it, which is a better answer for an
// author who opened a generated file — and JSON escaping is worst-case six
// bytes out per byte in, a control byte written as a `\uXXXX` escape. Sixteen
// mebibytes therefore leaves room for a maximal document, its escaping, and the
// envelope around it, while sitting far below what an unbounded header line was
// measured to cost. It bounds the pathological case without being reachable by
// an honest editor.
const MaxFrameBytes = 16 << 20

// ErrFrameTooLarge is returned when one frame exceeds [MaxFrameBytes].
//
// It ends the connection rather than skipping the frame, which is the
// fail-closed reading and the only sound one: the reader is mid-frame at a
// position no header told it the length of, so there is no next frame boundary
// left to resynchronize on. An editor reconnects; a peer sending 16 MiB of
// header does not get to keep the connection.
var ErrFrameTooLarge = errors.New("lsp: JSON-RPC frame exceeded the maximum size")

// NewBoundedStream returns the object stream the language server reads and
// writes, with each frame bounded by [MaxFrameBytes].
//
// It is otherwise exactly what [jsonrpc2.NewBufferedStream] with a
// [jsonrpc2.VSCodeObjectCodec] does — the same codec, the same buffering, the
// same one-writer-at-a-time discipline — so that the only difference between
// this and the construction it replaces is the bound.
func NewBoundedStream(conn io.ReadWriteCloser) jsonrpc2.ObjectStream {
	limited := &frameLimitReader{r: conn, limit: MaxFrameBytes}
	return &boundedStream{
		conn:    conn,
		limited: limited,
		r:       bufio.NewReader(limited),
		w:       bufio.NewWriter(conn),
	}
}

// A boundedStream is a [jsonrpc2.ObjectStream] whose reads are bounded per
// frame.
type boundedStream struct {
	conn    io.Closer
	limited *frameLimitReader
	r       *bufio.Reader

	// mu guards the writer. A frame is written and flushed while it is held,
	// so two goroutines replying at once cannot interleave their bytes — the
	// same guarantee jsonrpc2's own buffered stream makes, and one the server
	// depends on because it serves a goroutine per message.
	mu sync.Mutex
	w  *bufio.Writer
}

// ReadObject implements [jsonrpc2.ObjectStream].
//
// The budget is reset per frame rather than per connection: the bound is on how
// large one message may be, not on how long a session may last.
func (s *boundedStream) ReadObject(v any) error {
	s.limited.reset()
	return jsonrpc2.VSCodeObjectCodec{}.ReadObject(s.r, v)
}

// WriteObject implements [jsonrpc2.ObjectStream].
func (s *boundedStream) WriteObject(obj any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := (jsonrpc2.VSCodeObjectCodec{}).WriteObject(s.w, obj); err != nil {
		return err
	}
	return s.w.Flush()
}

// Close implements [jsonrpc2.ObjectStream].
func (s *boundedStream) Close() error { return s.conn.Close() }

// A frameLimitReader fails once more than limit bytes have been read since the
// last reset.
//
// It sits *under* the [bufio.Reader] the codec parses through, which is the
// only position from which it can bound a read the codec performs without
// asking anyone's permission. The consequence is that the count is of bytes
// pulled from the connection rather than of bytes the codec consumed, so a
// frame may be allowed up to one buffer-fill more than the limit — bounding to
// within four kibibytes, which is the whole of what this is for.
type frameLimitReader struct {
	r     io.Reader
	n     int
	limit int
}

// reset starts a new frame's budget.
func (f *frameLimitReader) reset() { f.n = 0 }

// Read implements [io.Reader].
func (f *frameLimitReader) Read(p []byte) (int, error) {
	remaining := f.limit - f.n
	if remaining <= 0 {
		return 0, fmt.Errorf("%w of %d bytes", ErrFrameTooLarge, f.limit)
	}
	// Short the read rather than only checking afterwards, so that the byte
	// that trips the limit is never read into memory at all.
	if len(p) > remaining {
		p = p[:remaining]
	}
	n, err := f.r.Read(p)
	f.n += n
	return n, err
}
