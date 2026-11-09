package transport

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Chunk framing for the BODY of a stream or request.
//
// The original protocol marked the end of a body by half-closing the TCP
// connection (client CloseWrite -> server reads EOF) and then waited for the
// server's reply on the same connection. That works on a clean path but not
// behind QEMU's user-mode NAT, which -- measured 2026-10-04 with a packet
// capture inside the guest -- stops forwarding host-to-guest data beyond the
// first segment once the guest has half-closed (a 4.7 KB response arrived as
// 1,440 bytes and then nothing for 15 minutes). Many NATs and firewalls handle
// half-closed connections badly, so the transport no longer uses half-close at
// all: the body is a sequence of [4-byte big-endian length][data] chunks ended
// by a zero-length chunk, and the sender then simply waits for the reply.
const maxChunk = 1 << 20

// chunkWriter frames everything written to it. Close writes the terminator;
// it does NOT close the underlying connection.
type chunkWriter struct {
	w   io.Writer
	buf []byte
}

func newChunkWriter(w io.Writer) *chunkWriter { return &chunkWriter{w: w} }

func (c *chunkWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n := len(p)
		if n > maxChunk {
			n = maxChunk
		}
		if cap(c.buf) < 4+n {
			c.buf = make([]byte, 4+n)
		}
		b := c.buf[:4+n]
		binary.BigEndian.PutUint32(b[:4], uint32(n))
		copy(b[4:], p[:n])
		if _, err := c.w.Write(b); err != nil {
			return total, err
		}
		total += n
		p = p[n:]
	}
	return total, nil
}

// Close writes the zero-length terminator chunk.
func (c *chunkWriter) Close() error {
	var z [4]byte
	_, err := c.w.Write(z[:])
	return err
}

// chunkReader is the inverse: it yields the de-framed bytes and io.EOF at the
// terminator. An EOF on the underlying connection before the terminator is an
// error (io.ErrUnexpectedEOF), never a clean end of body.
type chunkReader struct {
	r      io.Reader
	remain uint32
	done   bool
}

func newChunkReader(r io.Reader) *chunkReader { return &chunkReader{r: r} }

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.done {
		return 0, io.EOF
	}
	if c.remain == 0 {
		var h [4]byte
		if _, err := io.ReadFull(c.r, h[:]); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return 0, fmt.Errorf("reading chunk header: %w", err)
		}
		c.remain = binary.BigEndian.Uint32(h[:])
		if c.remain == 0 {
			c.done = true
			return 0, io.EOF
		}
		if c.remain > maxChunk {
			return 0, fmt.Errorf("chunk of %d bytes exceeds the %d byte limit", c.remain, maxChunk)
		}
	}
	if uint32(len(p)) > c.remain {
		p = p[:c.remain]
	}
	n, err := c.r.Read(p)
	c.remain -= uint32(n)
	if err == io.EOF && c.remain > 0 {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}
