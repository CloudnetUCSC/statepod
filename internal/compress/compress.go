// Package compress provides the optional zstd stage that sits after the
// delta encoder and before the transport : the source may
// compress a delta directory's tar stream before it goes on the wire, and the
// destination transparently decompresses it.
package compress

import (
	"bufio"
	"io"

	"github.com/klauspost/compress/zstd"
)

// zstdMagic is the 4-byte frame magic every zstd stream starts with
// (RFC 8878 §3.1.1). Sniffing it lets the destination stay agnostic to the
// source's --zstd-level setting: a plain tar stream (level 0, or any
// non-synth-precopy mode) passes through untouched.
var zstdMagic = [4]byte{0x28, 0xB5, 0x2F, 0xFD}

// Level maps a nominal zstd level (as used for the 1/3/9 benchmark) onto
// klauspost/compress's speed-tier enum, which does not expose the full 1-22
// range the reference zstd CLI does.
func Level(level int) zstd.EncoderLevel {
	switch {
	case level <= 1:
		return zstd.SpeedFastest
	case level <= 3:
		return zstd.SpeedDefault
	default:
		return zstd.SpeedBetterCompression
	}
}

// Compress wraps r so that reading the result yields r's bytes zstd-compressed
// at the given level. level <= 0 disables compression: r is returned
// unchanged, byte for byte, so the toggle is a no-op rather than a
// zero-length zstd frame.
func Compress(r io.Reader, level int) io.Reader {
	if level <= 0 {
		return r
	}
	pr, pw := io.Pipe()
	go func() {
		enc, err := zstd.NewWriter(pw, zstd.WithEncoderLevel(Level(level)))
		if err != nil {
			pw.CloseWithError(err)
			return
		}
		if _, err := io.Copy(enc, r); err != nil {
			enc.Close()
			pw.CloseWithError(err)
			return
		}
		pw.CloseWithError(enc.Close())
	}()
	return pr
}

// DecompressAuto peeks the first 4 bytes of r and, if they match the zstd
// frame magic, returns a reader that transparently decompresses; otherwise r
// is returned with those bytes intact (nothing consumed as far as the
// caller can tell). The returned reader should be closed when the caller is
// done reading from it to release the decoder's resources.
func DecompressAuto(r io.Reader) (io.ReadCloser, error) {
	br := bufio.NewReaderSize(r, 4096)
	magic, err := br.Peek(4)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(magic) == 4 && magic[0] == zstdMagic[0] && magic[1] == zstdMagic[1] && magic[2] == zstdMagic[2] && magic[3] == zstdMagic[3] {
		dec, err := zstd.NewReader(br)
		if err != nil {
			return nil, err
		}
		return decoderCloser{dec}, nil
	}
	return io.NopCloser(br), nil
}

type decoderCloser struct{ d *zstd.Decoder }

func (d decoderCloser) Read(p []byte) (int, error) { return d.d.Read(p) }
func (d decoderCloser) Close() error               { d.d.Close(); return nil }
