package transport

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
)

func TestChunkRoundTrip(t *testing.T) {
	for _, size := range []int{0, 1, 4095, 32 << 10, (1 << 20) + 7, 5 << 20} {
		data := make([]byte, size)
		rand.New(rand.NewSource(int64(size))).Read(data)

		var wire bytes.Buffer
		cw := newChunkWriter(&wire)
		// feed in awkward piece sizes, like io.Copy from a pipe would
		for off := 0; off < len(data); {
			n := 1 + rand.Intn(70000)
			if off+n > len(data) {
				n = len(data) - off
			}
			if _, err := cw.Write(data[off : off+n]); err != nil {
				t.Fatal(err)
			}
			off += n
		}
		if err := cw.Close(); err != nil {
			t.Fatal(err)
		}

		got, err := io.ReadAll(newChunkReader(&wire))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("size %d: roundtrip mismatch (err %v, got %d bytes)", size, err, len(got))
		}
	}
}

// A connection that dies before the terminator must be an error, not a clean
// end of body: a truncated checkpoint must never be accepted as complete.
func TestChunkTruncationIsAnError(t *testing.T) {
	var wire bytes.Buffer
	cw := newChunkWriter(&wire)
	_, _ = cw.Write(bytes.Repeat([]byte("a"), 10000))
	_ = cw.Close()
	full := wire.Bytes()

	for _, cut := range []int{2, 4, 100, len(full) - 5, len(full) - 1} {
		_, err := io.ReadAll(newChunkReader(bytes.NewReader(full[:cut])))
		if err == nil {
			t.Errorf("cut at %d of %d: truncated body accepted as complete", cut, len(full))
		}
	}
	if _, err := io.ReadAll(newChunkReader(bytes.NewReader(full))); err != nil {
		t.Errorf("complete body rejected: %v", err)
	}
}
