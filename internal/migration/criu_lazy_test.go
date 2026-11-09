package migration

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// TestSpliceBothWaysAndCounts: bytes flow in both directions through the splice,
// are counted per direction, and the splice finishes (closing both ends) as soon
// as one side hangs up.
func TestSpliceBothWaysAndCounts(t *testing.T) {
	// a1 <-> a2 is one pipe, b1 <-> b2 another; splice(a2, b1) joins them.
	a1, a2 := net.Pipe()
	b1, b2 := net.Pipe()
	var fromA, toA atomic.Int64
	done := splice(a2, b1, &fromA, &toA)

	go func() { _, _ = a1.Write([]byte("pages-from-A")) }()
	buf := make([]byte, len("pages-from-A"))
	if _, err := io.ReadFull(b2, buf); err != nil || string(buf) != "pages-from-A" {
		t.Fatalf("A->B: got %q, %v", buf, err)
	}
	go func() { _, _ = b2.Write([]byte("req")) }()
	buf = make([]byte, 3)
	if _, err := io.ReadFull(a1, buf); err != nil || string(buf) != "req" {
		t.Fatalf("B->A: got %q, %v", buf, err)
	}

	a1.Close() // one end hangs up: the whole splice must wind down
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("splice did not finish after one side closed")
	}
	if fromA.Load() != int64(len("pages-from-A")) || toA.Load() != 3 {
		t.Fatalf("counts: fromA=%d toA=%d, want %d and 3", fromA.Load(), toA.Load(), len("pages-from-A"))
	}
	if _, err := b2.Write([]byte("x")); err == nil {
		t.Fatal("the far end should have been closed by the splice")
	}
}
