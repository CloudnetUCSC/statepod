package pipeline

import "testing"

// The M5 in_parent gate must be per task: CRIU resolves an in_parent page in the
// parent dump's pagemap of the SAME task, so a page at the same address in a
// different process (or in a task the baseline never had) must not pass.
func TestHashAtVaddrIsPerPagemapKey(t *testing.T) {
	var h1, h2 PageHash
	h1[0], h2[0] = 1, 2
	b := &Baseline{byVaddr: map[int]map[uint64]PageHash{
		1:  {0x1000: h1},
		7:  {0x1000: h2},
		-3: {0x2000: h1}, // a shared-memory segment's pagemap
	}}

	if h, ok := b.HashAtVaddr(1, 0x1000); !ok || h != h1 {
		t.Fatalf("pid 1 @0x1000: got %v %v, want h1", h, ok)
	}
	if h, ok := b.HashAtVaddr(7, 0x1000); !ok || h != h2 {
		t.Fatalf("pid 7 @0x1000 must be its own page (h2), got %v %v", h, ok)
	}
	if _, ok := b.HashAtVaddr(261, 0x1000); ok {
		t.Fatal("a pid the baseline never had must not pass the gate (CRIU: \"No parent for snapshot pagemap\")")
	}
	if _, ok := b.HashAtVaddr(1, 0x2000); ok {
		t.Fatal("pid 1 has no page at 0x2000 even though shm -3 does")
	}
	if _, ok := b.HashAtVaddr(-3, 0x2000); !ok {
		t.Fatal("the shared-memory pagemap key must resolve its own pages")
	}
}
