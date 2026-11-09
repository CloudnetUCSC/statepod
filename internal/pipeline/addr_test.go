package pipeline

import "testing"

func TestComposeSplitAddr(t *testing.T) {
	for _, c := range []struct {
		ord   int
		vaddr uint64
	}{{0, 0x1000}, {5, 0x7ffd_ffff_f000}, {65535, 0x7fff_ffff_f000}} {
		a, err := ComposeAddr(c.ord, c.vaddr)
		if err != nil {
			t.Fatal(err)
		}
		o, v := SplitAddr(a)
		if o != c.ord || v != c.vaddr {
			t.Fatalf("round trip (%d, 0x%x) -> (%d, 0x%x)", c.ord, c.vaddr, o, v)
		}
	}
	// the same vaddr in two tasks must be two different identities
	a0, _ := ComposeAddr(0, 0x1000)
	a1, _ := ComposeAddr(1, 0x1000)
	if a0 == a1 {
		t.Fatal("same vaddr in different pagemaps collided")
	}
	if _, err := ComposeAddr(0, 0xffffffffff600000); err == nil {
		t.Fatal("a vsyscall address must be rejected")
	}
	if _, err := ComposeAddr(maxPagemapCount, 0x1000); err == nil {
		t.Fatal("ordinal overflow must be rejected")
	}
}

func TestDecodePageServerDst(t *testing.T) {
	// the lazy-pages daemon names a task by its plain pid
	if k, err := DecodePageServerDst(234); err != nil || k != 234 {
		t.Fatalf("pid 234: %d %v", k, err)
	}
	if _, err := DecodePageServerDst(0); err == nil {
		t.Fatal("dst_id 0 must be an error, never a guess")
	}
}
