package pagecmp

import (
	"bytes"
	"testing"

	"github.com/checkpoint-restore/go-criu/v7/crit/images/pagemap"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func entry(vaddr uint64, nrPages, flags uint32) *pagemap.PagemapEntry {
	return &pagemap.PagemapEntry{
		Vaddr:   proto.Uint64(vaddr),
		NrPages: proto.Uint32(nrPages),
		Flags:   proto.Uint32(flags),
	}
}

// entry42 mimics a CRIU ≥ 4.2 entry: legacy field 2 written as 0, real count
// in field 5 (unknown to go-criu's proto).
func entry42(vaddr uint64, nrPages uint64, flags uint32) *pagemap.PagemapEntry {
	pe := entry(vaddr, 0, flags)
	unknown := protowire.AppendTag(nil, 5, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, nrPages)
	pe.ProtoReflect().SetUnknown(unknown)
	return pe
}

func TestEntryNrPagesField5(t *testing.T) {
	if got := entryNrPages(entry42(0x1000, 600, pePresent)); got != 600 {
		t.Fatalf("field-5 nr_pages = %d, want 600", got)
	}
	if got := entryNrPages(entry(0x1000, 7, pePresent)); got != 7 {
		t.Fatalf("legacy field-2 nr_pages = %d, want 7", got)
	}
}

// page returns a 4 KiB page filled with b.
func page(b byte) []byte {
	p := make([]byte, PageSize)
	for i := range p {
		p[i] = b
	}
	return p
}

func load(t *testing.T, entries []*pagemap.PagemapEntry, pages ...[]byte) *PageSet {
	t.Helper()
	ps := newPageSet()
	if err := ps.addEntries(1, entries, bytes.NewReader(bytes.Join(pages, nil))); err != nil {
		t.Fatalf("addEntries: %v", err)
	}
	return ps
}

func TestCompareIdentical(t *testing.T) {
	entries := []*pagemap.PagemapEntry{
		entry42(0x1000, 2, pePresent),
		entry(0x5000, 1, pePresent),
	}
	a := load(t, entries, page(1), page(2), page(3))
	b := load(t, entries, page(1), page(2), page(3))

	res := Compare(a, b)
	if res.PagesA != 3 || res.PagesB != 3 {
		t.Fatalf("pages = %d/%d, want 3/3", res.PagesA, res.PagesB)
	}
	if res.ContentHits != 3 || res.ContentHitRate != 1.0 {
		t.Fatalf("content hits = %d (%.2f), want 3 (1.00)", res.ContentHits, res.ContentHitRate)
	}
	if res.AddrHits != 3 || res.AddrHitRate != 1.0 {
		t.Fatalf("addr hits = %d (%.2f), want 3 (1.00)", res.AddrHits, res.AddrHitRate)
	}
}

func TestComparePartialOverlap(t *testing.T) {
	entries := []*pagemap.PagemapEntry{entry(0x1000, 3, pePresent)}
	a := load(t, entries, page(1), page(2), page(9))
	b := load(t, entries, page(1), page(2), page(3))

	res := Compare(a, b)
	if res.ContentHits != 2 {
		t.Fatalf("content hits = %d, want 2", res.ContentHits)
	}
	if res.AddrHits != 2 {
		t.Fatalf("addr hits = %d, want 2", res.AddrHits)
	}
}

// Same content at shifted addresses: content-hit (manifest lookup) is
// position-independent, addr-hit is not — the distinction the ASLR
// experiments measure.
func TestCompareShiftedAddresses(t *testing.T) {
	a := load(t, []*pagemap.PagemapEntry{entry(0x1000, 2, pePresent)}, page(1), page(2))
	b := load(t, []*pagemap.PagemapEntry{entry(0x8000, 2, pePresent)}, page(1), page(2))

	res := Compare(a, b)
	if res.ContentHits != 2 || res.ContentHitRate != 1.0 {
		t.Fatalf("content hits = %d (%.2f), want 2 (1.00)", res.ContentHits, res.ContentHitRate)
	}
	if res.AddrHits != 0 {
		t.Fatalf("addr hits = %d, want 0", res.AddrHits)
	}
}

// Duplicate pages in A each count toward the hit rate — a manifest reference
// is sent per page, so this is the transfer-relevant accounting.
func TestCompareDuplicatesCountPerPage(t *testing.T) {
	a := load(t, []*pagemap.PagemapEntry{entry(0x1000, 3, pePresent)}, page(7), page(7), page(7))
	b := load(t, []*pagemap.PagemapEntry{entry(0x1000, 1, pePresent)}, page(7))

	res := Compare(a, b)
	if res.ContentHits != 3 {
		t.Fatalf("content hits = %d, want 3", res.ContentHits)
	}
	if res.UniqueA != 1 {
		t.Fatalf("unique_a = %d, want 1", res.UniqueA)
	}
}

func TestZeroPageCount(t *testing.T) {
	a := load(t, []*pagemap.PagemapEntry{entry(0x1000, 2, pePresent)}, page(0), page(4))
	if a.ZeroPages != 1 {
		t.Fatalf("zero pages = %d, want 1", a.ZeroPages)
	}
}

func TestParentEntryRejected(t *testing.T) {
	ps := newPageSet()
	err := ps.addEntries(1, []*pagemap.PagemapEntry{entry(0x1000, 1, peParent)}, bytes.NewReader(nil))
	if err == nil {
		t.Fatal("expected error for PE_PARENT entry, got nil")
	}
}

func TestShortPagesFileRejected(t *testing.T) {
	ps := newPageSet()
	err := ps.addEntries(1, []*pagemap.PagemapEntry{entry(0x1000, 2, pePresent)}, bytes.NewReader(page(1)))
	if err == nil {
		t.Fatal("expected error for short pages file, got nil")
	}
}
