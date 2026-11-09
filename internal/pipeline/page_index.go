package pipeline

import "fmt"

// PageIndex is an in-memory (composite page address) -> page-bytes lookup over a materialized
// dump directory (Task 3: the residual set's raw bytes
// live only in the source's own retained "./checkpoint/final", never shipped
// at freeze — this is what the source's pull-server and background pusher
// read from). Workloads on this testbed are small enough (single-digit MB of
// dirty pages) that loading every present page into memory once is simpler
// than a seek-based file offset index, and avoids re-opening pages-*.img per
// request during the drain.
type PageIndex struct {
	pages map[uint64][]byte
}

// IndexDumpPages loads every present page in dir (a materialized CRIU dump,
// e.g. "./checkpoint/final") into memory, keyed by composite page address
// (ComposeAddr: pagemap ordinal + vaddr).
func IndexDumpPages(dir string) (*PageIndex, error) {
	ords, err := PagemapOrdinals(dir)
	if err != nil {
		return nil, err
	}
	idx := &PageIndex{pages: make(map[uint64][]byte)}
	err = ForEachPage(dir, func(key int, vaddr uint64, page []byte) error {
		addr, err := ComposeAddr(ords[key], vaddr)
		if err != nil {
			// Not lazy-eligible (vsyscall, ...): the destination never asks for it.
			return nil
		}
		cp := make([]byte, len(page))
		copy(cp, page)
		idx.pages[addr] = cp
		return nil
	})
	if err != nil {
		return nil, err
	}
	return idx, nil
}

// Read returns the nrPages contiguous pages starting at the composite address
// addr (the run stays inside one pagemap: a page-server request never spans tasks).
func (idx *PageIndex) Read(vaddr, nrPages uint64) ([]byte, error) {
	out := make([]byte, 0, nrPages*PageSize)
	for i := uint64(0); i < nrPages; i++ {
		p, ok := idx.pages[vaddr+i*PageSize]
		if !ok {
			return nil, fmt.Errorf("page at vaddr 0x%x not found in dump", vaddr+i*PageSize)
		}
		out = append(out, p...)
	}
	return out, nil
}
