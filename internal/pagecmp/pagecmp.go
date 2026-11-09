// Package pagecmp compares the memory-page content of two CRIU checkpoint
// directories (§5): overlap is computed over dumped anonymous
// pages only — never RSS — because file-backed pages are bytes CRIU never
// sends anyway. This is the measurement tool behind the hit-rate
// experiments; the reported content hit rate is exactly the design's
// "baseline hit rate" = matched pages / dumped pages.
package pagecmp

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/checkpoint-restore/go-criu/v7/crit"
	"github.com/checkpoint-restore/go-criu/v7/crit/images/pagemap"
	"google.golang.org/protobuf/encoding/protowire"
)

// PageSize is CRIU's page granularity on x86-64.
const PageSize = 4096

// Pagemap entry flags (criu/include/pagemap.h).
const (
	peParent  = 1 << 0 // pages are in parent snapshot
	peLazy    = 1 << 1 // pages can be lazily restored
	pePresent = 1 << 2 // pages are present in pages-*.img
)

// pageAddr identifies a page by its owning process (virtual pid inside the
// dump) and virtual address, so address-matched comparison stays meaningful
// if a dump ever contains more than one pagemap.
type pageAddr struct {
	Pid   int
	Vaddr uint64
}

// PageSet is the page content of one checkpoint directory.
type PageSet struct {
	NumPages  int                   // dumped pages present in pages-*.img
	ZeroPages int                   // dumped pages that are all zeroes
	Hashes    map[[32]byte]int      // content hash → occurrence count
	ByAddr    map[pageAddr][32]byte // (pid, vaddr) → content hash
}

var zeroPageHash = sha256.Sum256(make([]byte, PageSize))

func newPageSet() *PageSet {
	return &PageSet{
		Hashes: make(map[[32]byte]int),
		ByAddr: make(map[pageAddr][32]byte),
	}
}

// entryNrPages returns the page count of a pagemap entry. CRIU ≥ 4.2 moved
// nr_pages to proto field 5 (uint64) and writes the legacy required field 2
// (now compat_nr_pages) as 0; go-criu's bundled proto (≤ v7.2.x) still maps
// only field 2, so field 5 lands in the entry's unknown-field bytes. Prefer
// field 5 when present, else field 2 — the inverse of criu's own reader
// compat (criu/pagemap.c: !has_nr_pages → compat_nr_pages).
func entryNrPages(pe *pagemap.PagemapEntry) uint64 {
	b := pe.ProtoReflect().GetUnknown()
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			break
		}
		b = b[n:]
		if num == 5 && typ == protowire.VarintType {
			if v, n := protowire.ConsumeVarint(b); n >= 0 {
				return v
			}
			break
		}
		if n = protowire.ConsumeFieldValue(num, typ, b); n < 0 {
			break
		}
		b = b[n:]
	}
	return uint64(pe.GetNrPages())
}

// add indexes one dumped page.
func (ps *PageSet) add(pid int, vaddr uint64, page []byte) {
	h := sha256.Sum256(page)
	ps.Hashes[h]++
	ps.ByAddr[pageAddr{pid, vaddr}] = h
	ps.NumPages++
	if h == zeroPageHash {
		ps.ZeroPages++
	}
}

// forEachPage reads pages from r in pagemap-entry order and calls fn for each
// with its virtual address (the page buffer is reused between calls).
func forEachPage(entries []*pagemap.PagemapEntry, r io.Reader, fn func(vaddr uint64, page []byte)) error {
	buf := make([]byte, PageSize)
	for _, pe := range entries {
		flags := pe.GetFlags()
		if flags&pePresent == 0 {
			return fmt.Errorf("pagemap entry vaddr=0x%x flags=0x%x has no pages in this dump (PE_PARENT/lazy chains unsupported — compare full dumps)", pe.GetVaddr(), flags)
		}
		nrPages := entryNrPages(pe)
		for i := uint64(0); i < nrPages; i++ {
			if _, err := io.ReadFull(r, buf); err != nil {
				return fmt.Errorf("pages file short: pagemap describes more pages than the file holds: %w", err)
			}
			fn(pe.GetVaddr()+i*PageSize, buf)
		}
	}
	return nil
}

// addEntries consumes pages from r in pagemap-entry order and indexes them.
// Split out from LoadDump so tests can feed synthetic entries + page bytes.
func (ps *PageSet) addEntries(pid int, entries []*pagemap.PagemapEntry, r io.Reader) error {
	return forEachPage(entries, r, func(vaddr uint64, page []byte) { ps.add(pid, vaddr, page) })
}

// walkDump calls fn for every dumped page of every pagemap-*.img / pages-*.img
// pair in a checkpoint directory. pid is the process's pid in the dump, or
// -shmid for a shared-memory segment (pagemap-shmem-<id>.img, e.g. PostgreSQL's
// shared_buffers). Fails on parent-chained (pre-dump) images: full dumps only.
func walkDump(dir string, fn func(pid int, vaddr uint64, page []byte)) error {
	pagemaps, err := filepath.Glob(filepath.Join(dir, "pagemap-*.img"))
	if err != nil {
		return err
	}
	if len(pagemaps) == 0 {
		return fmt.Errorf("no pagemap-*.img in %s — not a checkpoint directory?", dir)
	}
	sort.Strings(pagemaps)

	for _, pmPath := range pagemaps {
		var pid int
		if c, _ := fmt.Sscanf(filepath.Base(pmPath), "pagemap-shmem-%d.img", &pid); c == 1 {
			pid = -pid // keep shm ids apart from process ids in the (pid, vaddr) key
		} else if _, err := fmt.Sscanf(filepath.Base(pmPath), "pagemap-%d.img", &pid); err != nil {
			return fmt.Errorf("unexpected pagemap file name %s: %w", pmPath, err)
		}

		f, err := os.Open(pmPath)
		if err != nil {
			return err
		}
		img, err := crit.New(f, nil, "", false, false).Decode(&pagemap.PagemapEntry{})
		f.Close()
		if err != nil {
			return fmt.Errorf("decoding %s: %w", pmPath, err)
		}
		if len(img.Entries) == 0 {
			return fmt.Errorf("%s: empty pagemap image", pmPath)
		}
		head, ok := img.Entries[0].Message.(*pagemap.PagemapHead)
		if !ok {
			return fmt.Errorf("%s: first entry is %T, want PagemapHead", pmPath, img.Entries[0].Message)
		}
		entries := make([]*pagemap.PagemapEntry, 0, len(img.Entries)-1)
		for _, e := range img.Entries[1:] {
			pe, ok := e.Message.(*pagemap.PagemapEntry)
			if !ok {
				return fmt.Errorf("%s: entry is %T, want PagemapEntry", pmPath, e.Message)
			}
			entries = append(entries, pe)
		}

		pagesPath := filepath.Join(dir, fmt.Sprintf("pages-%d.img", head.GetPagesId()))
		pf, err := os.Open(pagesPath)
		if err != nil {
			return err
		}
		br := bufio.NewReaderSize(pf, 1<<20)
		p := pid
		loadErr := forEachPage(entries, br, func(vaddr uint64, page []byte) { fn(p, vaddr, page) })
		if loadErr == nil {
			// The pagemap must account for the whole pages file.
			if _, err := br.Read(make([]byte, 1)); err != io.EOF {
				loadErr = fmt.Errorf("%s holds more pages than %s describes", pagesPath, pmPath)
			}
		}
		pf.Close()
		if loadErr != nil {
			return loadErr
		}
	}
	return nil
}

// LoadDump indexes every dumped page of a checkpoint directory (see walkDump).
func LoadDump(dir string) (*PageSet, error) {
	ps := newPageSet()
	if err := walkDump(dir, ps.add); err != nil {
		return nil, err
	}
	return ps, nil
}

// Result reports the overlap of dump A against dump B (A = subject, e.g. a
// source-side dump; B = baseline, e.g. a synthesized template).
type Result struct {
	DumpA string `json:"dump_a"`
	DumpB string `json:"dump_b"`

	PagesA     int `json:"pages_a"`      // dumped pages in A (hit-rate denominator)
	PagesB     int `json:"pages_b"`      // dumped pages in B
	UniqueA    int `json:"unique_a"`     // distinct page contents in A
	UniqueB    int `json:"unique_b"`     // distinct page contents in B
	ZeroPagesA int `json:"zero_pages_a"` // all-zero pages in A

	// Content hit: A-page whose content exists anywhere in B — exactly a
	// manifest lookup (position-independent, duplicates each count).
	ContentHits    int     `json:"content_hits"`
	ContentHitRate float64 `json:"content_hit_rate"`

	// Addr hit: A-page whose content matches B's page at the same (pid,
	// vaddr) — diagnostic for how much of the overlap is layout-stable
	// (ASLR-off vs ASLR-on experiments).
	AddrHits    int     `json:"addr_hits"`
	AddrHitRate float64 `json:"addr_hit_rate"`
}

// Compare computes A's overlap with B.
func Compare(a, b *PageSet) *Result {
	res := &Result{
		PagesA:     a.NumPages,
		PagesB:     b.NumPages,
		UniqueA:    len(a.Hashes),
		UniqueB:    len(b.Hashes),
		ZeroPagesA: a.ZeroPages,
	}
	for h, n := range a.Hashes {
		if _, ok := b.Hashes[h]; ok {
			res.ContentHits += n
		}
	}
	for addr, h := range a.ByAddr {
		if bh, ok := b.ByAddr[addr]; ok && bh == h {
			res.AddrHits++
		}
	}
	if a.NumPages > 0 {
		res.ContentHitRate = float64(res.ContentHits) / float64(a.NumPages)
		res.AddrHitRate = float64(res.AddrHits) / float64(a.NumPages)
	}
	return res
}

// CompareDirs loads both checkpoint directories and compares them.
func CompareDirs(dirA, dirB string) (*Result, error) {
	a, err := LoadDump(dirA)
	if err != nil {
		return nil, fmt.Errorf("loading %s: %w", dirA, err)
	}
	b, err := LoadDump(dirB)
	if err != nil {
		return nil, fmt.Errorf("loading %s: %w", dirB, err)
	}
	res := Compare(a, b)
	res.DumpA = dirA
	res.DumpB = dirB
	return res, nil
}
