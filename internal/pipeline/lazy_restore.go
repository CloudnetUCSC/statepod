package pipeline

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/checkpoint-restore/go-criu/v7/crit"
	"github.com/checkpoint-restore/go-criu/v7/crit/images/pagemap"
	"google.golang.org/protobuf/proto"
)

// ParentLinkName is CRIU's own convention for the parent-chain symlink
// (criu/include/image.h: CR_PARENT_LINK "parent").
const ParentLinkName = "parent"

// LazyRestoreStats reports the destination-side classification when building
// a lazy-restore images dir.
type LazyRestoreStats struct {
	TotalPages int
	Parent     int // resolved locally via in_parent (address+content match)
	Lazy       int // deferred to on-fault WAN pull (residual set, Task 3 drains it)
	Eager      int // not lazy-eligible (vdso/vvar/...): real bytes present now, plain PE_PRESENT
	// LazyVaddrs is the exact drain set: every vaddr classified Lazy
	// above, in no particular order. The destination hands this list back to
	// the source as the pending set the background pusher walks and the
	// pull-fault path (and the drain-complete check) tracks against.
	//
	// The entries are composite page addresses (ComposeAddr: pagemap ordinal + vaddr),
	// not bare vaddrs: see addr.go. PidOrdinal maps each pagemap key to its ordinal so
	// the page server can turn CRIU's (dst_id, vaddr) into the same identity.
	LazyVaddrs []uint64
	PidOrdinal map[int]int
}

// BuildLazyRestoreDir turns a received residual directory (Task 1's
// EncodeResidualMap output) into a CRIU restore-ready images dir for
// `runc restore --lazy-pages`: metadata .imgs are copied through, a `parent`
// symlink (relative — CRIU's own image.c:752 warns absolute paths may not
// resolve at restore, confirmed live in the spike) points at the local
// baseline dump, and each pagemap-<id>.img is rewritten page-by-page:
//
//   - PE_PRESENT (real bytes, no parent, no lazy) when Task 1 shipped the
//     page eagerly because it isn't in a lazy-eligible VMA (vdso/vvar/
//     vsyscall/stack/locked/hugetlb — CRIU never registers these for
//     userfaultfd at all, confirmed live: marking one PE_LAZY crashed
//     restore with "Invalid ELF magic" on a vdso page).
//   - PE_PARENT (0 bytes) when the baseline has a page at the SAME vaddr with
//     the SAME content hash (— a manifest content-hash hit alone is
//     not sufficient, since CRIU's in_parent lookup is address-only and does
//     no content check of its own).
//   - PE_PRESENT|PE_LAZY (0 bytes) otherwise. CRIU's restore premap phase
//     skips reading these (criu/mem.c: `if (opts.lazy_pages &&
//     pagemap_lazy(pr->pe))` skip-reads and counts them lazy) and the
//     lazy-pages daemon later registers exactly these entries with
//     userfaultfd (criu/uffd.c collect_iovs: `if (!pagemap_lazy(pr->pe))
//     continue`), fetching content on first fault via the fake page server
//     (internal/pageserver) from the still-frozen source.
func BuildLazyRestoreDir(residualDir, baselineDir, outDir string, baseline *Baseline) (*LazyRestoreStats, error) {
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return nil, err
	}
	if err := copyResidualMeta(residualDir, outDir); err != nil {
		return nil, err
	}
	if err := linkParent(outDir, baselineDir); err != nil {
		return nil, err
	}

	pms, err := ReadPagemaps(residualDir)
	if err != nil {
		return nil, err
	}

	stats := &LazyRestoreStats{PidOrdinal: make(map[int]int, len(pms))}
	for ord, pm := range pms {
		stats.PidOrdinal[pm.Pid] = ord
		s, err := buildLazyPagemap(residualDir, outDir, pm, ord, baseline)
		if err != nil {
			return nil, err
		}
		stats.TotalPages += s.TotalPages
		stats.Parent += s.Parent
		stats.Lazy += s.Lazy
		stats.Eager += s.Eager
		stats.LazyVaddrs = append(stats.LazyVaddrs, s.LazyVaddrs...)
	}
	return stats, nil
}

// residualVerdict is one page's classification as shipped by Task 1's
// EncodeResidualMap: a manifest content-hash hit (with the hash, so the
// destination can apply the address gate), a lazy-eligible miss, or a
// non-lazy-eligible page shipped eagerly with its real bytes (vdso/vvar/
// vsyscall/stack/...; see residual.go's package doc).
type residualVerdict struct {
	Hit  bool
	Hash PageHash
	Raw  []byte // non-nil only for rtagRaw (eager) pages
}

// readResidualVerdicts parses a pages-<pagesID>.residual file written by
// encodeResidualPages, in the same per-page order the pagemap describes.
func readResidualVerdicts(dir string, pagesID uint32, wantPages uint64) ([]residualVerdict, error) {
	path := filepath.Join(dir, fmt.Sprintf("pages-%d.residual", pagesID))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)

	var magic [4]byte
	if _, err := io.ReadFull(br, magic[:]); err != nil {
		return nil, fmt.Errorf("%s: reading magic: %w", path, err)
	}
	if magic != residualMagic {
		return nil, fmt.Errorf("%s: bad residual magic %v", path, magic)
	}
	ver, err := br.ReadByte()
	if err != nil {
		return nil, err
	}
	if ver != ResidualVersion {
		return nil, fmt.Errorf("%s: residual version %d unsupported (want %d)", path, ver, ResidualVersion)
	}
	var hdr [12]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return nil, err
	}
	numPages := getU64(hdr[4:])
	if numPages != wantPages {
		return nil, fmt.Errorf("%s: header says %d pages, pagemap describes %d", path, numPages, wantPages)
	}

	out := make([]residualVerdict, numPages)
	for i := range out {
		tag, err := br.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("%s: reading tag %d/%d: %w", path, i, numPages, err)
		}
		switch tag {
		case rtagHit:
			var h PageHash
			if _, err := io.ReadFull(br, h[:]); err != nil {
				return nil, err
			}
			out[i] = residualVerdict{Hit: true, Hash: h}
		case rtagMiss:
			out[i] = residualVerdict{Hit: false}
		case rtagRaw:
			raw := make([]byte, PageSize)
			if _, err := io.ReadFull(br, raw); err != nil {
				return nil, err
			}
			out[i] = residualVerdict{Raw: raw}
		default:
			return nil, fmt.Errorf("%s: bad tag %d at page %d/%d", path, tag, i, numPages)
		}
	}
	return out, nil
}

// buildLazyPagemap writes the rewritten pagemap-<pid>.img + an empty
// pages-<pagesID>.img for one pagemap of the residual dir.
func buildLazyPagemap(residualDir, outDir string, pm Pagemap, ordinal int, baseline *Baseline) (*LazyRestoreStats, error) {
	verdicts, err := readResidualVerdicts(residualDir, pm.PagesID, pm.TotalPages)
	if err != nil {
		return nil, err
	}

	img := &crit.CriuImage{
		Magic:   "PAGEMAP",
		Entries: []*crit.CriuEntry{{Message: &pagemap.PagemapHead{PagesId: proto.Uint32(pm.PagesID)}}},
	}

	pf, err := os.Create(pagesPath(outDir, pm.PagesID))
	if err != nil {
		return nil, err
	}
	defer pf.Close()
	bw := bufio.NewWriterSize(pf, 1<<20)

	// Emitting one pagemap entry per page sent a real `criu lazy-pages`
	// restore into a multi-GB logging spiral in the VM test —
	// some O(n²) path in CRIU's own IOV bookkeeping that a real dump never
	// exercises, because real dumps naturally merge contiguous same-flag
	// pages into one entry. Mirror that shape: accumulate a run of
	// contiguous pages with identical flags and flush it as a single entry
	// only when the classification changes.
	var run struct {
		open    bool
		start   uint64
		nrPages uint64
		flags   uint32
	}
	flush := func() {
		if !run.open {
			return
		}
		pe := &pagemap.PagemapEntry{
			Vaddr:   proto.Uint64(run.start),
			NrPages: proto.Uint32(uint32(run.nrPages)),
			Flags:   proto.Uint32(run.flags),
		}
		img.Entries = append(img.Entries, &crit.CriuEntry{Message: pe})
		run.open = false
	}
	extend := func(vaddr uint64, flags uint32) {
		if run.open && run.flags == flags && run.start+run.nrPages*PageSize == vaddr {
			run.nrPages++
			return
		}
		flush()
		run.open, run.start, run.nrPages, run.flags = true, vaddr, 1, flags
	}

	stats := &LazyRestoreStats{}
	vi := 0
	for _, e := range pm.Entries {
		if !e.Present {
			// Not expected for a plain final dump (Task 1's source is
			// always a single freeze, not a chain) — preserved as-is for
			// safety rather than silently dropped. Flush first: this entry
			// is not part of any page run being merged.
			flush()
			pe := &pagemap.PagemapEntry{
				Vaddr:   proto.Uint64(e.Vaddr),
				NrPages: proto.Uint32(uint32(e.NrPages)),
				Flags:   proto.Uint32(e.Flags),
			}
			img.Entries = append(img.Entries, &crit.CriuEntry{Message: pe})
			continue
		}
		for i := uint64(0); i < e.NrPages; i++ {
			vaddr := e.Vaddr + i*PageSize
			v := verdicts[vi]
			vi++
			stats.TotalPages++

			var flags uint32
			switch {
			case v.Raw != nil:
				// Not lazy-eligible (vdso/vvar/vsyscall/stack/...): real
				// bytes are already in hand, restore reads them normally.
				flags = pePresent
				stats.Eager++
				if _, err := bw.Write(v.Raw); err != nil {
					return nil, err
				}
			case v.Hit:
				if baseHash, ok := baseline.HashAtVaddr(pm.Pid, vaddr); ok && baseHash == v.Hash {
					flags = peParent
					stats.Parent++
				} else {
					// Content-hash hit, but CRIU's in_parent lookup is
					// address-only and failed it - fall back to
					// lazy (this page is known lazy-eligible, or Task 1
					// would have shipped it as rtagRaw instead). Real bytes
					// come from the frozen source on fault, not
					// from anything shipped here.
					//
					// PE_LAZY alone, NOT PE_PRESENT|PE_LAZY: real CRIU only
					// sets PE_PRESENT alongside PE_LAZY when the dumper
					// actually transferred the page's bytes into the image
					// (page-xfer.c: ppb_xfer_flags, `xfer->transfer_lazy`).
					// We never do - the bytes aren't here, they're fetched
					// on fault - so claiming PE_PRESENT caused a real
					// restore hang (multi-GB log spiral: some read path
					// expected bytes this pages file doesn't have and
					// looped retrying a 0-byte read forever).
					flags = peLazy
					stats.Lazy++
					addr, err := ComposeAddr(ordinal, vaddr)
					if err != nil {
						return nil, fmt.Errorf("lazy page in pagemap %d: %w", pm.Pid, err)
					}
					stats.LazyVaddrs = append(stats.LazyVaddrs, addr)
				}
			default:
				flags = peLazy
				stats.Lazy++
				addr, err := ComposeAddr(ordinal, vaddr)
				if err != nil {
					return nil, fmt.Errorf("lazy page in pagemap %d: %w", pm.Pid, err)
				}
				stats.LazyVaddrs = append(stats.LazyVaddrs, addr)
			}

			extend(vaddr, flags)
		}
	}
	flush()

	pmFile, err := os.Create(filepath.Join(outDir, PagemapFileName(pm.Pid)))
	if err != nil {
		return nil, err
	}
	defer pmFile.Close()
	if err := crit.New(nil, pmFile, "", false, false).Encode(img); err != nil {
		return nil, err
	}

	return stats, bw.Flush()
}

// copyResidualMeta copies everything from residualDir into outDir except the
// files BuildLazyRestoreDir rewrites or doesn't need: pagemap-*.img (rewritten
// by buildLazyPagemap), pages-*.residual, and the residual meta sidecar.
func copyResidualMeta(residualDir, outDir string) error {
	entries, err := os.ReadDir(residualDir)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		if ent.IsDir() || ent.Type()&os.ModeSymlink != 0 {
			continue
		}
		name := ent.Name()
		if name == ResidualMetaName {
			continue
		}
		var id uint32
		if _, ok := PagemapKey(name); ok {
			continue
		}
		if n, _ := fmt.Sscanf(name, "pages-%d.residual", &id); n == 1 {
			continue
		}
		if err := copyFile(filepath.Join(residualDir, name), filepath.Join(outDir, name)); err != nil {
			return err
		}
	}
	return nil
}

// linkParent creates CRIU's `parent` convention symlink in outDir pointing at
// baselineDir, using a path relative to outDir — an absolute path is silently
// unreliable at restore time (criu/image.c:752-754; reproduced live in the
// spike before this fix).
func linkParent(outDir, baselineDir string) error {
	rel, err := filepath.Rel(outDir, baselineDir)
	if err != nil {
		return err
	}
	return os.Symlink(rel, filepath.Join(outDir, ParentLinkName))
}
