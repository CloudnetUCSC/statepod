package pipeline

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// residualMagic tags an M5 residual-map stream: the
// tiny cutover payload that lets the destination build a lazy-restore images
// dir without most raw page bytes crossing the wire at freeze time.
//
// Unlike EncodeDump (M1's precopy delta), this writes raw page bytes only for
// the rare VMAs that can never be lazily restored — a hit page in a
// lazy-eligible VMA carries only its 16 B hash so the destination can apply
// its own address+content gate (content-hash membership alone is
// necessary but not sufficient to mark a page in_parent, since CRIU's own
// in_parent lookup is address-only with no content check of its own); a miss
// page in a lazy-eligible VMA carries nothing at all, fetched later by the
// hybrid push/pull drain. But CRIU will never register vdso/vvar/
// vsyscall/stack/locked/hugetlb pages for userfaultfd at all (criu's own
// vma_entry_can_be_lazy, vma.h:123) — found live in the VM
// test, where marking a vdso page PE_LAZY crashed restore ("Invalid ELF
// magic") because it can never be filled in after the fact. So any page in
// such a VMA ships its real bytes now, unconditionally, regardless of hit or
// miss — there is no "later" for it. These VMAs are tiny and fixed-size
// (typically a handful of pages total), so this doesn't reintroduce the
// state-size dependence M5 exists to remove.
var residualMagic = [4]byte{'S', 'P', 'R', 'M'}

// ResidualVersion is the residual-map wire-format version.
const ResidualVersion = 2

// Page record tags in a .residual stream.
const (
	rtagHit  byte = 0 // 16-byte manifest hash follows (lazy-eligible VMA)
	rtagMiss byte = 1 // no payload — residual page, fetched later
	rtagRaw  byte = 2 // 4096-byte raw page follows — not lazy-eligible, no deferring possible
)

// ResidualMetaName is the sidecar describing a residual directory.
const ResidualMetaName = "residual.meta.json"

// ResidualMeta is the residual directory's manifest-of-record. Unlike
// DeltaMeta (M1), there is no whole-image checksum here: M5 has no raw page
// bytes at freeze to checksum against, so image integrity for the residual set
// is verified once the drain completes, not at cutover.
type ResidualMeta struct {
	Version        int    `json:"version"`
	ImageDigest    string `json:"image_digest"`
	BaselineDumpID string `json:"baseline_dump_id"`
	TotalPages     int    `json:"total_pages"`
	Hits           int    `json:"hits"`
	Misses         int    `json:"misses"`
	Eager          int    `json:"eager"`
}

// ResidualStats reports the freeze-time classification.
type ResidualStats struct {
	TotalPages int
	Hits       int // lazy-eligible manifest content-hash hits; carried as a 16 B ref only
	Misses     int // lazy-eligible residual set: not shipped now, drained later
	Eager      int // not lazy-eligible (vdso/vvar/vsyscall/stack/...): raw bytes shipped now
}

// HitRate is matched pages / dumped pages (baseline hit rate).
func (s *ResidualStats) HitRate() float64 {
	if s.TotalPages == 0 {
		return 0
	}
	return float64(s.Hits) / float64(s.TotalPages)
}

// EncodeResidualMap builds the M5 cutover payload in outDir from the source
// dump in srcDir: metadata .img files (including pagemap-*.img — the
// destination needs the original page layout to build its restore images dir
// in Task 2) are copied verbatim, and each pages-<id>.img is replaced by a
// pages-<id>.residual recording each page's classification against manifest m
// — nothing else. No raw page bytes are written here.
func EncodeResidualMap(srcDir, outDir string, m *Manifest) (*ResidualStats, error) {
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return nil, err
	}
	if err := copyMetaImages(srcDir, outDir); err != nil {
		return nil, err
	}

	pms, err := ReadPagemaps(srcDir)
	if err != nil {
		return nil, err
	}

	stats := &ResidualStats{}
	for _, pm := range pms {
		// A shared-memory segment's pagemap (negative key, e.g. PostgreSQL's
		// shared_buffers) belongs to no single process, so there is no mm image to
		// read, and its pages are MAP_SHARED, which CRIU cannot restore lazily: with
		// no VMAs every page is lazy-ineligible, i.e. shipped eagerly at cutover.
		// (M5's benefit therefore excludes shared memory: a stated limitation.)
		var vmas []vmaRange
		if pm.Pid >= 0 {
			var err error
			vmas, err = readVMAs(srcDir, pm.Pid)
			if err != nil {
				return nil, fmt.Errorf("reading VMAs for pid %d: %w", pm.Pid, err)
			}
		}
		s, err := encodeResidualPages(srcDir, outDir, pm, m, vmas)
		if err != nil {
			return nil, err
		}
		stats.TotalPages += s.TotalPages
		stats.Hits += s.Hits
		stats.Misses += s.Misses
		stats.Eager += s.Eager
	}

	meta := ResidualMeta{
		Version:        ResidualVersion,
		ImageDigest:    m.ImageDigest,
		BaselineDumpID: m.BaselineDumpID,
		TotalPages:     stats.TotalPages,
		Hits:           stats.Hits,
		Misses:         stats.Misses,
		Eager:          stats.Eager,
	}
	metaData, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.WriteFile(filepath.Join(outDir, ResidualMetaName), metaData, 0644); err != nil {
		return nil, err
	}
	return stats, nil
}

// writeResidualHeader writes the same 13-byte layout as writeDeltaHeader
// (version[1] pagesID[4] numPages[8]) but with ResidualVersion, not
// DeltaVersion — the two formats are versioned independently.
func writeResidualHeader(w *bufio.Writer, pagesID uint32, numPages uint64) error {
	if err := w.WriteByte(ResidualVersion); err != nil {
		return err
	}
	var b [12]byte
	putU32(b[0:], pagesID)
	putU64(b[4:], numPages)
	_, err := w.Write(b[:])
	return err
}

// encodeResidualPages writes one pages-<id>.residual for pm's present pages.
func encodeResidualPages(srcDir, outDir string, pm Pagemap, m *Manifest, vmas []vmaRange) (*ResidualStats, error) {
	pp := pagesPath(srcDir, pm.PagesID)
	var br *bufio.Reader
	if pm.TotalPages > 0 {
		in, err := os.Open(pp)
		if err != nil {
			return nil, err
		}
		defer in.Close()
		br = bufio.NewReaderSize(in, 1<<20)
	} else {
		br = bufio.NewReader(bytes.NewReader(nil))
	}

	outPath := filepath.Join(outDir, fmt.Sprintf("pages-%d.residual", pm.PagesID))
	out, err := os.Create(outPath)
	if err != nil {
		return nil, err
	}
	defer out.Close()
	bw := bufio.NewWriterSize(out, 1<<20)

	if _, err := bw.Write(residualMagic[:]); err != nil {
		return nil, err
	}
	if err := writeResidualHeader(bw, pm.PagesID, pm.TotalPages); err != nil {
		return nil, err
	}

	stats := &ResidualStats{}
	page := make([]byte, PageSize)
	for _, e := range pm.Entries {
		// PE_PARENT entries (a chained round's clean set) cost nothing here
		// either — same reasoning as EncodeDump.
		if !e.Present {
			continue
		}
		for i := uint64(0); i < e.NrPages; i++ {
			if _, err := io.ReadFull(br, page); err != nil {
				return nil, fmt.Errorf("%s short: pagemap describes more pages than the file holds: %w", pp, err)
			}
			stats.TotalPages++
			vaddr := e.Vaddr + i*PageSize

			if !lazyEligible(vmas, vaddr) {
				// No "later" for this page (vdso/vvar/vsyscall/stack/...) -
				// ship it now regardless of hit/miss so Task 2 never needs a
				// fallback it doesn't have.
				stats.Eager++
				if err := bw.WriteByte(rtagRaw); err != nil {
					return nil, err
				}
				if _, err := bw.Write(page); err != nil {
					return nil, err
				}
				continue
			}

			h := HashPage(page)
			if m.Has(h) {
				stats.Hits++
				if err := bw.WriteByte(rtagHit); err != nil {
					return nil, err
				}
				if _, err := bw.Write(h[:]); err != nil {
					return nil, err
				}
			} else {
				stats.Misses++
				if err := bw.WriteByte(rtagMiss); err != nil {
					return nil, err
				}
			}
		}
	}
	if _, err := br.Read(make([]byte, 1)); err != io.EOF {
		return nil, fmt.Errorf("%s holds more pages than %s describes", pp, pm.Path)
	}
	return stats, bw.Flush()
}
