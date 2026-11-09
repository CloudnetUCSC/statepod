package pipeline

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/zeebo/blake3"
)

// deltaMagic tags a per-pages delta stream ("StatePod Delta Pages").
var deltaMagic = [4]byte{'S', 'P', 'D', 'P'}

// DeltaVersion is the delta wire-format version.
const DeltaVersion = 1

// Page record tags in a .delta stream.
const (
	tagRef byte = 0 // 16-byte manifest hash: page is in the destination baseline
	tagRaw byte = 1 // 4096 raw bytes: page is a manifest miss, sent verbatim
)

// DeltaMetaName is the sidecar describing a delta directory.
const DeltaMetaName = "delta.meta.json"

// DeltaMeta is the delta directory's manifest-of-record: what baseline the
// delta was cut against and the whole-image integrity checksum verified before
// restore (§3.3).
type DeltaMeta struct {
	Version        int    `json:"version"`
	ImageDigest    string `json:"image_digest"`
	BaselineDumpID string `json:"baseline_dump_id"`
	WholeChecksum  string `json:"whole_checksum"` // blake3-128 hex over all pages in canonical order
	TotalPages     int    `json:"total_pages"`
	Hits           int    `json:"hits"`
	Misses         int    `json:"misses"`
}

// EncodeStats reports the reconciliation outcome for metrics.
type EncodeStats struct {
	TotalPages    int
	Hits          int   // pages resolved to a 16 B baseline ref
	Misses        int   // pages sent raw
	RefBytes      int64 // bytes of refs on the wire (Hits * (1+HashLen))
	RawBytes      int64 // bytes of raw pages on the wire (Misses * (1+PageSize))
	WholeChecksum string
}

// HitRate is matched pages / dumped pages (baseline hit rate).
func (s *EncodeStats) HitRate() float64 {
	if s.TotalPages == 0 {
		return 0
	}
	return float64(s.Hits) / float64(s.TotalPages)
}

// EncodeOptions carries the optional M2 critical-path hook.
type EncodeOptions struct {
	// PageHasher, if non-nil, supplies the hash for a page. It is the M2 seam:
	// for a clean page it returns the source's precomputed self-baseline hash
	// (zero hashing on the critical path); for a dirty page it hashes now.
	// nil → every page is hashed here (M1-only path, correct but no M2 saving).
	PageHasher func(pid int, vaddr uint64, page []byte) PageHash
}

// EncodeDump reconciles the source dump in srcDir against manifest m and writes
// a delta directory to outDir (§3–4): all metadata .img files are
// copied verbatim, and each pages-<id>.img is replaced by a pages-<id>.delta of
// per-page records — a 16 B ref for a manifest hit, the raw 4 KiB page for a
// miss. A whole-image checksum is recorded for pre-restore verification.
func EncodeDump(srcDir, outDir string, m *Manifest, opts EncodeOptions) (*EncodeStats, error) {
	hasher := opts.PageHasher
	if hasher == nil {
		hasher = func(_ int, _ uint64, p []byte) PageHash { return HashPage(p) }
	}

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

	stats := &EncodeStats{}
	whole := blake3.New()
	for _, pm := range pms {
		s, err := encodePages(srcDir, outDir, pm, m, hasher, whole)
		if err != nil {
			return nil, err
		}
		stats.TotalPages += s.TotalPages
		stats.Hits += s.Hits
		stats.Misses += s.Misses
	}
	stats.RefBytes = int64(stats.Hits) * (1 + HashLen)
	stats.RawBytes = int64(stats.Misses) * (1 + PageSize)
	sum := whole.Sum(nil)
	stats.WholeChecksum = hex.EncodeToString(sum[:HashLen])

	meta := DeltaMeta{
		Version:        DeltaVersion,
		ImageDigest:    m.ImageDigest,
		BaselineDumpID: m.BaselineDumpID,
		WholeChecksum:  stats.WholeChecksum,
		TotalPages:     stats.TotalPages,
		Hits:           stats.Hits,
		Misses:         stats.Misses,
	}
	metaData, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.WriteFile(filepath.Join(outDir, DeltaMetaName), metaData, 0644); err != nil {
		return nil, err
	}
	return stats, nil
}

// encodePages writes one pages-<id>.delta and folds its pages into the whole
// checksum. Returns the per-pagemap stats.
func encodePages(srcDir, outDir string, pm Pagemap, m *Manifest, hasher func(int, uint64, []byte) PageHash, whole io.Writer) (*EncodeStats, error) {
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
		// A chained round in which nothing was dirtied: CRIU may not write a
		// pages file at all. Emit an empty delta so the round still round-trips.
		br = bufio.NewReader(bytes.NewReader(nil))
	}

	deltaPath := filepath.Join(outDir, fmt.Sprintf("pages-%d.delta", pm.PagesID))
	out, err := os.Create(deltaPath)
	if err != nil {
		return nil, err
	}
	defer out.Close()
	bw := bufio.NewWriterSize(out, 1<<20)

	if _, err := bw.Write(deltaMagic[:]); err != nil {
		return nil, err
	}
	if err := writeDeltaHeader(bw, pm.PagesID, pm.TotalPages); err != nil {
		return nil, err
	}

	stats := &EncodeStats{}
	page := make([]byte, PageSize)
	for _, e := range pm.Entries {
		// PE_PARENT entries are the clean set CRIU's soft-dirty tracking
		// already excluded from this round's pages file — they cost nothing on
		// the wire and nothing on the critical path.
		if !e.Present {
			continue
		}
		for i := uint64(0); i < e.NrPages; i++ {
			if _, err := io.ReadFull(br, page); err != nil {
				return nil, fmt.Errorf("%s short: pagemap describes more pages than the file holds: %w", pp, err)
			}
			whole.Write(page)
			stats.TotalPages++
			h := hasher(pm.Pid, e.Vaddr+i*PageSize, page)
			if m.Has(h) {
				stats.Hits++
				if err := bw.WriteByte(tagRef); err != nil {
					return nil, err
				}
				if _, err := bw.Write(h[:]); err != nil {
					return nil, err
				}
			} else {
				stats.Misses++
				if err := bw.WriteByte(tagRaw); err != nil {
					return nil, err
				}
				if _, err := bw.Write(page); err != nil {
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

func writeDeltaHeader(w *bufio.Writer, pagesID uint32, numPages uint64) error {
	if err := w.WriteByte(DeltaVersion); err != nil {
		return err
	}
	var b [12]byte
	putU32(b[0:], pagesID)
	putU64(b[4:], numPages)
	_, err := w.Write(b[:])
	return err
}

// copyMetaImages copies every file in srcDir except pages-*.img and any stray
// delta artifacts into outDir. These are the small protobuf .img files
// (pagemap, mm, core, fdinfo, …) plus runc's descriptors.json etc. — everything
// CRIU restore needs besides the page bytes.
//
// Symlinks are skipped: in a chained dump CRIU leaves a "parent" symlink to the
// previous round's directory, which is meaningless (and, if followed, a
// directory) on the destination. The receiver rebuilds the chain locally.
func copyMetaImages(srcDir, outDir string) error {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		if ent.IsDir() || ent.Type()&os.ModeSymlink != 0 {
			continue
		}
		name := ent.Name()
		if isPagesImg(name) || name == DeltaMetaName || filepath.Ext(name) == ".delta" {
			continue
		}
		if err := copyFile(filepath.Join(srcDir, name), filepath.Join(outDir, name)); err != nil {
			return err
		}
	}
	return nil
}

// isPagesImg reports whether name is a pages-<id>.img file.
func isPagesImg(name string) bool {
	var id uint32
	n, err := fmt.Sscanf(name, "pages-%d.img", &id)
	return err == nil && n == 1
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	fi, err := in.Stat()
	if err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fi.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func putU32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

func putU64(b []byte, v uint64) {
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * i))
	}
}

func getU32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func getU64(b []byte) uint64 {
	var v uint64
	for i := 0; i < 8; i++ {
		v |= uint64(b[i]) << (8 * i)
	}
	return v
}
