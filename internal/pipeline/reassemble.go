package pipeline

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/zeebo/blake3"
)

// ReassembleStats reports what the destination reconstructed.
type ReassembleStats struct {
	TotalPages    int
	Hits          int // pages resolved from the local baseline (refs)
	Misses        int // pages taken raw from the delta
	WholeChecksum string
}

// ErrChecksumMismatch is returned when the reassembled dump's whole-image
// checksum disagrees with the delta's recorded checksum (§3.3): the
// migration layer treats this as the trigger to re-request pages / fall back to
// a full transfer.
type ErrChecksumMismatch struct {
	Want, Got string
}

func (e *ErrChecksumMismatch) Error() string {
	return fmt.Sprintf("reassembled image checksum mismatch: delta=%s reassembled=%s", e.Want, e.Got)
}

// ReassembleDump rebuilds a CRIU-restorable dump in outDir from a delta
// directory and the local baseline: metadata .img files are
// copied through, each pages-<id>.delta is expanded back to pages-<id>.img by
// resolving refs against the baseline, and the whole-image checksum is verified
// before the dump is handed to runc restore.
func ReassembleDump(deltaDir, outDir string, b *Baseline) (*ReassembleStats, error) {
	metaData, err := os.ReadFile(filepath.Join(deltaDir, DeltaMetaName))
	if err != nil {
		return nil, fmt.Errorf("reading delta meta: %w", err)
	}
	var meta DeltaMeta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return nil, fmt.Errorf("parsing delta meta: %w", err)
	}
	if meta.Version != DeltaVersion {
		return nil, fmt.Errorf("delta version %d unsupported (want %d)", meta.Version, DeltaVersion)
	}
	if meta.BaselineDumpID != "" && b.DumpID != "" && meta.BaselineDumpID != b.DumpID {
		return nil, fmt.Errorf("baseline mismatch: delta cut against %s but local baseline is %s", meta.BaselineDumpID, b.DumpID)
	}

	if err := os.MkdirAll(outDir, 0755); err != nil {
		return nil, err
	}
	if err := copyThroughMeta(deltaDir, outDir); err != nil {
		return nil, err
	}

	stats := &ReassembleStats{}
	deltas, err := filepath.Glob(filepath.Join(deltaDir, "pages-*.delta"))
	if err != nil {
		return nil, err
	}
	if len(deltas) == 0 {
		return nil, fmt.Errorf("no pages-*.delta in %s", deltaDir)
	}
	for _, d := range deltas {
		s, err := reassemblePages(d, outDir, b)
		if err != nil {
			return nil, err
		}
		stats.TotalPages += s.TotalPages
		stats.Hits += s.Hits
		stats.Misses += s.Misses
	}

	// Verify the whole-image checksum over the reassembled pages in the same
	// canonical order the source hashed (ReadPagemaps order — identical because
	// the pagemap .img files were copied through verbatim).
	whole := blake3.New()
	if err := ForEachPage(outDir, func(_ int, _ uint64, page []byte) error {
		whole.Write(page)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("re-checksum failed: %w", err)
	}
	sum := whole.Sum(nil)
	stats.WholeChecksum = hex.EncodeToString(sum[:HashLen])
	if meta.WholeChecksum != "" && stats.WholeChecksum != meta.WholeChecksum {
		return stats, &ErrChecksumMismatch{Want: meta.WholeChecksum, Got: stats.WholeChecksum}
	}
	return stats, nil
}

// reassemblePages expands one pages-<id>.delta into pages-<id>.img in outDir.
func reassemblePages(deltaPath, outDir string, b *Baseline) (*ReassembleStats, error) {
	in, err := os.Open(deltaPath)
	if err != nil {
		return nil, err
	}
	defer in.Close()
	br := bufio.NewReaderSize(in, 1<<20)

	var magic [4]byte
	if _, err := io.ReadFull(br, magic[:]); err != nil {
		return nil, fmt.Errorf("%s: reading magic: %w", deltaPath, err)
	}
	if magic != deltaMagic {
		return nil, fmt.Errorf("%s: bad delta magic %v", deltaPath, magic)
	}
	ver, err := br.ReadByte()
	if err != nil {
		return nil, err
	}
	if ver != DeltaVersion {
		return nil, fmt.Errorf("%s: delta version %d unsupported (want %d)", deltaPath, ver, DeltaVersion)
	}
	var hdr [12]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return nil, err
	}
	pagesID := getU32(hdr[0:])
	numPages := getU64(hdr[4:])

	outPath := pagesPath(outDir, pagesID)
	out, err := os.Create(outPath)
	if err != nil {
		return nil, err
	}
	defer out.Close()
	bw := bufio.NewWriterSize(out, 1<<20)

	stats := &ReassembleStats{}
	var h PageHash
	raw := make([]byte, PageSize)
	for i := uint64(0); i < numPages; i++ {
		tag, err := br.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("%s: reading tag for page %d/%d: %w", deltaPath, i, numPages, err)
		}
		switch tag {
		case tagRef:
			if _, err := io.ReadFull(br, h[:]); err != nil {
				return nil, err
			}
			page, ok := b.Page(h)
			if !ok {
				return nil, fmt.Errorf("%s: ref %x not in local baseline (integrity failure — page %d/%d)", deltaPath, h, i, numPages)
			}
			if _, err := bw.Write(page); err != nil {
				return nil, err
			}
			stats.Hits++
		case tagRaw:
			if _, err := io.ReadFull(br, raw); err != nil {
				return nil, err
			}
			if _, err := bw.Write(raw); err != nil {
				return nil, err
			}
			stats.Misses++
		default:
			return nil, fmt.Errorf("%s: bad page tag %d at page %d/%d", deltaPath, tag, i, numPages)
		}
		stats.TotalPages++
	}
	return stats, bw.Flush()
}

// copyThroughMeta copies the delta directory's metadata .img files (everything
// that is not a .delta or the meta sidecar) into outDir.
func copyThroughMeta(deltaDir, outDir string) error {
	entries, err := os.ReadDir(deltaDir)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		if ent.IsDir() || ent.Type()&os.ModeSymlink != 0 {
			continue
		}
		name := ent.Name()
		if name == DeltaMetaName || filepath.Ext(name) == ".delta" {
			continue
		}
		if err := copyFile(filepath.Join(deltaDir, name), filepath.Join(outDir, name)); err != nil {
			return err
		}
	}
	return nil
}
