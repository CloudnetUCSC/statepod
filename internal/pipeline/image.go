package pipeline

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/checkpoint-restore/go-criu/v7/crit"
	"github.com/checkpoint-restore/go-criu/v7/crit/images/pagemap"
	"google.golang.org/protobuf/encoding/protowire"
)

// Pagemap entry flags (criu/include/pagemap.h:152).
//
// PE_PARENT is how M2 arrives for free (route B): in a
// --parent-path-chained dump CRIU uses its own soft-dirty tracking to mark
// every page unchanged since the parent round as PE_PARENT — those pages are
// absent from this round's pages-*.img. Verified on the testbed: a chained
// pre-dump of a loaded Redis showed 448 PE_PARENT pages and 213 PE_PRESENT,
// with the pages file holding exactly 213 pages. So a chained dump *is* the
// exact dirty set, and critical-path hashing is O(dirty) with no ioctl of our
// own.
const (
	peParent  = 1 << 0 // pages are in a parent snapshot — clean since last round
	peLazy    = 1 << 1 // pages can be lazily restored (M5)
	pePresent = 1 << 2 // pages are present in this dump's pages-*.img
)

// Entry is one pagemap entry: nr_pages contiguous pages starting at vaddr.
// When Present, their bytes live in pages-<PagesID>.img; otherwise they resolve
// through the parent chain and contribute no bytes here.
type Entry struct {
	Vaddr   uint64
	NrPages uint64
	Flags   uint32
	Present bool
}

// Pagemap is one parsed pagemap-<Pid>.img plus the id of the pages file it
// references. Entries are in file order — the exact order the present pages
// appear in pages-<PagesID>.img — which the delta stream mirrors on both ends.
// TotalPages counts only present pages, i.e. exactly what the pages file holds.
type Pagemap struct {
	Path       string
	Pid        int
	PagesID    uint32
	Entries    []Entry
	TotalPages uint64 // present pages only
}

// entryNrPages returns the page count of a pagemap entry. CRIU ≥ 4.2 moved
// nr_pages to proto field 5 (uint64) and writes the legacy required field 2
// (now compat_nr_pages) as 0; go-criu's bundled proto (≤ v7.2.x) still maps
// only field 2, so field 5 lands in the entry's unknown-field bytes. Prefer
// field 5 when present, else field 2 (mirrors internal/pagecmp — kept separate
// so the well-tested pagecmp tool stays untouched).
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

// PagemapKey parses a pagemap file base name. CRIU writes one pagemap per
// process (pagemap-<pid>.img) and one per shared-memory segment
// (pagemap-shmem-<shmid>.img: PostgreSQL's shared_buffers, anything using
// SysV/POSIX shm or MAP_SHARED anon memory). A shared-memory pagemap is keyed
// by the NEGATIVE of its segment id so one int identifies either kind;
// PagemapFileName inverts it.
func PagemapKey(base string) (int, bool) {
	var n int
	if c, _ := fmt.Sscanf(base, "pagemap-shmem-%d.img", &n); c == 1 {
		return -n, true
	}
	if c, _ := fmt.Sscanf(base, "pagemap-%d.img", &n); c == 1 {
		return n, true
	}
	return 0, false
}

// PagemapFileName is the inverse of PagemapKey.
func PagemapFileName(key int) string {
	if key < 0 {
		return fmt.Sprintf("pagemap-shmem-%d.img", -key)
	}
	return fmt.Sprintf("pagemap-%d.img", key)
}

// ReadPagemaps parses every pagemap-*.img in dir (sorted by filename). Both
// full dumps and parent-chained (pre-dump) dumps are accepted: non-present
// entries are kept for the record but carry no page bytes. The returned slice
// order + each Pagemap's Entries order defines the canonical page order used
// everywhere in the pipeline (encode, checksum, reassemble).
func ReadPagemaps(dir string) ([]Pagemap, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "pagemap-*.img"))
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no pagemap-*.img in %s — not a checkpoint directory?", dir)
	}
	sort.Strings(paths)

	out := make([]Pagemap, 0, len(paths))
	for _, p := range paths {
		pid, ok := PagemapKey(filepath.Base(p))
		if !ok {
			return nil, fmt.Errorf("unexpected pagemap file name %s", p)
		}

		f, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		img, err := crit.New(f, nil, "", false, false).Decode(&pagemap.PagemapEntry{})
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("decoding %s: %w", p, err)
		}
		if len(img.Entries) == 0 {
			return nil, fmt.Errorf("%s: empty pagemap image", p)
		}
		head, ok := img.Entries[0].Message.(*pagemap.PagemapHead)
		if !ok {
			return nil, fmt.Errorf("%s: first entry is %T, want PagemapHead", p, img.Entries[0].Message)
		}

		pm := Pagemap{Path: p, Pid: pid, PagesID: head.GetPagesId()}
		for _, e := range img.Entries[1:] {
			pe, ok := e.Message.(*pagemap.PagemapEntry)
			if !ok {
				return nil, fmt.Errorf("%s: entry is %T, want PagemapEntry", p, e.Message)
			}
			flags := pe.GetFlags()
			present := flags&pePresent != 0
			nr := entryNrPages(pe)
			pm.Entries = append(pm.Entries, Entry{Vaddr: pe.GetVaddr(), NrPages: nr, Flags: flags, Present: present})
			if present {
				pm.TotalPages += nr
			}
		}
		out = append(out, pm)
	}
	return out, nil
}

// pagesPath returns the pages-<id>.img path for a pagemap in dir.
func pagesPath(dir string, pagesID uint32) string {
	return filepath.Join(dir, fmt.Sprintf("pages-%d.img", pagesID))
}

// ForEachPage streams every present page of dir's dumps in canonical order,
// calling fn(pid, vaddr, page) with a PageSize slice reused between calls
// (copy it if you need to retain it). Enforces that each pages file holds
// exactly the pages its pagemap describes — no more, no fewer.
func ForEachPage(dir string, fn func(pid int, vaddr uint64, page []byte) error) error {
	pms, err := ReadPagemaps(dir)
	if err != nil {
		return err
	}
	buf := make([]byte, PageSize)
	for _, pm := range pms {
		pp := pagesPath(dir, pm.PagesID)
		f, err := os.Open(pp)
		if err != nil {
			return err
		}
		br := bufio.NewReaderSize(f, 1<<20)
		var read uint64
		for _, e := range pm.Entries {
			if !e.Present {
				continue // resolves through the parent chain; no bytes here
			}
			for i := uint64(0); i < e.NrPages; i++ {
				if _, err := io.ReadFull(br, buf); err != nil {
					f.Close()
					return fmt.Errorf("%s short: pagemap describes more pages than the file holds: %w", pp, err)
				}
				if err := fn(pm.Pid, e.Vaddr+i*PageSize, buf); err != nil {
					f.Close()
					return err
				}
				read++
			}
		}
		// The pagemap must account for the whole pages file.
		if _, err := br.Read(make([]byte, 1)); err != io.EOF {
			f.Close()
			return fmt.Errorf("%s holds more pages than %s describes", pp, pm.Path)
		}
		f.Close()
		_ = read
	}
	return nil
}

// used to silence unused-const linters if a flag ever goes unreferenced
var _ = peParent
var _ = peLazy
