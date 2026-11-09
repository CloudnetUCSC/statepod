package pipeline

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/zeebo/blake3"
)

// bufioReader wraps r in a buffered reader unless it already is one.
func bufioReader(r io.Reader) *bufio.Reader {
	if br, ok := r.(*bufio.Reader); ok {
		return br
	}
	return bufio.NewReaderSize(r, 1<<16)
}

// Baseline is the destination-local synthesized dump indexed for reconciliation
// : a content-addressed table mapping each page's blake3-128 hash
// to that page's bytes. A received 16 B ref is resolved by a lookup here — the
// pages themselves never left this host.
type Baseline struct {
	Digest string
	DumpID string
	byHash map[PageHash][]byte
	// byVaddr is keyed by pagemap key (the task's pid, or the negative shm id) and then
	// vaddr: CRIU resolves an in_parent page in the parent dump's pagemap of the SAME
	// task, so an address-level answer must be per task. (A single global table let
	// one process's page stand in for another's, and let tasks the baseline never
	// had pass the gate: CRIU then failed with "No parent for snapshot pagemap" on
	// PostgreSQL, whose worker pids differ between instances.)
	byVaddr map[int]map[uint64]PageHash
}

// LoadBaseline parses a stored template dump directory and builds the
// hash→content table. digest is the image digest the baseline is keyed by
// (templates are stored per image digest). The BaselineDumpID is
// derived from the page-hash set, so identical baselines get identical ids and
// a re-synthesis that changes the set changes the id (manifest versioning).
func LoadBaseline(dir, digest string) (*Baseline, error) {
	b := &Baseline{Digest: digest, byHash: make(map[PageHash][]byte), byVaddr: make(map[int]map[uint64]PageHash)}
	err := ForEachPage(dir, func(key int, vaddr uint64, page []byte) error {
		h := HashPage(page)
		if _, ok := b.byHash[h]; !ok {
			p := make([]byte, PageSize)
			copy(p, page)
			b.byHash[h] = p
		}
		m := b.byVaddr[key]
		if m == nil {
			m = make(map[uint64]PageHash)
			b.byVaddr[key] = m
		}
		m[vaddr] = h
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("loading baseline %s: %w", dir, err)
	}
	if len(b.byHash) == 0 {
		return nil, fmt.Errorf("baseline %s has no pages", dir)
	}
	b.DumpID = baselineDumpID(b.byHash)
	return b, nil
}

// baselineDumpID is a blake3-256 (hex) over the sorted unique page hashes: a
// stable identifier for the baseline's content set.
func baselineDumpID(byHash map[PageHash][]byte) string {
	hs := make([]PageHash, 0, len(byHash))
	for h := range byHash {
		hs = append(hs, h)
	}
	sortHashes(hs)
	hh := blake3.New()
	for _, h := range hs {
		hh.Write(h[:])
	}
	sum := hh.Sum(nil)
	return hex.EncodeToString(sum[:16])
}

// Page returns the bytes for a baseline page hash, resolving a received ref.
func (b *Baseline) Page(h PageHash) ([]byte, bool) {
	p, ok := b.byHash[h]
	return p, ok
}

// HashAtVaddr returns the baseline's own page hash at vaddr in the pagemap
// `key` (the task's pid; negative for a shared-memory segment), if the baseline
// has a page there at all. This is the address-level half of the
// in_parent eligibility gate: CRIU's in_parent mechanism is a pure address
// lookup with no content check of its own (criu/pagemap.c:203, "Missing
// <vaddr> in parent pagemap"), so a page is only safe to mark in_parent if
// the baseline has a page at this exact vaddr AND its hash matches — content-
// hash membership alone (Manifest.Has) is not sufficient.
func (b *Baseline) HashAtVaddr(key int, vaddr uint64) (PageHash, bool) {
	h, ok := b.byVaddr[key][vaddr]
	return h, ok
}

// Manifest builds the page-hash manifest to ship to the source.
func (b *Baseline) Manifest() *Manifest {
	m := &Manifest{
		ImageDigest:    b.Digest,
		BaselineDumpID: b.DumpID,
		Hashes:         make(map[PageHash]struct{}, len(b.byHash)),
	}
	for h := range b.byHash {
		m.Hashes[h] = struct{}{}
	}
	return m
}

// NumPages reports the number of distinct baseline pages (manifest cardinality).
func (b *Baseline) NumPages() int { return len(b.byHash) }
