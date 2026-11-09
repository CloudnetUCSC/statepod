package pipeline

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/checkpoint-restore/go-criu/v7/crit"
	"github.com/checkpoint-restore/go-criu/v7/crit/images/mm"
	"github.com/checkpoint-restore/go-criu/v7/crit/images/pagemap"
	"github.com/checkpoint-restore/go-criu/v7/crit/images/vma"
	"google.golang.org/protobuf/proto"
)

// writeDefaultMM writes a minimal mm-<pid>.img with a single VMA spanning the
// entire address space, flagged anonymous+private (lazy-eligible) — enough
// for readVMAs/lazyEligible to treat every synthetic test page as an ordinary
// heap-like page unless a test overrides it. Every field here is "required"
// in mm.proto/vma.proto (proto2), so all must be set for Marshal to succeed;
// the values themselves (beyond Vmas) don't matter to any pipeline code.
func writeDefaultMM(t *testing.T, dir string, pid int) {
	t.Helper()
	vmas := []*vma.VmaEntry{{
		Start:  proto.Uint64(0),
		End:    proto.Uint64(1 << 47),
		Pgoff:  proto.Uint64(0),
		Shmid:  proto.Uint64(0),
		Prot:   proto.Uint32(0x3), // PROT_READ|PROT_WRITE
		Flags:  proto.Uint32(mapAnon | mapPrivate),
		Status: proto.Uint32(1), // VMA_AREA_REGULAR
		Fd:     proto.Int64(-1),
	}}
	writeMM(t, dir, pid, vmas)
}

// writeMM writes an mm-<pid>.img with the given VMA list, for tests that need
// specific (non-lazy-eligible) VMA flags.
func writeMM(t *testing.T, dir string, pid int, vmas []*vma.VmaEntry) {
	t.Helper()
	me := &mm.MmEntry{
		MmStartCode:  proto.Uint64(0),
		MmEndCode:    proto.Uint64(0),
		MmStartData:  proto.Uint64(0),
		MmEndData:    proto.Uint64(0),
		MmStartStack: proto.Uint64(0),
		MmStartBrk:   proto.Uint64(0),
		MmBrk:        proto.Uint64(0),
		MmArgStart:   proto.Uint64(0),
		MmArgEnd:     proto.Uint64(0),
		MmEnvStart:   proto.Uint64(0),
		MmEnvEnd:     proto.Uint64(0),
		ExeFileId:    proto.Uint32(0),
		Vmas:         vmas,
	}
	img := &crit.CriuImage{Magic: "MM", Entries: []*crit.CriuEntry{{Message: me}}}
	f, err := os.Create(filepath.Join(dir, "mm-"+itoa(pid)+".img"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := crit.New(nil, f, "", false, false).Encode(img); err != nil {
		t.Fatalf("encode mm: %v", err)
	}
}

// mkPage returns a deterministic PageSize page filled from seed.
func mkPage(seed byte) []byte {
	p := make([]byte, PageSize)
	for i := range p {
		p[i] = seed + byte(i%7)
	}
	return p
}

// writeDump writes a synthetic CRIU dump (one pagemap-<pid>.img covering all
// pages contiguously + its pages-<pagesID>.img) into dir, using go-criu's own
// encoder so ReadPagemaps parses it exactly as it would a real dump.
func writeDump(t *testing.T, dir string, pid int, pagesID uint32, baseVaddr uint64, pages [][]byte) {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}

	head := &pagemap.PagemapHead{PagesId: proto.Uint32(pagesID)}
	entry := &pagemap.PagemapEntry{
		Vaddr:   proto.Uint64(baseVaddr),
		NrPages: proto.Uint32(uint32(len(pages))),
		Flags:   proto.Uint32(pePresent),
	}
	img := &crit.CriuImage{
		Magic:     "PAGEMAP",
		EntryType: &pagemap.PagemapEntry{},
		Entries: []*crit.CriuEntry{
			{Message: head},
			{Message: entry},
		},
	}
	pmPath := filepath.Join(dir, "pagemap-"+itoa(pid)+".img")
	f, err := os.Create(pmPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := crit.New(nil, f, "", false, false).Encode(img); err != nil {
		f.Close()
		t.Fatalf("encode pagemap: %v", err)
	}
	f.Close()

	var raw bytes.Buffer
	for _, p := range pages {
		raw.Write(p)
	}
	if err := os.WriteFile(pagesPath(dir, pagesID), raw.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	// A metadata file that must survive copy-through to reassembly.
	if err := os.WriteFile(filepath.Join(dir, "core-"+itoa(pid)+".img"), []byte("meta"), 0644); err != nil {
		t.Fatal(err)
	}
	writeDefaultMM(t, dir, pid)
}

// specEntry describes one pagemap entry for writeChainedDump.
type specEntry struct {
	vaddr   uint64
	nrPages uint32
	present bool // false => PE_PARENT: pages live in the parent, not this file
}

// writeChainedDump writes a dump whose pagemap mixes PE_PRESENT and PE_PARENT
// entries, as CRIU produces for a --parent-path round. pages supplies bytes for
// the present entries only, in order.
func writeChainedDump(t *testing.T, dir string, pid int, pagesID uint32, spec []specEntry, pages [][]byte) {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	entries := []*crit.CriuEntry{{Message: &pagemap.PagemapHead{PagesId: proto.Uint32(pagesID)}}}
	for _, s := range spec {
		flags := uint32(peParent)
		if s.present {
			flags = pePresent
		}
		entries = append(entries, &crit.CriuEntry{Message: &pagemap.PagemapEntry{
			Vaddr:   proto.Uint64(s.vaddr),
			NrPages: proto.Uint32(s.nrPages),
			Flags:   proto.Uint32(flags),
		}})
	}
	img := &crit.CriuImage{Magic: "PAGEMAP", EntryType: &pagemap.PagemapEntry{}, Entries: entries}
	f, err := os.Create(filepath.Join(dir, "pagemap-"+itoa(pid)+".img"))
	if err != nil {
		t.Fatal(err)
	}
	if err := crit.New(nil, f, "", false, false).Encode(img); err != nil {
		f.Close()
		t.Fatalf("encode pagemap: %v", err)
	}
	f.Close()

	var raw bytes.Buffer
	for _, p := range pages {
		raw.Write(p)
	}
	if err := os.WriteFile(pagesPath(dir, pagesID), raw.Bytes(), 0644); err != nil {
		t.Fatal(err)
	}
	writeDefaultMM(t, dir, pid)
}

// TestChainedDumpParentEntries covers the M2 path: a parent-chained
// round carries only its dirty pages, PE_PARENT entries contribute nothing to
// the wire or the critical path, and reassembly reproduces the pages file
// exactly while preserving the pagemap so restore can walk the chain.
func TestChainedDumpParentEntries(t *testing.T) {
	tmp := t.TempDir()

	baseA, baseB := mkPage(10), mkPage(20)
	baseDir := filepath.Join(tmp, "baseline")
	writeDump(t, baseDir, 1, 100, 0x1000, [][]byte{baseA, baseB})
	base, err := LoadBaseline(baseDir, "sha256:img")
	if err != nil {
		t.Fatal(err)
	}

	// Dirty round: 2 present pages (one baseline hit, one miss), 3 clean pages
	// living in the parent, then 1 more present page (a hit).
	dirtyMiss := mkPage(77)
	spec := []specEntry{
		{vaddr: 0x1000, nrPages: 2, present: true},
		{vaddr: 0x3000, nrPages: 3, present: false},
		{vaddr: 0x6000, nrPages: 1, present: true},
	}
	present := [][]byte{baseA, dirtyMiss, baseB}
	srcDir := filepath.Join(tmp, "src")
	writeChainedDump(t, srcDir, 7, 200, spec, present)

	pms, err := ReadPagemaps(srcDir)
	if err != nil {
		t.Fatal(err)
	}
	if pms[0].TotalPages != 3 {
		t.Fatalf("TotalPages = %d, want 3 (present pages only)", pms[0].TotalPages)
	}

	deltaDir := filepath.Join(tmp, "delta")
	stats, err := EncodeDump(srcDir, deltaDir, base.Manifest(), EncodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Only the 3 dirty pages are hashed/shipped; the 3 parent pages cost nothing.
	if stats.TotalPages != 3 || stats.Hits != 2 || stats.Misses != 1 {
		t.Fatalf("stats = %+v, want total=3 hits=2 misses=1", stats)
	}

	outDir := filepath.Join(tmp, "out")
	rstats, err := ReassembleDump(deltaDir, outDir, base)
	if err != nil {
		t.Fatal(err)
	}
	if rstats.TotalPages != 3 || rstats.WholeChecksum != stats.WholeChecksum {
		t.Fatalf("reassemble stats = %+v (checksum want %s)", rstats, stats.WholeChecksum)
	}
	srcBytes, _ := os.ReadFile(pagesPath(srcDir, 200))
	outBytes, _ := os.ReadFile(pagesPath(outDir, 200))
	if !bytes.Equal(srcBytes, outBytes) {
		t.Fatal("reassembled pages differ from source")
	}
	// The pagemap must survive verbatim, PE_PARENT entries included, or restore
	// cannot resolve the chain.
	outPms, err := ReadPagemaps(outDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(outPms[0].Entries) != 3 || outPms[0].Entries[1].Present {
		t.Fatalf("pagemap not preserved: %+v", outPms[0].Entries)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func TestHashPageDeterministic(t *testing.T) {
	p := mkPage(3)
	if HashPage(p) != HashPage(p) {
		t.Fatal("hash not deterministic")
	}
	if HashPage(mkPage(3)) == HashPage(mkPage(4)) {
		t.Fatal("distinct pages collided")
	}
	if len(HashPage(p)) != HashLen {
		t.Fatalf("hash len %d, want %d", len(HashPage(p)), HashLen)
	}
}

func TestManifestRoundtrip(t *testing.T) {
	m := &Manifest{
		ImageDigest:    "sha256:abc",
		BaselineDumpID: "deadbeef",
		Hashes:         map[PageHash]struct{}{},
	}
	for i := 0; i < 50; i++ {
		m.Hashes[HashPage(mkPage(byte(i)))] = struct{}{}
	}
	var buf bytes.Buffer
	if err := m.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	got, err := DecodeManifest(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.ImageDigest != m.ImageDigest || got.BaselineDumpID != m.BaselineDumpID {
		t.Fatalf("header mismatch: %+v", got)
	}
	if len(got.Hashes) != len(m.Hashes) {
		t.Fatalf("hash count %d, want %d", len(got.Hashes), len(m.Hashes))
	}
	for h := range m.Hashes {
		if !got.Has(h) {
			t.Fatalf("missing hash %x after roundtrip", h)
		}
	}
}

// TestEncodeReassembleRoundtrip is the core integrity property: for a dump
// whose pages partly overlap a baseline, encode→reassemble reproduces the
// original dump byte-for-byte, with the expected hit/miss split.
func TestEncodeReassembleRoundtrip(t *testing.T) {
	tmp := t.TempDir()

	// Baseline holds pages A, B, C.
	baseA, baseB, baseC := mkPage(10), mkPage(20), mkPage(30)
	baseDir := filepath.Join(tmp, "baseline")
	writeDump(t, baseDir, 1, 100, 0x1000, [][]byte{baseA, baseB, baseC})

	// Source dump: A (hit), B (hit), X (miss), C (hit), Y (miss).
	x, y := mkPage(40), mkPage(50)
	srcPages := [][]byte{baseA, baseB, x, baseC, y}
	srcDir := filepath.Join(tmp, "src")
	writeDump(t, srcDir, 7, 200, 0x555555554000, srcPages)

	base, err := LoadBaseline(baseDir, "sha256:img")
	if err != nil {
		t.Fatal(err)
	}
	m := base.Manifest()

	deltaDir := filepath.Join(tmp, "delta")
	stats, err := EncodeDump(srcDir, deltaDir, m, EncodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalPages != 5 || stats.Hits != 3 || stats.Misses != 2 {
		t.Fatalf("stats = %+v, want total=5 hits=3 misses=2", stats)
	}

	outDir := filepath.Join(tmp, "out")
	rstats, err := ReassembleDump(deltaDir, outDir, base)
	if err != nil {
		t.Fatal(err)
	}
	if rstats.Hits != 3 || rstats.Misses != 2 {
		t.Fatalf("reassemble stats = %+v", rstats)
	}
	if rstats.WholeChecksum != stats.WholeChecksum {
		t.Fatalf("checksum mismatch: enc=%s dec=%s", stats.WholeChecksum, rstats.WholeChecksum)
	}

	// Reassembled pages-*.img must equal the source's byte-for-byte.
	srcPagesBytes, err := os.ReadFile(pagesPath(srcDir, 200))
	if err != nil {
		t.Fatal(err)
	}
	outPagesBytes, err := os.ReadFile(pagesPath(outDir, 200))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(srcPagesBytes, outPagesBytes) {
		t.Fatal("reassembled pages differ from source")
	}
	// Metadata copied through.
	if _, err := os.Stat(filepath.Join(outDir, "core-7.img")); err != nil {
		t.Fatalf("metadata not copied through: %v", err)
	}
}

// TestReassembleMissingRef ensures a ref absent from the local baseline is a
// hard integrity failure, not silent corruption.
func TestReassembleMissingRef(t *testing.T) {
	tmp := t.TempDir()
	a := mkPage(1)
	srcDir := filepath.Join(tmp, "src")
	writeDump(t, srcDir, 1, 10, 0x1000, [][]byte{a})

	// Manifest claims to hold a's hash, but the local baseline used for
	// reassembly does not — the source would have sent a ref it can't resolve.
	m := &Manifest{ImageDigest: "x", Hashes: map[PageHash]struct{}{HashPage(a): {}}}
	deltaDir := filepath.Join(tmp, "delta")
	if _, err := EncodeDump(srcDir, deltaDir, m, EncodeOptions{}); err != nil {
		t.Fatal(err)
	}

	empty := &Baseline{Digest: "x", byHash: map[PageHash][]byte{}}
	if _, err := ReassembleDump(deltaDir, filepath.Join(tmp, "out"), empty); err == nil {
		t.Fatal("expected integrity failure for unresolvable ref")
	}
}

// TestChecksumMismatch corrupts a baseline page so a ref resolves to the wrong
// bytes, and checks the whole-image checksum catches it.
func TestChecksumMismatch(t *testing.T) {
	tmp := t.TempDir()
	a := mkPage(1)
	baseDir := filepath.Join(tmp, "baseline")
	writeDump(t, baseDir, 1, 100, 0x1000, [][]byte{a})
	srcDir := filepath.Join(tmp, "src")
	writeDump(t, srcDir, 1, 10, 0x1000, [][]byte{a})

	base, err := LoadBaseline(baseDir, "x")
	if err != nil {
		t.Fatal(err)
	}
	m := base.Manifest()
	deltaDir := filepath.Join(tmp, "delta")
	if _, err := EncodeDump(srcDir, deltaDir, m, EncodeOptions{}); err != nil {
		t.Fatal(err)
	}

	// Swap the baseline's stored bytes for this hash to a different page.
	for h := range base.byHash {
		base.byHash[h] = mkPage(99)
	}
	_, err = ReassembleDump(deltaDir, filepath.Join(tmp, "out"), base)
	if _, ok := err.(*ErrChecksumMismatch); !ok {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
}
