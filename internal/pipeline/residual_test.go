package pipeline

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/checkpoint-restore/go-criu/v7/crit/images/vma"
	"google.golang.org/protobuf/proto"
)

// TestEncodeResidualMap_HitMissClassification mirrors
// TestEncodeReassembleRoundtrip's scenario (M1) but checks the M5 residual
// encoder: same hit/miss split, no raw bytes anywhere in the output.
func TestEncodeResidualMap_HitMissClassification(t *testing.T) {
	tmp := t.TempDir()

	baseA, baseB, baseC := mkPage(10), mkPage(20), mkPage(30)
	baseDir := filepath.Join(tmp, "baseline")
	writeDump(t, baseDir, 1, 100, 0x1000, [][]byte{baseA, baseB, baseC})
	base, err := LoadBaseline(baseDir, "sha256:img")
	if err != nil {
		t.Fatal(err)
	}
	m := base.Manifest()

	// Source dump: A (hit), B (hit), X (miss), C (hit), Y (miss).
	x, y := mkPage(40), mkPage(50)
	srcPages := [][]byte{baseA, baseB, x, baseC, y}
	srcDir := filepath.Join(tmp, "src")
	writeDump(t, srcDir, 7, 200, 0x555555554000, srcPages)

	outDir := filepath.Join(tmp, "residual")
	stats, err := EncodeResidualMap(srcDir, outDir, m)
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalPages != 5 || stats.Hits != 3 || stats.Misses != 2 {
		t.Fatalf("stats = %+v, want total=5 hits=3 misses=2", stats)
	}

	// Metadata (pagemap + core) must survive, so the destination knows the
	// original page layout for restore-dir construction.
	if _, err := os.Stat(filepath.Join(outDir, "core-7.img")); err != nil {
		t.Fatalf("metadata not copied through: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "pagemap-7.img")); err != nil {
		t.Fatalf("pagemap not copied through: %v", err)
	}

	// The residual meta sidecar must match the stats.
	metaData, err := os.ReadFile(filepath.Join(outDir, ResidualMetaName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(metaData, []byte(`"hits": 3`)) || !bytes.Contains(metaData, []byte(`"misses": 2`)) {
		t.Fatalf("residual meta doesn't reflect stats: %s", metaData)
	}

	// No raw page bytes anywhere: the .residual file's exact size is
	// magic(4) + header(1+4+8) + hits*(1+16) + misses*1 — never PageSize.
	residualBytes, err := os.ReadFile(filepath.Join(outDir, "pages-200.residual"))
	if err != nil {
		t.Fatal(err)
	}
	wantSize := 4 + 13 + stats.Hits*(1+HashLen) + stats.Misses*1
	if len(residualBytes) != wantSize {
		t.Fatalf("residual file size = %d, want %d", len(residualBytes), wantSize)
	}
}

// TestEncodeResidualMap_TinyPayload is the post-copy headline property made
// concrete: the freeze payload must stay small regardless of dumped state
// size, unlike EncodeDump's delta (M1), which embeds full raw bytes for every
// miss and therefore grows with the miss set.
func TestEncodeResidualMap_TinyPayload(t *testing.T) {
	tmp := t.TempDir()

	const n = 200
	pages := make([][]byte, n)
	for i := range pages {
		pages[i] = mkPage(byte(i))
	}
	srcDir := filepath.Join(tmp, "src")
	writeDump(t, srcDir, 1, 10, 0x1000, pages)

	// Empty baseline: every page misses. This is the worst case for the
	// residual map (maximum miss count) and the best case for EncodeDump's
	// delta (which would ship every page's full bytes) — the comparison
	// should still favor the residual map enormously.
	m := &Manifest{ImageDigest: "x", Hashes: map[PageHash]struct{}{}}

	residualDir := filepath.Join(tmp, "residual")
	stats, err := EncodeResidualMap(srcDir, residualDir, m)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Misses != n {
		t.Fatalf("misses = %d, want %d (empty baseline)", stats.Misses, n)
	}

	residualBytes, err := os.ReadFile(filepath.Join(residualDir, "pages-10.residual"))
	if err != nil {
		t.Fatal(err)
	}
	rawDumpSize := n * PageSize
	if len(residualBytes) >= rawDumpSize/100 {
		t.Fatalf("residual payload %d B not tiny vs raw dump %d B", len(residualBytes), rawDumpSize)
	}
}

// TestEncodeResidualMap_ChainedParentEntries confirms PE_PARENT entries (a
// chained round's clean set) cost nothing in the residual map either.
func TestEncodeResidualMap_ChainedParentEntries(t *testing.T) {
	tmp := t.TempDir()

	baseA, baseB := mkPage(10), mkPage(20)
	baseDir := filepath.Join(tmp, "baseline")
	writeDump(t, baseDir, 1, 100, 0x1000, [][]byte{baseA, baseB})
	base, err := LoadBaseline(baseDir, "sha256:img")
	if err != nil {
		t.Fatal(err)
	}

	dirtyMiss := mkPage(77)
	spec := []specEntry{
		{vaddr: 0x1000, nrPages: 2, present: true},
		{vaddr: 0x3000, nrPages: 3, present: false},
		{vaddr: 0x6000, nrPages: 1, present: true},
	}
	present := [][]byte{baseA, dirtyMiss, baseB}
	srcDir := filepath.Join(tmp, "src")
	writeChainedDump(t, srcDir, 7, 200, spec, present)

	outDir := filepath.Join(tmp, "residual")
	stats, err := EncodeResidualMap(srcDir, outDir, base.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalPages != 3 || stats.Hits != 2 || stats.Misses != 1 {
		t.Fatalf("stats = %+v, want total=3 hits=2 misses=1", stats)
	}
}

// TestEncodeResidualMap_NonLazyEligibleVMA is the regression guard for the
// vdso failure found live in the VM test ("Invalid ELF magic"):
// a page in a VMA CRIU will never register for userfaultfd (vdso/vvar/
// vsyscall/stack/locked/hugetlb) must ship its real bytes now, unconditionally
// - even when it happens to be a manifest content-hash hit, since there is no
// "later" for it to be lazily fetched at.
func TestEncodeResidualMap_NonLazyEligibleVMA(t *testing.T) {
	tmp := t.TempDir()

	vdsoPage, missPage := mkPage(1), mkPage(2)
	baseDir := filepath.Join(tmp, "baseline")
	writeDump(t, baseDir, 1, 100, 0x1000, [][]byte{vdsoPage})
	base, err := LoadBaseline(baseDir, "sha256:img")
	if err != nil {
		t.Fatal(err)
	}

	srcDir := filepath.Join(tmp, "src")
	writeDump(t, srcDir, 7, 200, 0x1000, [][]byte{vdsoPage, missPage})
	// Override the default all-anon-private VMA: 0x1000 (vdsoPage, a manifest
	// hit) sits in a vdso VMA; 0x2000 (missPage) stays ordinary/lazy-eligible.
	writeMM(t, srcDir, 7, []*vma.VmaEntry{
		{Start: proto.Uint64(0x1000), End: proto.Uint64(0x2000), Pgoff: proto.Uint64(0), Shmid: proto.Uint64(0),
			Prot: proto.Uint32(0x1), Flags: proto.Uint32(0), Status: proto.Uint32(1 | vmaAreaVdso), Fd: proto.Int64(-1)},
		{Start: proto.Uint64(0x2000), End: proto.Uint64(0x3000), Pgoff: proto.Uint64(0), Shmid: proto.Uint64(0),
			Prot: proto.Uint32(0x3), Flags: proto.Uint32(mapAnon | mapPrivate), Status: proto.Uint32(1), Fd: proto.Int64(-1)},
	})

	stats, err := EncodeResidualMap(srcDir, filepath.Join(tmp, "residual"), base.Manifest())
	if err != nil {
		t.Fatal(err)
	}
	// The vdso page is a manifest hit content-wise, but must count as Eager,
	// not Hit - and the ordinary page (a genuine miss) stays lazy-eligible.
	if stats.TotalPages != 2 || stats.Eager != 1 || stats.Hits != 0 || stats.Misses != 1 {
		t.Fatalf("stats = %+v, want total=2 eager=1 hits=0 misses=1", stats)
	}

	residualBytes, err := os.ReadFile(filepath.Join(tmp, "residual", "pages-200.residual"))
	if err != nil {
		t.Fatal(err)
	}
	// magic(4) + header(13) + eager(1 tag + 4096 bytes) + miss(1 tag).
	wantSize := 4 + 13 + (1 + PageSize) + 1
	if len(residualBytes) != wantSize {
		t.Fatalf("residual size = %d, want %d", len(residualBytes), wantSize)
	}
}
