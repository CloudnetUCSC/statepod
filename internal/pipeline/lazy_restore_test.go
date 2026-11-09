package pipeline

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/checkpoint-restore/go-criu/v7/crit/images/vma"
	"google.golang.org/protobuf/proto"
)

// TestBuildLazyRestoreDir_Classification exercises the happy path: pages that
// match the baseline both by address and content become PE_PARENT (0 bytes,
// resolved from the parent chain); everything else becomes PE_PRESENT|PE_LAZY
// (0 bytes, deferred to userfaultfd).
func TestBuildLazyRestoreDir_Classification(t *testing.T) {
	tmp := t.TempDir()

	baseA, baseB, baseC := mkPage(10), mkPage(20), mkPage(30)
	baselineDir := filepath.Join(tmp, "baseline")
	writeDump(t, baselineDir, 1, 100, 0x1000, [][]byte{baseA, baseB, baseC})
	baseline, err := LoadBaseline(baselineDir, "sha256:img")
	if err != nil {
		t.Fatal(err)
	}
	m := baseline.Manifest()

	// Source dump: A, B, C at the SAME vaddrs as the baseline (hits), plus
	// two misses at addresses the baseline doesn't have at all.
	x, y := mkPage(40), mkPage(50)
	srcDir := filepath.Join(tmp, "src")
	writeChainedDump(t, srcDir, 1, 200, []specEntry{
		{vaddr: 0x1000, nrPages: 3, present: true}, // A, B, C
		{vaddr: 0x9000, nrPages: 2, present: true}, // X, Y
	}, [][]byte{baseA, baseB, baseC, x, y})

	residualDir := filepath.Join(tmp, "residual")
	if _, err := EncodeResidualMap(srcDir, residualDir, m); err != nil {
		t.Fatal(err)
	}

	outDir := filepath.Join(tmp, "restore")
	stats, err := BuildLazyRestoreDir(residualDir, baselineDir, outDir, baseline)
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalPages != 5 || stats.Parent != 3 || stats.Lazy != 2 {
		t.Fatalf("stats = %+v, want total=5 parent=3 lazy=2", stats)
	}

	// pages-*.img must be empty: neither PE_PARENT nor PE_LAZY needs local bytes.
	pagesData, err := os.ReadFile(pagesPath(outDir, 200))
	if err != nil {
		t.Fatal(err)
	}
	if len(pagesData) != 0 {
		t.Fatalf("pages-200.img = %d bytes, want 0 (all pages are parent or lazy)", len(pagesData))
	}

	// The parent symlink must be relative (an absolute path is unreliable at
	// restore time, criu/image.c:752) and must resolve to the baseline dir.
	link, err := os.Readlink(filepath.Join(outDir, ParentLinkName))
	if err != nil {
		t.Fatal(err)
	}
	if filepath.IsAbs(link) {
		t.Fatalf("parent symlink is absolute (%s) — criu warns this may not work on restore", link)
	}
	resolved := filepath.Clean(filepath.Join(outDir, link))
	wantBaseline := filepath.Clean(baselineDir)
	if resolved != wantBaseline {
		t.Fatalf("parent symlink resolves to %s, want %s", resolved, wantBaseline)
	}

	// Verify the flags landed correctly. Contiguous same-flag pages are
	// merged into one entry (matching real CRIU dump shape — one entry per
	// page sent a real `criu lazy-pages` restore into a multi-GB logging
	// spiral in the VM test), so check by address, not raw entry count.
	outPms, err := ReadPagemaps(outDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(outPms[0].Entries) != 2 {
		t.Fatalf("got %d entries, want 2 (contiguous runs merged: 3 parent + 2 lazy)", len(outPms[0].Entries))
	}
	for _, vaddr := range []uint64{0x1000, 0x2000, 0x3000} {
		if got := flagsAt(t, outPms[0].Entries, vaddr); got != peParent {
			t.Fatalf("flags at %#x = %#x, want PE_PARENT", vaddr, got)
		}
	}
	for _, vaddr := range []uint64{0x9000, 0xa000} {
		// PE_LAZY alone, not PE_PRESENT|PE_LAZY: real CRIU only adds
		// PE_PRESENT alongside PE_LAZY when it actually transferred the
		// page's bytes (page-xfer.c ppb_xfer_flags) - we never do, so
		// claiming PE_PRESENT here caused a real multi-GB restore hang
		// (some read path expected bytes this pages file doesn't have).
		if got := flagsAt(t, outPms[0].Entries, vaddr); got != peLazy {
			t.Fatalf("flags at %#x = %#x, want PE_LAZY (not PE_PRESENT|PE_LAZY)", vaddr, got)
		}
	}
}

// flagsAt returns the flags of the entry covering vaddr, failing the test if
// none does.
func flagsAt(t *testing.T, entries []Entry, vaddr uint64) uint32 {
	t.Helper()
	for _, e := range entries {
		if vaddr >= e.Vaddr && vaddr < e.Vaddr+e.NrPages*PageSize {
			return e.Flags
		}
	}
	t.Fatalf("no entry covers vaddr %#x", vaddr)
	return 0
}

// TestBuildLazyRestoreDir_AddressGate is the address-gate regression guard: a page
// whose content hash matches the baseline, but at a DIFFERENT address than
// where the baseline has it, must NOT be marked PE_PARENT — CRIU's in_parent
// lookup is address-only with no content check, so this would either fail the
// restore (observed live in the spike: "Missing <vaddr> in parent
// pagemap") or, worse, silently resolve to whatever unrelated page happens to
// live at that address in the parent.
func TestBuildLazyRestoreDir_AddressGate(t *testing.T) {
	tmp := t.TempDir()

	shared := mkPage(77) // same content, different address in each dump
	baselineDir := filepath.Join(tmp, "baseline")
	writeDump(t, baselineDir, 1, 100, 0x9000, [][]byte{shared})
	baseline, err := LoadBaseline(baselineDir, "sha256:img")
	if err != nil {
		t.Fatal(err)
	}
	m := baseline.Manifest()

	srcDir := filepath.Join(tmp, "src")
	writeDump(t, srcDir, 7, 200, 0x1000, [][]byte{shared}) // same content, address 0x1000

	residualDir := filepath.Join(tmp, "residual")
	stats, err := EncodeResidualMap(srcDir, residualDir, m)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Hits != 1 {
		t.Fatalf("expected a manifest content-hash hit (the M1 predicate), got %+v", stats)
	}

	outDir := filepath.Join(tmp, "restore")
	lstats, err := BuildLazyRestoreDir(residualDir, baselineDir, outDir, baseline)
	if err != nil {
		t.Fatal(err)
	}
	if lstats.Parent != 0 || lstats.Lazy != 1 {
		t.Fatalf("content-hash hit at the wrong address must not become PE_PARENT: stats = %+v", lstats)
	}

	outPms, err := ReadPagemaps(outDir)
	if err != nil {
		t.Fatal(err)
	}
	if outPms[0].Entries[0].Flags != peLazy {
		t.Fatalf("flags = %#x, want PE_LAZY (address mismatch must fall back to lazy)", outPms[0].Entries[0].Flags)
	}
}

// TestBuildLazyRestoreDir_EagerPassThrough is the end-to-end regression guard
// for the vdso failure found live in the VM test: a non-lazy-eligible page
// must land in the final restore dir as plain PE_PRESENT with its real bytes
// intact, never PE_LAZY (there is no fault-time opportunity to fill it in)
// and never PE_PARENT (Task 1 never gives it a chance to be one — see
// residual.go's doc comment on the eager/hit trade-off).
func TestBuildLazyRestoreDir_EagerPassThrough(t *testing.T) {
	tmp := t.TempDir()

	vdsoPage := mkPage(1)
	baselineDir := filepath.Join(tmp, "baseline")
	writeDump(t, baselineDir, 1, 100, 0x1000, [][]byte{vdsoPage})
	baseline, err := LoadBaseline(baselineDir, "sha256:img")
	if err != nil {
		t.Fatal(err)
	}

	srcDir := filepath.Join(tmp, "src")
	writeDump(t, srcDir, 7, 200, 0x1000, [][]byte{vdsoPage})
	writeMM(t, srcDir, 7, []*vma.VmaEntry{
		{Start: proto.Uint64(0x1000), End: proto.Uint64(0x2000), Pgoff: proto.Uint64(0), Shmid: proto.Uint64(0),
			Prot: proto.Uint32(0x1), Flags: proto.Uint32(0), Status: proto.Uint32(1 | vmaAreaVdso), Fd: proto.Int64(-1)},
	})

	residualDir := filepath.Join(tmp, "residual")
	if _, err := EncodeResidualMap(srcDir, residualDir, baseline.Manifest()); err != nil {
		t.Fatal(err)
	}

	outDir := filepath.Join(tmp, "restore")
	stats, err := BuildLazyRestoreDir(residualDir, baselineDir, outDir, baseline)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Eager != 1 || stats.Parent != 0 || stats.Lazy != 0 {
		t.Fatalf("stats = %+v, want eager=1 parent=0 lazy=0", stats)
	}

	outPms, err := ReadPagemaps(outDir)
	if err != nil {
		t.Fatal(err)
	}
	if outPms[0].Entries[0].Flags != pePresent {
		t.Fatalf("flags = %#x, want plain PE_PRESENT", outPms[0].Entries[0].Flags)
	}
	got, err := os.ReadFile(pagesPath(outDir, 200))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, vdsoPage) {
		t.Fatal("eager page bytes not passed through intact")
	}
}

// A task whose pid the baseline does not have must get NO in_parent pages even where
// address and content match another task's: CRIU would look in the parent dump's
// pagemap of the same pid and fail ("No parent for snapshot pagemap" on PostgreSQL's
// workers). Everything becomes lazy instead.
func TestBuildLazyRestoreDir_UnknownPidIsNeverParent(t *testing.T) {
	tmp := t.TempDir()
	baseA, baseB := mkPage(10), mkPage(20)
	baselineDir := filepath.Join(tmp, "baseline")
	writeDump(t, baselineDir, 1, 100, 0x1000, [][]byte{baseA, baseB})
	baseline, err := LoadBaseline(baselineDir, "sha256:img")
	if err != nil {
		t.Fatal(err)
	}
	srcDir := filepath.Join(tmp, "src")
	writeChainedDump(t, srcDir, 261, 200, []specEntry{
		{vaddr: 0x1000, nrPages: 2, present: true}, // same address and content as pid 1's pages
	}, [][]byte{baseA, baseB})
	residualDir := filepath.Join(tmp, "residual")
	if _, err := EncodeResidualMap(srcDir, residualDir, baseline.Manifest()); err != nil {
		t.Fatal(err)
	}
	stats, err := BuildLazyRestoreDir(residualDir, baselineDir, filepath.Join(tmp, "restore"), baseline)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Parent != 0 || stats.Lazy != 2 {
		t.Fatalf("stats = %+v, want parent=0 lazy=2 for a pid the baseline lacks", stats)
	}
}
