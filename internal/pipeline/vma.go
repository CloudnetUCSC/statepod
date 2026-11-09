package pipeline

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/checkpoint-restore/go-criu/v7/crit"
	"github.com/checkpoint-restore/go-criu/v7/crit/images/mm"
)

// Linux mmap(2) flag bits (uapi/asm-generic/mman-common.h) — stable ABI,
// not versioned by CRIU.
const (
	mapPrivate = 0x02
	mapAnon    = 0x20
	mapLocked  = 0x2000
	mapHugetlb = 0x40000
)

// CRIU VMA status bits (criu/include/image.h:85-98).
const (
	vmaAreaStack    = 1 << 1
	vmaAreaVsyscall = 1 << 2
	vmaAreaVdso     = 1 << 3
	vmaAreaVvar     = 1 << 12
)

// vmaRange is one VMA's address range plus the flags/status needed to decide
// lazy-restore eligibility.
type vmaRange struct {
	Start, End    uint64
	Flags, Status uint32
}

// canBeLazy mirrors criu's vma_entry_can_be_lazy (criu/include/vma.h:123)
// plus the dump-side stack exclusion (criu/mem.c:247, `!is_stack(...)`) — a
// page in a VMA that fails this can NEVER be served via userfaultfd, at any
// point, so it must be present in the images dir before restore starts (no
// deferring, no lazy fallback). Found the hard way in the VM
// test: a vdso page marked PE_LAZY made restore crash ("Invalid ELF magic")
// because CRIU never registers vdso/vvar/vsyscall/stack/locked/hugetlb pages
// for lazy fault-in at all — they're simply never filled in.
func (v vmaRange) canBeLazy() bool {
	return v.Flags&mapAnon != 0 && v.Flags&mapPrivate != 0 &&
		v.Flags&mapLocked == 0 && v.Flags&mapHugetlb == 0 &&
		v.Status&vmaAreaVdso == 0 && v.Status&vmaAreaVvar == 0 &&
		v.Status&vmaAreaVsyscall == 0 && v.Status&vmaAreaStack == 0
}

// readVMAs parses mm-<pid>.img's VMA list (mm.proto field 14, embedded in the
// single MmEntry that file holds).
func readVMAs(dir string, pid int) ([]vmaRange, error) {
	path := filepath.Join(dir, fmt.Sprintf("mm-%d.img", pid))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	img, err := crit.New(f, nil, "", false, false).Decode(&mm.MmEntry{})
	if err != nil {
		return nil, fmt.Errorf("decoding %s: %w", path, err)
	}
	if len(img.Entries) == 0 {
		return nil, fmt.Errorf("%s: empty mm image", path)
	}
	me, ok := img.Entries[0].Message.(*mm.MmEntry)
	if !ok {
		return nil, fmt.Errorf("%s: entry is %T, want MmEntry", path, img.Entries[0].Message)
	}

	out := make([]vmaRange, 0, len(me.Vmas))
	for _, v := range me.Vmas {
		out = append(out, vmaRange{Start: v.GetStart(), End: v.GetEnd(), Flags: v.GetFlags(), Status: v.GetStatus()})
	}
	return out, nil
}

// lazyEligible reports whether vaddr falls in a VMA CRIU will allow to be
// lazily restored. A page outside every known VMA (shouldn't happen for
// anything a pagemap describes) is conservatively treated as ineligible —
// safer to ship its bytes than to guess.
func lazyEligible(vmas []vmaRange, vaddr uint64) bool {
	for _, v := range vmas {
		if vaddr >= v.Start && vaddr < v.End {
			return v.canBeLazy()
		}
	}
	return false
}

// CRIU VMA status bits used to name memory regions (criu/include/image.h).
const (
	vmaAreaHeap    = 1 << 5
	vmaFilePrivate = 1 << 6
	vmaFileShared  = 1 << 7
	vmaAnonShared  = 1 << 8
	vmaAnonPrivate = 1 << 9
	vmaAreaSysvipc = 1 << 10
)

// VMA is one mapping of a dumped process, exported for analysis tools
// (pagecmp's composition breakdown).
type VMA struct {
	Start, End    uint64
	Flags, Status uint32
}

// Region names the kind of memory a VMA is, for reporting where dumped pages
// come from: heap (brk), stack, anon (private anonymous mmap: allocator arenas,
// runtime heaps), file-cow (private file mappings written to: relocations,
// .data), shared (anonymous shared / SysV / file shared), other (vdso etc.).
func (v VMA) Region() string {
	switch {
	case v.Status&vmaAreaHeap != 0:
		return "heap"
	case v.Status&vmaAreaStack != 0:
		return "stack"
	case v.Status&(vmaAnonShared|vmaAreaSysvipc|vmaFileShared) != 0:
		return "shared"
	case v.Status&vmaFilePrivate != 0:
		return "file-cow"
	case v.Status&vmaAnonPrivate != 0:
		return "anon"
	default:
		return "other"
	}
}

// ReadVMAs returns the VMAs of process pid in checkpoint directory dir.
func ReadVMAs(dir string, pid int) ([]VMA, error) {
	in, err := readVMAs(dir, pid)
	if err != nil {
		return nil, err
	}
	out := make([]VMA, len(in))
	for i, v := range in {
		out[i] = VMA{Start: v.Start, End: v.End, Flags: v.Flags, Status: v.Status}
	}
	return out, nil
}
