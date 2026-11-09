package pipeline

import "fmt"

// A lazy page is identified by (task, vaddr), not vaddr alone: in a multi-process
// container the same virtual address holds different content in different tasks
// (copy-on-write after fork; PostgreSQL's workers), and CRIU's page-server requests
// name the task (dst_id). The M5 machinery (residual set, drain channel, page store)
// carries one opaque uint64 per page, so that uint64 is a composite: the pagemap's
// ORDINAL (its index in ReadPagemaps order, identical on source and destination
// because both hold the same pagemap files) above bit 47, and the real vaddr below.
// User-space x86-64 addresses are < 2^47, so the composition is lossless; an
// address above that (vsyscall) is never lazy-eligible and is rejected here.
const (
	addrOrdinalShift = 47
	maxComposedVaddr = uint64(1) << addrOrdinalShift
	maxPagemapCount  = 1 << 16
)

// ComposeAddr builds the opaque page identity for (pagemap ordinal, vaddr).
func ComposeAddr(ordinal int, vaddr uint64) (uint64, error) {
	if ordinal < 0 || ordinal >= maxPagemapCount {
		return 0, fmt.Errorf("pagemap ordinal %d out of range", ordinal)
	}
	if vaddr >= maxComposedVaddr {
		return 0, fmt.Errorf("vaddr 0x%x is outside the 47-bit user address space", vaddr)
	}
	return uint64(ordinal)<<addrOrdinalShift | vaddr, nil
}

// SplitAddr is the inverse of ComposeAddr.
func SplitAddr(a uint64) (ordinal int, vaddr uint64) {
	return int(a >> addrOrdinalShift), a & (maxComposedVaddr - 1)
}

// PagemapOrdinals maps each pagemap key (pid, or the negative shm id) of the dump
// in dir to its ordinal: the index in ReadPagemaps order.
func PagemapOrdinals(dir string) (map[int]int, error) {
	pms, err := ReadPagemaps(dir)
	if err != nil {
		return nil, err
	}
	out := make(map[int]int, len(pms))
	for i, pm := range pms {
		out[pm.Pid] = i
	}
	return out, nil
}

// DecodePageServerDst maps the dst_id of a page-server request from `criu lazy-pages`
// to a pagemap key as ReadPagemaps reports it. The lazy-pages daemon names the TASK
// by its plain pid (measured 2026-10-06: dst_id 0xea = pid 234); CRIU's dump-side
// encoding (id << 8 | type) is not used on this path, and an earlier version of this
// function assumed it and refused every request. Only tasks are ever lazy (shared
// memory is MAP_SHARED, hence eager), so a shm id never arrives here.
func DecodePageServerDst(dstID uint64) (key int, err error) {
	if dstID == 0 || dstID > 1<<31 {
		return 0, fmt.Errorf("implausible page-server dst_id 0x%x (expected a task pid)", dstID)
	}
	return int(dstID), nil
}
