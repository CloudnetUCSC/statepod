package migration

import (
	"fmt"
	"os"
	"os/exec"
)

// defaultTmpfsSize bounds the RAM-backed checkpoint dir (rung 1):
// sized for a ~1 GB destination, where dumps were single-digit MBs.
// STATEPOD_TMPFS_SIZE (a tmpfs size= value, e.g. "50%") overrides it: on the lab
// VMs a 64 MB workload that rewrites itself every round needs ~540 MB of rounds
// and deltas ("no space left on device", 2026-10-27), a 1 GB one several GB.
const defaultTmpfsSize = "512m"

func tmpfsSize() string {
	if v := os.Getenv("STATEPOD_TMPFS_SIZE"); v != "" {
		return v
	}
	return defaultTmpfsSize
}

// EnsureCheckpointRoot prepares dir as the checkpoint root shared by every
// migration mode, on either the source or the destination host. With
// tmpfs=true it's mounted as tmpfs — idempotent, a no-op if already mounted
// — so CRIU's image writes on this host never touch disk. With tmpfs=false
// any prior tmpfs mount left over from an earlier run is torn down so dir
// falls back to the plain pre-rung-1 disk directory.
func EnsureCheckpointRoot(dir string, tmpfs bool) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create checkpoint root %s: %w", dir, err)
	}

	mounted, err := isMounted(dir)
	if err != nil {
		return err
	}

	switch {
	case tmpfs && !mounted:
		if out, err := exec.Command("sudo", "mount", "-t", "tmpfs", "-o", "size="+tmpfsSize(), "tmpfs", dir).CombinedOutput(); err != nil {
			return fmt.Errorf("failed to mount tmpfs at %s: %w (%s)", dir, err, out)
		}
	case !tmpfs && mounted:
		if out, err := exec.Command("sudo", "umount", dir).CombinedOutput(); err != nil {
			return fmt.Errorf("failed to unmount tmpfs at %s: %w (%s)", dir, err, out)
		}
	}
	return nil
}

// isMounted reports whether dir is itself a mount point (any filesystem —
// the checkpoint root is only ever mounted by ensureCheckpointRoot, so a hit
// here always means our own tmpfs mount from a prior run).
func isMounted(dir string) (bool, error) {
	err := exec.Command("mountpoint", "-q", dir).Run()
	if err == nil {
		return true, nil
	}
	if _, ok := err.(*exec.ExitError); ok {
		return false, nil
	}
	return false, fmt.Errorf("mountpoint check failed for %s: %w", dir, err)
}
