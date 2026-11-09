package migration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/transport"
)

// checkpointRoot is the process-cwd-relative dir CRIU images are written to
// and read from on both source and destination .
const checkpointRoot = "./checkpoint"

// rsyncPort is only consulted by the precopy-rsync/sync-rsync modes (transport
// A/B); empty falls back to DefaultRsyncPort, matching the
// receiver's own default when its --rsync-port flag is left unset.
// precopyRounds is the number of chained --pre-dump rounds before the freeze
// (0 = single freeze dump), shared by PreCopyStrategy and SynthPreCopyStrategy.
// The CLI layer resolves the mode-dependent default (4 for precopy/criu-incr/
// precopy-rsync to preserve the historical Baseline 2/3 shape, 0 for
// synth-precopy) before calling in here — see internal/cli/migrate.go.
// zstdLevel is the compression stage, synth-precopy only; 0 disables it.
// tmpfsCheckpoint is the toggle: true mounts the
// checkpoint dir as tmpfs on this host so CRIU's image I/O never touches disk.
// disableFSUpper is the ablation toggle for M4:
// synth-precopy only, skips the overlay upperdir stream entirely.
// drainTimeout is synth-postcopy only: how long to
// wait for the destination's drain-complete signal before giving up and
// thawing the source.
// finalMode is synth-precopy and synth-postcopy: "" /
// "catchup" (default) or "availability".
func Migrate(ctx context.Context, mode, dest, rsyncPort string, precopyRounds, zstdLevel int, tmpfsCheckpoint, disableFSUpper bool, drainTimeout time.Duration, finalMode string, mergeRestore bool, baselineZstdSet bool, fsSync string) error {
	cfg, err := container.ReadConfig()
	if err != nil {
		return fmt.Errorf("failed to read statepod config: %w", err)
	}

	fmt.Println("Migrating container:", cfg.ContainerID)
	fmt.Println("Mode:", mode)
	fmt.Println("Destination:", dest)

	// Initialize the transport client and connect to the destination server
	client := transport.NewTCPClient()
	if err := client.Connect(ctx, dest); err != nil {
		return fmt.Errorf("failed to connect to destination: %w", err)
	}
	defer client.Close()

	// The plain precopy/criu-incr baselines are uncompressed unless the caller
	// explicitly asked for zstd; synth-precopy always uses its own.
	baselineZstd := 0
	if baselineZstdSet {
		baselineZstd = zstdLevel
	}

	switch fsSync {
	case "":
	case "flat", "rsync":
		if mode != "precopy" && mode != "criu-incr" {
			return fmt.Errorf("--fs-sync applies to the plain precopy and criu-incr baselines only (synth-precopy, synth-postcopy and criu-lazy always move the upper layer in rsync rounds, sync ships the data directory), not %s", mode)
		}
	default:
		return fmt.Errorf("invalid --fs-sync %q (want \"flat\" or \"rsync\")", fsSync)
	}

	// Select the migration strategy
	var strategy Strategy
	switch mode {
	case "precopy":
		strategy = &PreCopyStrategy{PreCopyRounds: precopyRounds, TmpfsCheckpoint: tmpfsCheckpoint, ZstdLevel: baselineZstd, FSSync: fsSync, RsyncPort: rsyncPort}
	case "criu-incr":
		strategy = &PreCopyStrategy{AutoDedup: true, PreCopyRounds: precopyRounds, TmpfsCheckpoint: tmpfsCheckpoint, ZstdLevel: baselineZstd, FSSync: fsSync, RsyncPort: rsyncPort}
	case "precopy-rsync":
		strategy = &PreCopyStrategy{Transport: "rsync", RsyncPort: rsyncPort, PreCopyRounds: precopyRounds, TmpfsCheckpoint: tmpfsCheckpoint}
	case "synth-precopy":
		strategy = &SynthPreCopyStrategy{PreCopyRounds: precopyRounds, ZstdLevel: zstdLevel, TmpfsCheckpoint: tmpfsCheckpoint, DisableFSUpper: disableFSUpper, FinalMode: finalMode, MergeRestore: mergeRestore, RsyncPort: rsyncPort}
	case "criu-lazy":
		strategy = &CriuLazyStrategy{TmpfsCheckpoint: tmpfsCheckpoint, DrainTimeout: max(drainTimeout, 10*time.Minute), RsyncPort: rsyncPort}
	case "synth-postcopy":
		strategy = &SynthPostCopyStrategy{DrainTimeout: drainTimeout, FinalMode: finalMode, RsyncPort: rsyncPort}
	case "postcopy":
		strategy = &PostCopyStrategy{}
	case "hybrid":
		strategy = &HybridStrategy{}
	case "sync":
		strategy = &SyncStrategy{}
	case "sync-rsync":
		strategy = &SyncStrategy{Transport: "rsync", RsyncPort: rsyncPort}
	case "restart-volume":
		// volume-sync restart baseline : pre-synced writable layer,
		// graceful stop, cold start from disk at the destination
		strategy = &VolumeRestartStrategy{RsyncPort: rsyncPort, ZstdLevel: baselineZstd}
	default:
		return fmt.Errorf("invalid migration mode: %s", mode)
	}

	// Execute the selected strategy
	if err := strategy.Execute(ctx, cfg, client, dest); err != nil {
		return fmt.Errorf("migration strategy failed: %w", err)
	}

	fmt.Println("Migration completed successfully.")
	return nil
}

// ensureCheckpointDirs (re)creates the pre0..pre3/final dump dirs under
// checkpointRoot. tmpfs selects rung 1 of the disk-free ladder
// : checkpointRoot itself is mounted as tmpfs rather than
// wiped, since RemoveAll-ing a mount point fails with "device busy" — only
// its contents are cleared per round.
func ensureCheckpointDirs(tmpfs bool) error {
	if err := EnsureCheckpointRoot(checkpointRoot, tmpfs); err != nil {
		return err
	}
	rounds := []string{"pre0", "pre1", "pre2", "pre3", "final"}
	for _, round := range rounds {
		dir := filepath.Join(checkpointRoot, round)
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("failed to remove existing checkpoint directory %s: %w", dir, err)
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create checkpoint directory %s: %w", dir, err)
		}
	}
	return nil
}
