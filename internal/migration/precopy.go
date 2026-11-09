package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/compress"
	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/transport"
)

// DefaultRsyncPort is the port the receiver's companion rsync daemon listens
// on for the precopy-rsync/sync-rsync transport A/B (addendum).
const DefaultRsyncPort = "8730"

// PreCopyStrategy implements the checkpoint-based live migration path.
// AutoDedup toggles CRIU's own --auto-dedup on each pre-dump iteration,
// turning this into the stock-CRIU-incremental baseline
// instead of the plain precopy baseline.
// Transport selects how the checkpoint img dirs move to the destination:
// "" / "tar" = statepod's own tar+custom-TCP transport (default); "rsync" =
// rsync against the receiver's companion daemon (addendum transport
// A/B — same CRIU-produced files either way).
// PreCopyRounds is the number of chained --pre-dump rounds before the final
// freeze (0 = single freeze dump), same knob as SynthPreCopyStrategy's — see
// its doc comment for why rounds cost bytes on this testbed. The CLI layer
// (cmd/statepod, internal/cli/migrate.go) defaults this to 4 when the user
// doesn't pass --precopy-rounds explicitly, preserving the historical
// Baseline 2/3 shape (pre0..pre3 + final, documented in the design)
// ; pass --precopy-rounds 0 for the single-shot ablation that
// isolates a baseline round's cost from synth-precopy's own round count.
type PreCopyStrategy struct {
	AutoDedup     bool
	Transport     string
	RsyncPort     string
	PreCopyRounds int
	// ZstdLevel compresses each checkpoint stream with zstd, giving
	// the "vanilla + zstd" / "criu-incr + zstd" baselines: it separates the
	// share of the pipeline's reduction that is plain compression from what the
	// baseline-reconciliation mechanisms add. 0 (default) = uncompressed, the
	// historical Baseline 2/3. The receiver detects the zstd frame itself.
	ZstdLevel int
	// TmpfsCheckpoint is the toggle: mounts the
	// checkpoint dir as tmpfs on this host instead of plain disk.
	TmpfsCheckpoint bool
	// FSSync ("" = off, "flat") ships the writable (overlay upper) layer after
	// the final dump; see the comment at its use in Execute. Needed for a
	// correct baseline whenever the workload keeps state in the container
	// filesystem.
	FSSync string
}

func (s *PreCopyStrategy) Execute(ctx context.Context, cfg *container.Config, destClient transport.Client, destAddr string) error {
	mode := "precopy"
	if s.AutoDedup {
		mode = "criu-incr"
	} else if s.Transport == "rsync" {
		mode = "precopy-rsync"
	}
	fmt.Printf("Executing Pre-Copy Strategy (mode=%s) for container: %s\n", mode, cfg.ContainerID)
	metrics := &MigrationMetrics{
		Mode:               mode,
		ContainerID:        cfg.ContainerID,
		MigrationStartTime: time.Now(),
	}

	// Helper to calculate directory size
	getDirSize := func(path string) (int64, error) {
		var size int64
		err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				size += info.Size()
			}
			return nil
		})
		return size, err
	}

	// lastWire is the byte count that actually crossed the network for the most
	// recent streamDir call (== the tar size when uncompressed; the zstd output
	// size when ZstdLevel > 0). TotalDataTransferredBytes is corrected to it.
	var lastWire int64

	// We'll define a quick helper to stream a directory over the client
	streamDir := func(dirPath string, streamType string) (float64, int64, error) {
		fmt.Printf("Streaming %s as %s...\n", dirPath, streamType)

		// Reconnect for each stream because our protocol uses EOF to signal end-of-stream
		if err := destClient.Connect(ctx, destAddr); err != nil {
			return 0, 0, fmt.Errorf("failed to reconnect for stream %s: %w", streamType, err)
		}

		// For MVP, we can tar the directory and stream the stdout
		// Use sudo to ensure we can read files with restricted permissions (like 600)
		cmd := exec.CommandContext(ctx, "sudo", "tar", "-cf", "-", "-C", dirPath, ".")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return 0, 0, err
		}

		if err := cmd.Start(); err != nil {
			return 0, 0, err
		}

		transferStart := time.Now()
		wire := &countingReader{r: compress.Compress(stdout, s.ZstdLevel)}
		if err := destClient.SendStream(ctx, streamType, wire); err != nil {
			return 0, 0, err
		}
		lastWire = wire.n
		transferTime := time.Since(transferStart)

		if err := cmd.Wait(); err != nil {
			fmt.Printf("tar error: %s\n", stderr.String())
			return 0, 0, fmt.Errorf("tar failed for %s: %w", streamType, err)
		}

		sz, _ := getDirSize(dirPath)
		bandwidth := float64(sz*8) / (transferTime.Seconds() * 1000000) // Mbps
		fmt.Printf("Transfer %s: size=%d bytes, time=%v, bandwidth=%.2f Mbps\n", streamType, sz, transferTime, bandwidth)

		return float64(transferTime.Milliseconds()), sz, nil
	}

	// rsync-transport alternative to streamDir (addendum): pushes the
	// same CRIU-produced directory to the receiver's companion rsync daemon
	// instead of tar+custom-TCP. -a (archive, so delta-matching against
	// anything the receiver already has under this module applies) plus -z
	// (compression) — the realistic "just rsync it" usage, not an isolated
	// ablation of the delta algorithm alone; report bytes/time as measured,
	// flags and all.
	rsyncPush := func(dirPath string, streamType string) (float64, int64, error) {
		rsyncPort := s.RsyncPort
		if rsyncPort == "" {
			rsyncPort = DefaultRsyncPort
		}
		destHost := strings.SplitN(destAddr, ":", 2)[0]
		target := fmt.Sprintf("rsync://%s:%s/checkpoint/%s/", destHost, rsyncPort, streamType)
		fmt.Printf("rsync-pushing %s to %s...\n", dirPath, target)

		cmd := exec.CommandContext(ctx, "sudo", "rsync", "-az", "--stats", dirPath+"/", target)
		cmd.Env = append(os.Environ(), "LC_ALL=C")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		transferStart := time.Now()
		if err := cmd.Run(); err != nil {
			return 0, 0, fmt.Errorf("rsync push failed for %s: %s: %w", streamType, stderr.String(), err)
		}
		transferTime := time.Since(transferStart)

		sentBytes := parseRsyncSentBytes(stdout.String())
		bandwidth := float64(sentBytes*8) / (transferTime.Seconds() * 1000000)
		fmt.Printf("rsync transfer %s: sent=%d bytes, time=%v, bandwidth=%.2f Mbps\n", streamType, sentBytes, transferTime, bandwidth)

		return float64(transferTime.Milliseconds()), sentBytes, nil
	}
	transfer := streamDir
	if s.Transport == "rsync" {
		transfer = rsyncPush
	}

	// Helper to stream a single file
	streamFile := func(filePath string, streamType string) error {
		fmt.Printf("Streaming %s as %s...\n", filePath, streamType)
		if err := destClient.Connect(ctx, destAddr); err != nil {
			return fmt.Errorf("failed to reconnect for stream %s: %w", streamType, err)
		}

		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("failed to open metadata file %s: %w", filePath, err)
		}
		defer file.Close()

		return destClient.SendStream(ctx, streamType, file)
	}

	// Ensure checkpoint directories exist
	if err := ensureCheckpointDirs(s.TmpfsCheckpoint); err != nil {
		return err
	}

	// --fs-sync=rsync: the filesystem pre-copy every variant uses (fsprecopy.go),
	// bulk first (same order as synth-precopy): live rounds before the memory
	// rounds, a short catch-up before the freeze, the final round after it.
	fsr := newFSPrecopy(ctx, destClient, destAddr, s.RsyncPort, s.ZstdLevel, metrics)
	if s.FSSync == "rsync" && cfg.Stateful {
		metrics.Mark("src_fs_precopy_start")
		if err := fsr.precopyLive(); err != nil {
			return fmt.Errorf("filesystem pre-copy failed, source untouched: %w", err)
		}
		metrics.Mark("src_fs_precopy_end")
	}

	// Chained pre-dump rounds (0 = none, single freeze dump below carries
	// everything). Matches SynthPreCopyStrategy's round structure so the two
	// can be compared at matched round counts.
	for i := 0; i < s.PreCopyRounds; i++ {
		dumpDir := fmt.Sprintf("./checkpoint/pre%d", i)
		fmt.Printf("Starting pre-dump round %d\n", i)
		args := []string{"runc", "checkpoint", cfg.ContainerID, "--pre-dump", "--image-path", dumpDir}
		if i > 0 {
			args = append(args, "--parent-path", fmt.Sprintf("./../pre%d", i-1))
			if s.AutoDedup {
				// De-dupes the *parent* image (pre{i-1}) against pre{i}, not the
				// image being streamed this round — CRIU's own incremental
				// mechanism, measured as-is (Baseline 3).
				args = append(args, "--auto-dedup")
			}
		}
		cmd := sudoRunc(ctx, args...)
		iterStart := metrics.Mark(fmt.Sprintf("src_predump_%d_start", i))
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("pre-dump round %d failed: %w", i, err)
		}
		metrics.Mark(fmt.Sprintf("src_predump_%d_end", i))
		sz, _ := getDirSize(dumpDir)
		metrics.Iterations = append(metrics.Iterations, PreCopyIterationInfo{
			Iteration:         i,
			Duration:          float64(time.Since(iterStart).Milliseconds()),
			SizeInBytes:       sz,
			TransferStartTime: time.Now(),
		})
		metrics.TotalDataTransferredBytes += sz

		ts, sz, err := transfer(dumpDir, fmt.Sprintf("pre%d", i))
		if err != nil {
			return fmt.Errorf("failed to stream pre%d: %w", i, err)
		}
		if s.ZstdLevel > 0 && s.Transport != "rsync" {
			metrics.TotalDataTransferredBytes += lastWire - sz // count wire bytes, not tar size
		}
		metrics.Mark(fmt.Sprintf("src_ship_pre%d_end", i))
		metrics.Iterations[i].TransferTime = ts
		metrics.Iterations[i].Bandwidth = float64(sz*8) / (ts / 1000.0 * 1000000)
	}

	if s.FSSync == "rsync" && cfg.Stateful {
		// short catch-up of what changed during the memory rounds
		if err := fsr.catchUp("live-last"); err != nil {
			return fmt.Errorf("filesystem pre-copy failed, source untouched: %w", err)
		}
	}

	fmt.Println("Starting final freeze dump")
	// Final pre-dump (freezes the container). With rounds, it carries only
	// what changed since the last round; without them it's a full dump.
	finalArgs := []string{"runc", "checkpoint", cfg.ContainerID, "--image-path", "./checkpoint/final", "--leave-running=false", "--tcp-established"}
	if s.PreCopyRounds > 0 {
		finalArgs = append(finalArgs, "--parent-path", fmt.Sprintf("./../pre%d", s.PreCopyRounds-1))
	}
	cmd := sudoRunc(ctx, finalArgs...)
	metrics.FreezeStartTime = metrics.Mark("src_final_dump_start")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("final dump failed: %w", err)
	}
	metrics.Mark("src_final_dump_end")

	// Filesystem baseline: plain CRIU migration carries no files
	// from the container's writable layer, so a workload that keeps state there
	// (PostgreSQL's data directory, the synthetic workload's files) restores
	// against missing or stale files. --fs-sync=flat is the plain stop-and-copy
	// answer: with the process dead (consistent, nothing to race) tar the whole
	// upper layer and ship it before the final stream, inside the downtime window.
	// This is the baseline M4 (pre-staged, resent only if changed) is measured
	// against; it is a labelled variant, not part of the historical Baseline 2/3.
	if s.FSSync == "rsync" && cfg.Stateful {
		metrics.Mark("src_fs_sync_start")
		if err := fsr.final(); err != nil {
			return fmt.Errorf("fs-sync (final rsync round) failed: %w", err)
		}
		metrics.Mark("src_fs_sync_end")
	} else if s.FSSync != "" && cfg.Stateful {
		metrics.Mark("src_fs_sync_start")
		ms, wire, err := tarStreamDir(ctx, destClient, destAddr, container.UpperDir, "fs_upper", s.ZstdLevel)
		if err != nil {
			return fmt.Errorf("fs-sync (upper layer) failed: %w", err)
		}
		metrics.FSUpperBytes += wire
		metrics.FSUpperMs += ms
		metrics.TotalDataTransferredBytes += wire
		metrics.Mark("src_fs_sync_end")
		fmt.Printf("Synced upper layer (%s): %d B on the wire in %.0f ms\n", s.FSSync, wire, ms)
	}

	// Stream metadata files too
	metrics.Mark("src_ship_final_start")
	if err := streamFile("statepod.json", "statepod_cfg"); err != nil {
		return err
	}
	fi, _ := os.Stat("statepod.json")
	if fi != nil {
		metrics.TotalDataTransferredBytes += fi.Size()
	}

	if err := streamFile("config.json", "oci_config"); err != nil {
		return err
	}
	fi, _ = os.Stat("config.json")
	if fi != nil {
		metrics.TotalDataTransferredBytes += fi.Size()
	}

	if s.Transport == "rsync" {
		// Push the final image via rsync, then explicitly trigger restore —
		// unlike the tar "final" stream, there's no data-carrying stream for
		// the receiver's "final" handler to hook the restore call onto.
		if _, _, err := rsyncPush("./checkpoint/final", "final"); err != nil {
			return fmt.Errorf("failed to rsync-push final: %w", err)
		}
		if err := destClient.Connect(ctx, destAddr); err != nil {
			return fmt.Errorf("failed to reconnect for final_rsync trigger: %w", err)
		}
		if err := destClient.SendStream(ctx, "final_rsync", bytes.NewReader([]byte{})); err != nil {
			return fmt.Errorf("failed to send final_rsync trigger: %w", err)
		}
	} else if _, _, err := streamDir("./checkpoint/final", "final"); err != nil {
		return fmt.Errorf("failed to stream final: %w", err)
	}
	sz, _ := getDirSize("./checkpoint/final")
	metrics.TotalDataTransferredBytes += sz
	if s.ZstdLevel > 0 && s.Transport != "rsync" {
		metrics.TotalDataTransferredBytes += lastWire - sz
	}
	metrics.ZstdLevel = s.ZstdLevel

	// Record finish time for source-side downtime calculation
	// The final stream ACK arrives only after the container is restored on the destination
	sourceSideFinishTime := metrics.Mark("src_final_ack")
	metrics.DowntimeSourceSide = float64(sourceSideFinishTime.Sub(metrics.FreezeStartTime).Milliseconds())
	metrics.TotalMigrationTime = float64(sourceSideFinishTime.Sub(metrics.MigrationStartTime).Milliseconds())
	fmt.Printf("Source-side Downtime: %.2f ms\n", metrics.DowntimeSourceSide)

	// Finally, send the metrics
	metricsData, _ := json.Marshal(metrics)
	if err := destClient.Connect(ctx, destAddr); err != nil {
		return fmt.Errorf("failed to reconnect for metrics: %w", err)
	}
	if err := destClient.SendStream(ctx, "metrics", bytes.NewReader(metricsData)); err != nil {
		return fmt.Errorf("failed to stream metrics: %w", err)
	}

	return nil
}
