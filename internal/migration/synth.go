package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/compress"
	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/pipeline"
	"github.com/CloudnetUCSC/statepod/internal/transport"
)

// SynthPreCopyStrategy is the core system path (`synth-precopy`):
// M1 baseline reconciliation + M2 exact dirty-set. Ahead of the burst the
// source pulls the destination's page-hash manifest; at burst it freezes,
// diffs its dump against that manifest, and ships only 16 B refs for pages the
// destination can synthesize itself plus raw pages for the misses. The
// destination reassembles a CRIU-restorable dump from its local baseline.
//
// The tar-based full-dump transport stays available under --mode precopy
// (Baselines 1–3 need it); this is the alternative encoder, not a replacement.
// PreCopyRounds is the number of chained --pre-dump rounds run before the
// freeze. 0 (the default) means a single freeze dump.
//
// Rounds are the M2 substrate: each chained round's pages-*.img holds only the
// pages dirtied since the previous round (CRIU's own soft-dirty tracking marks
// the rest PE_PARENT), so the dirty set is exact and the critical path only
// ever hashes dirty pages. They are also what M5 restores through.
//
// The default is 0 because on this testbed, for low-churn Redis, rounds cost
// substantially more bytes with no demonstrated downtime benefit: 4 rounds sent
// 2.95 MB vs 1.16-1.18 MB single-shot, because Redis's background cron
// re-dirties ~120 pages every round and each round re-ships them. (Downtime is
// NOT a basis for this default — back-to-back single-shot runs measured 6190 ms
// and 8844 ms, so the SSH-tunnel link noise swamps the difference. Downtime
// comparisons need the direct WAN path and n>=5.)
//
// Rounds stay exposed rather than compiled away: they are the churn-boundary
// knob the evaluation plots, and the substrate M5 restores through.
// ZstdLevel is the compression stage: applied to each delta directory's
// tar stream after the delta encoder, before the transport. 0 disables it
// (the tar stream goes out raw); the destination detects the zstd frame magic
// itself, so this is a purely source-side, per-run toggle (internal/compress).
type SynthPreCopyStrategy struct {
	// RsyncPort is the receiver's rsync daemon, target of the filesystem
	// pre-copy rounds (fsprecopy.go); "" = DefaultRsyncPort.
	RsyncPort     string
	PreCopyRounds int
	ZstdLevel     int
	// TmpfsCheckpoint is the toggle: mounts the
	// checkpoint dir as tmpfs on this host instead of plain disk.
	TmpfsCheckpoint bool
	// DisableFSUpper is the ablation toggle for M4 (
	// ): skips the overlay upperdir stream for stateful bundles that
	// would otherwise send it, isolating M4's contribution for the ablation
	// matrix (the "+M1+M2+compression+M3" cell vs "+M4"). M1/M2 are ablated
	// via --mode (precopy vs synth-precopy) and --precopy-rounds, compression
	// via --zstd-level 0, and M3 via --tmpfs-checkpoint — this is the one
	// mechanism in this strategy that previously had no independent off switch.
	DisableFSUpper bool
	// FinalMode selects how the last phase closes the write window:
	// FinalModeCatchup ("" / "catchup", the default) pauses the source only
	// after the destination holds a verified checkpoint, takes a final dump
	// against it (soft-dirty: only pages changed since, plus fresh process
	// metadata), ships that small delta and restores - no acknowledged write
	// is lost. FinalModeAvailability is the original behaviour: the
	// source keeps serving after its final dump until the destination ACKs,
	// so writes in that ~3 s window are acknowledged and then lost. Kept only
	// as a labelled ablation.
	FinalMode string
	// MergeRestore (opt-in) sends the final delta as
	// "synth_final_restore": the destination restores as soon as the image
	// verifies and ACKs only once it is serving, so the separate
	// synth_restore_go connection + round trip leave the critical path. The
	// source stays paused until that ACK (nothing can write meanwhile). If the
	// outcome is ambiguous (ACK lost) the source asks the destination for its
	// status before choosing between kill and resume, so it cannot end up with
	// both sides live. Single attempt: a failed merged restore is not retried.
	MergeRestore bool
}

const (
	FinalModeCatchup      = "catchup"
	FinalModeAvailability = "availability"
)

func (s *SynthPreCopyStrategy) Execute(ctx context.Context, cfg *container.Config, destClient transport.Client, destAddr string) error {
	fmt.Printf("Executing Synth-PreCopy Strategy (M1+M2) for container: %s\n", cfg.ContainerID)
	metrics := &MigrationMetrics{
		Mode:               "synth-precopy",
		ContainerID:        cfg.ContainerID,
		MigrationStartTime: time.Now(),
		Pipeline:           &PipelineMetrics{},
	}

	if cfg.ImageID == "" {
		return fmt.Errorf("statepod.json has no image_id — synth-precopy is keyed by image digest; recreate the bundle")
	}

	catchup := s.FinalMode != FinalModeAvailability
	// The catch-up final dump needs a verified parent checkpoint on the
	// destination: the last pre-dump round plays that role (and, unlike a
	// --leave-running full dump, a pre-dump clears soft-dirty so the final
	// dump's dirty set is exactly "changed since the checkpoint").
	rounds := s.PreCopyRounds
	if catchup && rounds < 1 {
		rounds = 1
	}
	if catchup {
		metrics.FinalMode = FinalModeCatchup
	} else {
		metrics.FinalMode = FinalModeAvailability
	}

	// --- Pre-burst: manifest exchange (off the critical path) ---
	fmt.Println("Fetching baseline manifest from destination for image", cfg.ImageID)
	manifestBytes, err := destClient.Request(ctx, destAddr, "manifest", strings.NewReader(cfg.TemplateKey()))
	if err != nil {
		return fmt.Errorf("manifest exchange failed: %w", err)
	}
	if len(manifestBytes) == 0 {
		return fmt.Errorf("destination returned an empty manifest for image %s — run `statepod template generate` there first", cfg.ImageID)
	}
	manifest, err := pipeline.DecodeManifest(bytes.NewReader(manifestBytes))
	if err != nil {
		return fmt.Errorf("decoding manifest: %w", err)
	}
	metrics.Pipeline.ManifestBytes = int64(len(manifestBytes))
	metrics.Mark("src_manifest_fetched")
	fmt.Printf("Manifest: %d baseline page hashes (%d bytes), baseline %s\n",
		len(manifest.Hashes), len(manifestBytes), manifest.BaselineDumpID)

	if err := ensureCheckpointDirs(s.TmpfsCheckpoint); err != nil {
		return err
	}

	// reconcileAndShip encodes one round's dump against the manifest and streams
	// the resulting delta. Returns the delta's size on the wire.
	reconcileAndShip := func(dumpDir, deltaDir, streamType string) (int64, error) {
		// runc checkpoint runs under sudo, so the images are root-owned; the
		// reconciliation pipeline must be able to read them.
		if err := exec.CommandContext(ctx, "sudo", "chmod", "-R", "a+rX", dumpDir).Run(); err != nil {
			return 0, fmt.Errorf("failed to make %s readable: %w", dumpDir, err)
		}
		encStart := time.Now()
		metrics.Mark("src_encode_" + streamType + "_start")
		stats, err := pipeline.EncodeDump(dumpDir, deltaDir, manifest, pipeline.EncodeOptions{})
		if err != nil {
			return 0, fmt.Errorf("delta encoding failed for %s: %w", dumpDir, err)
		}
		encMs := float64(time.Since(encStart).Milliseconds())

		metrics.Pipeline.EncodeMs += encMs
		metrics.Pipeline.TotalPages += stats.TotalPages
		metrics.Pipeline.Hits += stats.Hits
		metrics.Pipeline.Misses += stats.Misses
		metrics.Pipeline.RefBytes += stats.RefBytes
		metrics.Pipeline.RawBytes += stats.RawBytes

		dumpSize, _ := dirSize(dumpDir)
		deltaSize, _ := dirSize(deltaDir)
		metrics.Pipeline.DeltaBytes += deltaSize

		metrics.Mark("src_encode_" + streamType + "_end")
		streamMs, wireBytes, err := tarStreamDir(ctx, destClient, destAddr, deltaDir, streamType, s.ZstdLevel)
		if err != nil {
			return 0, fmt.Errorf("failed to stream %s: %w", streamType, err)
		}
		metrics.Mark("src_ship_" + streamType + "_end")
		fmt.Printf("  %s: %d dirty pages, %d hits (%.1f%%), %d misses; dump %d B -> delta %d B -> wire %d B in %.0f ms\n",
			streamType, stats.TotalPages, stats.Hits, stats.HitRate()*100, stats.Misses,
			dumpSize, deltaSize, wireBytes, encMs)
		metrics.TotalDataTransferredBytes += wireBytes
		metrics.Pipeline.CompressedBytes += wireBytes
		metrics.Pipeline.StreamMs += streamMs
		return wireBytes, nil
	}

	// Catch-up mode moves the upper layer in rsync rounds (fsprecopy.go). The
	// bulk goes FIRST, before the memory rounds: memory dirtied while it is on the
	// wire is then carried by the memory rounds instead of the final dump (with the
	// filesystem rounds after the memory rounds, a 12 MB PostgreSQL layer at 2 Mb/s
	// left ~55 s of dirtying for the final dump, 2026-10-26).
	fsr := newFSPrecopy(ctx, destClient, destAddr, s.RsyncPort, s.ZstdLevel, metrics)
	carriesFS := cfg.Stateful && !s.DisableFSUpper
	if catchup && carriesFS {
		metrics.Mark("src_fs_precopy_start")
		if err := fsr.precopyLive(); err != nil {
			return fmt.Errorf("filesystem pre-copy failed, source untouched: %w", err)
		}
		metrics.Mark("src_fs_precopy_end")
	}

	// --- Iterative precopy rounds (M2 via CRIU's own soft-dirty tracking) ---
	// Round 0 dumps everything; each chained round's pages-*.img holds only the
	// pages dirtied since the previous round (PE_PARENT marks the rest), so the
	// dirty set is exact and the critical path only ever hashes dirty pages
	//. The container keeps serving throughout.
	for i := 0; i < rounds; i++ {
		dumpDir := fmt.Sprintf("./checkpoint/pre%d", i)
		args := []string{"runc", "checkpoint", cfg.ContainerID, "--pre-dump", "--image-path", dumpDir}
		if i > 0 {
			args = append(args, "--parent-path", fmt.Sprintf("./../pre%d", i-1))
		}
		fmt.Printf("Pre-dump round %d\n", i)
		iterStart := metrics.Mark(fmt.Sprintf("src_predump_%d_start", i))
		cmd := sudoRunc(ctx, args...)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("pre-dump round %d failed: %s: %w", i, stderr.String(), err)
		}
		dumpMs := float64(time.Since(iterStart).Milliseconds())
		metrics.Mark(fmt.Sprintf("src_predump_%d_end", i))

		deltaSize, err := reconcileAndShip(dumpDir, fmt.Sprintf("./checkpoint/delta_pre%d", i), fmt.Sprintf("synth_pre%d", i))
		if err != nil {
			return err
		}
		metrics.Iterations = append(metrics.Iterations, PreCopyIterationInfo{
			Iteration:    i,
			Duration:     dumpMs,
			SizeInBytes:  deltaSize,
			TransferTime: float64(time.Since(iterStart).Milliseconds()) - dumpMs,
		})
	}

	// --- Freeze: with rounds, the final dump carries only what changed since
	// the last round; without them it is a full dump. --leave-running keeps
	// the source fully serving through shipping + destination-side
	// verification (failure handling): the source is only disrupted
	// once the destination has ACKed a checksummed, restorable image (the
	// kill + synth_restore_go trigger below). Documented, honest gap: writes
	// between this dump and the eventual kill are lost — a small window (one
	// shipping+verification round trip), not a full drain (contrast
	// M5 postcopy drain window).
	// resumeSource thaws the paused source after a failure past the pause
	// (catch-up mode only). Nothing has been killed, so the source simply
	// keeps serving; the migration is abandoned.
	resumeSource := func(reason string) {
		fmt.Printf("Resuming source container (%s)\n", reason)
		if err := exec.CommandContext(ctx, "sudo", "runc", "resume", cfg.ContainerID).Run(); err != nil {
			fmt.Printf("Warning: failed to resume source container: %v\n", err)
		}
	}

	// shipConfigs pre-stages what does not need the source paused (11f). In
	// catch-up mode the upper layer moves in rsync rounds (fsr below); shipUpper
	// (a whole-layer tar) remains only for the availability-order ablation.
	shipUpper := func(live bool) error {
		if !cfg.Stateful || s.DisableFSUpper {
			return nil
		}
		upperMs, upperBytes, err := tarStreamDirLive(ctx, destClient, destAddr, container.UpperDir, "fs_upper", s.ZstdLevel, live)
		if err != nil {
			return fmt.Errorf("failed to stream upperdir: %w", err)
		}
		metrics.FSUpperBytes += upperBytes
		metrics.FSUpperMs += upperMs
		metrics.TotalDataTransferredBytes += upperBytes
		fmt.Printf("Streamed upperdir (M4): %d B in %.0f ms\n", upperBytes, upperMs)
		return nil
	}
	shipConfigs := func() error {
		for _, f := range []struct{ path, streamType string }{
			{"statepod.json", "statepod_cfg"},
			{"config.json", "oci_config"},
		} {
			if err := streamOneFile(ctx, destClient, destAddr, f.path, f.streamType); err != nil {
				return err
			}
			if fi, statErr := os.Stat(f.path); statErr == nil {
				metrics.TotalDataTransferredBytes += fi.Size()
			}
		}
		return nil
	}
	configsShipped := false

	if catchup {
		metrics.Mark("src_prestage_start")
		for attempt := 1; ; attempt++ {
			err := shipConfigs()
			if err == nil && carriesFS {
				// short catch-up of what changed during the memory rounds; the
				// final round after the pause carries the rest
				err = fsr.catchUp("live-last")
			}
			if err == nil {
				break
			}
			if attempt >= 3 {
				return fmt.Errorf("pre-stage failed after %d attempts, source untouched: %w", attempt, err)
			}
			fmt.Printf("Retrying pre-stage (attempt %d/3) after: %v\n", attempt+1, err)
		}
		configsShipped = true
		metrics.Mark("src_prestage_end")

		// Stop-the-world starts here: from this instant no client request is
		// answered, so no write can be acknowledged that the final dump below
		// does not contain. (A frozen Redis never replies +OK; the kernel may
		// still ACK the TCP segment, but that is not an application ack.)
		fmt.Println("Pausing source (catch-up final phase)")
		metrics.FreezeStartTime = metrics.Mark("src_pause")
		if err := exec.CommandContext(ctx, "sudo", "runc", "pause", cfg.ContainerID).Run(); err != nil {
			return fmt.Errorf("failed to pause source container: %w", err)
		}
	}

	fmt.Println("Starting final dump")
	dumpStart := metrics.Mark("src_final_dump_start")
	if !catchup {
		metrics.FreezeStartTime = dumpStart
	}
	finalArgs := []string{"runc", "checkpoint", cfg.ContainerID,
		"--image-path", "./checkpoint/final", "--leave-running=true", "--tcp-established"}
	if rounds > 0 {
		finalArgs = append(finalArgs, "--parent-path", fmt.Sprintf("./../pre%d", rounds-1))
	}
	dumpCmd := sudoRunc(ctx, finalArgs...)
	var dumpErr bytes.Buffer
	dumpCmd.Stderr = &dumpErr
	if err := dumpCmd.Run(); err != nil {
		if catchup {
			resumeSource("final dump failed")
		}
		return fmt.Errorf("final dump failed: %s: %w", dumpErr.String(), err)
	}
	if catchup {
		// CRIU's --leave-running thaws what it froze; confirm it did not also
		// thaw the runc-paused container. If it did, re-pause immediately and
		// record it: the gap (dump end -> re-pause) is a residual write window.
		if st, err := container.GetRuncState(ctx, cfg.ContainerID); err == nil && st.Status != "paused" {
			fmt.Printf("WARNING: container is %q after the final dump, not paused; re-pausing\n", st.Status)
			metrics.Mark("src_repause")
			_ = exec.CommandContext(ctx, "sudo", "runc", "pause", cfg.ContainerID).Run()
			metrics.RepausedAfterDump = true
		}
	}
	metrics.Mark("src_final_dump_end")

	// Encode once: the final dump is static on disk once taken, so unlike
	// shipping below, re-running this on retry would only double-count page
	// stats without changing what gets sent.
	if err := exec.CommandContext(ctx, "sudo", "chmod", "-R", "a+rX", "./checkpoint/final").Run(); err != nil {
		return fmt.Errorf("failed to make final dump readable: %w", err)
	}
	encStart := time.Now()
	metrics.Mark("src_encode_final_start")
	finalStats, err := pipeline.EncodeDump("./checkpoint/final", "./checkpoint/delta_final", manifest, pipeline.EncodeOptions{})
	if err != nil {
		if catchup {
			resumeSource("final delta encoding failed")
		}
		return fmt.Errorf("delta encoding failed for final: %w", err)
	}
	metrics.Pipeline.EncodeMs += float64(time.Since(encStart).Milliseconds())
	metrics.Mark("src_encode_final_end")
	metrics.Pipeline.TotalPages += finalStats.TotalPages
	metrics.Pipeline.Hits += finalStats.Hits
	metrics.Pipeline.Misses += finalStats.Misses
	metrics.Pipeline.RefBytes += finalStats.RefBytes
	metrics.Pipeline.RawBytes += finalStats.RawBytes
	finalDumpSize, _ := dirSize("./checkpoint/final")
	finalDeltaSize, _ := dirSize("./checkpoint/delta_final")
	metrics.Pipeline.DeltaBytes += finalDeltaSize
	metrics.Pipeline.FinalPages = finalStats.TotalPages
	metrics.Pipeline.FinalHits = finalStats.Hits
	metrics.Pipeline.FinalDeltaBytes = finalDeltaSize
	fmt.Printf("  synth_final: %d dirty pages, %d hits (%.1f%%), %d misses; dump %d B -> delta %d B\n",
		finalStats.TotalPages, finalStats.Hits, finalStats.HitRate()*100, finalStats.Misses, finalDumpSize, finalDeltaSize)

	// shipFinal sends metadata + upperdir + the already-encoded final delta
	// and waits for the destination's ACK that it has reassembled and
	// checksum-verified a restorable image (§3.3's re-request/
	// full-transfer fallback trigger, now actually wired up rather than
	// failing the migration outright). Nothing here disturbs the source
	// (still --leave-running), so a failure is always safe to retry from
	// scratch — the encoded delta on disk doesn't change between attempts.
	shipFinal := func() error {
		metrics.Mark("src_ship_final_start")
		if !configsShipped {
			if err := shipConfigs(); err != nil {
				return err
			}
			configsShipped = true
		}

		// M4: stream the overlay upperdir — the only
		// filesystem data that migrates. lowerdir (the prefetched image
		// content) is already identical on both sides via the pinned image
		// digest and is never sent. Non-stateful bundles have no upperdir
		// (flat rootfs, restart baseline) so this is skipped for them. Sent
		// ahead of synth_final so the destination's overlay is already
		// remounted by the time synth_final's ACK confirms readiness. Unlike
		// the memory dump, upperdir keeps evolving on the still-live source,
		// so a retry naturally ships a fresher snapshot — assignment, not
		// accumulation, records the winning attempt's numbers.
		if carriesFS {
			if catchup {
				// Source paused: the final rsync round sends only what changed since
				// the last live round (block deltas for files changed in place).
				if err := fsr.final(); err != nil {
					return err
				}
			} else if err := shipUpper(false); err != nil { // availability order (ablation only)
				return err
			}
		}

		finalStream := "synth_final"
		if s.MergeRestore {
			finalStream = "synth_final_restore"
		}
		streamMs, wireBytes, err := tarStreamDir(ctx, destClient, destAddr, "./checkpoint/delta_final", finalStream, s.ZstdLevel)
		if err != nil {
			return fmt.Errorf("failed to stream %s: %w", finalStream, err)
		}
		metrics.TotalDataTransferredBytes += wireBytes
		metrics.Pipeline.CompressedBytes += wireBytes
		metrics.Pipeline.StreamMs += streamMs
		metrics.Mark("src_ship_final_ack")
		return nil
	}

	maxFinalAttempts := 3
	if s.MergeRestore {
		maxFinalAttempts = 1
	}
	var shipErr error
	for attempt := 1; attempt <= maxFinalAttempts; attempt++ {
		if attempt > 1 {
			fmt.Printf("Retrying final delta (attempt %d/%d) after: %v\n", attempt, maxFinalAttempts, shipErr)
		}
		if shipErr = shipFinal(); shipErr == nil {
			break
		}
	}
	if shipErr != nil && s.MergeRestore {
		// Ambiguous: the destination may or may not have restored. Ask it.
		status, qerr := queryDestStatus(ctx, destClient, destAddr)
		if qerr == nil && string(bytes.TrimSpace(status)) == "running" {
			fmt.Printf("Merged final stream reported %v but the destination is running — treating as success\n", shipErr)
			metrics.Mark("src_ack_recovered_by_status")
			shipErr = nil
		} else if qerr == nil {
			resumeSource("merged restore failed on the destination")
			return fmt.Errorf("merged final restore failed (%v); destination not running, source resumed", shipErr)
		} else {
			// Cannot tell. Leave the source paused rather than risk two live copies.
			return fmt.Errorf("merged final restore outcome unknown (%v; status query: %v): source left paused for inspection", shipErr, qerr)
		}
	}
	if shipErr != nil {
		// Give up, source serving. A fallback to restart migration used to run here: its first
		// step kills the source, so a dead destination cost the service (fault matrix).
		fmt.Printf("Final delta failed after %d attempts (%v); source kept, migration abandoned\n",
			maxFinalAttempts, shipErr)
		if catchup {
			resumeSource("final delta shipping failed")
		}
		return fmt.Errorf("final delta failed after %d attempts, source kept: %w", maxFinalAttempts, shipErr)
	}

	if metrics.Pipeline.TotalPages > 0 {
		metrics.Pipeline.HitRate = float64(metrics.Pipeline.Hits) / float64(metrics.Pipeline.TotalPages)
	}
	if metrics.Pipeline.CompressedBytes > 0 {
		metrics.Pipeline.CompressionRatio = float64(metrics.Pipeline.DeltaBytes) / float64(metrics.Pipeline.CompressedBytes)
	}
	fmt.Printf("Reconciliation totals: %d pages shipped, %d hits (%.1f%%), %d misses; %d B on wire\n",
		metrics.Pipeline.TotalPages, metrics.Pipeline.Hits, metrics.Pipeline.HitRate*100,
		metrics.Pipeline.Misses, metrics.TotalDataTransferredBytes)

	// The destination holds a verified, restorable image: restore it there. In catch-up
	// mode the paused source is killed only once the restore is confirmed; in the
	// availability order it is killed first and a failure past this point has no live
	// source left to fall back to.
	var sourceSideFinishTime time.Time
	if s.MergeRestore {
		// The final stream's ACK already means "restored and serving".
		sourceSideFinishTime = metrics.Mark("src_restore_go_acked")
		metrics.SourceKillTime = metrics.Mark("src_kill")
		if err := exec.CommandContext(ctx, "sudo", "runc", "kill", cfg.ContainerID, "SIGKILL").Run(); err != nil {
			fmt.Printf("Warning: failed to kill source container: %v\n", err)
		}
		if err := exec.CommandContext(ctx, "sudo", "runc", "delete", cfg.ContainerID).Run(); err != nil {
			fmt.Printf("Warning: failed to delete source container state: %v\n", err)
		}
	} else if catchup {
		// The source stays paused (intact) until the destination confirms the restore and is
		// killed only then. Killing it first left nothing serving when the receiver
		// died between the kill and the restore (fault matrix, 2026-11-01).
		if err := destClient.Connect(ctx, destAddr); err != nil {
			resumeSource("destination unreachable before the restore")
			return fmt.Errorf("failed to reconnect for synth_restore_go, source resumed: %w", err)
		}
		if err := destClient.SendStream(ctx, "synth_restore_go", bytes.NewReader([]byte{})); err != nil {
			if errors.Is(err, transport.ErrStreamFailed) {
				resumeSource("destination restore failed")
				return fmt.Errorf("destination restore failed, source resumed: %w", err)
			}
			// Lost ACK: the restore may or may not have happened. Same rule as the merged
			// restore: ask, and never run two copies.
			status, qerr := queryDestStatus(ctx, destClient, destAddr)
			switch {
			case qerr == nil && string(bytes.TrimSpace(status)) == "running":
				fmt.Printf("Restore trigger reported %v but the destination is running — treating as success\n", err)
				metrics.Mark("src_ack_recovered_by_status")
			case qerr == nil:
				resumeSource("destination did not restore")
				return fmt.Errorf("destination restore failed (%v), source resumed", err)
			default:
				return fmt.Errorf("restore outcome unknown (%v; status query: %v): source left paused for inspection", err, qerr)
			}
		}
		sourceSideFinishTime = metrics.Mark("src_restore_go_acked")
		metrics.SourceKillTime = metrics.Mark("src_kill")
		if err := exec.CommandContext(ctx, "sudo", "runc", "kill", cfg.ContainerID, "SIGKILL").Run(); err != nil {
			fmt.Printf("Warning: failed to kill source container: %v\n", err)
		}
		if err := exec.CommandContext(ctx, "sudo", "runc", "delete", cfg.ContainerID).Run(); err != nil {
			fmt.Printf("Warning: failed to delete source container state: %v\n", err)
		}
	} else {
		// availability order (ablation only): the source is still serving, so it is killed first
		metrics.SourceKillTime = metrics.Mark("src_kill")
		if err := exec.CommandContext(ctx, "sudo", "runc", "kill", cfg.ContainerID, "SIGKILL").Run(); err != nil {
			fmt.Printf("Warning: failed to kill source container: %v\n", err)
		}
		if err := exec.CommandContext(ctx, "sudo", "runc", "delete", cfg.ContainerID).Run(); err != nil {
			fmt.Printf("Warning: failed to delete source container state: %v\n", err)
		}
		if err := destClient.Connect(ctx, destAddr); err != nil {
			return fmt.Errorf("failed to reconnect for synth_restore_go: %w", err)
		}
		if err := destClient.SendStream(ctx, "synth_restore_go", bytes.NewReader([]byte{})); err != nil {
			return fmt.Errorf("source already killed but destination restore failed (no live source to fall back to): %w", err)
		}
		sourceSideFinishTime = metrics.Mark("src_restore_go_acked")
	}
	// Client-visible outage starts when the source stops answering: the pause
	// in catch-up mode, the kill in availability mode (the source served
	// through shipping there).
	downtimeStart := metrics.SourceKillTime
	if catchup {
		downtimeStart = metrics.FreezeStartTime
	}
	metrics.DowntimeSourceSide = float64(sourceSideFinishTime.Sub(downtimeStart).Milliseconds())
	metrics.TotalMigrationTime = float64(sourceSideFinishTime.Sub(metrics.MigrationStartTime).Milliseconds())
	fmt.Printf("Source-side Downtime: %.2f ms\n", metrics.DowntimeSourceSide)

	metricsData, _ := json.Marshal(metrics)
	if err := destClient.Connect(ctx, destAddr); err != nil {
		return fmt.Errorf("failed to reconnect for metrics: %w", err)
	}
	if err := destClient.SendStream(ctx, "metrics", bytes.NewReader(metricsData)); err != nil {
		return fmt.Errorf("failed to stream metrics: %w", err)
	}
	return nil
}

// dirSize sums the file sizes under path.
func dirSize(path string) (int64, error) {
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

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// queryDestStatus asks the destination whether the merged restore happened, for up to
// statusRetryWindow: a healed network or a restarted receiver settles the outcome here
// instead of leaving the source paused for an operator.
func queryDestStatus(ctx context.Context, client transport.Client, destAddr string) ([]byte, error) {
	var lastErr error
	deadline := time.Now().Add(statusRetryWindow)
	for {
		qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		status, err := client.Request(qctx, destAddr, "dest_status", bytes.NewReader(nil))
		cancel()
		if err == nil {
			return status, nil
		}
		lastErr = err
		if time.Now().Add(3 * time.Second).After(deadline) {
			return nil, lastErr
		}
		time.Sleep(3 * time.Second)
	}
}

// statusRetryWindow: same bound as a paused source's other waits.
const statusRetryWindow = pauseStallLimit

// tarStreamDir tars a directory, optionally zstd-compresses it (:
// zstdLevel <= 0 disables the stage, sending the raw tar stream), and streams
// the result under streamType. Returns the transfer duration in ms and the
// actual bytes placed on the wire (post-compression). Same wire shape as the
// precopy tar path so the receiver side stays uniform — the destination
// detects the zstd frame magic itself (internal/compress.DecompressAuto), so
// no protocol change is needed to carry the toggle.
func tarStreamDir(ctx context.Context, client transport.Client, destAddr, dirPath, streamType string, zstdLevel int) (float64, int64, error) {
	return tarStreamDirLive(ctx, client, destAddr, dirPath, streamType, zstdLevel, false)
}

// tarStreamDirLive is tarStreamDir for a directory that may still be written
// while it is read (the pre-pause upper-layer send). With tolerateChanged, GNU
// tar's exit status 1 ("file changed as we read it": the stream is complete, some
// content may be torn) is not an error: the caller re-sends whatever changed
// after its marker once the source is paused, so a torn pre-send is overwritten.
// Without this a busy filesystem failed every pre-stage attempt and aborted the
// migration. Any other tar failure (exit 2: unreadable/vanished files) still is.
func tarStreamDirLive(ctx context.Context, client transport.Client, destAddr, dirPath, streamType string, zstdLevel int, tolerateChanged bool) (float64, int64, error) {
	fmt.Printf("Streaming %s as %s (zstd level %d)...\n", dirPath, streamType, zstdLevel)
	// Reconnect per stream: the protocol uses EOF to signal end-of-stream.
	if err := client.Connect(ctx, destAddr); err != nil {
		return 0, 0, fmt.Errorf("failed to reconnect for stream %s: %w", streamType, err)
	}
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
	wire := &countingReader{r: compress.Compress(stdout, zstdLevel)}
	start := time.Now()
	if err := client.SendStream(ctx, streamType, wire); err != nil {
		return 0, 0, err
	}
	elapsed := time.Since(start)
	if err := cmd.Wait(); err != nil {
		var ee *exec.ExitError
		if tolerateChanged && errors.As(err, &ee) && ee.ExitCode() == 1 {
			fmt.Printf("Note: %s changed while being read (tar: %s); the post-pause check resends it\n",
				streamType, strings.TrimSpace(stderr.String()))
		} else {
			return 0, 0, fmt.Errorf("tar failed for %s: %s: %w", streamType, stderr.String(), err)
		}
	}
	return float64(elapsed.Milliseconds()), wire.n, nil
}

// countingReader tallies bytes as they pass through Read, so the caller can
// learn the actual wire size of a stream without buffering it.
type countingReader struct {
	r io.Reader
	n int64
	// onRead, if set, is called after every read that returned data
	onRead func()
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	if n > 0 && c.onRead != nil {
		c.onRead()
	}
	return n, err
}

// streamOneFile sends a single file under streamType.
func streamOneFile(ctx context.Context, client transport.Client, destAddr, filePath, streamType string) error {
	fmt.Printf("Streaming %s as %s...\n", filePath, streamType)
	if err := client.Connect(ctx, destAddr); err != nil {
		return fmt.Errorf("failed to reconnect for stream %s: %w", streamType, err)
	}
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", filePath, err)
	}
	defer f.Close()
	return client.SendStream(ctx, streamType, f)
}
