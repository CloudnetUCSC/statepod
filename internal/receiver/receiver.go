package receiver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/compress"
	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/migration"
	"github.com/CloudnetUCSC/statepod/internal/network"
	"github.com/CloudnetUCSC/statepod/internal/pipeline"
	"github.com/CloudnetUCSC/statepod/internal/transport"
)

// StartReceiver starts the TCP server to listen for incoming migration streams
// and handles the container restore process. metricsPath overrides where the
// migration metrics JSON is written; empty means auto-detect an experiment dir.
// rsyncPort starts a companion rsync daemon (addendum: --mode
// precopy-rsync/sync-rsync transport A/B against statepod's own tar+TCP
// transport) serving "checkpoint" and "rootfs" modules rooted at the current
// bundle dir; empty disables it. tmpfsCheckpoint is the toggle
// : mounts ./checkpoint as tmpfs on this host for the
// receiver's lifetime, so CRIU restore's image reads never touch disk here.
// Must match the source's own --tmpfs-checkpoint setting to make a clean A/B.
func StartReceiver(ctx context.Context, listenAddr string, metricsPath string, rsyncPort string, tmpfsCheckpoint bool) error {
	cfg, err := container.ReadConfig()
	if err != nil {
		return fmt.Errorf("failed to read statepod config: %w", err)
	}

	// Warm the template baseline now, minutes before any source connects: parsing
	// and hashing it takes seconds and used to land on the migration's critical
	// path (M5's residual request, while the source is paused). Errors are
	// ignored here -- a missing template is reported when it is actually needed.
	prefetchBaseline(cfg)

	if err := migration.EnsureCheckpointRoot(filepath.Join(".", "checkpoint"), tmpfsCheckpoint); err != nil {
		return fmt.Errorf("failed to prepare checkpoint root: %w", err)
	}

	if rsyncPort != "" {
		stopRsyncDaemon, err := startRsyncDaemon(ctx, rsyncPort)
		if err != nil {
			return fmt.Errorf("failed to start rsync daemon: %w", err)
		}
		defer stopRsyncDaemon()
	}

	server := transport.NewTCPServer()
	var metrics migration.MigrationMetrics
	metricsReceived := make(chan bool, 1)
	// Destination-side reassembly time (synth-precopy): measured here, folded
	// into the metrics JSON once the source's metrics stream arrives.
	var reassembleMs float64

	// Helper to extract a tar stream into a directory. Streams compressed by
	// the source's zstd stage are detected and transparently
	// decompressed here (internal/compress.DecompressAuto) — this handler
	// doesn't need to know the source's --zstd-level setting.
	extractStream := func(streamType string, r io.Reader) error {
		destDir := filepath.Join(".", "checkpoint", streamType)
		if err := os.MkdirAll(destDir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", destDir, err)
		}

		dr, err := compress.DecompressAuto(r)
		if err != nil {
			return fmt.Errorf("decompressing stream %s: %w", streamType, err)
		}
		defer dr.Close()

		fmt.Printf("Receiving and extracting stream: %s\n", streamType)
		// Use sudo to ensure we can write into directories potentially owned by root
		cmd := exec.CommandContext(ctx, "sudo", "tar", "-xf", "-", "-C", destDir)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		cmd.Stdin = dr
		if err := cmd.Run(); err != nil {
			fmt.Printf("tar extraction error for %s: %s\n", streamType, stderr.String())
			return fmt.Errorf("tar extraction failed for %s: %w", streamType, err)
		}
		return nil
	}

	// Pre-burst manifest exchange: the source asks for the
	// page-hash manifest of its image digest; we serve the one stored beside
	// the synthesized template. This is the only pre-burst WAN payload and the
	// only baseline-derived bytes that ever leave this host.
	server.ReceiveRequest("manifest", func(req io.Reader) ([]byte, error) {
		digest, err := io.ReadAll(req)
		if err != nil {
			return nil, err
		}
		fmt.Printf("Serving manifest for image %s\n", string(digest))
		go prestageHostNetwork(ctx, cfg)
		prefetchBaseline(cfg)
		return container.TemplateManifestBytes(string(digest))
	})

	// Register handlers for our pre-copy streams
	server.ReceiveStream("pre0", func(r io.Reader) error { return extractStream("pre0", r) })
	server.ReceiveStream("pre1", func(r io.Reader) error { return extractStream("pre1", r) })
	server.ReceiveStream("pre2", func(r io.Reader) error { return extractStream("pre2", r) })
	server.ReceiveStream("pre3", func(r io.Reader) error { return extractStream("pre3", r) })

	// Metadata handlers
	server.ReceiveStream("statepod_cfg", func(r io.Reader) error {
		fmt.Println("Receiving statepod.json...")
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		// Fail fast on image mismatch: CRIU restore maps files from the local
		// rootfs, so source and destination bundles must come from the same
		// image build. A mismatch otherwise surfaces later as a cryptic
		// "File ... has bad size" restore failure.
		var srcCfg container.Config
		if err := json.Unmarshal(data, &srcCfg); err != nil {
			return fmt.Errorf("invalid statepod.json from source: %w", err)
		}
		if srcCfg.ImageID != "" && cfg.ImageID != "" && srcCfg.ImageID != cfg.ImageID {
			return fmt.Errorf("image mismatch: source bundle built from %s but destination bundle from %s; recreate the destination bundle from the same image digest", srcCfg.ImageID, cfg.ImageID)
		}
		go prestageHostNetwork(ctx, cfg)
		return os.WriteFile("statepod.json", data, 0644)
	})
	server.ReceiveStream("oci_config", func(r io.Reader) error {
		fmt.Println("Receiving config.json...")
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		return os.WriteFile("config.json", data, 0644)
	})

	server.ReceiveStream("metrics", func(r io.Reader) error {
		fmt.Println("Receiving migration metrics from source...")
		err := json.NewDecoder(r).Decode(&metrics)
		if err == nil {
			metricsReceived <- true
		}
		return err
	})

	// Channel to signal when migration is complete
	migrationDone := make(chan error, 1)

	server.ReceiveStream("final", func(r io.Reader) error {
		if err := extractStream("final", r); err != nil {
			return err
		}
		markDst("final_received")

		fmt.Println("Final migration stream received. Restoring container...")

		err := restoreContainer(ctx, cfg)
		migrationDone <- err
		return err
	})

	// synth-precopy: each arriving stream is a delta directory —
	// metadata .img files plus per-pages .delta streams of 16 B refs (pages the
	// destination can synthesize from its local baseline) and raw misses. Each
	// round is reassembled into its own chain directory; CRIU's PE_PARENT
	// entries, carried through verbatim in the pagemaps, make the final restore
	// walk the chain.
	//
	// The baseline is parsed once and reused across rounds.
	var baseline *pipeline.Baseline
	loadBaseline := func() (*pipeline.Baseline, error) {
		if baseline != nil {
			return baseline, nil
		}
		b, err := loadTemplateBaseline(cfg)
		if err != nil {
			return nil, err
		}
		baseline = b
		return b, nil
	}

	// synthRound expands one received delta dir into the CRIU image dir the
	// restore chain expects.
	synthRound := func(round string, r io.Reader) error {
		if err := extractStream("delta_"+round, r); err != nil {
			return err
		}
		base, err := loadBaseline()
		if err != nil {
			return err
		}
		outDir := "./checkpoint/" + round
		// Start from a clean image dir so no stale pages-*.img from a prior run
		// can be picked up by restore.
		if err := exec.CommandContext(ctx, "sudo", "rm", "-rf", outDir).Run(); err != nil {
			return err
		}
		start := time.Now()
		markDst("reassemble_" + round + "_start")
		stats, err := pipeline.ReassembleDump("./checkpoint/delta_"+round, outDir, base)
		if err != nil {
			// A checksum mismatch is the design's re-request / full-transfer
			// fallback trigger (§3.3); today it fails the migration
			// loudly rather than corrupting a restore.
			return fmt.Errorf("reassembly of %s failed: %w", round, err)
		}
		reassembleMs += float64(time.Since(start).Milliseconds())
		markDst("reassemble_" + round + "_end")
		fmt.Printf("Reassembled %s: %d pages (%d from local baseline, %d received raw); checksum %s OK\n",
			round, stats.TotalPages, stats.Hits, stats.Misses, stats.WholeChecksum)
		return nil
	}

	// Register handlers for more rounds than we expect to use: the source's
	// --precopy-rounds is a knob, and an unused registration costs nothing.
	for i := 0; i < 8; i++ {
		round := fmt.Sprintf("pre%d", i)
		server.ReceiveStream("synth_"+round, func(r io.Reader) error {
			return synthRound(round, r)
		})
	}

	// M4: the source's overlay upperdir — the only
	// filesystem data a synth-precopy migration carries. Arrives ahead of
	// synth_final. The overlay must come down before the upperdir underneath
	// it is replaced (overlayfs expects changes through the merged mount, not
	// direct writes under a mounted upperdir) and go back up before restore
	// needs "rootfs" populated.
	server.ReceiveStream("fs_upper", func(r io.Reader) error {
		fmt.Println("Receiving upperdir stream (M4)...")
		if err := container.UnmountOverlay(); err != nil {
			return fmt.Errorf("failed to unmount overlay before upperdir sync: %w", err)
		}
		if err := exec.CommandContext(ctx, "sudo", "rm", "-rf", container.UpperDir).Run(); err != nil {
			return fmt.Errorf("failed to clear upperdir: %w", err)
		}
		if err := os.MkdirAll(container.UpperDir, 0755); err != nil {
			return fmt.Errorf("failed to recreate upperdir: %w", err)
		}
		dr, err := compress.DecompressAuto(r)
		if err != nil {
			return fmt.Errorf("decompressing upperdir stream: %w", err)
		}
		defer dr.Close()
		cmd := exec.CommandContext(ctx, "sudo", "tar", "-xf", "-", "-C", container.UpperDir)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		cmd.Stdin = dr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("tar extraction failed for upperdir: %s: %w", stderr.String(), err)
		}
		if err := container.MountOverlay(); err != nil {
			return fmt.Errorf("failed to remount overlay after upperdir sync: %w", err)
		}
		return nil
	})

	// Filesystem pre-copy (migration/fsprecopy.go): the source rsyncs the upperdir
	// into the daemon's [upper] module in rounds. The overlay comes down for the
	// rounds (no changes underneath a mounted upper) and goes back up after the
	// final round, before the restore needs "rootfs".
	server.ReceiveStream("fs_upper_open", func(r io.Reader) error {
		_, _ = io.Copy(io.Discard, r)
		fmt.Println("Filesystem pre-copy: overlay unmounted for the rsync rounds")
		if err := container.UnmountOverlay(); err != nil {
			return fmt.Errorf("failed to unmount overlay for the fs rounds: %w", err)
		}
		return os.MkdirAll(container.UpperDir, 0755)
	})
	server.ReceiveStream("fs_upper_close", func(r io.Reader) error {
		_, _ = io.Copy(io.Discard, r)
		fmt.Println("Filesystem pre-copy done: overlay remounted")
		return container.MountOverlay()
	})

	// failure handling: synth-precopy's final dump runs
	// --leave-running, so the source stays live through this reassembly —
	// this stream's ACK (the transport's success/failure byte) is exactly
	// the "destination holds a checksummed, restorable image" signal the
	// source's shipFinal retry loop waits for before it commits to killing
	// itself. Restore does NOT happen here: it waits for the explicit
	// synth_restore_go trigger below, sent only after the source has already
	// killed itself. A reassembly/checksum failure here is always safe for
	// the source to retry — nothing has been disturbed yet.
	server.ReceiveStream("synth_final", func(r io.Reader) error {
		return synthRound("final", r)
	})

	// The restore trigger. In catch-up mode the source is still paused and is killed only
	// after this ACKs; it asks "dest_status" if the ACK is lost. In the availability
	// order it has already killed itself.
	var restoredOK atomic.Bool
	server.ReceiveStream("synth_restore_go", func(r io.Reader) error {
		_, _ = io.ReadAll(r)
		markDst("restore_go_received")
		// Rebuild CRIU's relative parent-chain symlinks locally: they are not
		// shipped (a symlink to the source's directory layout is meaningless
		// here), and restore resolves PE_PARENT entries through them.
		if err := relinkPreDumpChain(); err != nil {
			migrationDone <- err
			return err
		}
		fmt.Println("Restoring container from reassembled checkpoint chain...")
		err := restoreContainer(ctx, cfg)
		if err == nil {
			restoredOK.Store(true)
		}
		migrationDone <- err
		return err
	})

	// (--merge-restore): the final delta stream itself triggers the
	// restore once reassembly + checksum verification pass, and its ACK means
	// "restored and serving". A failure here NACKs and the source (still
	// paused) resumes; "dest_status" lets the source resolve an ambiguous
	// outcome (lost ACK) without ever running both copies.
	server.ReceiveStream("synth_final_restore", func(r io.Reader) error {
		if err := synthRound("final", r); err != nil {
			return err
		}
		markDst("restore_go_received")
		if err := relinkPreDumpChain(); err != nil {
			migrationDone <- err
			return err
		}
		fmt.Println("Restoring container from reassembled checkpoint chain (merged final)...")
		err := restoreContainer(ctx, cfg)
		if err == nil {
			restoredOK.Store(true)
		}
		migrationDone <- err
		return err
	})
	server.ReceiveRequest("dest_status", func(req io.Reader) ([]byte, error) {
		_, _ = io.ReadAll(req)
		if restoredOK.Load() {
			return []byte("running"), nil
		}
		return []byte("absent"), nil
	})

	// M5: residual/residual_addr/
	// residual_restore_go/residual_push/residual_abort handlers, plus the
	// fake page server + drain-set bookkeeping. See internal/receiver/postcopy.go.
	pcState := registerPostcopyHandlers(ctx, server, cfg, migrationDone)

	// Stock CRIU lazy-pages baseline (--mode criu-lazy): lazy_carrier /
	// stock_images / stock_restore. See internal/receiver/criulazy.go.
	registerCriuLazyHandlers(ctx, server, cfg, migrationDone)

	// rsync-transport precopy (--mode precopy-rsync): the source has already
	// pushed ./checkpoint/pre0..pre3/final via the rsync daemon started
	// above, so unlike "final" there is nothing to tar-extract here — just
	// restore straight from the files already on disk.
	server.ReceiveStream("final_rsync", func(r io.Reader) error {
		_, _ = io.ReadAll(r)
		if err := relinkPreDumpChain(); err != nil {
			return err
		}
		fmt.Println("Received final_rsync trigger (files already placed via rsync). Restoring container...")
		err := restoreContainer(ctx, cfg)
		migrationDone <- err
		return err
	})

	server.ReceiveStream("sync_volume", func(r io.Reader) error {
		// Re-read config in case statepod.json was sent by source, so we have the correct VolumePath
		if updatedCfg, err := container.ReadConfig(); err == nil {
			cfg = updatedCfg
		}

		destDir := filepath.Join(".", "rootfs")
		if cfg.VolumePath != "" {
			destDir = filepath.Join(".", "rootfs", cfg.VolumePath)
		}

		if err := os.MkdirAll(destDir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", destDir, err)
		}

		fmt.Println("Receiving and extracting sync_volume stream...")
		cmd := exec.CommandContext(ctx, "sudo", "tar", "-xf", "-", "-C", destDir)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		cmd.Stdin = r
		if err := cmd.Run(); err != nil {
			fmt.Printf("tar extraction error for volume: %s\n", stderr.String())
			return fmt.Errorf("tar extraction failed for volume: %w", err)
		}

		fmt.Println("Volume sync stream received. Starting container from synced volume...")

		err := startSyncedContainer(ctx, cfg)
		migrationDone <- err
		return err
	})

	server.ReceiveStream("sync_start", func(r io.Reader) error {
		fmt.Println("Received sync_start stream. Starting container without volume sync...")
		_, _ = io.ReadAll(r)

		updatedCfg, err := container.ReadConfig()
		if err == nil {
			cfg = updatedCfg
		}

		err = startSyncedContainer(ctx, cfg)
		migrationDone <- err
		return err
	})

	// Start listening in a goroutine
	go func() {
		if err := server.Start(ctx, listenAddr); err != nil {
			// If it's not a normal stop, signal the error
			select {
			case migrationDone <- err:
			default:
			}
		}
	}()

	// Wait for migration to complete or context to be cancelled
	select {
	case <-ctx.Done():
		server.Stop()
		return ctx.Err()
	case err := <-migrationDone:
		// Wait for metrics to arrive before stopping the server
		fmt.Println("Waiting for final metrics stream...")
		select {
		case <-metricsReceived:
			fmt.Println("Metrics received successfully.")
		case <-time.After(10 * time.Second):
			fmt.Println("Warning: timed out waiting for metrics from source.")
		}

		server.Stop()
		if err == nil {
			// synth-postcopy's source already set RestoreEndTime (its own clock,
			// at the post-drain kill); keep it rather than overwrite it with this
			// host's clock. Other modes leave it zero.
			if metrics.RestoreEndTime.IsZero() {
				metrics.RestoreEndTime = time.Now()
			}
			if metrics.Pipeline != nil {
				metrics.Pipeline.ReassembleMs = reassembleMs
			}
			if metrics.Mode == "synth-postcopy" {
				pcState.fillMetrics(&metrics)
			}
			metrics.MergePhases(snapshotDst())
			if metricsPath == "" {
				// No explicit path given: auto-detect an experiment dir,
				// else fall back to the current directory.
				metricsPath = "migration_metrics.json"
				if _, statErr := os.Stat("../../experiment"); statErr == nil {
					metricsPath = "../../experiment/migration_metrics.json"
				} else if _, statErr := os.Stat("./experiment"); statErr == nil {
					metricsPath = "./experiment/migration_metrics.json"
				}
			}

			fmt.Printf("Saving metrics to %s...\n", metricsPath)
			if mErr := metrics.SaveMetrics(metricsPath); mErr != nil {
				fmt.Printf("Warning: failed to save metrics: %v\n", mErr)
			}
		}
		return err
	}
}

// startRsyncDaemon launches a companion `rsync --daemon` exposing the bundle's
// checkpoint/ and rootfs/ dirs as modules, for the precopy-rsync/sync-rsync
// transport A/B (addendum) against statepod's own tar+TCP transport.
// Same unauthenticated trust model as the existing TCP transport — this is a
// research testbed on a private link, not a hardened deployment.
func startRsyncDaemon(ctx context.Context, port string) (stop func(), err error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	for _, dir := range []string{"checkpoint", "rootfs", container.UpperDir} {
		if err := os.MkdirAll(filepath.Join(wd, dir), 0755); err != nil {
			return nil, fmt.Errorf("failed to create %s: %w", dir, err)
		}
	}

	confPath := filepath.Join(os.TempDir(), fmt.Sprintf("statepod_rsyncd_%s.conf", port))
	// munge symlinks = no: rsyncd's default symlink-munging (rewriting link
	// targets to /rsyncd-munged/<target> so they can't escape the module root)
	// corrupts CRIU's "parent" symlink (a real relative symlink to the prior
	// pre-dump dir, e.g. "../pre3") that the parent-chain restore depends on.
	// Safe to disable here: same trust model as statepod's own unauthenticated
	// TCP transport, not a hardened multi-tenant daemon.
	// [upper]: the filesystem pre-copy rounds (migration/fsprecopy.go) write the
	// overlay upperdir here while fs_upper_open has the overlay unmounted.
	conf := fmt.Sprintf("uid = root\ngid = root\nuse chroot = no\nread only = no\nmunge symlinks = no\n\n[checkpoint]\n    path = %s\n\n[rootfs]\n    path = %s\n\n[upper]\n    path = %s\n",
		filepath.Join(wd, "checkpoint"), filepath.Join(wd, "rootfs"), filepath.Join(wd, container.UpperDir))
	if err := os.WriteFile(confPath, []byte(conf), 0644); err != nil {
		return nil, fmt.Errorf("failed to write rsyncd config: %w", err)
	}

	cmd := exec.CommandContext(ctx, "sudo", "rsync", "--daemon", "--no-detach", "--port", port, "--config", confPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to launch rsync daemon: %w", err)
	}
	fmt.Printf("rsync daemon listening on :%s (modules: checkpoint, rootfs, upper)\n", port)

	stop = func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		// sudo's child (the actual rsync process) can survive killing sudo
		// itself; sweep it up by the unique port in its argv.
		_ = exec.Command("sudo", "pkill", "-f", fmt.Sprintf("rsync --daemon --no-detach --port %s", port)).Run()
		_ = os.Remove(confPath)
	}
	return stop, nil
}

// relinkPreDumpChain rebuilds CRIU's "parent" symlinks (pre1->pre0,
// pre2->pre1, pre3->pre2, final->pre3) after an rsync-transport push.
// rsync's default symlink handling strips the ".." component from CRIU's
// relative parent-chain symlinks (e.g. "./../pre3" arrives as bare "pre3"),
// which breaks restore's parent-path resolution (criu/image.c:open_parent
// follows the symlink relative to its own containing directory, so it needs
// the ".." to reach the sibling dir) — confirmed against tar-transport, which
// preserves it correctly, and against a `munge symlinks = no` daemon config,
// which does not fix it. Finding: a naive rsync/bash
// migration would need this same transport-specific fix-up to work at all.
func relinkPreDumpChain() error {
	// Discover the rounds actually present rather than assuming a fixed four:
	// synth-precopy's round count is a measured knob (--precopy-rounds), and
	// zero rounds means the final dump has no parent at all.
	var rounds []int
	for i := 0; ; i++ {
		if _, err := os.Stat(fmt.Sprintf("checkpoint/pre%d", i)); err != nil {
			break
		}
		rounds = append(rounds, i)
	}

	type link struct{ dir, parent string }
	var chain []link
	for _, i := range rounds {
		if i == 0 {
			continue // pre0 starts the chain; it has no parent
		}
		chain = append(chain, link{fmt.Sprintf("checkpoint/pre%d", i), fmt.Sprintf("pre%d", i-1)})
	}
	if n := len(rounds); n > 0 {
		chain = append(chain, link{"checkpoint/final", fmt.Sprintf("pre%d", rounds[n-1])})
	}

	for _, link := range chain {
		if _, err := os.Stat(link.dir); os.IsNotExist(err) {
			continue // rsync-transport sync mode / partial chains don't apply
		}
		parentLink := filepath.Join(link.dir, "parent")
		if err := os.Remove(parentLink); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove stale parent link %s: %w", parentLink, err)
		}
		if err := os.Symlink(filepath.Join("..", link.parent), parentLink); err != nil {
			return fmt.Errorf("failed to relink %s: %w", parentLink, err)
		}
	}
	return nil
}

func restoreContainer(ctx context.Context, cfg *container.Config) error {
	fmt.Println("Restoring container from checkpoint:", cfg.ContainerID)

	// Restore the container using the final checkpoint directory.
	// We use -d (detach) so it starts in the background and we can then set up networking.
	cmd := exec.CommandContext(ctx, "sudo", "runc", "restore", "-d", "--image-path", "./checkpoint/final", "--tcp-established", cfg.ContainerID)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	markDst("restore_start")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to restore container: %w", err)
	}
	markDst("restore_end")

	return finishRestoreNetworking(ctx, cfg)
}

// finishRestoreNetworking is the post-`runc restore` tail shared by every
// restore path (plain, rsync-transport, and M5 lazy-pages): fetch the new
// PID, wire up the bridge/NAT/veth/port-forward, and persist CurrentPID/
// CurrentState. Assumes the container is already restored and detached.
func finishRestoreNetworking(ctx context.Context, cfg *container.Config) error {
	state, err := container.GetRuncState(ctx, cfg.ContainerID)
	if err != nil {
		return fmt.Errorf("failed to get state after restore: %w", err)
	}

	pid := state.Pid
	fmt.Printf("Container restored with PID: %d\n", pid)
	markDst("state_fetched")

	// Normally already staged at the migration's first contact (no-op then);
	// does the work here only if that staging was skipped or failed.
	prestageHostNetwork(ctx, cfg)
	markDst("hostnet_ready")

	if err := network.SetupContainerNetwork(ctx, pid, cfg.ContainerIP, cfg.Gateway); err != nil {
		return fmt.Errorf("failed to setup container network: %w", err)
	}
	markDst("netns_attached")

	markDst("network_ready")
	fmt.Println("Container restored successfully!")
	cfg.CurrentPID = pid
	cfg.CurrentState = "running"

	return container.UpdateConfig(cfg)
}

func startSyncedContainer(ctx context.Context, cfg *container.Config) error {
	fmt.Println("Starting synced container:", cfg.ContainerID)
	markDst("start_begin")

	if err := exec.CommandContext(ctx, "sudo", "runc", "create", cfg.ContainerID).Run(); err != nil {
		return fmt.Errorf("failed to create runc container: %w", err)
	}

	state, err := container.GetRuncState(ctx, cfg.ContainerID)
	if err != nil {
		return fmt.Errorf("failed to get state after create: %w", err)
	}

	pid := state.Pid
	fmt.Printf("Synced container created with PID: %d\n", pid)

	if err := network.EnsureBridge(ctx, cfg.Gateway); err != nil {
		return fmt.Errorf("failed to ensure bridge: %w", err)
	}
	network.SetupNAT(ctx, cfg.Subnet)
	if err := network.SetupContainerNetwork(ctx, pid, cfg.ContainerIP, cfg.Gateway); err != nil {
		return fmt.Errorf("failed to setup container network: %w", err)
	}
	// Same host port as the precopy/restore path so an external proxy
	// (HAProxy failover testbed) sees one backend port regardless of
	// which migration mode brought the container up.
	if err := network.SetupPortForwarding(ctx, cfg.ContainerIP, cfg.RedisHostPort(), cfg.ServicePort()); err != nil {
		fmt.Printf("Warning: Failed to setup port forwarding: %v\n", err)
	}

	markDst("network_ready")
	if err := exec.CommandContext(ctx, "sudo", "runc", "start", cfg.ContainerID).Run(); err != nil {
		return fmt.Errorf("failed to start container: %w", err)
	}
	markDst("started")

	fmt.Println("Synced container started successfully!")
	cfg.CurrentPID = pid
	cfg.CurrentState = "running"
	return container.UpdateConfig(cfg)
}
