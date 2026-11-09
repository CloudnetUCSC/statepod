package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/transport"
)

// VolumeRestartStrategy is the volume-sync restart baseline (§1.6,
// D6): what moving a stateful workload looks like without live migration, the
// way a Kubernetes StatefulSet with a replicated or re-attached volume does it.
// The container's writable layer (where every workload here keeps its data
// directory) is pre-synced in the same rsync rounds every variant uses
// (fsprecopy.go) while the source serves; the source is then stopped
// GRACEFULLY (SIGTERM, the grace period, then SIGKILL, as the kubelet does), so
// the application flushes its own state to disk; a final round carries what
// changed; the destination cold-starts a fresh process from the image plus the
// synced layer, which recovers from disk (Redis replays its AOF, PostgreSQL
// opens its data directory). No process memory moves: in-memory-only state
// (caches, sessions, JIT/warm-up) is lost by design, and the application's own
// recovery time is part of what a client sees.
//
// Downtime (engine) = SIGTERM -> destination container started; the client
// probe adds the application's recovery until it answers (the honest number).
type VolumeRestartStrategy struct {
	RsyncPort string
	ZstdLevel int
	// Grace is the SIGTERM -> SIGKILL wait (0 = 30 s, the Kubernetes default).
	Grace time.Duration
}

func (s *VolumeRestartStrategy) Execute(ctx context.Context, cfg *container.Config, destClient transport.Client, destAddr string) error {
	fmt.Printf("Executing volume-sync restart strategy for container: %s\n", cfg.ContainerID)
	if !cfg.Stateful {
		return fmt.Errorf("restart-volume needs a stateful (overlay) bundle: a stateless container has no volume to sync")
	}
	metrics := &MigrationMetrics{Mode: "restart-volume", ContainerID: cfg.ContainerID, MigrationStartTime: time.Now()}
	grace := s.Grace
	if grace == 0 {
		grace = 30 * time.Second
	}

	// 1. Pre-sync the volume while the source serves.
	fsr := newFSPrecopy(ctx, destClient, destAddr, s.RsyncPort, s.ZstdLevel, metrics)
	metrics.Mark("src_fs_precopy_start")
	if err := fsr.precopyLive(); err != nil {
		return fmt.Errorf("volume pre-sync failed, source untouched: %w", err)
	}
	metrics.Mark("src_fs_precopy_end")

	// 2. Graceful stop: the service is down from here.
	fmt.Println("Stopping source gracefully (SIGTERM)")
	metrics.FreezeStartTime = metrics.Mark("src_sigterm")
	if err := exec.CommandContext(ctx, "sudo", "runc", "kill", cfg.ContainerID, "SIGTERM").Run(); err != nil {
		return fmt.Errorf("SIGTERM failed: %w", err)
	}
	deadline := time.Now().Add(grace)
	for {
		st, err := container.GetRuncState(ctx, cfg.ContainerID)
		if err != nil || st.Status == "stopped" {
			break
		}
		if time.Now().After(deadline) {
			fmt.Printf("Grace period (%s) over: SIGKILL\n", grace)
			metrics.Mark("src_sigkill")
			_ = exec.CommandContext(ctx, "sudo", "runc", "kill", cfg.ContainerID, "SIGKILL").Run()
			time.Sleep(time.Second)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	metrics.Mark("src_stopped")

	// 3. Final round (consistent: the application has exited), configs, cold start.
	metrics.Mark("src_ship_start")
	if err := fsr.final(); err != nil {
		return fmt.Errorf("final volume round failed: %w", err)
	}
	for _, f := range []struct{ path, streamType string }{{"statepod.json", "statepod_cfg"}, {"config.json", "oci_config"}} {
		if err := streamOneFile(ctx, destClient, destAddr, f.path, f.streamType); err != nil {
			return err
		}
		if fi, err := os.Stat(f.path); err == nil {
			metrics.TotalDataTransferredBytes += fi.Size()
		}
	}
	if err := destClient.Connect(ctx, destAddr); err != nil {
		return fmt.Errorf("failed to reconnect for sync_start: %w", err)
	}
	if err := destClient.SendStream(ctx, "sync_start", bytes.NewReader(nil)); err != nil {
		return fmt.Errorf("destination failed to start the container: %w", err)
	}
	started := metrics.Mark("src_start_acked")
	metrics.DowntimeSourceSide = float64(started.Sub(metrics.FreezeStartTime).Milliseconds())
	metrics.TotalMigrationTime = float64(started.Sub(metrics.MigrationStartTime).Milliseconds())
	fmt.Printf("Source-side Downtime: %.2f ms (to container start; the application then recovers from disk)\n", metrics.DowntimeSourceSide)

	data, _ := json.Marshal(metrics)
	if err := destClient.Connect(ctx, destAddr); err != nil {
		return fmt.Errorf("failed to reconnect for metrics: %w", err)
	}
	return destClient.SendStream(ctx, "metrics", bytes.NewReader(data))
}
