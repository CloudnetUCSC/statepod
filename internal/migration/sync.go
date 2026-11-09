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

	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/transport"
)

// SyncStrategy implements the restart-based migration path (kill, copy the
// data dir, cold-start fresh at the destination). Transport selects how the
// volume data moves: "" / "tar" = statepod's own tar+custom-TCP transport
// (default); "rsync" = rsync against the receiver's companion daemon
// (addendum transport A/B).
type SyncStrategy struct {
	Transport string
	RsyncPort string
}

func (s *SyncStrategy) Execute(ctx context.Context, cfg *container.Config, destClient transport.Client, destAddr string) error {
	mode := "sync"
	if s.Transport == "rsync" {
		mode = "sync-rsync"
	}
	fmt.Printf("Executing Volume Sync Strategy (mode=%s) for container: %s\n", mode, cfg.ContainerID)
	metrics := &MigrationMetrics{
		Mode:               mode,
		ContainerID:        cfg.ContainerID,
		MigrationStartTime: time.Now(),
	}

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

	streamDir := func(dirPath string, streamType string) (float64, int64, error) {
		fmt.Printf("Streaming %s as %s...\n", dirPath, streamType)

		if err := destClient.Connect(ctx, destAddr); err != nil {
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

		transferStart := time.Now()
		if err := destClient.SendStream(ctx, streamType, stdout); err != nil {
			return 0, 0, err
		}
		transferTime := time.Since(transferStart)

		if err := cmd.Wait(); err != nil {
			return 0, 0, fmt.Errorf("tar failed for %s: %w", streamType, err)
		}

		sz, _ := getDirSize(dirPath)
		return float64(transferTime.Milliseconds()), sz, nil
	}

	// rsync-transport alternative to streamDir (addendum) — see
	// precopy.go's rsyncPush for the flag/measurement rationale (-az --stats,
	// realistic "just rsync it" usage, LC_ALL=C for parseable stats).
	rsyncPush := func(dirPath string, destSubpath string) (float64, int64, error) {
		rsyncPort := s.RsyncPort
		if rsyncPort == "" {
			rsyncPort = DefaultRsyncPort
		}
		destHost := strings.SplitN(destAddr, ":", 2)[0]
		target := fmt.Sprintf("rsync://%s:%s/rootfs/%s/", destHost, rsyncPort, destSubpath)
		fmt.Printf("rsync-pushing %s to %s...\n", dirPath, target)

		cmd := exec.CommandContext(ctx, "sudo", "rsync", "-az", "--stats", dirPath+"/", target)
		cmd.Env = append(os.Environ(), "LC_ALL=C")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		transferStart := time.Now()
		if err := cmd.Run(); err != nil {
			return 0, 0, fmt.Errorf("rsync push failed for volume: %s: %w", stderr.String(), err)
		}
		transferTime := time.Since(transferStart)

		sentBytes := parseRsyncSentBytes(stdout.String())
		bandwidth := float64(sentBytes*8) / (transferTime.Seconds() * 1000000)
		fmt.Printf("rsync transfer volume: sent=%d bytes, time=%v, bandwidth=%.2f Mbps\n", sentBytes, transferTime, bandwidth)

		return float64(transferTime.Milliseconds()), sentBytes, nil
	}

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

	if cfg.IsRedis && cfg.VolumeSync {
		fmt.Println("Triggering Redis SAVE before syncing volume...")
		cmd := exec.CommandContext(ctx, "redis-cli", "-h", "10.10.0.10", "-p", "6379", "SAVE")
		if err := cmd.Run(); err != nil {
			fmt.Printf("Warning: Failed to trigger Redis save (is redis-cli installed?): %v\n", err)
		}
	}

	fmt.Println("Killing source container...")
	metrics.FreezeStartTime = metrics.Mark("src_kill")
	killCmd := exec.CommandContext(ctx, "sudo", "runc", "kill", cfg.ContainerID, "SIGKILL")
	if err := killCmd.Run(); err != nil {
		fmt.Printf("Warning: failed to kill container: %v\n", err)
	}

	// Optional delay to ensure full shutdown
	time.Sleep(1 * time.Second)

	fmt.Println("Synchronizing configs to destination...")
	metrics.Mark("src_ship_start")
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

	if cfg.VolumeSync {
		syncPath := "./rootfs"
		if cfg.VolumePath != "" {
			syncPath = filepath.Join("./rootfs", cfg.VolumePath)
		}

		if s.Transport == "rsync" {
			fmt.Println("Rsync-pushing volume, then triggering start (data already on disk)...")
			ts, sz, err := rsyncPush(syncPath, cfg.VolumePath)
			if err != nil {
				return fmt.Errorf("failed to rsync-push volume: %w", err)
			}
			metrics.TotalDataTransferredBytes += sz
			metrics.Iterations = append(metrics.Iterations, PreCopyIterationInfo{
				Iteration: 0, Duration: ts, SizeInBytes: sz, TransferTime: ts,
				Bandwidth: float64(sz*8) / (ts / 1000.0 * 1000000),
			})
			// Data is already on disk via rsync — reuse sync_start (the
			// existing "volume present via other means, just start" path),
			// not sync_volume (which expects a tar stream to extract).
			if err := destClient.Connect(ctx, destAddr); err != nil {
				return fmt.Errorf("failed to reconnect for stream sync_start: %w", err)
			}
			if err := destClient.SendStream(ctx, "sync_start", bytes.NewReader([]byte{})); err != nil {
				return fmt.Errorf("failed to send sync_start: %w", err)
			}
		} else {
			fmt.Println("Packaging and streaming volume...")
			ts, sz, err := streamDir(syncPath, "sync_volume")
			if err != nil {
				return fmt.Errorf("failed to stream volume: %w", err)
			}
			metrics.TotalDataTransferredBytes += sz

			metrics.Iterations = append(metrics.Iterations, PreCopyIterationInfo{
				Iteration:    0,
				Duration:     ts,
				SizeInBytes:  sz,
				TransferTime: ts,
				Bandwidth:    float64(sz*8) / (ts / 1000.0 * 1000000),
			})
		}
	} else {
		fmt.Println("Not performing volume sync. Sending sync_start to trigger start in destination.")
		if err := destClient.Connect(ctx, destAddr); err != nil {
			return fmt.Errorf("failed to reconnect for stream sync_start: %w", err)
		}
		if err := destClient.SendStream(ctx, "sync_start", bytes.NewReader([]byte{})); err != nil {
			return fmt.Errorf("failed to send sync_start: %w", err)
		}
	}

	sourceSideFinishTime := metrics.Mark("src_start_acked")
	metrics.DowntimeSourceSide = float64(sourceSideFinishTime.Sub(metrics.FreezeStartTime).Milliseconds())
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
