package container

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/network"
)

func Start(ctx context.Context, source string, volumeSync bool) error {
	cfg, err := ReadConfig()
	if err != nil {
		return err
	}

	if source != "" {
		// Moving receiver logic to a separate entry point in cli to avoid import cycles.
		return fmt.Errorf("receiver mode should be handled by the cli/receiver package now")
	}

	cfg.VolumeSync = volumeSync

	fmt.Println("Creating container:", cfg.ContainerID)

	if err := exec.CommandContext(ctx, "sudo", "runc", "create", cfg.ContainerID).Run(); err != nil {
		return err
	}

	state, err := GetRuncState(ctx, cfg.ContainerID)
	if err != nil {
		return err
	}

	pid := state.Pid

	if err := network.EnsureBridge(ctx, cfg.Gateway); err != nil {
		return err
	}

	network.SetupNAT(ctx, cfg.Subnet)

	if err := network.SetupContainerNetwork(ctx, pid, cfg.ContainerIP, cfg.Gateway); err != nil {
		return err
	}

	// host_port -1 = no external exposure (template synthesis instances must
	// not touch the DNAT rules a concurrently serving container relies on).
	if cfg.HostPort >= 0 {
		if err := network.SetupPortForwarding(ctx, cfg.ContainerIP, cfg.RedisHostPort(), cfg.ServicePort()); err != nil {
			fmt.Printf("Warning: Failed to setup port forwarding: %v\n", err)
		}
	}

	fmt.Println("Starting container:", cfg.ContainerID)
	switch {
	case cfg.Spec != nil:
		fmt.Printf("Workload %s listening at %s:%d\n", cfg.Spec.Name, cfg.ContainerIP, cfg.ServicePort())
	case cfg.IsRedis:
		fmt.Printf("Redis server running at %s:6379\n", cfg.ContainerIP)
	default:
		fmt.Printf("Node server running at http://%s:3000\n", cfg.ContainerIP)
	}

	if err := exec.CommandContext(ctx, "sudo", "runc", "start", cfg.ContainerID).Run(); err != nil {
		return err
	}

	// Readiness point (§3.2): the capture/tracking hooks (M1/M2)
	// anchor here, identically at source start and template synthesis.
	if readyIn, err := WaitReady(ctx, cfg, DefaultReadinessTimeout); err != nil {
		fmt.Printf("Warning: readiness probe failed: %v\n", err)
	} else {
		fmt.Printf("Workload ready in %s\n", readyIn.Round(time.Millisecond))
	}

	cfg.CurrentPID = pid
	cfg.CurrentState = "running"

	return UpdateConfig(cfg)
}

func Stop(ctx context.Context) error {
	cfg, err := ReadConfig()
	if err != nil {
		return fmt.Errorf("failed to read statepod config: %w", err)
	}
	fmt.Println("Stopping container:", cfg.ContainerID)

	cmd := exec.CommandContext(ctx, "sudo", "runc", "kill", cfg.ContainerID, "SIGKILL")

	if err := cmd.Run(); err != nil {
		return err
	}

	cfg.CurrentState = "stopped"
	cfg.CurrentPID = 0

	// Delete the container
	if err := exec.CommandContext(ctx, "sudo", "runc", "delete", cfg.ContainerID).Run(); err != nil {
		return err
	}

	return UpdateConfig(cfg)
}

func Status(ctx context.Context) error {
	cfg, err := ReadConfig()
	if err != nil {
		return fmt.Errorf("failed to read statepod config: %w", err)
	}

	fmt.Printf("Container ID: %s\n", cfg.ContainerID)
	fmt.Printf("Docker Source: %s\n", cfg.DockerSource)
	fmt.Printf("Created At: %s\n", cfg.CreatedAt)
	fmt.Printf("Current State: %s\n", cfg.CurrentState)
	fmt.Printf("Current PID: %d\n", cfg.CurrentPID)

	return nil
}
