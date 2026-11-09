package container

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/workload"
)

// DefaultReadinessTimeout bounds how long WaitReady polls before giving up.
const DefaultReadinessTimeout = 30 * time.Second

// WaitReady blocks until the workload inside the container reaches its
// readiness point (§3.2): Redis answers PING with +PONG, the Node
// server accepts a TCP connection. It returns how long readiness took. The
// same hook is used at source start and at destination template synthesis
// (M1) so both captures happen at an identical point in the workload's life.
func WaitReady(ctx context.Context, cfg *Config, timeout time.Duration) (time.Duration, error) {
	addr := net.JoinHostPort(cfg.ContainerIP, fmt.Sprintf("%d", cfg.ServicePort()))
	if cfg.Spec != nil {
		return waitReadySpec(ctx, addr, cfg.ContainerID, cfg.Spec.Ready, timeout)
	}
	return waitReadyAddr(ctx, addr, cfg.IsRedis, timeout)
}

// waitReadySpec polls the readiness check a workload.Spec declares (tcp, resp,
// http or exec) until it passes or the timeout elapses.
func waitReadySpec(ctx context.Context, addr, containerID string, r workload.Readiness, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	deadline := start.Add(timeout)
	for {
		if err := probeSpecOnce(ctx, addr, containerID, r); err == nil {
			return time.Since(start), nil
		}
		if time.Now().After(deadline) {
			return time.Since(start), fmt.Errorf("workload not ready after %s (%s, %s)", timeout, addr, r.Kind)
		}
		select {
		case <-ctx.Done():
			return time.Since(start), ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func probeSpecOnce(ctx context.Context, addr, containerID string, r workload.Readiness) error {
	switch r.Kind {
	case workload.ReadyRESP:
		return probeOnce(ctx, addr, true)
	case workload.ReadyTCP:
		return probeOnce(ctx, addr, false)
	case workload.ReadyHTTP:
		return probeHTTP(ctx, addr, r.Path)
	case workload.ReadyExec:
		args := append([]string{"runc", "exec", containerID}, r.Cmd...)
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return exec.CommandContext(cctx, "sudo", args...).Run()
	default:
		return fmt.Errorf("unknown readiness kind %q", r.Kind)
	}
}

// probeHTTP is ready on any 2xx/3xx answer to GET path.
func probeHTTP(ctx context.Context, addr, path string) error {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	cctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d from %s%s", resp.StatusCode, addr, path)
	}
	return nil
}

func waitReadyAddr(ctx context.Context, addr string, isRedis bool, timeout time.Duration) (time.Duration, error) {
	start := time.Now()
	deadline := start.Add(timeout)

	for {
		if err := probeOnce(ctx, addr, isRedis); err == nil {
			return time.Since(start), nil
		}
		if time.Now().After(deadline) {
			return time.Since(start), fmt.Errorf("workload not ready after %s (%s)", timeout, addr)
		}
		select {
		case <-ctx.Done():
			return time.Since(start), ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func probeOnce(ctx context.Context, addr string, isRedis bool) error {
	d := net.Dialer{Timeout: 500 * time.Millisecond}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Node: a successful accept on the listening socket is the readiness point.
	if !isRedis {
		return nil
	}

	// Redis: inline RESP PING, ready only on +PONG.
	if err := conn.SetDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		return err
	}
	if _, err := conn.Write([]byte("PING\r\n")); err != nil {
		return err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(line, "+PONG") {
		return fmt.Errorf("unexpected PING reply %q", line)
	}
	return nil
}
