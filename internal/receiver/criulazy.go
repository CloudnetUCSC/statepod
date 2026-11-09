package receiver

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/compress"
	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/transport"
)

// Destination side of the stock CRIU lazy-pages baseline (--mode criu-lazy; see
// migration.CriuLazyStrategy for the whole protocol). Three pieces:
//
//   - "lazy_carrier" (raw connection): the source's outbound connection, parked
//     until the lazy-pages daemon dials the loopback listener started by
//     "stock_restore", then spliced to it for the whole drain;
//   - "stock_images": the lazy dump's images (no pages) extracted into
//     ./checkpoint/final;
//   - "stock_restore": starts `criu lazy-pages --page-server` pointed at the
//     loopback listener, restores with `runc restore --lazy-pages`, and ACKs once
//     the container is serving (the source's downtime ends there).
type criuLazyState struct {
	carrierCh chan net.Conn // the parked carrier, handed to the splice
	pairDone  chan struct{} // closed when the carrier handler may return
	once      sync.Once
}

func registerCriuLazyHandlers(ctx context.Context, server transport.Server, cfg *container.Config, migrationDone chan error) {
	st := &criuLazyState{carrierCh: make(chan net.Conn, 1), pairDone: make(chan struct{})}
	const imgDir = "./checkpoint/final"

	server.ReceiveConn("lazy_carrier", func(conn net.Conn) error {
		markDst("lazy_carrier_opened")
		st.carrierCh <- conn
		<-st.pairDone // the splice owns the connection until the drain ends
		return nil
	})

	server.ReceiveStream("stock_images", func(r io.Reader) error {
		if err := os.MkdirAll(imgDir, 0755); err != nil {
			return err
		}
		dr, err := compress.DecompressAuto(r)
		if err != nil {
			return fmt.Errorf("decompressing stock_images: %w", err)
		}
		defer dr.Close()
		fmt.Println("Receiving and extracting the lazy dump images...")
		cmd := exec.CommandContext(ctx, "sudo", "tar", "-xf", "-", "-C", imgDir)
		var stderr bytes.Buffer
		cmd.Stderr, cmd.Stdin = &stderr, dr
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("tar extraction failed for stock_images: %s: %w", stderr.String(), err)
		}
		markDst("stock_images_received")
		return nil
	})

	server.ReceiveStream("stock_restore", func(r io.Reader) error {
		_, _ = io.ReadAll(r)
		markDst("restore_go_received")
		err := st.restore(ctx, cfg, imgDir, migrationDone)
		if err != nil {
			select {
			case migrationDone <- err:
			default:
			}
		}
		return err
	})
}

func (st *criuLazyState) restore(ctx context.Context, cfg *container.Config, imgDir string, migrationDone chan error) error {
	var carrier net.Conn
	select {
	case carrier = <-st.carrierCh:
	case <-time.After(60 * time.Second):
		return fmt.Errorf("no lazy_carrier connection from the source")
	case <-ctx.Done():
		return ctx.Err()
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("failed to listen for the lazy-pages daemon: %w", err)
	}
	_, port, _ := net.SplitHostPort(ln.Addr().String())

	// The daemon dials ln; splice that connection to the carrier. criu's
	// work-dir must be the SAME directory `runc restore` uses (it holds the
	// lazy-pages.socket the restore's UFFD monitor connects to) -- see M5's
	// handleRestoreGo for the failure it avoids.
	lazyCmd := exec.CommandContext(ctx, "sudo", "criu", "lazy-pages",
		"-D", imgDir, "--work-dir", imgDir, "--page-server", "--address", "127.0.0.1", "--port", port)
	lazyCmd.Stdout, lazyCmd.Stderr = os.Stdout, os.Stderr
	if err := lazyCmd.Start(); err != nil {
		ln.Close()
		return fmt.Errorf("failed to start criu lazy-pages: %w", err)
	}
	lazyExited := make(chan error, 1)
	go func() { lazyExited <- lazyCmd.Wait() }()

	spliceDone := make(chan struct{})
	go func() {
		defer close(spliceDone)
		defer ln.Close()
		daemonConn, err := ln.Accept()
		if err != nil {
			carrier.Close()
			return
		}
		markDst("lazy_daemon_connected")
		var wg sync.WaitGroup
		var once sync.Once
		closeBoth := func() { once.Do(func() { daemonConn.Close(); carrier.Close() }) }
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = io.Copy(carrier, daemonConn); closeBoth() }()
		go func() { defer wg.Done(); _, _ = io.Copy(daemonConn, carrier); closeBoth() }()
		wg.Wait()
	}()

	restoreCmd := exec.CommandContext(ctx, "sudo", "runc", "restore", "-d", "--lazy-pages",
		"--image-path", imgDir, "--tcp-established", cfg.ContainerID)
	restoreCmd.Stdout, restoreCmd.Stderr = os.Stdout, os.Stderr
	markDst("restore_start")
	if err := restoreCmd.Run(); err != nil {
		return fmt.Errorf("lazy-pages restore failed: %w", err)
	}
	markDst("restore_end")
	if err := finishRestoreNetworking(ctx, cfg); err != nil {
		return err
	}

	// Serving. The migration is over once the daemon has pulled everything and
	// exited (or the source closed the carrier at the end of the drain).
	go func() {
		select {
		case <-lazyExited:
		case <-spliceDone:
			// The page stream ended; give the daemon a moment to exit by itself.
			select {
			case <-lazyExited:
			case <-time.After(15 * time.Second):
				_ = lazyCmd.Process.Kill()
			}
		}
		markDst("lazy_drain_end")
		st.once.Do(func() { close(st.pairDone) })
		migrationDone <- nil
	}()
	return nil
}
