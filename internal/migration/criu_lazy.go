package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/transport"
)

// CriuLazyPort is the loopback port stock CRIU's page server listens on at the
// source and the destination's `criu lazy-pages` daemon dials at the destination;
// the carrier connection (below) joins the two.
const CriuLazyPort = 27027

// CriuLazyStrategy is the STOCK CRIU post-copy baseline ("measure M5's
// premise instead of citing it"): `runc checkpoint --lazy-pages` dumps the
// metadata and keeps the memory pages in the dump process, which serves them over
// CRIU's own page-server protocol to a `criu lazy-pages` daemon at the
// destination; the destination restores immediately from the images and pulls
// every page on fault (plus CRIU's background transfer). It uses none of this
// project's mechanisms: no baseline reconciliation (M1), no pre-staged rounds
// (M2), no compression, no residual map, no pause-first protocol -- it is what a
// user gets from CRIU alone, run over the same link.
//
// Transport. CRIU's lazy-pages daemon is the page-server CLIENT (it dials the
// source), but this testbed's source has no inbound path (NAT). The source
// therefore opens one long-lived outbound connection (the "lazy_carrier" raw
// connection of the existing transport) and splices it to CRIU's local page
// server, while the destination splices the same carrier to a loopback listener
// its `criu lazy-pages` dials. The carrier is a byte pipe: CRIU's protocol runs
// over it unmodified, so the bytes counted are exactly CRIU's own.
//
// Filesystem state. Stock CRIU carries none; like every other variant it moves
// the upper layer in rsync rounds (fsprecopy.go: live rounds, final round after
// the freeze; zstd 1 like every variant) so a workload that keeps files restores correctly. Downtime is source freeze (dump start) -> destination
// serving; eviction time is the moment the dump process exits, i.e. when the last
// page has been served and the source holds nothing.
type CriuLazyStrategy struct {
	// RsyncPort is the receiver's rsync daemon, target of the filesystem
	// pre-copy rounds (fsprecopy.go); "" = DefaultRsyncPort.
	RsyncPort       string
	TmpfsCheckpoint bool
	// DrainTimeout bounds the wait for the source dump process to exit after the
	// destination is serving (0 = 10 minutes).
	DrainTimeout time.Duration
}

func (s *CriuLazyStrategy) Execute(ctx context.Context, cfg *container.Config, destClient transport.Client, destAddr string) error {
	fmt.Printf("Executing stock CRIU lazy-pages strategy for container: %s\n", cfg.ContainerID)
	if os.Geteuid() != 0 {
		return fmt.Errorf("criu-lazy needs to run as root (the status pipe cannot cross sudo); run `sudo statepod migrate`")
	}
	metrics := &MigrationMetrics{
		Mode:               "criu-lazy",
		ContainerID:        cfg.ContainerID,
		MigrationStartTime: time.Now(),
	}
	if err := ensureCheckpointDirs(s.TmpfsCheckpoint); err != nil {
		return err
	}
	const imgDir = "./checkpoint/final"
	drainTimeout := s.DrainTimeout
	if drainTimeout == 0 {
		drainTimeout = 10 * time.Minute
	}

	// The carrier first: the destination parks it until its lazy-pages daemon dials.
	carrier, err := destClient.OpenConn(ctx, destAddr, "lazy_carrier")
	if err != nil {
		return fmt.Errorf("failed to open the lazy-pages carrier connection: %w", err)
	}
	defer carrier.Close()

	// Filesystem state moves EXACTLY like in every other variant (fsprecopy.go,
	// live rsync rounds, final round after the freeze, zstd level 1): it is
	// not what this baseline is compared on. Only CRIU's own page transfer stays
	// stock (uncompressed). The dry run of 2026-10-27 had it uncompressed here: 187 MB
	// of PostgreSQL files instead of ~12 MB, dominating the cell.
	fsr := newFSPrecopy(ctx, destClient, destAddr, s.RsyncPort, 1, metrics)
	if cfg.Stateful {
		metrics.Mark("src_fs_precopy_start")
		if err := fsr.precopyLive(); err != nil {
			return fmt.Errorf("filesystem pre-copy failed, source untouched: %w", err)
		}
		metrics.Mark("src_fs_precopy_end")
	}

	// --- Freeze + dump. The dump process stays alive serving pages. ---
	statusR, statusW, err := os.Pipe()
	if err != nil {
		return err
	}
	defer statusR.Close()
	dumpArgs := []string{"checkpoint", cfg.ContainerID,
		"--image-path", imgDir, "--leave-running=false", "--tcp-established",
		"--lazy-pages", "--page-server", fmt.Sprintf("127.0.0.1:%d", CriuLazyPort), "--status-fd", "3"}
	dump := exec.Command("runc", dumpArgs...) // not CommandContext: it must outlive a cancelled step to serve pages
	dump.ExtraFiles = []*os.File{statusW}
	var dumpLog syncBuilder
	dump.Stdout, dump.Stderr = &dumpLog, &dumpLog
	metrics.FreezeStartTime = metrics.Mark("src_final_dump_start")
	if err := dump.Start(); err != nil {
		statusW.Close()
		return fmt.Errorf("failed to start lazy dump: %w", err)
	}
	statusW.Close() // our copy: criu holds the other end; EOF here means criu/runc exited
	dumpDone := make(chan error, 1)
	go func() { dumpDone <- dump.Wait() }()

	// Ready = criu wrote a byte to the status fd (images complete, page server listening).
	readyCh := make(chan error, 1)
	go func() {
		b := make([]byte, 1)
		if _, err := statusR.Read(b); err != nil {
			readyCh <- fmt.Errorf("lazy dump ended before the page server was ready: %w", err)
			return
		}
		readyCh <- nil
	}()
	select {
	case err := <-readyCh:
		if err != nil {
			return fmt.Errorf("%w; runc output: %s", err, tail(dumpLog.String(), 600))
		}
	case <-time.After(3 * time.Minute):
		_ = dump.Process.Kill()
		return fmt.Errorf("timed out waiting for the lazy-pages page server; runc output: %s", tail(dumpLog.String(), 600))
	case <-ctx.Done():
		_ = dump.Process.Kill()
		return ctx.Err()
	}
	metrics.Mark("src_final_dump_end")

	// --- Splice CRIU's page server to the carrier (counts the bytes CRIU sends). ---
	local, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", CriuLazyPort), 10*time.Second)
	if err != nil {
		_ = dump.Process.Kill()
		return fmt.Errorf("failed to connect to the local page server: %w", err)
	}
	var pageBytes, reqBytes atomic.Int64
	spliceDone := splice(local, carrier, &pageBytes, &reqBytes)

	// --- Images (no pages), filesystem state, configs. ---
	metrics.Mark("src_ship_final_start")
	if _, wire, err := tarStreamDir(ctx, destClient, destAddr, imgDir, "stock_images", 0); err != nil {
		return fmt.Errorf("failed to ship the lazy dump images: %w", err)
	} else {
		metrics.TotalDataTransferredBytes += wire
	}
	if cfg.Stateful {
		metrics.Mark("src_fs_sync_start")
		if err := fsr.final(); err != nil {
			return fmt.Errorf("failed to ship the upper layer: %w", err)
		}
		metrics.Mark("src_fs_sync_end")
	}
	for _, f := range []struct{ path, streamType string }{{"statepod.json", "statepod_cfg"}, {"config.json", "oci_config"}} {
		if err := streamOneFile(ctx, destClient, destAddr, f.path, f.streamType); err != nil {
			return err
		}
		if fi, statErr := os.Stat(f.path); statErr == nil {
			metrics.TotalDataTransferredBytes += fi.Size()
		}
	}

	// --- Restore trigger: the ACK means the destination is serving (lazily). ---
	if err := destClient.Connect(ctx, destAddr); err != nil {
		return fmt.Errorf("failed to reconnect for the restore trigger: %w", err)
	}
	if err := destClient.SendStream(ctx, "stock_restore", strings.NewReader("go")); err != nil {
		return fmt.Errorf("destination failed to restore: %w", err)
	}
	restoreAck := metrics.Mark("src_restore_go_acked")
	metrics.RestoreGoTime = restoreAck
	metrics.DowntimeSourceSide = float64(restoreAck.Sub(metrics.FreezeStartTime).Milliseconds())
	fmt.Printf("Destination serving; source-side downtime %.0f ms. Waiting for the lazy-pages drain...\n", metrics.DowntimeSourceSide)

	// --- Drain: the dump process exits once every page has been served. ---
	metrics.Mark("src_drain_start")
	select {
	case err := <-dumpDone:
		if err != nil {
			return fmt.Errorf("lazy dump process failed during the drain: %w; output: %s", err, tail(dumpLog.String(), 600))
		}
	case <-time.After(drainTimeout):
		_ = dump.Process.Kill()
		return fmt.Errorf("lazy-pages drain did not finish within %s", drainTimeout)
	case <-ctx.Done():
		_ = dump.Process.Kill()
		return ctx.Err()
	}
	drainEnd := metrics.Mark("src_drain_end")
	_ = carrier.Close()
	<-spliceDone
	metrics.RestoreEndTime = metrics.Mark("src_kill") // nothing of the container is left on the source
	metrics.DrainCompleteTime = drainEnd
	metrics.TotalDataTransferredBytes += pageBytes.Load() + reqBytes.Load()
	metrics.Residual = &ResidualMetrics{PulledBytes: pageBytes.Load(), DrainMs: float64(drainEnd.Sub(restoreAck).Milliseconds())}
	metrics.TotalMigrationTime = float64(metrics.RestoreEndTime.Sub(metrics.MigrationStartTime).Milliseconds())
	fmt.Printf("Drain complete: %d B of pages served (%d B of requests) in %.0f ms\n",
		pageBytes.Load(), reqBytes.Load(), metrics.Residual.DrainMs)

	data, _ := json.Marshal(metrics)
	if err := destClient.Connect(ctx, destAddr); err != nil {
		return fmt.Errorf("failed to reconnect for metrics: %w", err)
	}
	return destClient.SendStream(ctx, "metrics", strings.NewReader(string(data)))
}

// splice copies both ways between a and b until either side ends, then closes
// both (CRIU's page-server protocol has no half-close semantics to preserve;
// the destination's daemon closing is the end of the drain). toA counts the
// bytes read from b and written to a; fromA the bytes read from a and written
// to b -- with a = the local page server and b = the carrier, `toA` is the
// requests and `fromA` the pages. The returned channel closes when both
// directions have finished.
func splice(a, b net.Conn, fromA, toA *atomic.Int64) <-chan struct{} {
	done := make(chan struct{})
	var once sync.Once
	closeBoth := func() { once.Do(func() { a.Close(); b.Close() }) }
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		n, _ := io.Copy(b, a)
		fromA.Add(n)
		closeBoth()
	}()
	go func() {
		defer wg.Done()
		n, _ := io.Copy(a, b)
		toA.Add(n)
		closeBoth()
	}()
	go func() { wg.Wait(); close(done) }()
	return done
}

// syncBuilder is a strings.Builder safe for the concurrent writes os/exec makes
// while the command runs and the reads we make for error messages.
type syncBuilder struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuilder) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuilder) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func tail(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return "..." + s[len(s)-n:]
	}
	return s
}
