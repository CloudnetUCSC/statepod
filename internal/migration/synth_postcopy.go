package migration

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/compress"
	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/pipeline"
	"github.com/CloudnetUCSC/statepod/internal/transport"
)

// SynthPostCopyStrategy is the M5 headline mode:
// M1+M4+M5. Freeze source -> ship a tiny residual map -> destination restores
// immediately via CRIU lazy-pages against its local baseline -> the residual
// set (misses + address-gated fallbacks) drains via hybrid push+pull
// while the source stays paused-but-intact, only killed once the destination
// signals drain-complete. Failure before that ACK -> source resumes, no
// restart fallback needed (unlike synth-precopy's path) since the
// source was never destroyed, only paused.
type SynthPostCopyStrategy struct {
	// RsyncPort is the receiver's rsync daemon, target of the filesystem
	// pre-copy rounds (fsprecopy.go); "" = DefaultRsyncPort.
	RsyncPort    string
	DrainTimeout time.Duration
	// FinalMode: "" / "catchup" = pause-first: the source
	// is frozen BEFORE the dump, so no write is acknowledged after the state
	// that gets migrated (static configs and the upper layer are pre-staged
	// while the source still serves, so only dump + residual map + restore sit
	// in the downtime). "availability" = the original order (dump with
	// --leave-running, ship, pause late), whose dump-to-pause window loses
	// acknowledged writes; kept only as a labelled ablation.
	FinalMode string
}

// pageRecordHeader is the 16 B (vaddr, nrPages) wire header used by both the
// drain channel's pull requests and each residual_push record (nrPages is
// always 1 for a push record; kept symmetric with pull for one decoder).
func encodePageRecordHeader(vaddr, nrPages uint64) []byte {
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], vaddr)
	binary.BigEndian.PutUint64(b[8:16], nrPages)
	return b[:]
}

func decodePageRecordHeader(b []byte) (vaddr, nrPages uint64) {
	return binary.BigEndian.Uint64(b[0:8]), binary.BigEndian.Uint64(b[8:16])
}

// Drain channel message types. The
// channel is one connection the SOURCE dials out to the destination and
// keeps open for the whole drain window — reversed from a naive "destination
// dials source" shape because the source (a NAT'd host on this testbed) has
// no inbound path from the WAN; only outbound works, same direction every
// other stream in this codebase already uses. Once open, the destination
// drives it (writes pull requests, reads responses; writes the drained
// signal when its pending set empties) even though the source dialed it —
// TCP doesn't care who dialed once the connection exists. Must match the
// receiver's own copy of these constants (internal/receiver/postcopy.go).
const (
	drainMsgPull    byte = 1
	drainMsgDrained byte = 3
	drainMsgFailed  byte = 4
)

func (s *SynthPostCopyStrategy) Execute(ctx context.Context, cfg *container.Config, destClient transport.Client, destAddr string) error {
	drainTimeout := s.DrainTimeout
	if drainTimeout <= 0 {
		drainTimeout = 60 * time.Second
	}

	fmt.Printf("Executing Synth-PostCopy Strategy (M1+M4+M5) for container: %s\n", cfg.ContainerID)
	metrics := &MigrationMetrics{
		Mode:               "synth-postcopy",
		ContainerID:        cfg.ContainerID,
		MigrationStartTime: time.Now(),
		Residual:           &ResidualMetrics{},
	}

	if cfg.ImageID == "" {
		return fmt.Errorf("statepod.json has no image_id — synth-postcopy is keyed by image digest; recreate the bundle")
	}
	pauseFirst := s.FinalMode != FinalModeAvailability
	if pauseFirst {
		metrics.FinalMode = FinalModeCatchup
	} else {
		metrics.FinalMode = FinalModeAvailability
	}

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
	fmt.Printf("Manifest: %d baseline page hashes, baseline %s\n", len(manifest.Hashes), manifest.BaselineDumpID)

	metrics.Mark("src_manifest_fetched")
	if err := EnsureCheckpointRoot(checkpointRoot, false); err != nil {
		return err
	}
	for _, dir := range []string{"./checkpoint/final", "./checkpoint/residual"} {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("failed to clear %s: %w", dir, err)
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create %s: %w", dir, err)
		}
	}

	// --- Dump (no disruption yet: --leave-running keeps the container serving
	// through the dump itself, same reasoning as synth-precopy's final dump). ---
	thawed := false
	thaw := func(reason string) {
		if thawed {
			return
		}
		thawed = true
		fmt.Printf("Thawing source container (%s)\n", reason)
		if err := exec.CommandContext(ctx, "sudo", "runc", "resume", cfg.ContainerID).Run(); err != nil {
			fmt.Printf("Warning: failed to resume source container: %v\n", err)
		}
	}

	// Static configs and (bulk of) the M4 upper layer: shipped before the
	// pause in pause-first mode so they are off the downtime critical path.
	configsShipped := false
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
		configsShipped = true
		return nil
	}
	shipUpper := func(live bool) error {
		if !cfg.Stateful {
			return nil
		}
		upperMs, upperBytes, err := tarStreamDirLive(ctx, destClient, destAddr, container.UpperDir, "fs_upper", 0, live)
		if err != nil {
			return fmt.Errorf("failed to stream upperdir: %w", err)
		}
		metrics.FSUpperBytes += upperBytes
		metrics.FSUpperMs += upperMs
		metrics.TotalDataTransferredBytes += upperBytes
		return nil
	}

	// Pause-first moves the upper layer in rsync rounds (fsprecopy.go): live
	// rounds before the pause, one final round after it.
	fsr := newFSPrecopy(ctx, destClient, destAddr, s.RsyncPort, residualZstdLevel, metrics)

	if pauseFirst {
		metrics.Mark("src_prestage_start")
		for attempt := 1; ; attempt++ {
			err := shipConfigs()
			if err == nil && cfg.Stateful {
				err = fsr.precopyLive() // source still live: the final round after the pause catches up
			}
			if err == nil {
				break
			}
			if attempt >= 3 {
				return fmt.Errorf("pre-stage failed after %d attempts, source untouched: %w", attempt, err)
			}
			fmt.Printf("Retrying pre-stage (attempt %d/3) after: %v\n", attempt+1, err)
		}
		metrics.Mark("src_prestage_end")

		fmt.Println("Pausing source BEFORE the dump (pause-first)")
		metrics.FreezeStartTime = metrics.Mark("src_pause")
		if err := exec.CommandContext(ctx, "sudo", "runc", "pause", cfg.ContainerID).Run(); err != nil {
			return fmt.Errorf("failed to pause source container: %w", err)
		}
	}

	fmt.Println("Starting M5 dump")
	metrics.Mark("src_final_dump_start")
	dumpCmd := sudoRunc(ctx, "runc", "checkpoint", cfg.ContainerID,
		"--image-path", "./checkpoint/final", "--leave-running=true", "--tcp-established")
	var dumpErr bytes.Buffer
	dumpCmd.Stderr = &dumpErr
	if err := dumpCmd.Run(); err != nil {
		if pauseFirst {
			thaw("M5 dump failed")
		}
		return fmt.Errorf("M5 dump failed: %s: %w", dumpErr.String(), err)
	}
	metrics.Mark("src_final_dump_end")
	if pauseFirst {
		// CRIU's --leave-running thaws what it froze; make sure it did not also
		// thaw the runc-paused container (else re-pause and record the gap).
		if st, err := container.GetRuncState(ctx, cfg.ContainerID); err == nil && st.Status != "paused" {
			fmt.Printf("WARNING: container is %q after the dump, not paused; re-pausing\n", st.Status)
			metrics.Mark("src_repause")
			_ = exec.CommandContext(ctx, "sudo", "runc", "pause", cfg.ContainerID).Run()
			metrics.RepausedAfterDump = true
		}
	}

	// --- Encode the residual map and index the dump for drain-serving. Both
	// operate purely on the static dump directory --leave-running left on
	// disk — the dump is already an atomic, consistent snapshot the instant
	// `runc checkpoint` returned, so none of this needs the container
	// stopped. Pausing here (an earlier version of this code did) charged
	// the whole encode+index+ship phase to client-visible downtime for no
	// correctness reason — confirmed live: it inflated downtime to 6-8s,
	// dwarfing the drain it exists to keep off the critical path. Deferred
	// below, same shape as the synth-precopy kill: freeze only once
	// everything retryable has already succeeded. ---
	if err := exec.CommandContext(ctx, "sudo", "chmod", "-R", "a+rX", "./checkpoint/final").Run(); err != nil {
		if pauseFirst {
			thaw("dump not readable")
		}
		return fmt.Errorf("failed to make dump readable: %w", err)
	}

	metrics.Mark("src_encode_start")
	residualStats, err := pipeline.EncodeResidualMap("./checkpoint/final", "./checkpoint/residual", manifest)
	if err != nil {
		if pauseFirst {
			thaw("residual map encoding failed")
		}
		return fmt.Errorf("residual map encoding failed: %w", err)
	}
	fmt.Printf("Residual map: %d pages, %d hits, %d misses, %d eager (%.1f%% hit rate)\n",
		residualStats.TotalPages, residualStats.Hits, residualStats.Misses, residualStats.Eager, residualStats.HitRate()*100)
	metrics.Residual.TotalPages = residualStats.TotalPages
	metrics.Residual.Eager = residualStats.Eager

	pageIndex, err := pipeline.IndexDumpPages("./checkpoint/final")
	if err != nil {
		if pauseFirst {
			thaw("dump indexing failed")
		}
		return fmt.Errorf("failed to index dump pages for drain serving: %w", err)
	}
	metrics.Mark("src_encode_end")

	// --- Ship metadata + M4 upperdir + residual map. Everything here is safe
	// to retry: the source is still fully alive and serving, nothing below
	// disturbs it. The residual Request's response is the destination's
	// exact drain set (the address gate can turn some manifest
	// hits into lazy pages too, so the source can't precompute this list
	// itself). ---
	var stalled atomic.Bool
	shipAndGetPending := func() ([]uint64, error) {
		metrics.Mark("src_ship_start")
		if !configsShipped {
			if err := shipConfigs(); err != nil {
				return nil, err
			}
		}
		if cfg.Stateful {
			if pauseFirst {
				// Source paused: the final rsync round sends only what changed.
				if err := fsr.final(); err != nil {
					return nil, err
				}
			} else if err := shipUpper(false); err != nil { // availability order (ablation only)
				return nil, err
			}
		}

		residualTarRaw, tarErrCh, err := tarPipeReader(ctx, "./checkpoint/residual")
		if err != nil {
			return nil, err
		}
		residualTar := &countingReader{r: compress.Compress(residualTarRaw, residualZstdLevel)}
		// Paused: no progress for pauseStallLimit gives up, nothing depends on this request yet.
		reqCtx, touch, stopGuard := withStallGuard(ctx, pauseStallLimit, func() { stalled.Store(true) })
		defer stopGuard()
		residualTar.onRead = touch
		respBytes, err := destClient.Request(reqCtx, destAddr, "residual", residualTar)
		if err != nil {
			// The request can fail before reading the body (destination unreachable):
			// nothing reads the tar stream then, so stop it or the wait below never returns
			// and the source stays paused (found by the fault matrix, 2026-10-29).
			residualTarRaw.CloseWithError(err)
		}
		tarErr := <-tarErrCh
		if err != nil {
			return nil, fmt.Errorf("residual request failed: %w", err)
		}
		if tarErr != nil {
			return nil, fmt.Errorf("failed to tar residual dir: %w", tarErr)
		}
		metrics.TotalDataTransferredBytes += residualTar.n
		metrics.Mark("src_ship_ack")
		var pending []uint64
		if err := json.Unmarshal(respBytes, &pending); err != nil {
			return nil, fmt.Errorf("destination rejected residual map (empty/invalid response, likely BuildLazyRestoreDir failure): %w", err)
		}
		return pending, nil
	}

	const maxShipAttempts = 3
	var pending []uint64
	var shipErr error
	for attempt := 1; attempt <= maxShipAttempts; attempt++ {
		if attempt > 1 {
			fmt.Printf("Retrying residual ship (attempt %d/%d) after: %v\n", attempt, maxShipAttempts, shipErr)
		}
		if pending, shipErr = shipAndGetPending(); shipErr == nil || stalled.Load() {
			break
		}
	}
	if shipErr != nil {
		// Availability mode: source was never paused — nothing to thaw. Pause-first
		// mode: it is paused, so thaw it before giving up.
		if pauseFirst {
			thaw("residual shipping failed")
		}
		return fmt.Errorf("residual shipping failed after %d attempts: %w", maxShipAttempts, shipErr)
	}
	fmt.Printf("Destination pending (lazy) set: %d pages\n", len(pending))

	// --- Freeze: pause now that everything retryable has already succeeded
	// (mirrors the synth-precopy kill point exactly) — this is the
	// actual "frozen-but-intact" instant and where client-visible downtime
	// starts. Thaw on failure from here on = `runc resume`. ---
	if !pauseFirst {
		fmt.Println("Pausing source container (frozen-but-intact)")
		metrics.FreezeStartTime = metrics.Mark("src_pause")
		if err := exec.CommandContext(ctx, "sudo", "runc", "pause", cfg.ContainerID).Run(); err != nil {
			return fmt.Errorf("failed to pause source container: %w", err)
		}
	}

	// --- Drain channel: one outbound connection this process keeps open,
	// answering the destination's pull requests until it signals drained.
	// See the drainMsg* doc comment above for why this is source-dialed.
	//
	// It is opened BEFORE the restore trigger, not after its ACK: the restore
	// itself can fault on a lazy page (e.g. the kernel writing the rseq area of a
	// static binary during `criu restore`), and that fault is served by pulling
	// the page from this source over this channel. Opening it only after the
	// ACK made restore wait for a page, the page wait for the channel and the
	// channel wait for the ACK -- a deadlock found on the 32 MB synthetic
	// workload (Redis escaped it only because its equivalent page is local). ---
	dialCtx, cancelDial := context.WithTimeout(ctx, pauseStallLimit)
	defer cancelDial()
	drainConn, err := destClient.OpenConn(dialCtx, destAddr, "residual_drain")
	if err != nil {
		thaw("failed to open drain channel")
		return fmt.Errorf("failed to open drain channel: %w", err)
	}
	defer drainConn.Close()
	drainErrCh := make(chan error, 1)
	go func() { drainErrCh <- serveDrainChannel(drainConn, pageIndex) }()

	// --- Commit point: once this ACKs, the destination is running `criu
	// lazy-pages` + `runc restore --lazy-pages` and is already serving. ---
	if err := destClient.Connect(dialCtx, destAddr); err != nil {
		thaw("failed to reconnect for residual_restore_go")
		return fmt.Errorf("failed to reconnect for residual_restore_go: %w", err)
	}
	if err := destClient.SendStream(ctx, "residual_restore_go", bytes.NewReader([]byte{})); err != nil {
		thaw("destination failed to restore from residual map")
		return fmt.Errorf("destination failed to restore from residual map: %w", err)
	}
	metrics.RestoreGoTime = metrics.Mark("src_restore_go_acked")
	fmt.Printf("Destination restored and serving (RestoreGoTime set); draining %d residual pages\n", len(pending))

	// --- Background pusher: streams the residual set opportunistically, on
	// its own transport.Client (independent connection, same reasoning as
	// the drain channel above — no shared mutable state with destClient). A
	// pull-fault on the destination for a page the pusher already delivered
	// is a harmless no-op there (same store, idempotent write). ---
	var pushedBytes atomic.Int64
	pushDone := make(chan error, 1)
	go func() {
		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			for _, vaddr := range pending {
				// Yield to on-fault pulls (see pullsActive).
				for pullPriority && pullsActive.Load() > 0 {
					time.Sleep(time.Millisecond)
				}
				page, err := pageIndex.Read(vaddr, 1)
				if err != nil {
					pw.CloseWithError(err)
					return
				}
				if _, err := pw.Write(encodePageRecordHeader(vaddr, 1)); err != nil {
					return
				}
				if _, err := pw.Write(page); err != nil {
					return
				}
				pushedBytes.Add(int64(len(page)))
			}
		}()
		pushClient := transport.NewTCPClient()
		defer pushClient.Close()
		// Small send buffer: bounds how much bulk push data can sit ahead of
		// a pull response on the shared uplink.
		if sb, ok := pushClient.(interface{ SetWriteBuffer(int) }); ok && pullPriority {
			sb.SetWriteBuffer(32 << 10)
		}
		if err := pushClient.Connect(ctx, destAddr); err != nil {
			pushDone <- fmt.Errorf("failed to connect for residual_push: %w", err)
			return
		}
		pushDone <- pushClient.SendStream(ctx, "residual_push", pr)
	}()
	go func() {
		if err := <-pushDone; err != nil {
			fmt.Printf("Warning: background push failed (drain may still complete via pull): %v\n", err)
		}
	}()

	drainStart := metrics.Mark("src_drain_start")
	select {
	case err := <-drainErrCh:
		if err != nil {
			thaw("drain channel failed")
			return fmt.Errorf("drain channel failed: %w", err)
		}
		// nil = the destination sent drainMsgDrained; drain is complete.
	case <-time.After(drainTimeout):
		_ = destClient.Connect(ctx, destAddr)
		_ = destClient.SendStream(ctx, "residual_abort", bytes.NewReader([]byte{}))
		thaw("drain timed out")
		return fmt.Errorf("residual drain did not complete within %s — destination told to discard, source resumed", drainTimeout)
	}
	// pushedBytes may still be short of the pusher's eventual total if pull
	// alone satisfied the drain first — an approximation, not the wire size
	// of what pull served (that's dest-side, folded in as PulledBytes).
	metrics.Residual.PushedBytes = pushedBytes.Load()
	metrics.TotalDataTransferredBytes += pushedBytes.Load()
	metrics.Residual.DrainMs = float64(time.Since(drainStart).Milliseconds())
	metrics.Mark("src_drain_end")
	fmt.Printf("Drain complete: %d B pushed in %.0f ms\n", pushedBytes.Load(), metrics.Residual.DrainMs)

	// --- Point of no return: destination has confirmed drain-complete, so
	// the source is no longer needed. ---
	if err := exec.CommandContext(ctx, "sudo", "runc", "kill", cfg.ContainerID, "SIGKILL").Run(); err != nil {
		fmt.Printf("Warning: failed to kill source container: %v\n", err)
	}
	if err := exec.CommandContext(ctx, "sudo", "runc", "delete", cfg.ContainerID).Run(); err != nil {
		fmt.Printf("Warning: failed to delete source container state: %v\n", err)
	}
	thawed = true // no longer meaningful past this point (container is gone)

	metrics.RestoreEndTime = metrics.Mark("src_kill")
	// Downtime is the client-visible outage: pause (source stops serving) to
	// RestoreGoTime (destination starts serving) — NOT RestoreGoTime to here.
	// Everything after RestoreGoTime is the drain, which M5 exists precisely
	// to keep off the downtime critical path (the design's "near-constant
	// downtime" claim); RestoreEndTime-RestoreGoTime is drain+kill duration,
	// a real number (see Residual.DrainMs) but a different one — folding it
	// into "downtime" would make M5 look like its downtime scales with
	// drain/state size, exactly the opposite of what it measures. Confirmed
	// live: drain_ms ranged 1.4s-6.5s across identical reps (fault-timing
	// variance, not state-size), while this number stayed small throughout.
	metrics.DowntimeSourceSide = float64(metrics.RestoreGoTime.Sub(metrics.FreezeStartTime).Milliseconds())
	metrics.TotalMigrationTime = float64(metrics.RestoreEndTime.Sub(metrics.MigrationStartTime).Milliseconds())
	fmt.Printf("Source-side Downtime: %.2f ms (drain took a further %.0f ms)\n", metrics.DowntimeSourceSide, metrics.Residual.DrainMs)

	metricsData, _ := json.Marshal(metrics)
	if err := destClient.Connect(ctx, destAddr); err != nil {
		return fmt.Errorf("failed to reconnect for metrics: %w", err)
	}
	if err := destClient.SendStream(ctx, "metrics", bytes.NewReader(metricsData)); err != nil {
		return fmt.Errorf("failed to stream metrics: %w", err)
	}
	return nil
}

// serveDrainChannel answers the destination's pull requests on conn (opened
// by this source via Client.OpenConn) until it reads drainMsgDrained (nil
// return: drain complete) or hits an error.
// residualZstdLevel compresses the residual-map request: its 191 "eager"
// (non-lazy-eligible) pages are shipped raw and make up most of its ~900 KB,
// so on a slow uplink the uncompressed request dominated M5's stop-to-serve.
const residualZstdLevel = 1

// pullsActive counts on-fault pull requests currently being served on the drain
// channel. With pullPriority enabled the background pusher yields while it is
// non-zero (see pullPriority for why this is off by default).
var pullsActive atomic.Int32

// pullPriority enables the pusher's yielding to on-fault pulls and its small
// send buffer. OFF by default: measured 2026-10-04 on the same setup, it made the
// background drain 5x slower (1.5 s -> 7-10 s for ~1.2 MB: the 32 KiB send buffer
// caps the push at window/RTT) with no demonstrated gain in client latency.
// Opt in with STATEPOD_M5_PRIORITY=1 to study it.
var pullPriority = os.Getenv("STATEPOD_M5_PRIORITY") != ""

// pauseStallLimit bounds how long a paused source waits with no progress before it
// resumes and gives the migration up. It counts silence, not total time: a large
// eager set on a slow link keeps making progress for minutes. Worst healthy request silence
// measured over the final campaign: 6.8 s (the destination building the restore directory).
const pauseStallLimit = 30 * time.Second

// withStallGuard returns a context cancelled after limit without a call to touch (onStall
// runs first), touch, and a func that stops the guard.
func withStallGuard(ctx context.Context, limit time.Duration, onStall func()) (context.Context, func(), func()) {
	gctx, cancel := context.WithCancel(ctx)
	var last atomic.Int64
	last.Store(time.Now().UnixNano())
	go func() {
		t := time.NewTicker(limit / 30)
		defer t.Stop()
		for {
			select {
			case <-gctx.Done():
				return
			case now := <-t.C:
				if now.UnixNano()-last.Load() > int64(limit) {
					onStall()
					cancel()
					return
				}
			}
		}
	}()
	return gctx, func() { last.Store(time.Now().UnixNano()) }, cancel
}

func serveDrainChannel(conn net.Conn, pageIndex *pipeline.PageIndex) error {
	var hdr [1 + 16]byte
	for {
		if _, err := io.ReadFull(conn, hdr[:1]); err != nil {
			return fmt.Errorf("reading drain channel message type: %w", err)
		}
		switch hdr[0] {
		case drainMsgPull:
			if _, err := io.ReadFull(conn, hdr[1:]); err != nil {
				return fmt.Errorf("reading pull request: %w", err)
			}
			vaddr, nrPages := decodePageRecordHeader(hdr[1:])
			pullsActive.Add(1)
			page, err := pageIndex.Read(vaddr, nrPages)
			if err != nil {
				pullsActive.Add(-1)
				return fmt.Errorf("pull(0x%x, %d): %w", vaddr, nrPages, err)
			}
			_, werr := conn.Write(page)
			pullsActive.Add(-1)
			if werr != nil {
				return fmt.Errorf("writing pull response: %w", werr)
			}
		case drainMsgDrained:
			return nil
		case drainMsgFailed:
			return fmt.Errorf("destination could not finish the restore and discarded it")
		default:
			return fmt.Errorf("unknown drain channel message type %d", hdr[0])
		}
	}
}

// tarPipeReader tars dirPath and returns a reader streaming the tar output,
// plus a channel that receives tar's final error (nil on success) once the
// reader has been fully drained. Mirrors tarStreamDir's tar invocation but
// hands back a plain io.Reader for use as a Request body (Request has no
// separate "stream then wait for command" shape the way SendStream does).
func tarPipeReader(ctx context.Context, dirPath string) (*io.PipeReader, <-chan error, error) {
	cmd := exec.CommandContext(ctx, "sudo", "tar", "-cf", "-", "-C", dirPath, ".")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}
	errCh := make(chan error, 1)
	pr, pw := io.Pipe()
	go func() {
		_, copyErr := io.Copy(pw, stdout)
		if copyErr != nil {
			stdout.Close() // reader gone: tar gets SIGPIPE instead of blocking cmd.Wait forever
		}
		waitErr := cmd.Wait()
		if copyErr != nil {
			pw.CloseWithError(copyErr)
			errCh <- copyErr
			return
		}
		pw.Close()
		if waitErr != nil {
			errCh <- fmt.Errorf("tar failed: %s: %w", stderr.String(), waitErr)
			return
		}
		errCh <- nil
	}()
	return pr, errCh, nil
}
