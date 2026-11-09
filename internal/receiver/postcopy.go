package receiver

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
	"path/filepath"
	"sync"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/compress"
	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/migration"
	"github.com/CloudnetUCSC/statepod/internal/pageserver"
	"github.com/CloudnetUCSC/statepod/internal/pipeline"
	"github.com/CloudnetUCSC/statepod/internal/transport"
)

// Drain channel message types — must match the source's own copy of these
// constants (internal/migration/synth_postcopy.go's doc comment explains why
// the source dials this connection, not the destination).
const (
	drainMsgPull    byte = 1
	drainMsgDrained byte = 3
	drainMsgFailed  byte = 4 // the restored process did not take the pages: the source must resume
)

// postcopyState is the destination-side M5 drain state (
// Task 3): the pending (lazy) vaddr set and the page store
// both the on-fault pull path (pageserver.Fetch) and the background-push
// receiver write into, plus the source-dialed drain channel connection pull
// requests and the drained signal ride. One instance per migration.
type postcopyState struct {
	mu             sync.Mutex
	outDir         string
	pending        map[uint64]bool
	pidOrdinal     map[int]int // pagemap key (pid / -shmid) -> ordinal, for composite page addresses
	store          map[uint64][]byte
	lazyStats      *pipeline.LazyRestoreStats
	pulledBytes    int64
	pullFaults     int
	drainedAt      time.Time
	drainOnce      sync.Once
	drainConn      net.Conn
	drainConnReady chan struct{}
	drainDone      chan struct{}
	// drainWriteMu serializes writes+their matching reads on drainConn: it's
	// a single connection shared by every pull-fault and the eventual
	// drained signal, and concurrent Fetch calls (or a fetch racing the
	// drained signal) would otherwise interleave requests/responses on it.
	drainWriteMu sync.Mutex
	// lazyDone is closed when the `criu lazy-pages` daemon exits. The daemon
	// exits only once every registered page has been injected into the
	// restored process; "pending is empty" above only means the pages are in
	// OUR store, not in the process. Tearing down before lazyDone leaves the
	// unfetched ranges un-populated, which read as zero pages once the uffd
	// closes (JVM: SIGSEGV at pc=0 right after the drain, 2026-10-08).
	lazyDone chan struct{}
	// restoreEnded is closed when `runc restore` returned; set with lazyDone.
	restoreEnded chan struct{}
	// lastFetch: the daemon's last page request; lazyDaemonStall counts from it.
	lastFetch time.Time
	// drainErr is set when the drain ended in drainMsgFailed; read after drainDone.
	drainErr error
	// abort discards the restored container (handleAbort), set by handleRestoreGo.
	abort func()
	// lazyExitMs: pages local -> daemon exit (ResidualMetrics.LazyExitMs).
	lazyExitMs  float64
	containerID string
}

// lazyDaemonStall: the daemon is stuck if it neither exited nor asked for a page for this
// long once the residual set is local. The source is still frozen and intact then, so it
// can resume. Every page is local at that point: a live daemon has no reason to wait.
const lazyDaemonStall = 30 * time.Second

func encodePageRecordHeader(vaddr, nrPages uint64) []byte {
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], vaddr)
	binary.BigEndian.PutUint64(b[8:16], nrPages)
	return b[:]
}

func decodePageRecordHeader(b []byte) (vaddr, nrPages uint64) {
	return binary.BigEndian.Uint64(b[0:8]), binary.BigEndian.Uint64(b[8:16])
}

// loadTemplateBaseline loads the local baseline dump for cfg's image
// — shared by synth-precopy's cached per-round loader
// (receiver.go) and M5's one-shot residual handler below.
func loadTemplateBaseline(cfg *container.Config) (*pipeline.Baseline, error) {
	// Parsing and hashing the template dump takes seconds. It is cached per key
	// and prefetched in the background when the manifest is requested (the
	// first contact of a migration), so it is off the stop-to-serve path: M5's
	// residual request used to pay ~6 s for it while the source was paused.
	baselineCache.Lock()
	defer baselineCache.Unlock()
	key := cfg.TemplateKey()
	if baselineCache.b != nil && baselineCache.key == key {
		return baselineCache.b, nil
	}
	tmplDir, err := container.TemplateDir(key)
	if err != nil {
		return nil, err
	}
	fmt.Println("Loading local baseline from", tmplDir)
	b, err := pipeline.LoadBaseline(tmplDir, key)
	if err != nil {
		return nil, fmt.Errorf("loading baseline (run `statepod template generate` here first): %w", err)
	}
	baselineCache.key, baselineCache.b = key, b
	return b, nil
}

var baselineCache struct {
	sync.Mutex
	key string
	b   *pipeline.Baseline
}

// prefetchBaseline warms the baseline cache; errors are ignored here (the
// real load reports them when it is actually needed).
func prefetchBaseline(cfg *container.Config) {
	go func() { _, _ = loadTemplateBaseline(cfg) }()
}

// registerPostcopyHandlers wires the M5 handlers into server and returns the
// state so the caller can fold its drain stats into the final metrics JSON
// (same pattern as synth-precopy's reassembleMs, receiver.go).
func registerPostcopyHandlers(ctx context.Context, server transport.Server, cfg *container.Config, migrationDone chan error) *postcopyState {
	s := &postcopyState{
		drainConnReady: make(chan struct{}),
		drainDone:      make(chan struct{}),
	}

	server.ReceiveRequest("residual", func(r io.Reader) ([]byte, error) {
		return s.handleResidual(ctx, cfg, r)
	})

	server.ReceiveStream("residual_restore_go", func(r io.Reader) error {
		_, _ = io.ReadAll(r)
		markDst("restore_go_received")
		fmt.Println("Received residual_restore_go: starting lazy-pages restore...")
		err := s.handleRestoreGo(ctx, cfg)
		if err != nil {
			migrationDone <- err
			return err
		}
		// Restore succeeding is NOT migration-complete for M5, unlike every
		// other mode (whose restore step IS their last one): the residual
		// set is still draining, the source hasn't killed itself or sent
		// metrics yet, and criu lazy-pages + the fake page server are still
		// actively serving faults. Signaling migrationDone here let the
		// outer StartReceiver select move to its 10s metrics-wait-then-Stop
		// path early — confirmed live on the VM pair: two reps happened to
		// drain inside that window, a third didn't, and the receiver
		// process exited mid-drain, killing criu lazy-pages and the drain
		// channel out from under the still-running source ("Connection
		// reset by peer" / drain channel EOF). Defer the signal to a
		// goroutine waiting on the real completion signal (drainDone,
		// closed by signalDrained) so this handler's own ACK to the source
		// isn't held up either.
		go func() {
			<-s.drainDone
			s.mu.Lock()
			derr := s.drainErr
			s.mu.Unlock()
			migrationDone <- derr
		}()
		return nil
	})

	server.ReceiveStream("residual_push", func(r io.Reader) error {
		return s.handleResidualPush(r)
	})

	server.ReceiveStream("residual_abort", func(r io.Reader) error {
		_, _ = io.ReadAll(r)
		err := s.handleAbort(ctx, cfg)
		select {
		case migrationDone <- fmt.Errorf("migration aborted by source (drain timeout)"):
		default:
		}
		return err
	})

	// The source dials this one out and keeps it open for the whole drain
	// window (it has no inbound path to be dialed on instead, on this
	// testbed). We drive it from here: write pull requests, read responses,
	// write the drained signal — same connection, reversed usage.
	server.ReceiveConn("residual_drain", func(conn net.Conn) error {
		s.mu.Lock()
		s.drainConn = conn
		s.mu.Unlock()
		close(s.drainConnReady)
		fmt.Println("Residual drain channel established with source")
		<-s.drainDone
		return nil
	})

	return s
}

// handleResidual extracts the shipped residual dir, builds the lazy-restore
// images dir against the local baseline (BuildLazyRestoreDir), and
// hands back the exact drain set. The response IS the pending list — the
// source cannot precompute it itself, the address gate can send
// some manifest hits to the lazy set too.
func (s *postcopyState) handleResidual(ctx context.Context, cfg *container.Config, r io.Reader) ([]byte, error) {
	residualDir := filepath.Join(".", "checkpoint", "residual")
	if err := exec.CommandContext(ctx, "sudo", "rm", "-rf", residualDir).Run(); err != nil {
		return nil, fmt.Errorf("failed to clear residual dir: %w", err)
	}
	if err := os.MkdirAll(residualDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create residual dir: %w", err)
	}
	fmt.Println("Receiving and extracting residual map...")
	cmd := exec.CommandContext(ctx, "sudo", "tar", "-xf", "-", "-C", residualDir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	// The residual tar is zstd-compressed by the source (the 191 eager pages
	// dominate its size and compress well); DecompressAuto also accepts a raw tar.
	dr, derr := compress.DecompressAuto(r)
	if derr != nil {
		return nil, fmt.Errorf("decompressing residual: %w", derr)
	}
	defer dr.Close()
	cmd.Stdin = dr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("tar extraction failed for residual: %s: %w", stderr.String(), err)
	}

	baseline, err := loadTemplateBaseline(cfg)
	if err != nil {
		return nil, err
	}
	tmplDir, err := container.TemplateDir(cfg.TemplateKey())
	if err != nil {
		return nil, err
	}

	outDir := filepath.Join(".", "checkpoint", "lazy_restore")
	if err := exec.CommandContext(ctx, "sudo", "rm", "-rf", outDir).Run(); err != nil {
		return nil, fmt.Errorf("failed to clear lazy restore dir: %w", err)
	}
	// linkParent (pipeline.BuildLazyRestoreDir) computes outDir's "parent"
	// symlink via filepath.Rel(outDir, baselineDir) — that requires both
	// paths in the same absolute/relative space. tmplDir is always absolute
	// (~/.statepod/templates/<digest>), so outDir must be too, or Rel fails
	// outright ("can't make <abs> relative to <rel>", confirmed live on the
	// VM pair).
	absOutDir, err := filepath.Abs(outDir)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve lazy restore dir: %w", err)
	}
	outDir = absOutDir

	stats, err := pipeline.BuildLazyRestoreDir(residualDir, tmplDir, outDir, baseline)
	if err != nil {
		return nil, fmt.Errorf("BuildLazyRestoreDir failed: %w", err)
	}
	fmt.Printf("Lazy restore dir built: %d pages, %d parent (local), %d lazy (drain set), %d eager\n",
		stats.TotalPages, stats.Parent, stats.Lazy, stats.Eager)

	s.mu.Lock()
	s.outDir = outDir
	s.lazyStats = stats
	s.pidOrdinal = stats.PidOrdinal
	s.pending = make(map[uint64]bool, len(stats.LazyVaddrs))
	for _, v := range stats.LazyVaddrs {
		s.pending[v] = true
	}
	s.store = make(map[uint64][]byte)
	empty := len(s.pending) == 0
	s.mu.Unlock()
	if empty {
		s.signalDrained()
	}

	return json.Marshal(stats.LazyVaddrs)
}

// handleRestoreGo launches the fake page server (internal/pageserver), points
// a real `criu lazy-pages` daemon at it, and restores with --lazy-pages. This
// is the M5 commit point: the container is serving again as
// soon as this returns, most residual pages still undelivered.
func (s *postcopyState) handleRestoreGo(ctx context.Context, cfg *container.Config) error {
	// criu lazy-pages's --work-dir must be the SAME directory `runc restore`
	// uses (its default work-dir is the image-path, since we pass no
	// separate one) — that's where the daemon creates lazy-pages.socket for
	// the restore's CRIU child to connect its UFFD monitor to. A separate
	// work dir here left that socket somewhere restore never looked,
	// confirmed live on the VM pair: "uffd: connect to lazy-pages.socket
	// failed: No such file or directory".
	workDir := s.outDir
	s.mu.Lock()
	s.abort = func() { _ = s.handleAbort(ctx, cfg) }
	s.containerID = cfg.ContainerID
	s.mu.Unlock()

	psServer := &pageserver.Server{Fetch: s.fetch}
	addr, _, err := psServer.ListenAndServe("127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("failed to start fake page server: %w", err)
	}
	_, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return fmt.Errorf("failed to parse page server addr %s: %w", addr, err)
	}
	fmt.Println("Fake page server (internal/pageserver) listening on", addr)

	lazyCmd := exec.CommandContext(ctx, "sudo", "criu", "lazy-pages",
		"-D", s.outDir, "--work-dir", workDir, "--page-server", "--address", "127.0.0.1", "--port", port)
	lazyCmd.Stdout = os.Stdout
	lazyCmd.Stderr = os.Stderr
	if err := lazyCmd.Start(); err != nil {
		return fmt.Errorf("failed to start criu lazy-pages: %w", err)
	}
	lazyDone := make(chan struct{})
	restoreEnded := make(chan struct{})
	defer close(restoreEnded)
	s.mu.Lock()
	s.lazyDone = lazyDone
	s.restoreEnded = restoreEnded
	s.mu.Unlock()
	go func() {
		err := lazyCmd.Wait()
		fmt.Printf("criu lazy-pages daemon exited: %v\n", err)
		close(lazyDone)
	}()

	restoreCmd := exec.CommandContext(ctx, "sudo", "runc", "restore", "-d", "--lazy-pages",
		"--image-path", s.outDir, "--tcp-established", cfg.ContainerID)
	restoreCmd.Stdout = os.Stdout
	restoreCmd.Stderr = os.Stderr
	markDst("restore_start")
	if err := restoreCmd.Run(); err != nil {
		return fmt.Errorf("lazy-pages restore failed: %w", err)
	}
	markDst("restore_end")

	return finishRestoreNetworking(ctx, cfg)
}

// fetch is pageserver.Fetch: answers a real UFFD fault relayed by `criu
// lazy-pages`. Local store hit (background push already delivered it, or an
// earlier fault already pulled it) resolves at memory speed; a miss pulls
// synchronously over the drain channel from the still-frozen source.
func (s *postcopyState) fetch(dstID, vaddr, nrPages uint64) ([]byte, error) {
	s.mu.Lock()
	s.lastFetch = time.Now()
	s.mu.Unlock()
	// CRIU names the task (dst_id); the residual set is keyed by composite page
	// address, so the same vaddr in two processes is two different pages.
	key, err := pipeline.DecodePageServerDst(dstID)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	ord, ok := s.pidOrdinal[key]
	s.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("page-server request for pagemap %d, which the residual set does not know", key)
	}
	vaddr, err = pipeline.ComposeAddr(ord, vaddr)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if data, ok := s.lookupLocked(vaddr, nrPages); ok {
		s.mu.Unlock()
		return data, nil
	}
	s.mu.Unlock()

	<-s.drainConnReady
	resp, err := s.pullOverDrainChannel(vaddr, nrPages)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	for i := uint64(0); i < nrPages; i++ {
		pv := vaddr + i*pipeline.PageSize
		s.store[pv] = resp[i*pipeline.PageSize : (i+1)*pipeline.PageSize]
		delete(s.pending, pv)
	}
	s.pulledBytes += int64(len(resp))
	s.pullFaults++
	empty := len(s.pending) == 0
	s.mu.Unlock()
	if empty {
		s.signalDrained()
	}
	return resp, nil
}

// pullOverDrainChannel writes one pull request and reads its response on the
// drain channel. drainWriteMu serializes concurrent Fetch calls onto the one
// shared connection — fine at this testbed's fault concurrency; a documented
// simplification, not a correctness gap (each request/response pair is still
// complete and correct, just not pipelined).
func (s *postcopyState) pullOverDrainChannel(vaddr, nrPages uint64) ([]byte, error) {
	s.drainWriteMu.Lock()
	defer s.drainWriteMu.Unlock()

	req := append([]byte{drainMsgPull}, encodePageRecordHeader(vaddr, nrPages)...)
	if _, err := s.drainConn.Write(req); err != nil {
		return nil, fmt.Errorf("drain channel write failed: %w", err)
	}
	resp := make([]byte, nrPages*pipeline.PageSize)
	if _, err := io.ReadFull(s.drainConn, resp); err != nil {
		return nil, fmt.Errorf("drain channel read failed: %w", err)
	}
	return resp, nil
}

// lookupLocked returns the nrPages contiguous pages from the local store if
// all of them are already present. Caller holds s.mu.
func (s *postcopyState) lookupLocked(vaddr, nrPages uint64) ([]byte, bool) {
	out := make([]byte, 0, nrPages*pipeline.PageSize)
	for i := uint64(0); i < nrPages; i++ {
		p, ok := s.store[vaddr+i*pipeline.PageSize]
		if !ok {
			return nil, false
		}
		out = append(out, p...)
	}
	return out, true
}

// handleResidualPush parses the background pusher's (vaddr, page) record
// stream (source's synth_postcopy.go, one record per residual page) into the
// same store/pending bookkeeping as fetch — whichever of push or pull
// delivers a page first wins; the other is a harmless duplicate write.
func (s *postcopyState) handleResidualPush(r io.Reader) error {
	hdr := make([]byte, 16)
	count := 0
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("residual_push: reading record header: %w", err)
		}
		vaddr, nrPages := decodePageRecordHeader(hdr)
		if nrPages != 1 {
			return fmt.Errorf("residual_push: unexpected nrPages %d (want 1)", nrPages)
		}
		page := make([]byte, pipeline.PageSize)
		if _, err := io.ReadFull(r, page); err != nil {
			return fmt.Errorf("residual_push: reading page for vaddr 0x%x: %w", vaddr, err)
		}

		s.mu.Lock()
		if _, already := s.store[vaddr]; !already {
			s.store[vaddr] = page
		}
		delete(s.pending, vaddr)
		empty := len(s.pending) == 0
		s.mu.Unlock()
		count++
		if empty {
			s.signalDrained()
		}
	}
	fmt.Printf("residual_push: received %d pages\n", count)
	return nil
}

// signalDrained runs once, when the pending set hits zero. The pages are then in OUR store,
// not yet in the process, so the source (still frozen, still able to resume) is told only
// after the daemon has injected them and the container runs. It must not block: the caller
// may be the page-server fetch the daemon is waiting on.
func (s *postcopyState) signalDrained() {
	s.drainOnce.Do(func() {
		s.mu.Lock()
		s.drainedAt = time.Now()
		s.lastFetch = s.drainedAt
		s.mu.Unlock()
		go s.finishDrain()
	})
}

// finishDrain is the M5 commit decision on the destination. Telling the source
// "drained" as soon as the pages were local let it kill itself while the daemon was stuck:
// the migration reported success with a dead service, with no copy left (2 runs, 2026-10-09
// and 2026-10-28).
func (s *postcopyState) finishDrain() {
	<-s.drainConnReady
	why := s.waitLazyInjected()
	if why == "" {
		s.mu.Lock()
		id := s.containerID
		s.mu.Unlock()
		if st, err := container.GetRuncState(context.Background(), id); err != nil {
			why = fmt.Sprintf("cannot read the restored container's state: %v", err)
		} else if st.Status != "running" {
			why = fmt.Sprintf("restored container is %q", st.Status)
		}
	}
	msg := drainMsgDrained
	if why == "" {
		fmt.Println("Residual set injected; notifying source over drain channel")
	} else {
		msg = drainMsgFailed
		fmt.Printf("Drain failed: %s; discarding the restore, source resumes\n", why)
		logStuckRestore()
		s.mu.Lock()
		s.drainErr = fmt.Errorf("drain failed: %s", why)
		abort := s.abort
		s.mu.Unlock()
		if abort != nil {
			abort()
		}
	}
	s.drainWriteMu.Lock()
	_, err := s.drainConn.Write([]byte{msg})
	s.drainWriteMu.Unlock()
	if err != nil {
		fmt.Printf("Warning: failed to notify source of the drain result: %v\n", err)
	}
	close(s.drainDone)
}

// waitLazyInjected waits for `runc restore` to return and the daemon to exit; "" on success,
// else why not. A restore or daemon that started late or is still asking for pages is alive:
// only silence counts as stuck.
func (s *postcopyState) waitLazyInjected() string {
	for {
		s.mu.Lock()
		lazyDone, restoreEnded, last, start := s.lazyDone, s.restoreEnded, s.lastFetch, s.drainedAt
		s.mu.Unlock()
		if lazyDone != nil {
			select {
			case <-lazyDone:
				select {
				case <-restoreEnded:
					s.mu.Lock()
					s.lazyExitMs = float64(time.Since(start).Milliseconds())
					s.mu.Unlock()
					return ""
				default:
				}
			default:
			}
		}
		if time.Since(last) > lazyDaemonStall {
			return fmt.Sprintf("lazy-pages daemon stuck: no exit and no page request for %s", lazyDaemonStall)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// logStuckRestore records where the daemon and the restored tasks are blocked, for the cause
// of a stall (nothing else is left of it once they are killed).
func logStuckRestore() {
	out, _ := exec.Command("sudo", "sh", "-c", `ps -eo pid,stat,wchan:32,cmd | grep -E '[c]riu|[r]edis|[r]unc' ; `+
		`for p in $(pgrep -f '[c]riu lazy-pages'); do echo "--- daemon $p"; cat /proc/$p/stack; done`).CombinedOutput()
	fmt.Printf("--- stuck restore state ---\n%s\n---\n", out)
}

// handleAbort discards a half-drained restore (source gave up waiting for
// drain-complete failure path): kill and delete, no retry — the
// source is the surviving copy now.
func (s *postcopyState) handleAbort(ctx context.Context, cfg *container.Config) error {
	fmt.Println("Received residual_abort: discarding half-drained restore")
	if err := exec.CommandContext(ctx, "sudo", "runc", "kill", cfg.ContainerID, "SIGKILL").Run(); err != nil {
		fmt.Printf("Warning: failed to kill half-drained container: %v\n", err)
	}
	if err := exec.CommandContext(ctx, "sudo", "runc", "delete", cfg.ContainerID).Run(); err != nil {
		fmt.Printf("Warning: failed to delete half-drained container: %v\n", err)
	}
	return nil
}

// fillMetrics folds the destination-only drain stats into the metrics the
// source sent, right before saving — same pattern as reassembleMs
// (receiver.go), since PulledBytes/PullFaults/DrainCompleteTime only exist
// on this side.
func (s *postcopyState) fillMetrics(m *migration.MigrationMetrics) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m.Residual == nil {
		m.Residual = &migration.ResidualMetrics{}
	}
	if s.lazyStats != nil {
		m.Residual.Parent = s.lazyStats.Parent
		m.Residual.Lazy = s.lazyStats.Lazy
	}
	m.Residual.PulledBytes = s.pulledBytes
	m.Residual.PullFaults = s.pullFaults
	m.Residual.LazyExitMs = s.lazyExitMs
	if !s.drainedAt.IsZero() {
		m.DrainCompleteTime = s.drainedAt
	}
}
