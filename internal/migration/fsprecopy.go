package migration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/transport"
)

// Filesystem pre-copy . The container's writable layer (the
// overlay upperdir) moves in rsync rounds to the receiver's rsync daemon
// ("upper" module): live rounds while the source still runs, then one final round
// after it has stopped, so only what changed since the last round is sent inside
// the downtime. Every variant that carries filesystem state uses this same
// mechanism (synth-precopy/M4, synth-postcopy/M5, stock lazy-pages and the
// --fs-sync=rsync pre-copy baseline): the filesystem part is a standard tool,
// identical across variants, not a contribution of this work (prior art: layered
// storage migration, Ma et al. 2019; OverlayFS migration; volume sync with rsync).
//
// rsync flags: -a (includes devices: overlay whiteouts are 0/0 character
// devices), -H hard links, -A ACLs, -X xattrs (trusted.overlay.* marks opaque
// directories; the earlier tar stream dropped them), --numeric-ids, --delete
// (entries removed on the source since the last round), --inplace (block delta
// against the copy the destination received in the previous round; the
// destination container is not running, so nothing reads a half-updated file).
// Compression uses the variant's zstd level (none at 0), like its memory streams.

const (
	fsRoundSmall   = 64 << 10 // a live round below this many bytes ends the live rounds
	fsMaxLiveRound = 4        // live rounds at most (the first one carries the whole layer)
)

// FSRound is one rsync pass, reported in the metrics.
type FSRound struct {
	Label string  `json:"label"`
	Bytes int64   `json:"bytes"`
	Ms    float64 `json:"ms"`
	Live  bool    `json:"live"`
	// Skipped: nothing changed since the previous round, so no rsync ran (no round trips)
	Skipped bool `json:"skipped,omitempty"`
}

type fsPrecopy struct {
	ctx      context.Context
	client   transport.Client
	destAddr string
	target   string
	zstd     int
	metrics  *MigrationMetrics
	open     bool
	// change detection between rounds: a marker file written just before each round and
	// every path of the layer then (upperTreeChangedSince, upper.go). An rsync pass
	// costs several round trips even when nothing changed (~3 s at 200 ms RTT), so rounds
	// run only when the layer changed (measured 2026-10-27: four unconditional rounds put
	// ~3 s of rsync handshakes into M4's downtime on Redis, whose layer never changes).
	marker      string
	pathsAtSend []string
	sentOnce    bool
}

func newFSPrecopy(ctx context.Context, client transport.Client, destAddr, rsyncPort string, zstd int, m *MigrationMetrics) *fsPrecopy {
	if rsyncPort == "" {
		rsyncPort = DefaultRsyncPort
	}
	host := strings.SplitN(destAddr, ":", 2)[0]
	return &fsPrecopy{ctx: ctx, client: client, destAddr: destAddr, zstd: zstd, metrics: m,
		target: fmt.Sprintf("rsync://%s:%s/upper/", host, rsyncPort), marker: "./.fs_round_marker"}
}

// changed reports whether the layer changed since the last round started (always
// true before the first round). Anything unreadable counts as changed.
func (f *fsPrecopy) changed() bool {
	if !f.sentOnce {
		return true
	}
	ch, why := upperTreeChangedSince(f.ctx, container.UpperDir, f.marker, f.pathsAtSend)
	if ch {
		fmt.Printf("fs layer changed since the last round: %s\n", why)
	}
	return ch
}

func (f *fsPrecopy) skip(label string, live bool) {
	f.metrics.FSRounds = append(f.metrics.FSRounds, FSRound{Label: label, Live: live, Skipped: true})
	fmt.Printf("fs round %s skipped: layer unchanged since the last round\n", label)
}

// control sends an empty stream of the given type (fs_upper_open / fs_upper_close).
func (f *fsPrecopy) control(streamType string) error {
	// a control message is instant: bound it, a source may be paused and waiting on it
	ctx, cancel := context.WithTimeout(f.ctx, pauseStallLimit)
	defer cancel()
	if err := f.client.Connect(ctx, f.destAddr); err != nil {
		return fmt.Errorf("failed to connect for %s: %w", streamType, err)
	}
	if err := f.client.SendStream(ctx, streamType, strings.NewReader("")); err != nil {
		return fmt.Errorf("%s failed: %w", streamType, err)
	}
	return nil
}

// round runs one rsync pass of the upper layer. live: the source may still be
// writing, so rsync's "some files vanished before they could be transferred"
// (exit 24) is not an error; the next round (at the latest the final one, with
// the source stopped) carries the current state.
func (f *fsPrecopy) round(label string, live bool) (int64, error) {
	if !f.open {
		// the destination unmounts its overlay while rsync writes the upperdir
		// underneath (overlayfs does not support changes to a mounted upper)
		if err := f.control("fs_upper_open"); err != nil {
			return 0, err
		}
		f.open = true
	}
	// marker BEFORE rsync reads anything: a write during the pass is newer than it
	if err := os.WriteFile(f.marker, nil, 0644); err != nil {
		return 0, fmt.Errorf("fs round %s: marker: %w", label, err)
	}
	f.pathsAtSend = upperPaths(f.ctx, container.UpperDir)
	f.sentOnce = true
	args := []string{"rsync", "-aHAX", "--numeric-ids", "--delete", "--inplace", "--stats"}
	if f.zstd > 0 {
		args = append(args, "--compress", "--compress-choice=zstd", fmt.Sprintf("--compress-level=%d", f.zstd))
	}
	if !live { // source paused: a stalled round must fail; rsync's limits count silence
		secs := int(pauseStallLimit.Seconds())
		args = append(args, fmt.Sprintf("--timeout=%d", secs), fmt.Sprintf("--contimeout=%d", secs))
	}
	args = append(args, container.UpperDir+"/", f.target) // migrate runs in the bundle dir
	cmd := exec.CommandContext(f.ctx, "sudo", args...)
	cmd.Env = append(os.Environ(), "LC_ALL=C")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	start := time.Now()
	err := cmd.Run()
	ms := float64(time.Since(start).Milliseconds())
	if err != nil {
		var ee *exec.ExitError
		if !(live && errors.As(err, &ee) && ee.ExitCode() == 24) {
			return 0, fmt.Errorf("fs round %s: rsync failed: %s: %w", label, strings.TrimSpace(stderr.String()), err)
		}
	}
	sent := parseRsyncSentBytes(stdout.String())
	f.metrics.FSRounds = append(f.metrics.FSRounds, FSRound{Label: label, Bytes: sent, Ms: ms, Live: live})
	f.metrics.FSUpperBytes += sent
	f.metrics.FSUpperMs += ms
	f.metrics.TotalDataTransferredBytes += sent
	fmt.Printf("fs round %s: %d B on the wire in %.0f ms%s\n", label, sent, ms, map[bool]string{true: " (live)", false: ""}[live])
	return sent, nil
}

// precopyLive runs live rounds while the layer keeps changing, until one sends
// less than fsRoundSmall bytes or fsMaxLiveRound rounds have run, then remounts
// the destination's overlay early if the layer is quiet (settleIfQuiet). The
// source keeps serving throughout.
func (f *fsPrecopy) precopyLive() error {
	for i := 0; i < fsMaxLiveRound; i++ {
		if i > 0 && !f.changed() {
			break
		}
		sent, err := f.round(fmt.Sprintf("live%d", i), true)
		if err != nil {
			return err
		}
		if i > 0 && sent < fsRoundSmall {
			break
		}
	}
	return f.settleIfQuiet()
}

// catchUp is one more live round if the layer changed (e.g. during the memory
// pre-dump rounds), then settleIfQuiet.
func (f *fsPrecopy) catchUp(label string) error {
	if f.changed() {
		if _, err := f.round(label, true); err != nil {
			return err
		}
	} else {
		f.skip(label, true)
	}
	return f.settleIfQuiet()
}

// settleIfQuiet remounts the destination's overlay while the source still serves
// if nothing changed since the last round, so an unchanged layer costs no round
// trip inside the downtime; final() reopens it if a write lands after all.
func (f *fsPrecopy) settleIfQuiet() error {
	if f.open && !f.changed() {
		return f.close()
	}
	return nil
}

// final runs the last round (source stopped: consistent) only if the layer
// changed since the previous one, and remounts the destination's overlay. Safe to
// call again on a retry: rsync only sends what still differs.
func (f *fsPrecopy) final() error {
	if f.changed() {
		if _, err := f.round("final", false); err != nil {
			return err
		}
	} else {
		f.skip("final", false)
	}
	return f.close()
}

// close remounts the destination's overlay if a round opened it.
func (f *fsPrecopy) close() error {
	if !f.open {
		return nil
	}
	if err := f.control("fs_upper_close"); err != nil {
		return err
	}
	f.open = false
	return nil
}
