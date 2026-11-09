package migration

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// MigrationMetrics stores key metrics for the migration process.
type MigrationMetrics struct {
	Mode                      string                 `json:"mode"`
	ContainerID               string                 `json:"container_id"`
	MigrationStartTime        time.Time              `json:"migration_start_time"`
	FreezeStartTime           time.Time              `json:"freeze_start_time"`
	RestoreEndTime            time.Time              `json:"restore_end_time"`
	TotalMigrationTime        float64                `json:"total_migration_time_ms"`
	DowntimeSourceSide        float64                `json:"downtime_source_side_ms"`
	TotalDataTransferredBytes int64                  `json:"total_data_transferred_bytes"`
	Iterations                []PreCopyIterationInfo `json:"iterations,omitempty"`
	Pipeline                  *PipelineMetrics       `json:"pipeline,omitempty"`
	// SourceKillTime is set only by synth-precopy's ACK-gated final phase
	// (failure handling): the source runs --leave-running through
	// shipping + destination-side verification, so FreezeStartTime no longer
	// marks source disruption — SourceKillTime does, and downtime is measured
	// from here. Zero for strategies that still freeze-and-kill at dump time
	// (precopy/criu-incr/sync).
	SourceKillTime time.Time `json:"source_kill_time,omitempty"`
	// FSUpperBytes/FSUpperMs are the M4 filesystem-delta stage (
	// ): the overlay upperdir tar stream, synth-precopy only. Kept
	// separate from PipelineMetrics, which is specifically the M1+M2 memory
	// page-reconciliation pipeline (its CompressionRatio is DeltaBytes over
	// CompressedBytes; folding unrelated upperdir bytes into CompressedBytes
	// would corrupt that ratio).
	FSUpperBytes int64   `json:"fs_upper_bytes,omitempty"`
	FSUpperMs    float64 `json:"fs_upper_ms,omitempty"`
	// FSRounds are the filesystem pre-copy rounds (fsprecopy.go): one entry per
	// rsync pass, live ones before the stop and the final one inside the downtime.
	FSRounds []FSRound `json:"fs_rounds,omitempty"`
	// RestoreGoTime is synth-postcopy's commit point:
	// set once the destination has ACKed "residual_restore_go" and is already
	// serving via lazy-pages restore. Downtime is measured from here, not
	// FreezeStartTime — the pause in step 3 disturbs the source, but the
	// destination isn't up yet at that instant, so RestoreGoTime is the actual
	// point service resumes. Mirrors SourceKillTime's role in synth-precopy.
	RestoreGoTime time.Time `json:"restore_go_time,omitempty"`
	// DrainCompleteTime is when the destination's pending residual set hit
	// zero (the "residual_drained" signal) — separate from downtime, this is
	// the fault-latency-tail measurement the evaluation wants alongside it.
	DrainCompleteTime time.Time        `json:"drain_complete_time,omitempty"`
	Residual          *ResidualMetrics `json:"residual,omitempty"`
	// Phases is the per-phase timeline: a named wall-clock timestamp
	// for every step of the migration, in every mode, so a derived interval
	// (write window, stop-to-serve, ship time) never has to be inferred from a
	// mode-specific field. Keys carry their clock domain as a prefix: "src_*"
	// were taken on the source host, "dst_*" on the destination. The two hosts'
	// clocks are not synchronized (~2 s skew observed), so only subtract
	// timestamps with the same prefix; cross-host intervals need a clock-offset
	// estimate and are never computed here.
	Phases map[string]time.Time `json:"phases,omitempty"`
	// FinalMode is synth-precopy's final-phase mode: "catchup" or
	// "availability". Empty for other strategies.
	FinalMode string `json:"final_mode,omitempty"`
	// ZstdLevel is the zstd level the plain precopy/criu-incr baselines ran
	// with; 0/omitted = uncompressed.
	ZstdLevel int `json:"zstd_level,omitempty"`
	// RepausedAfterDump is set if the container was found running (not paused)
	// after the catch-up final dump and had to be re-paused: a residual write
	// window, never silently ignored.
	RepausedAfterDump bool `json:"repaused_after_dump,omitempty"`
	// UpperResent is set if the M4 upper layer changed after the pre-pause
	// send and was sent again at the final phase.
	UpperResent bool `json:"upper_resent,omitempty"`
}

// Mark records the current time under name in the Phases timeline and returns
// it. Re-marking a name overwrites (a retried step records its last attempt).
// Not safe for concurrent use — call from the strategy's main flow only.
func (m *MigrationMetrics) Mark(name string) time.Time {
	t := time.Now()
	if m.Phases == nil {
		m.Phases = make(map[string]time.Time)
	}
	m.Phases[name] = t
	return t
}

// MergePhases folds another host's timeline into m (used by the receiver to
// add its "dst_*" marks to the source-supplied metrics). Existing keys win,
// so a destination never overwrites a source-side timestamp.
func (m *MigrationMetrics) MergePhases(other map[string]time.Time) {
	if len(other) == 0 {
		return
	}
	if m.Phases == nil {
		m.Phases = make(map[string]time.Time, len(other))
	}
	for k, v := range other {
		if _, ok := m.Phases[k]; !ok {
			m.Phases[k] = v
		}
	}
}

// ResidualMetrics reports the M5 residual-map/drain outcome (
// §9), synth-postcopy only.
type ResidualMetrics struct {
	TotalPages  int     `json:"total_pages"`
	Parent      int     `json:"parent"`                 // resolved locally via in_parent
	Lazy        int     `json:"lazy"`                   // drain set at cutover
	Eager       int     `json:"eager"`                  // not lazy-eligible, shipped raw at cutover
	PushedBytes int64   `json:"pushed_bytes"`           // background pusher's contribution to the drain
	PulledBytes int64   `json:"pulled_bytes"`           // on-fault WAN pulls (pageserver Fetch misses)
	PullFaults  int     `json:"pull_faults"`            // count of on-fault pulls (fault-latency tail)
	DrainMs     float64 `json:"drain_ms"`               // RestoreGoTime -> DrainCompleteTime
	LazyExitMs  float64 `json:"lazy_exit_ms,omitempty"` // residual set local -> pages injected, daemon exited
}

// PipelineMetrics reports the M1+M2 reconciliation outcome for the synth-precopy
// mode: baseline hit rate, bytes on wire, and per-stage timing.
type PipelineMetrics struct {
	TotalPages    int     `json:"total_pages"`
	Hits          int     `json:"hits"`           // pages resolved to a 16 B baseline ref
	Misses        int     `json:"misses"`         // pages sent raw
	HitRate       float64 `json:"hit_rate"`       // matched / dumped pages
	RefBytes      int64   `json:"ref_bytes"`      // ref bytes on the wire
	RawBytes      int64   `json:"raw_bytes"`      // raw-page bytes on the wire
	ManifestBytes int64   `json:"manifest_bytes"` // pre-burst manifest payload (amortized)
	DeltaBytes    int64   `json:"delta_bytes"`    // total delta dir size streamed (refs+raw+metadata imgs), pre-compression
	EncodeMs      float64 `json:"encode_ms"`      // source-side delta encode time
	ReassembleMs  float64 `json:"reassemble_ms"`  // destination-side reassemble time (filled on dest)

	// (zstd stage, internal/compress): CompressedBytes is DeltaBytes'
	// actual size on the wire after the stage (equal to DeltaBytes when
	// --zstd-level 0 disables it). StreamMs is tar+compress+network time for
	// the delta streams, summed across rounds — compression is pipelined
	// through the network write (internal/compress.Compress feeds SendStream
	// directly), so it cannot be isolated from transfer time without
	// buffering the whole stream first, which would defeat the point of
	// streaming it. CompressionRatio is DeltaBytes/CompressedBytes (1.0 when
	// disabled).
	// Final-phase delta (catch-up round in catchup mode, the final dump in
	// availability mode): dirty pages, how many resolved to baseline refs, and
	// the encoded delta size before compression.
	FinalPages      int   `json:"final_pages"`
	FinalHits       int   `json:"final_hits"`
	FinalDeltaBytes int64 `json:"final_delta_bytes"`

	CompressedBytes  int64   `json:"compressed_bytes"`
	StreamMs         float64 `json:"stream_ms"`
	CompressionRatio float64 `json:"compression_ratio"`
}

// PreCopyIterationInfo stores details about each pre-copy iteration.
type PreCopyIterationInfo struct {
	Iteration         int       `json:"iteration"`
	Duration          float64   `json:"duration_ms"`
	SizeInBytes       int64     `json:"size_bytes"`
	TransferStartTime time.Time `json:"-"`
	TransferTime      float64   `json:"transfer_time_ms"`
	Bandwidth         float64   `json:"bandwidth_mbps"`
}

// SaveMetrics saves the metrics to a JSON file in the experiment folder.
func (m *MigrationMetrics) SaveMetrics(filePath string) error {
	// the former "downtime_ms" (RestoreEndTime - FreezeStartTime) is
	// retired - it subtracted a destination-clock timestamp from a
	// source-clock one (~2 s host skew) and, for M5, included the drain. Use
	// downtime_source_side_ms (all source-clock) or the client probe's outage.
	// Every strategy now sets TotalMigrationTime itself from source-clock
	// timestamps; only fall back to the cross-clock difference for a metrics
	// file that arrives without one.
	if m.TotalMigrationTime == 0 && !m.MigrationStartTime.IsZero() && !m.RestoreEndTime.IsZero() {
		m.TotalMigrationTime = float64(m.RestoreEndTime.Sub(m.MigrationStartTime).Milliseconds())
	}

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metrics: %w", err)
	}

	return os.WriteFile(filePath, data, 0644)
}
