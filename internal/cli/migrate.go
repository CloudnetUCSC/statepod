package cli

import (
	"time"

	"github.com/CloudnetUCSC/statepod/internal/migration"
	"github.com/spf13/cobra"
)

// legacyPreCopyRoundsDefault preserves the historical shape of Baseline 2/3
// (pre0..pre3 + final, hardcoded before --precopy-rounds existed for these
// modes) when the user doesn't pass --precopy-rounds explicitly. Pass
// --precopy-rounds 0 for the single-shot ablation that matches synth-precopy's
// own default and isolates a baseline round's cost from round count.
const legacyPreCopyRoundsDefault = 4

func newMigrateCommand() *cobra.Command {
	var mode string
	var dest string
	var rsyncPort string
	var precopyRounds int
	var zstdLevel int
	var tmpfsCheckpoint bool
	var noFSUpper bool
	var drainTimeout time.Duration
	var finalMode string
	var mergeRestore bool
	var fsSync string

	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate the running container",
		RunE: func(cmd *cobra.Command, args []string) error {
			rounds := precopyRounds
			if !cmd.Flags().Changed("precopy-rounds") {
				switch mode {
				case "precopy", "criu-incr", "precopy-rsync":
					rounds = legacyPreCopyRoundsDefault
				}
			}
			return migration.Migrate(cmd.Context(), mode, dest, rsyncPort, rounds, zstdLevel, tmpfsCheckpoint, noFSUpper, drainTimeout, finalMode, mergeRestore, cmd.Flags().Changed("zstd-level"), fsSync)
		},
	}

	cmd.Flags().StringVar(&mode, "mode", "precopy", "Migration mode (precopy/synth-precopy/synth-postcopy/criu-lazy/postcopy/hybrid/sync/criu-incr/precopy-rsync/sync-rsync)")
	cmd.Flags().StringVar(&dest, "dest", "", "Destination IP address")
	cmd.Flags().StringVar(&rsyncPort, "rsync-port", migration.DefaultRsyncPort, "Receiver's rsync daemon port (precopy-rsync/sync-rsync modes only; must match the receiver's --rsync-port)")

	cmd.Flags().IntVar(&precopyRounds, "precopy-rounds", 0, "Chained --pre-dump rounds before the freeze (0 = single freeze dump). Default is 0 for synth-precopy; precopy/criu-incr/precopy-rsync default to 4 (historical Baseline 2/3 shape) unless set explicitly. Rounds give an exact dirty set via CRIU's soft-dirty tracking (M2) but re-ship pages a background-dirtying workload keeps touching")
	cmd.Flags().IntVar(&zstdLevel, "zstd-level", 1, "zstd level for the delta stream (0 disables compression). synth-precopy defaults to 1; the plain precopy/criu-incr baselines are uncompressed unless this flag is given explicitly (vanilla+zstd, criu-incr+zstd). Nominal 1/3/9 map onto klauspost/compress's speed tiers found no ratio/latency payoff above SpeedFastest on the redis benchmark, hence default 1. The destination auto-detects the zstd frame so this is a source-side-only toggle")
	cmd.Flags().BoolVar(&tmpfsCheckpoint, "tmpfs-checkpoint", false, "Mount the local checkpoint dir as tmpfs so CRIU's image writes never touch disk on this host. Source-side only; pass the matching flag to the receiver's `start` command for the destination side. Default false (plain disk) for the baseline")
	cmd.Flags().BoolVar(&noFSUpper, "no-fs-upper", false, "Disable the M4 overlay upperdir stream, synth-precopy only. Ablation toggle for isolating M4's contribution against the +M1+M2+compression+M3 cell; the other mechanisms are already ablated via --mode, --precopy-rounds, and --zstd-level 0")
	cmd.Flags().DurationVar(&drainTimeout, "drain-timeout", 60*time.Second, "How long to wait for the destination's residual-drain signal before giving up and thawing the source (synth-postcopy only). The drain channel itself needs no address flag: the source dials out to --dest and keeps that connection open, since a NAT'd source has no inbound path for the destination to dial back")

	cmd.Flags().StringVar(&finalMode, "final-mode", "catchup", "synth-precopy final phase: \"catchup\" pauses the source only after the destination holds a verified checkpoint, dumps the pages changed since, and restores - no acknowledged write is lost; \"availability\" is the original behaviour (source keeps serving until the destination ACKs; writes in that window are lost) kept as a labelled ablation")

	cmd.Flags().BoolVar(&mergeRestore, "merge-restore", false, "synth-precopy only (experimental): send the final delta as one stream that makes the destination restore as soon as the image verifies and ACK only once it is serving, removing the separate restore-go connection and round trip. The source stays paused until that ACK; an ambiguous outcome is resolved by asking the destination for its status")

	cmd.Flags().StringVar(&fsSync, "fs-sync", "", "precopy/criu-incr only: how the container's writable (overlay upper) layer moves. \"rsync\": the filesystem pre-copy every other variant uses (live rsync rounds, final round after the freeze; fsprecopy.go) - the fair baseline. \"flat\": stop-and-copy tar of the whole layer after the final dump (historical baseline). Off by default (the historical Baseline 2/3 carry no filesystem state)")

	cmd.MarkFlagRequired("dest")

	return cmd
}
