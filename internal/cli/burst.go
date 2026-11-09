package cli

import (
	"fmt"

	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/migration"
	"github.com/spf13/cobra"
)

// newBurstCommand is the statefulness-aware dispatcher: it reads the
// container's classification from statepod.json and picks the migration
// strategy — the full synth-precopy stack (M1+M2+M3+M4 + compression,
// for stateful containers, restart-based migration (sync mode:
// stop at source, fresh start at destination) for stateless ones.
func newBurstCommand() *cobra.Command {
	var dest string

	cmd := &cobra.Command{
		Use:   "burst",
		Short: "Migrate the container using the strategy matching its statefulness",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := container.ReadConfig()
			if err != nil {
				return fmt.Errorf("failed to read statepod config: %w", err)
			}

			mode := "sync"
			var zstdLevel int
			var tmpfsCheckpoint bool
			if cfg.Stateful {
				// synth-precopy is the full stack — M1+M2+M3+M4 +
				// compression — not Baseline 2's plain precopy. burst is the
				// one-command production path, so it runs the core system's own
				// recommended defaults rather than internal/cli/migrate.go's
				// ablation-harness defaults (--tmpfs-checkpoint defaults false
				// there so a bare `migrate` stays comparable across modes).
				// precopyRounds stays 0: synth-precopy's own measured default
				// (synth.go) — chained rounds cost more bytes than
				// they save against this testbed's workloads.
				mode = "synth-precopy"
				zstdLevel = 1          //
				tmpfsCheckpoint = true // M3 rung 1
			}
			fmt.Printf("Burst dispatch: container %s is %s -> using %s migration\n",
				cfg.ContainerID, statefulness(cfg.Stateful), mode)

			// burst is the fixed one-command production path, not the
			// ablation harness (--no-fs-upper) — it always runs the full
			// stack when stateful.
			return migration.Migrate(cmd.Context(), mode, dest, migration.DefaultRsyncPort, 0, zstdLevel, tmpfsCheckpoint, false, 0, "", false, false, "")
		},
	}

	cmd.Flags().StringVar(&dest, "dest", "", "Destination IP:port")
	cmd.MarkFlagRequired("dest")

	return cmd
}

func statefulness(stateful bool) string {
	if stateful {
		return "stateful"
	}
	return "stateless"
}
