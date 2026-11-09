package cli

import (
	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/receiver"
	"github.com/spf13/cobra"
)

func newStartCommand() *cobra.Command {
	var source string
	var volumeSync bool
	var listenAddr string
	var metricsOut string
	var rsyncPort string
	var tmpfsCheckpoint bool
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the container in current directory",
		RunE: func(cmd *cobra.Command, args []string) error {
			if source != "" {
				return receiver.StartReceiver(cmd.Context(), listenAddr, metricsOut, rsyncPort, tmpfsCheckpoint)
			}
			return container.Start(cmd.Context(), source, volumeSync)
		},
	}

	cmd.Flags().StringVar(&source, "source", "", "Source IP address")
	cmd.Flags().BoolVar(&volumeSync, "volume-sync", false, "Enable volume sync for redis")
	cmd.Flags().StringVar(&listenAddr, "listen", "0.0.0.0:8080", "Address to listen on for migration streams (receiver mode)")
	cmd.Flags().StringVar(&metricsOut, "metrics-out", "", "Path to write migration metrics JSON (receiver mode; default: auto-detect experiment dir)")
	cmd.Flags().StringVar(&rsyncPort, "rsync-port", "", "Start a companion rsync daemon on this port for --mode precopy-rsync/sync-rsync (receiver mode; empty disables it)")
	cmd.Flags().BoolVar(&tmpfsCheckpoint, "tmpfs-checkpoint", false, "Mount ./checkpoint as tmpfs (receiver mode). Must match the source's --tmpfs-checkpoint setting for a clean A/B")

	return cmd
}
