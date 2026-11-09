package cli

import (
	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/spf13/cobra"
)

func newStopCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop the container in current directory",
		RunE: func(cmd *cobra.Command, args []string) error {
			return container.Stop(cmd.Context())
		},
	}

	return cmd
}
