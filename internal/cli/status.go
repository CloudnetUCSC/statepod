package cli

import (
	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/spf13/cobra"
)

func newStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the status of the container in current directory",
		RunE: func(cmd *cobra.Command, args []string) error {
			return container.Status(cmd.Context())
		},
	}

	return cmd
}
