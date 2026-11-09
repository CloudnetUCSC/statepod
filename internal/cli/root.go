package cli

import (
	"github.com/spf13/cobra"
)

func NewRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "statepod",
		Short: "A simple container engine",
		Long:  "StatePod is a simple container engine for managing stateful containers.",
	}
	cmd.AddCommand(newCreateCommand())
	cmd.AddCommand(newStartCommand())
	cmd.AddCommand(newStopCommand())
	cmd.AddCommand(newMigrateCommand())
	cmd.AddCommand(newBurstCommand())
	cmd.AddCommand(newStatusCommand())
	cmd.AddCommand(newTemplateCommand())
	cmd.AddCommand(newPagecmpCommand())
	return cmd
}
