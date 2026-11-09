package cli

import (
	"fmt"

	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/spf13/cobra"
)

func newTemplateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "template",
		Short: "Manage synthesized baseline templates (destination-side, design M1)",
	}
	cmd.AddCommand(newTemplateGenerateCommand())
	cmd.AddCommand(newTemplateKeyCommand())
	return cmd
}

func newTemplateGenerateCommand() *cobra.Command {
	var withNodeServer bool
	var withRedis bool
	var workloadArg string

	cmd := &cobra.Command{
		Use:   "generate [image]",
		Short: "Cold-start a throwaway instance, CRIU-dump it at readiness, store as the baseline for the image digest",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			wl, err := resolveWorkload(workloadArg, withNodeServer, withRedis)
			if err != nil {
				return err
			}
			_, err = container.GenerateTemplate(cmd.Context(), args[0], wl)
			return err
		},
	}

	cmd.Flags().StringVar(&workloadArg, "workload", "", "Workload to template: a built-in (redis, node) or a workload spec JSON file")
	cmd.Flags().BoolVar(&withNodeServer, "with-node-server", false, "Template for the Node.js server workload")
	cmd.Flags().BoolVar(&withRedis, "redis", false, "Template for the Redis workload")

	return cmd
}

// newTemplateKeyCommand prints the template-store key of the bundle in the
// current directory (image ID, plus a workload-spec fingerprint for
// non-built-in workloads). The experiment runner uses it to find the stored
// baseline without re-implementing the key derivation.
func newTemplateKeyCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "key",
		Short: "Print the template-store key of this bundle",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := container.ReadConfig()
			if err != nil {
				return err
			}
			if cfg.ImageID == "" {
				return fmt.Errorf("bundle has no image_id")
			}
			fmt.Println(cfg.TemplateKey())
			return nil
		},
	}
}
