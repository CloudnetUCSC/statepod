package cli

import (
	"fmt"

	"github.com/CloudnetUCSC/statepod/internal/container"
	"github.com/CloudnetUCSC/statepod/internal/workload"
	"github.com/spf13/cobra"
)

// resolveWorkload maps --workload and the legacy --redis/--with-node-server
// shorthands to a Spec (nil = a plain container with no workload). Giving more
// than one is an error rather than a silent precedence.
func resolveWorkload(workloadArg string, withNodeServer, withRedis bool) (*workload.Spec, error) {
	n := 0
	if workloadArg != "" {
		n++
	}
	if withNodeServer {
		n++
		workloadArg = "node"
	}
	if withRedis {
		n++
		workloadArg = "redis"
	}
	if n > 1 {
		return nil, fmt.Errorf("give only one of --workload, --redis, --with-node-server")
	}
	if workloadArg == "" {
		return nil, nil
	}
	return workload.Load(workloadArg)
}

func newCreateCommand() *cobra.Command {
	var withNodeServer bool
	var withRedis bool
	var workloadArg string
	var stateful bool
	var deterministic bool
	var subnet string
	var gateway string
	var ip string
	var hostPort int

	cmd := &cobra.Command{
		Use:   "create [image]",
		Short: "Create a statepod workspace from an image",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			image := args[0]
			wl, err := resolveWorkload(workloadArg, withNodeServer, withRedis)
			if err != nil {
				return err
			}
			// The workload declares whether it is stateful (Redis is);
			// default the classification to that unless --stateful was set.
			if !cmd.Flags().Changed("stateful") {
				stateful = wl != nil && wl.Stateful
			}
			return container.CreateContainer(cmd.Context(), image, wl, stateful, deterministic, subnet, gateway, ip, hostPort)
		},
	}

	cmd.Flags().StringVar(&workloadArg, "workload", "", "Workload to run: a built-in (redis, node) or the path of a workload spec JSON file (see internal/workload)")
	cmd.Flags().BoolVar(&withNodeServer, "with-node-server", false, "Shorthand for --workload node")
	cmd.Flags().BoolVar(&withRedis, "redis", false, "Shorthand for --workload redis")
	cmd.Flags().BoolVar(&stateful, "stateful", false, "Classify container as stateful (burst uses live migration instead of restart)")
	cmd.Flags().BoolVar(&deterministic, "deterministic", false, "Launch the workload with ASLR disabled via the nrand wrapper (deterministic memory layout, design M1)")
	cmd.Flags().StringVar(&subnet, "subnet", "10.10.0.0/24", "Subnet for the container network")
	cmd.Flags().StringVar(&gateway, "gateway", "10.10.0.1", "Gateway IP for the container network")
	cmd.Flags().StringVar(&ip, "ip", "10.10.0.10", "IP address for the container")
	cmd.Flags().IntVar(&hostPort, "host-port", 6380, "Host port DNAT-forwarded to the container service port (Redis)")

	return cmd
}
