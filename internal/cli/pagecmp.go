package cli

import (
	"encoding/json"
	"fmt"

	"github.com/CloudnetUCSC/statepod/internal/pagecmp"
	"github.com/spf13/cobra"
)

func newPagecmpCommand() *cobra.Command {
	var composition bool
	var vmaDir string
	cmd := &cobra.Command{
		Use:   "pagecmp [dump-a] [dump-b]",
		Short: "Page overlap of checkpoint dir A against B (A = subject, B = baseline); dumped anonymous pages only, never RSS",
		Long: "Page overlap of checkpoint dir A against B. With --composition: A's pages broken down by memory region " +
			"(heap, stack, anon, file-cow, shared, other) into zero / baseline hit / unique, with the zstd-1 size of " +
			"the unique pages (RQ1). B may be omitted with --composition (no baseline).",
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if composition {
				b := ""
				if len(args) == 2 {
					b = args[1]
				}
				c, err := pagecmp.Compose(args[0], b, vmaDir)
				if err != nil {
					return err
				}
				fmt.Print(c.JSON())
				return nil
			}
			if len(args) != 2 {
				return fmt.Errorf("pagecmp needs two dumps (or --composition)")
			}
			res, err := pagecmp.CompareDirs(args[0], args[1])
			if err != nil {
				return err
			}
			out, err := json.MarshalIndent(res, "", "  ")
			if err != nil {
				return err
			}
			fmt.Println(string(out))
			return nil
		},
	}
	cmd.Flags().BoolVar(&composition, "composition", false, "per-region zero/hit/unique breakdown of dump A against baseline B")
	cmd.Flags().StringVar(&vmaDir, "vma-dir", "", "--composition: dump holding the mm-<pid>.img VMA lists (default dump A; a pre-dump has none)")
	return cmd
}
