package main

import (
	"fmt"
	"os"

	"github.com/CloudnetUCSC/statepod/internal/cli"
)

func main() {
	cmd := cli.NewRootCommand()
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
