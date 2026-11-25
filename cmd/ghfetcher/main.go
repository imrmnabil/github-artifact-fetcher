package main

import (
	"fmt"
	"os"

	"ghfetcher/internal/cli"
)

func main() {
	// Support single-dash `-all` seamlessly alongside `--all` and `-a`
	args := make([]string, len(os.Args))
	for i, a := range os.Args {
		if a == "-all" {
			args[i] = "--all"
		} else {
			args[i] = a
		}
	}
	os.Args = args

	if err := cli.NewRootCmd().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
