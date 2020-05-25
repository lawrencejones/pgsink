package main

import (
	"os"

	"github.com/lawrencejones/pgsink/cmd/pgsink/cmd"
)

func main() {
	if err := cmd.Run(); err != nil {
		os.Exit(1)
	}
}
