package main

import (
	"os"

	"github.com/lawrencejones/pg2sink/cmd/pg2sink/cmd"
)

func main() {
	if err := cmd.Run(); err != nil {
		os.Exit(1)
	}
}
