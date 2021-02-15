package main

import (
	"github.com/alecthomas/kingpin"
	"github.com/lawrencejones/pgsink/cmd/pgsink/cmd"
)

func main() {
	if err := cmd.Run(); err != nil {
		kingpin.Fatalf(err.Error())
	}
}
