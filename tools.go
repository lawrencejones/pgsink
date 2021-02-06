// +build tools

package main

import (
	_ "github.com/go-jet/jet/v2/cmd/jet"
	_ "github.com/onsi/ginkgo/ginkgo"
	_ "goa.design/goa/v3/cmd/goa"
	_ "golang.org/x/tools/cmd/goimports"
)
