// +build tools

package tools

import (
	_ "github.com/fzipp/gocyclo/cmd/gocyclo"
	_ "github.com/kisielk/errcheck"
	_ "golang.org/x/lint/golint"
)
