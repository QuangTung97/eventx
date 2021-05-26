package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
)

const modName = "github.com/QuangTung97/eventx"

var goRegexp = regexp.MustCompile(`^.+\.go$`)

const (
	toolsFile    = "tools.go"
	overrideFile = "override.go"
)

var excluded = []string{toolsFile, overrideFile}
var initExcluded = []string{toolsFile}

func stringIn(s string, list []string) bool {
	for _, e := range list {
		if s == e {
			return true
		}
	}
	return false
}

func fileToBeCopied(filename string, overrideExisted bool) bool {
	if overrideExisted {
		return !stringIn(filename, excluded)
	}
	return !stringIn(filename, initExcluded)
}

func makeDirOrRemoveFiles() bool {
	overrideExisted := false

	err := os.MkdirAll("eventx", os.ModePerm)
	if err != nil {
		panic(err)
	}

	destInfo, err := ioutil.ReadDir("eventx")
	for _, f := range destInfo {
		if f.IsDir() {
			continue
		}

		if f.Name() != "override.go" {
			file := path.Join("eventx", f.Name())
			fmt.Println("Delete:", file)
			err := os.Remove(file)
			if err != nil {
				panic(err)
			}
		} else {
			overrideExisted = true
		}
	}
	return overrideExisted
}

func main() {
	overrideExisted := makeDirOrRemoveFiles()

	bytes, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", modName).Output()
	if err != nil {
		panic(err)
	}
	dir := strings.TrimSpace(string(bytes))
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	for _, f := range infos {
		name := f.Name()
		if goRegexp.MatchString(name) && fileToBeCopied(name, overrideExisted) && !f.IsDir() {
			fmt.Println("Copy:", name)
			err := exec.Command("cp", path.Join(dir, name), "eventx").Run()
			if err != nil {
				panic(err)
			}
		}
	}
}
