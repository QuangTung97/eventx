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

var excluded = []string{"tools.go", "override.go"}

func stringIn(s string, list []string) bool {
	for _, e := range list {
		if s == e {
			return true
		}
	}
	return false
}

func makeDirOrRemoveFiles() {
	err := os.MkdirAll("eventx", os.ModePerm)
	if err != nil {
		panic(err)
	}

	destInfo, err := ioutil.ReadDir("eventx")
	for _, f := range destInfo {
		if !f.IsDir() && f.Name() != "override.go" {
			file := path.Join("eventx", f.Name())
			fmt.Println("Delete:", file)
			err := os.Remove(file)
			if err != nil {
				panic(err)
			}
		}
	}
}

func main() {
	makeDirOrRemoveFiles()

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
		if goRegexp.MatchString(name) && !stringIn(name, excluded) && !f.IsDir() {
			fmt.Println("Copy:", f.Name())
			err := exec.Command("cp", path.Join(dir, name), "eventx").Run()
			if err != nil {
				panic(err)
			}
		}
	}
}
