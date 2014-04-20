package main

import "github.com/dinedal/godisco"

func main() {
	disco := godisco.NewDiscoWorker()

	disco.Debug("hello world")

	for {
	}
}
