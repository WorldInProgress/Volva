package main

import (
	"fmt"

	zmq "github.com/pebbe/zmq4"
)

func main() {
    major, minor, patch := zmq.Version()
    fmt.Printf("ZMQ version: %d.%d.%d\n", major, minor, patch)
}