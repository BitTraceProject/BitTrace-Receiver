package main

import "github.com/BitTraceProject/BitTrace-Receiver/server"

func main() {
	s := server.NewMqServer("127.0.0.1:8082")
	s.Run()
}
