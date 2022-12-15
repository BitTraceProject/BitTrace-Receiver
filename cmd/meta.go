package main

import "github.com/BitTraceProject/BitTrace-Receiver/server"

func main() {
	s := server.NewMetaServer("127.0.0.1:8081")
	s.Run()
}
