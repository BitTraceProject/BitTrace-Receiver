package tests

import (
	"testing"

	"github.com/BitTraceProject/BitTrace-Receiver/server"
)

func TestMetaServer(t *testing.T) {
	s := server.NewMetaServer("127.0.0.1:8081")
	err := s.Run()
	if err != nil {
		t.Fatal(err)
	}
}
