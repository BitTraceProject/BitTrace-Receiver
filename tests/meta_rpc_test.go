package tests

import (
	"net/rpc/jsonrpc"
	"testing"

	"github.com/BitTraceProject/BitTrace-Receiver/server"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
)

func TestMetaServer(t *testing.T) {
	s := server.NewMetaServer("127.0.0.1:8081")
	err := s.Run()
	if err != nil {
		t.Fatal(err)
	}
}

func TestMetaClient(t *testing.T) {
	c, err := jsonrpc.Dial("tcp", "127.0.0.1:8081")
	if err != nil {
		t.Fatal("dialing:", err)
	}
	defer c.Close()

	args := &protocol.MetaGetValueArgs{Key: "META_SERVER"}
	var reply protocol.MetaGetValueReply
	err = c.Call("MetaServerAPI.GetValue", args, &reply)
	if err != nil {
		t.Fatal("MetaServerAPI error:", err)
	}
	t.Log(reply.Value, reply.OK)
}

func TestMetaClientGetValue(t *testing.T) {
	c, err := jsonrpc.Dial("tcp", "127.0.0.1:8081")
	if err != nil {
		t.Fatal("dialing:", err)
	}
	defer c.Close()

	args := &protocol.MetaGetValueArgs{Key: "META_SERVER"}
	var reply protocol.MetaGetValueReply
	err = c.Call("MetaServerAPI.GetValue", args, &reply)
	if err != nil {
		t.Fatal("MetaServerAPI error:", err)
	}
	t.Log(reply.Value, reply.OK)
}

func TestMetaClientSetValue(t *testing.T) {
	c, err := jsonrpc.Dial("tcp", "127.0.0.1:8081")
	if err != nil {
		t.Fatal("dialing:", err)
	}
	defer c.Close()

	args1 := &protocol.MetaSetValueArgs{Key: "key1", Value: "value1"}
	var reply1 protocol.MetaSetValueReply
	err = c.Call("MetaServerAPI.SetValue", args1, &reply1)
	if err != nil {
		t.Fatal("MetaServerAPI error:", err)
	}
	t.Log("set: ", reply1.OK)

	args2 := &protocol.MetaGetValueArgs{Key: "key1"}
	var reply2 protocol.MetaGetValueReply
	err = c.Call("MetaServerAPI.GetValue", args2, &reply2)
	if err != nil {
		t.Fatal("MetaServerAPI error:", err)
	}
	t.Log("get: ", reply2.Value, reply2.OK)
}
