package tests

import (
	"net/rpc/jsonrpc"
	"testing"

	"github.com/BitTraceProject/BitTrace-Receiver/server"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
)

func TestMqServer(t *testing.T) {
	s := server.NewMqServer("127.0.0.1:8082")
	err := s.Run()
	if err != nil {
		t.Fatal(err)
	}
}

func TestMqClientPushMessage(t *testing.T) {
	c, err := jsonrpc.Dial("tcp", "127.0.0.1:8082")
	if err != nil {
		t.Fatal("dialing:", err)
	}
	defer c.Close()

	args1 := &protocol.MqPushMessageArgs{Message: protocol.MqMessage{Tag: "tag1", Msg: []byte{1, 2, 3, 4}}}
	var reply1 protocol.MqPushMessageReply
	err = c.Call("MqServerAPI.PushMessage", args1, &reply1)
	if err != nil {
		t.Fatal("MqServerAPI error:", err)
	}
	t.Log(reply1.OK)

	args2 := &protocol.MqPushMessageArgs{Message: protocol.MqMessage{Tag: "tag1", Msg: []byte{1, 2}}}
	var reply2 protocol.MqPushMessageReply
	err = c.Call("MqServerAPI.PushMessage", args2, &reply2)
	if err != nil {
		t.Fatal("MqServerAPI error:", err)
	}
	t.Log(reply2.OK)
}

func TestMqClientFilterMessage(t *testing.T) {
	c, err := jsonrpc.Dial("tcp", "127.0.0.1:8082")
	if err != nil {
		t.Fatal("dialing:", err)
	}
	defer c.Close()

	args := &protocol.MqFilterMessageArgs{Tag: "tag1"}
	var reply protocol.MqFilterMessageReply
	err = c.Call("MqServerAPI.FilterMessage", args, &reply)
	if err != nil {
		t.Fatal("MqServerAPI error:", err)
	}
	t.Log(reply)
}

func TestMqClientClearMessage(t *testing.T) {
	c, err := jsonrpc.Dial("tcp", "127.0.0.1:8082")
	if err != nil {
		t.Fatal("dialing:", err)
	}
	defer c.Close()

	args := &protocol.MqClearMessageArgs{Tag: "tag1"}
	var reply protocol.MqClearMessageReply
	err = c.Call("MqServerAPI.ClearMessage", args, &reply)
	if err != nil {
		t.Fatal("MqServerAPI error:", err)
	}
	t.Log(reply)
}
